import os
import re
import io
import json
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

# --- [1] 환경 설정 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

# API 엔드포인트
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
STRUCTURED_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# --- [2] 시트별 헤더 정의 (요청하신 필드 구성 100% 반영) ---
HEADERS = {
    "유상증자": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "증자방식", "보통주발행수", "기타주발행수", 
        "1주당액면가(원)", "신주발행가액(원)", "증자전보통주(주)", "증자전기타주(주)", "시설자금(억)", "영업양수(억)", 
        "운영자금(억)", "채무상환(억)", "타법인취득(억)", "기타자금(억)", "청약일", "납입일", "투자자(대상자)"
    ],
    "전환사채": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "회차", "사채종류", "발행방법", "권면총액(원)", 
        "표면이자율(%)", "만기이자율(%)", "사채만기일", "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", 
        "타법인취득(억)", "기타자금(억)", "전환비율(%)", "전환가액(원)", "최저조정가액(원)", "전환청구시작일", 
        "전환청구종료일", "청약일", "납입일", "대표주관사/투자자"
    ],
    "교환사채": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "회차", "사채종류", "발행방법", "권면총액(원)", 
        "표면이자율(%)", "만기이자율(%)", "사채만기일", "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", 
        "타법인취득(억)", "기타자금(억)", "교환비율(%)", "교환가액(원)", "교환청구시작일", "교환청구종료일", 
        "청약일", "납입일", "대표주관사/투자자"
    ]
}

# --- [3] 유틸리티 함수 ---
def clean_str(val):
    return str(val).strip() if val is not None else ""

def amount_eok(won):
    try:
        val = re.sub(r"[^\d\-\.]", "", str(won))
        return str(round(int(float(val)) / 100_000_000, 2))
    except: return "0"

def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r") as f: return set(json.load(f))
        except: return set()
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen_set), f)

def get_or_create_ws(sh, title):
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        print(f"[{title}] 시트를 생성합니다.")
        ws = sh.add_worksheet(title=title, rows="1000", cols="30")
    if not ws.row_values(1):
        ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

# --- [4] HTML 보조 파싱 (투자자 등 JSON에 없는 정보 추출) ---
def extract_from_html(rcept_no):
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    res = {"investor": ""}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        soup = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if m: res["investor"] = m.group(2)[:40].strip()
    except: pass
    return res

# --- [5] 데이터 조립 (시트 필드 1:1 매핑) ---
def build_row(r_type, list_item, data, html_data):
    rn = clean_str(list_item.get("rcept_no"))
    cn = clean_str(list_item.get("corp_name"))
    mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "기타"}.get(list_item.get("corp_cls"), list_item.get("corp_cls"))
    rpt = clean_str(list_item.get("report_nm"))
    bd = clean_str(data.get("bddd")) # 이사회결의일

    # 자금 목적 (억원 변환)
    f, b, o, d, c, e = [amount_eok(data.get(k)) for k in ["fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp", "fdpp_ocsa", "fdpp_etc"]]
    
    uw_inv = clean_str(data.get("rpmcmp")) if data.get("rpmcmp") else html_data["investor"]

    if r_type == "유상증자":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("ic_mthn")), 
            clean_str(data.get("nstk_ostk_cnt")), clean_str(data.get("nstk_estk_cnt")),
            clean_str(data.get("fv_ps")), clean_str(data.get("tisstk_prc")),
            clean_str(data.get("bfic_tisstk_ostk")), clean_str(data.get("bfic_tisstk_estk")),
            f, b, o, d, c, e, clean_str(data.get("sbscpn_bgd")), clean_str(data.get("pymdt")), html_data["investor"]
        ]
    elif r_type == "전환사채":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            f, b, o, d, c, e, clean_str(data.get("cv_rt")), clean_str(data.get("cv_prc")), clean_str(data.get("act_mktprcfl_cvprc_lwtrsprc")),
            clean_str(data.get("cvrqpd_bgd")), clean_str(data.get("cvrqpd_edd")), clean_str(data.get("sbd")), clean_str(data.get("pymd")), uw_inv
        ]
    elif r_type == "교환사채":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            f, b, o, d, c, e, clean_str(data.get("ex_rt")), clean_str(data.get("ex_prc")), clean_str(data.get("exrqpd_bgd")),
            clean_str(data.get("exrqpd_edd")), clean_str(data.get("sbd")), clean_str(data.get("pymd")), uw_inv
        ]

# --- [6] 메인 실행부 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    seen = load_seen()
    worksheets = {name: get_or_create_ws(sh, name) for name in HEADERS.keys()}
    sheet_seen = {name: set(worksheets[name].col_values(1)[1:]) for name in HEADERS.keys()}

    # 공시 목록 검색
    list_res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "page_count": "100"}).json()
    items = list_res.get("list", [])
    print(f"📋 DART 목록 확인: {len(items)}건")

    rows_to_add = {name: [] for name in HEADERS.keys()}
    newly_seen = set()

    for it in items:
        rpt = it.get("report_nm", "")
        if "유상" in rpt and "결정" in rpt: r_type = "유상증자"
        elif "전환사채" in rpt and "결정" in rpt: r_type = "전환사채"
        elif "교환사채" in rpt and "결정" in rpt: r_type = "교환사채"
        else: continue

        r_no = it.get("rcept_no")
        if r_no in seen or r_no in sheet_seen[r_type]:
            continue

        print(f"🔍 새 공시 분석: [{it.get('corp_name')}] {rpt}")
        
        # 상세 데이터 (JSON) 호출
        detail_res = requests.get(STRUCTURED_APIS[r_type], params={"crtfc_key": DART_API_KEY, "corp_code": it.get("corp_code")}).json()
        detail = next((d for d in detail_res.get("list", []) if d.get("rcept_no") == r_no), None)
        
        if detail:
            html_data = extract_from_html(r_no)
            row = build_row(r_type, it, detail, html_data)
            rows_to_add[r_type].append(row)
            newly_seen.add(r_no)
            print(f"   -> ✅ 데이터 매핑 완료")
        else:
            print(f"   -> ⏳ 상세 데이터 지연 중 (다음 실행 시 재시도)")

    for name, data_rows in rows_to_add.items():
        if data_rows:
            worksheets[name].append_rows(data_rows, value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트: {len(data_rows)}건 추가 완료")

    if newly_seen:
        seen.update(newly_seen)
        save_seen(seen)

if __name__ == "__main__":
    main()
