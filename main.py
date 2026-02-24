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

# --- [1] 환경 및 API 설정 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

# API 엔드포인트 (대표님이 지정하신 전용 상세 JSON API 포함)
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
DETAIL_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# --- [2] 시트별 최적화 헤더 정의 ---
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

# --- [3] 데이터 정제 및 상태 관리 유틸리티 ---
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
        print(f"[{title}] 시트를 신규 생성합니다.")
        ws = sh.add_worksheet(title=title, rows="1000", cols="30")
    if not ws.row_values(1):
        ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

# --- [4] HTML 보조 분석 (투자자 등 텍스트 정보 추출) ---
def get_investor_from_html(rcept_no):
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        soup = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        return m.group(2)[:40].strip() if m else ""
    except: return ""

# --- [5] 행 데이터 조립 (JSON 데이터 우선 매핑) ---
def build_row(r_type, list_item, data, investor):
    rn = clean_str(list_item.get("rcept_no"))
    cn = clean_str(list_item.get("corp_name"))
    mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "기타"}.get(list_item.get("corp_cls"), list_item.get("corp_cls"))
    rpt = clean_str(list_item.get("report_nm"))
    bd = clean_str(data.get("bddd"))

    # 자금 조달 목적 억원 단위 변환
    purposes = [amount_eok(data.get(k)) for k in ["fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp", "fdpp_ocsa", "fdpp_etc"]]
    
    uw_inv = clean_str(data.get("rpmcmp")) if data.get("rpmcmp") else investor

    if r_type == "유상증자":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("ic_mthn")), 
            clean_str(data.get("nstk_ostk_cnt")), clean_str(data.get("nstk_estk_cnt")),
            clean_str(data.get("fv_ps")), clean_str(data.get("tisstk_prc")),
            clean_str(data.get("bfic_tisstk_ostk")), clean_str(data.get("bfic_tisstk_estk")),
            *purposes, clean_str(data.get("sbscpn_bgd")), clean_str(data.get("pymdt")), investor
        ]
    elif r_type == "전환사채":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            *purposes, clean_str(data.get("cv_rt")), clean_str(data.get("cv_prc")), clean_str(data.get("act_mktprcfl_cvprc_lwtrsprc")),
            clean_str(data.get("cvrqpd_bgd")), clean_str(data.get("cvrqpd_edd")), clean_str(data.get("sbd")), clean_str(data.get("pymd")), uw_inv
        ]
    elif r_type == "교환사채":
        return [
            rn, cn, mr, rpt, bd, clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            *purposes, clean_str(data.get("ex_rt")), clean_str(data.get("ex_prc")), clean_str(data.get("exrqpd_bgd")),
            clean_str(data.get("exrqpd_edd")), clean_str(data.get("sbd")), clean_str(data.get("pymd")), uw_inv
        ]

# --- [6] 메인 엔진 ---
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

    # 공시 목록 검색 (list.json)
    list_res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "page_count": "100"}).json()
    items = list_res.get("list", [])
    print(f"📋 DART 목록 검색 완료: {len(items)}건 확인됨.")

    rows_to_add = {name: [] for name in HEADERS.keys()}
    newly_processed = set()

    for it in items:
        rpt = it.get("report_nm", "")
        r_type = ""
        if "유상" in rpt and "결정" in rpt: r_type = "유상증자"
        elif "전환사채" in rpt and "결정" in rpt: r_type = "전환사채"
        elif "교환사채" in rpt and "결정" in rpt: r_type = "교환사채"
        else: continue

        r_no = it.get("rcept_no")
        # 시트나 seen.json에 있으면 중복 수집 방지 (테스트 시 삭제하면 다시 가져옴)
        if r_no in seen or r_no in sheet_seen.get(r_type, set()):
            continue

        print(f"🔍 신규 타겟 분석: [{it.get('corp_name')}] {rpt}")
        
        # 💡 핵심: 전용 상세 JSON API 호출 (piicDecsn, cvbdIsDecsn, exbdIsDecsn)
        detail_res = requests.get(DETAIL_APIS[r_type], params={"crtfc_key": DART_API_KEY, "corp_code": it.get("corp_code")}).json()
        detail = next((d for d in detail_res.get("list", []) if d.get("rcept_no") == r_no), None)
        
        if detail:
            investor = get_investor_from_html(r_no) # 투자자 정보만 HTML에서 보조적으로 추출
            row = build_row(r_type, it, detail, investor)
            rows_to_append = rows_to_add[r_type]
            rows_to_append.append(row)
            newly_processed.add(r_no)
            print(f"   -> ✅ 상세 수치 매핑 완료")
        else:
            print(f"   -> ⏳ 상세 API 데이터 지연 중 (다음 실행 시 자동 재시도)")

    for name, data_rows in rows_to_add.items():
        if data_rows:
            worksheets[name].append_rows(data_rows, value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트에 {len(data_rows)}건 업데이트 완료.")

    if newly_processed:
        seen.update(newly_processed)
        save_seen(seen)

if __name__ == "__main__":
    main()
