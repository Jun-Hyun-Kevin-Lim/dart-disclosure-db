import os
import re
import io
import json
import zipfile
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

# --- [1] 환경 설정 및 API 연결 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0")) # 0은 오늘, 테스트 시 1~3으로 변경 가능
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

# OpenAPI URL
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
DETAIL_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json","https://opendart.fss.or.kr/api/piicDecsn.xml",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json","https://opendart.fss.or.kr/api/cvbdIsDecsn.xml",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json","https://opendart.fss.or.kr/api/exbdIsDecsn.xml"
}

# --- [2] 시트별 독립 필드 정의 (대표님 요청 스펙 100% 반영) ---
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
def clean(val):
    return str(val).strip() if val is not None else ""

def to_eok(won):
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
        ws = sh.add_worksheet(title=title, rows="1000", cols="50")
    if not ws.row_values(1):
        ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

# --- [4] HTML 보조 분석 (투자자 명단 등 텍스트 정보) ---
def get_extra_from_html(rcept_no):
    try:
        r = requests.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
        soup = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        return m.group(2)[:40].strip() if m else ""
    except: return ""

# --- [5] 행 데이터 조립 (OpenAPI 키값 1:1 매칭) ---
def build_row(r_type, list_item, d, inv):
    rn = clean(list_item.get("rcept_no"))
    cn = clean(list_item.get("corp_name"))
    mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX"}.get(list_item.get("corp_cls"), "기타")
    rpt = clean(list_item.get("report_nm"))
    bd = clean(d.get("bddd")) # 이사회결의일

    # 자금 목적 (억원 단위 자동 변환)
    f, b, o, dtrp, c, e = [to_eok(d.get(k)) for k in ["fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp", "fdpp_ocsa", "fdpp_etc"]]
    
    uw_inv = clean(d.get("rpmcmp")) if d.get("rpmcmp") else inv

    if r_type == "유상증자":
        return [
            rn, cn, mr, rpt, bd, clean(d.get("ic_mthn")), clean(d.get("nstk_ostk_cnt")), clean(d.get("nstk_estk_cnt")),
            clean(d.get("fv_ps")), clean(d.get("tisstk_prc")), clean(d.get("bfic_tisstk_ostk")), clean(d.get("bfic_tisstk_estk")),
            f, b, o, dtrp, c, e, clean(d.get("sbscpn_bgd")), clean(d.get("pymdt")), inv
        ]
    elif r_type == "전환사채":
        return [
            rn, cn, mr, rpt, bd, clean(d.get("bd_tm")), clean(d.get("bd_knd")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")),
            clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e,
            clean(d.get("cv_rt")), clean(d.get("cv_prc")), clean(d.get("act_mktprcfl_cvprc_lwtrsprc")),
            clean(d.get("cvrqpd_bgd")), clean(d.get("cvrqpd_edd")), clean(d.get("sbd")), clean(d.get("pymd")), uw_inv
        ]
    elif r_type == "교환사채":
        return [
            rn, cn, mr, rpt, bd, clean(d.get("bd_tm")), clean(d.get("bd_knd")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")),
            clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e,
            clean(d.get("ex_rt")), clean(d.get("ex_prc")), clean(d.get("exrqpd_bgd")), clean(d.get("exrqpd_edd")),
            clean(d.get("sbd")), clean(d.get("pymd")), uw_inv
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
    
    # 1. 공시 목록 검색 (list.json)
    res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "page_count": "100"}).json()
    items = res.get("list", [])
    print(f"📋 DART 목록 확인: {len(items)}건")

    rows_to_add = {name: [] for name in HEADERS.keys()}
    newly_seen = set()

    for it in items:
        rpt = it.get("report_nm", "")
        r_type = ""
        if "유상" in rpt and "결정" in rpt: r_type = "유상증자"
        elif "전환사채" in rpt and "결정" in rpt: r_type = "전환사채"
        elif "교환사채" in rpt and "결정" in rpt: r_type = "교환사채"
        else: continue

        r_no = it.get("rcept_no")
        
        # 2. 중복 체크: 시트 실시간 확인 + seen.json 확인
        # (시트에서 행을 삭제하면 r_no가 시트에 없으므로 다시 가져옵니다)
        sheet_seen = set(worksheets[r_type].col_values(1)[1:])
        if r_no in seen or r_no in sheet_seen:
            continue

        print(f"🔍 신규 타겟 분석: [{it.get('corp_name')}] {rpt}")
        
        # 3. 상세 OpenAPI 호출 (piicDecsn 등)
        detail_res = requests.get(DETAIL_APIS[r_type], params={"crtfc_key": DART_API_KEY, "corp_code": it.get("corp_code")}).json()
        detail = next((d for d in detail_res.get("list", []) if d.get("rcept_no") == r_no), None)
        
        if detail:
            inv = get_extra_from_html(r_no) # 보조 정보 추출
            row = build_row(r_type, it, detail, inv)
            rows_to_add[r_type].append(row)
            newly_seen.add(r_no)
            print(f"   -> ✅ 데이터 정밀 매핑 완료")
        else:
            # 목록에는 있으나 전용 API에 아직 데이터가 처리되지 않은 경우
            print(f"   -> ⏳ 금감원 전용 API 데이터 생성 지연 중 (다음 주기 실행 시 자동 재시도)")

    # 4. 시트 업데이트
    for name, data in rows_to_add.items():
        if data:
            worksheets[name].append_rows(data, value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트: {len(data)}건 업데이트 완료")

    if newly_seen:
        seen.update(newly_seen)
        save_seen(seen)

if __name__ == "__main__":
    main()
