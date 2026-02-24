import os
import re
import io
import json
import zipfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

API_URLS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"

# === 1. 시트별 맞춤형 헤더 정의 ===
HEADERS = {
    "유상증자": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "증자방식", 
        "보통주발행수", "기타주발행수", "1주당액면가(원)", "신주발행가액(원)", "증자전보통주(주)", "증자전기타주(주)",
        "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", "타법인취득(억)", "기타자금(억)", 
        "청약일", "납입일", "투자자(대상자)"
    ],
    "전환사채": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "회차", "사채종류", "발행방법", 
        "권면총액(원)", "표면이자율(%)", "만기이자율(%)", "사채만기일", 
        "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", "타법인취득(억)", "기타자금(억)", 
        "전환비율(%)", "전환가액(원)", "최저조정가액(원)", "전환청구시작일", "전환청구종료일", 
        "청약일", "납입일", "대표주관사/투자자"
    ],
    "교환사채": [
        "접수번호", "회사명", "시장구분", "보고서명", "이사회결의일", "회차", "사채종류", "발행방법", 
        "권면총액(원)", "표면이자율(%)", "만기이자율(%)", "사채만기일", 
        "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", "타법인취득(억)", "기타자금(억)", 
        "교환비율(%)", "교환가액(원)", "교환청구시작일", "교환청구종료일", 
        "청약일", "납입일", "대표주관사/투자자"
    ]
}

def require_env(name: str, value: str):
    if not value: raise RuntimeError(f"Missing env var: {name}")

def clean_str(x) -> str:
    if x is None: return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def parse_int_maybe(s: str):
    s = clean_str(s)
    if not s: return None
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."): return None
    try: return int(float(t)) if "." in t else int(t)
    except: return None

def amount_eok(won: int):
    """원 -> 억원 변환 (소수점 2자리)"""
    if not won: return "0"
    return str(round(won / 100_000_000, 2))

# === 상태 관리 시스템 ===
def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r") as f: return set(json.load(f))
        except: return set()
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen_set), f)

def get_sheet_seen(ws):
    col = ws.col_values(1)
    if not col or col[0].strip() == "접수번호":
        return set(x.strip() for x in col[1:] if x.strip())
    return set(x.strip() for x in col if x.strip())

# === DART API 통신부 ===
def dart_list_json(bgn_de: str, end_de: str):
    require_env("DART_API_KEY", DART_API_KEY)
    results, page_no = [], 1
    while page_no <= MAX_PAGES:
        params = {"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de, "sort": "date", "sort_mth": "desc", "page_no": str(page_no), "page_count": str(PAGE_COUNT)}
        r = requests.get(LIST_URL, params=params, timeout=30)
        data = r.json()
        if data.get("status") != "000": break
        results.extend(data.get("list", []))
        if page_no >= data.get("total_page", 1): break
        page_no += 1
    return results

def get_json_data(corp_code: str, rcept_no: str, report_type: str):
    url = API_URLS.get(report_type)
    if not url: return {}
    try:
        r = requests.get(url, params={"crtfc_key": DART_API_KEY, "corp_code": corp_code}, timeout=30)
        data = r.json()
        if data.get("status") == "000":
            for row in data.get("list", []):
                if str(row.get("rcept_no", "")).strip() == str(rcept_no).strip():
                    return row
    except: pass
    return {}

def extract_investor_html(rcept_no: str) -> str:
    try:
        r = requests.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        text = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml").get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if m: return m.group(2)[:30].strip()
    except: pass
    return ""

# === 시트별 데이터 조립 ===
def build_row(list_item: dict, report_type: str, data: dict, investor: str):
    # 공통 항목
    rn = clean_str(list_item.get("rcept_no"))
    cn = clean_str(list_item.get("corp_name"))
    mr = clean_str(list_item.get("corp_cls"))
    mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}.get(mr, mr)
    rpt = clean_str(list_item.get("report_nm"))
    bd = clean_str(data.get("bddd")) # 이사회결의일
    
    # 자금 목적 공통 (억원)
    fclt = amount_eok(parse_int_maybe(data.get("fdpp_fclt")))
    bsn = amount_eok(parse_int_maybe(data.get("fdpp_bsninh")))
    op = amount_eok(parse_int_maybe(data.get("fdpp_op")))
    dtrp = amount_eok(parse_int_maybe(data.get("fdpp_dtrp")))
    ocsa = amount_eok(parse_int_maybe(data.get("fdpp_ocsa")))
    etc = amount_eok(parse_int_maybe(data.get("fdpp_etc")))
    
    # 추가 정보 조합 (투자자 or 주관사)
    rpmcmp = clean_str(data.get("rpmcmp"))
    inv_or_uw = rpmcmp if rpmcmp else investor

    if report_type == "유상증자":
        return [
            rn, cn, mr, rpt, bd,
            clean_str(data.get("ic_mthn")), # 증자방식
            clean_str(data.get("nstk_ostk_cnt")), # 보통주
            clean_str(data.get("nstk_estk_cnt")), # 기타주
            clean_str(data.get("fv_ps")), # 1주당액면가
            clean_str(data.get("tisstk_prc")), # 신주발행가액
            clean_str(data.get("bfic_tisstk_ostk")), # 증자전 보통주
            clean_str(data.get("bfic_tisstk_estk")), # 증자전 기타주
            fclt, bsn, op, dtrp, ocsa, etc,
            clean_str(data.get("sbscpn_bgd")), # 청약일
            clean_str(data.get("pymdt")), # 납입일
            investor
        ]
        
    elif report_type == "전환사채":
        return [
            rn, cn, mr, rpt, bd,
            clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            fclt, bsn, op, dtrp, ocsa, etc,
            clean_str(data.get("cv_rt")), clean_str(data.get("cv_prc")), clean_str(data.get("act_mktprcfl_cvprc_lwtrsprc")),
            clean_str(data.get("cvrqpd_bgd")), clean_str(data.get("cvrqpd_edd")),
            clean_str(data.get("sbd")), clean_str(data.get("pymd")), inv_or_uw
        ]
        
    elif report_type == "교환사채":
        return [
            rn, cn, mr, rpt, bd,
            clean_str(data.get("bd_tm")), clean_str(data.get("bd_knd")), clean_str(data.get("bdis_mthn")),
            clean_str(data.get("bd_fta")), clean_str(data.get("bd_intr_ex")), clean_str(data.get("bd_intr_sf")), clean_str(data.get("bd_mtd")),
            fclt, bsn, op, dtrp, ocsa, etc,
            clean_str(data.get("ex_rt")), clean_str(data.get("ex_prc")),
            clean_str(data.get("exrqpd_bgd")), clean_str(data.get("exrqpd_edd")),
            clean_str(data.get("sbd")), clean_str(data.get("pymd")), inv_or_uw
        ]

def main():
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.authorize(Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn = today - timedelta(days=LOOKBACK_DAYS)
    bgn_de, end_de = bgn.strftime("%Y%m%d"), today.strftime("%Y%m%d")

    sheet_names = ["유상증자", "전환사채", "교환사채"]
    worksheets = {}
    
    local_seen = load_seen()
    sheet_seen = set()

    # 시트 로드 및 헤더 세팅
    for name in sheet_names:
        ws = get_or_create_worksheet(sh, name)
        if not ws.row_values(1): ws.append_row(HEADERS[name], value_input_option="USER_ENTERED")
        worksheets[name] = ws
        sheet_seen.update(get_sheet_seen(ws))

    items = dart_list_json(bgn_de, end_de)
    print(f"📋 공시 목록 검색 완료: {bgn_de}~{end_de} (총 {len(items)}건)")
    
    rows_to_append = {"유상증자": [], "전환사채": [], "교환사채": []}
    newly_processed = set()

    for it in items:
        rpt = it.get("report_nm", "")
        if "유상" in rpt and "결정" in rpt: r_type = "유상증자"
        elif "전환사채" in rpt and "결정" in rpt: r_type = "전환사채"
        elif "교환사채" in rpt and "결정" in rpt: r_type = "교환사채"
        else: continue 

        r_no = it.get("rcept_no")
        print(f"\n🔍 타겟 발견: [{it.get('corp_name')}] {rpt} ({r_no})")

        # ✨ 여기서 중복을 거릅니다! 다시 가져오려면 seen.json과 시트에서 지우면 됩니다.
        if r_no in sheet_seen or r_no in local_seen:
            print("   -> 🚫 이미 처리된 공시입니다. 패스.")
            continue
            
        json_data = get_json_data(it.get("corp_code"), r_no, r_type)
        if json_data:
            investor = extract_investor_html(r_no)
            row = build_row(it, r_type, json_data, investor)
            rows_to_append[r_type].append(row)
            newly_processed.add(r_no)
            print("   -> ✅ 데이터 맞춤형 매핑 완료.")
        else:
            print("   -> ⏳ 금감원 데이터 처리 지연. 다음 실행 시 재시도합니다.")

    for name in sheet_names:
        if rows_to_append[name]:
            worksheets[name].append_rows(rows_to_append[name], value_input_option="USER_ENTERED")
            print(f"✅ {name} 시트: {len(rows_to_append[name])}건 업데이트.")

    if newly_processed:
        local_seen.update(newly_processed)
        save_seen(local_seen)

if __name__ == "__main__":
    main()
