import os
import re
import io
import json
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import gspread
from google.oauth2.service_account import Credentials

DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "DART").strip()

# 당일 데이터만 가져오도록 기본값 0으로 변경
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

# 유상증자, 전환사채, 교환사채 모두 캐치
TARGET_REPORT_RE = re.compile(r"(유상\s*증자\s*결정|전환\s*사채\s*권\s*발행\s*결정|교환\s*사채\s*권\s*발행\s*결정)")

# DART API 엔드포인트
LIST_URL = "https://opendart.fss.or.kr/api/list.xml"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
API_URLS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvcbndDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/excbndDecsn.json"
}

def require_env(name: str, value: str):
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")

def clean_str(x) -> str:
    if x is None: return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def parse_int_maybe(s: str):
    s = clean_str(s)
    if not s: return None
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."): return None
    try:
        return int(float(t)) if "." in t else int(t)
    except:
        return None

def amount_won_to_eok(won: int):
    if not won: return ""
    return round(won / 100_000_000, 2)

def get_gsheet_client():
    require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_worksheet():
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    gc = get_gsheet_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)

def ensure_header(ws):
    header = [
        "접수번호", "회사명", "상장시장", "보고서명", "이사회결의일", "발행방식", "발행상품",
        "발행수량(주/권면)", "발행(전환)가(원)", "기준주가(원)", "조달금액(억원)", "할인/할증률",
        "증자전 주식수", "증자비율(%)", "청약일", "납입일", "자금용도", "투자자/대상자", "주관사"
    ]
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(header, value_input_option="USER_ENTERED")

def get_processed_rcept_set(ws):
    col = ws.col_values(1)
    if not col or col[0].strip() == "접수번호":
        return set(x.strip() for x in col[1:] if x.strip())
    return set(x.strip() for x in col if x.strip())

def dart_list(bgn_de: str, end_de: str):
    require_env("DART_API_KEY", DART_API_KEY)
    results = []
    page_no = 1
    while page_no <= MAX_PAGES:
        params = {
            "crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de,
            "pblntf_ty": "B", "sort": "date", "sort_mth": "desc",
            "page_no": str(page_no), "page_count": str(PAGE_COUNT),
        }
        r = requests.get(LIST_URL, params=params, timeout=30)
        root = ET.fromstring(r.content)
        if root.findtext("status") != "000": break
        
        for node in root.findall("list"):
            results.append({child.tag: (child.text or "").strip() for child in list(node)})
            
        if page_no >= (parse_int_maybe(root.findtext("total_page")) or 1): break
        page_no += 1
    return results

def get_structured_dart_data(corp_code: str, bgn_de: str, end_de: str, rcept_no: str, report_nm: str):
    """보고서 종류에 맞는 공식 JSON API를 호출하여 정확한 데이터를 가져옵니다."""
    report_type = ""
    if "유상" in report_nm: report_type = "유상증자"
    elif "전환" in report_nm: report_type = "전환사채"
    elif "교환" in report_nm: report_type = "교환사채"
    else: return report_type, {}

    url = API_URLS[report_type]
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bgn_de": bgn_de, "end_de": end_de}
    
    r = requests.get(url, params=params, timeout=30)
    data = r.json()
    if data.get("status") == "000":
        for row in data.get("list", []):
            if str(row.get("rcept_no", "")).strip() == str(rcept_no).strip():
                return report_type, row
    return report_type, {}

def extract_purpose(data: dict) -> str:
    """자금조달 목적을 파싱하여 문자열로 반환합니다."""
    purpose_parts = []
    labels = [
        ("fdpp_fclt", "시설"), ("fdpp_op", "운영"), ("fdpp_bsninh", "영업양수"),
        ("fdpp_dtrp", "채무상환"), ("fdpp_ocsa", "타법인증권취득"), ("fdpp_etc", "기타")
    ]
    for key, label in labels:
        v = parse_int_maybe(data.get(key))
        if v and v > 0:
            purpose_parts.append(f"{label}:{amount_won_to_eok(v)}억")
    return ", ".join(purpose_parts)

def parse_html_for_investor_and_underwriter(rcept_no: str) -> dict:
    """JSON API에서 제공하지 않는 투자자/주관사 정보만 HTML 문서에서 보조적으로 추출합니다."""
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    out = {"투자자": "", "주관사": ""}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        html = zf.read(html_file).decode("utf-8", errors="ignore")
        
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        
        # 단순 키워드 근처 텍스트 추출 (보조 수단)
        investor_match = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if investor_match: out["투자자"] = investor_match.group(2)[:30].strip()
        
        underwriter_match = re.search(r"(주관회사|대표주관회사|인수회사)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+증권)", text)
        if underwriter_match: out["주관사"] = underwriter_match.group(2)[:30].strip()
    except:
        pass
    return out

def build_row(list_item: dict, report_type: str, data: dict, doc_data: dict):
    corp_name = list_item.get("corp_name", "")
    market = list_item.get("corp_cls", "")
    market = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}.get(market, market)
    report_nm = list_item.get("report_nm", "")
    rcept_no = list_item.get("rcept_no", "")
    
    board_date = data.get("bddd", "")
    purpose = extract_purpose(data)
    
    investor = doc_data.get("투자자", "")
    underwriter = doc_data.get("주관사", "")

    if report_type == "유상증자":
        method = data.get("ic_mthn", "")
        product = "유상증자"
        qty = (parse_int_maybe(data.get("nstk_ostk_cnt")) or 0) + (parse_int_maybe(data.get("nstk_estk_cnt")) or 0)
        issue_price = data.get("tisstk_prc", "")
        base_price = data.get("bsstk_prc", "")
        total_amount = parse_int_maybe(data.get("fdpp_totam"))
        discount = data.get("drt", "")
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("pymdt", "")
        
        pre_qty = (parse_int_maybe(data.get("bfic_tisstk_ostk")) or 0) + (parse_int_maybe(data.get("bfic_tisstk_estk")) or 0)
        ratio = round((qty / pre_qty) * 100, 2) if pre_qty and qty else ""
        
    else: # 전환사채 / 교환사채
        method = data.get("fnd_mthd", "")
        product = report_type
        qty = data.get("bnd_fac_totam", "") # 사채 권면총액
        issue_price = data.get("cnv_prc") if report_type == "전환사채" else data.get("exch_prc")
        base_price = ""
        total_amount = parse_int_maybe(data.get("bnd_fac_totam"))
        discount = ""
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("sbpmcb_pymdt", "")
        pre_qty, ratio = "", ""

    amount_eok = amount_won_to_eok(total_amount) if total_amount else ""

    return [
        rcept_no, corp_name, market, report_nm, board_date, method, product,
        str(qty), str(issue_price), str(base_price), str(amount_eok), discount,
        str(pre_qty), str(ratio), sub_date, pay_date, purpose, investor, underwriter
    ]

def main():
    require_env("DART_API_KEY", DART_API_KEY)
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn = today - timedelta(days=LOOKBACK_DAYS)
    bgn_de = bgn.strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    ws = open_worksheet()
    ensure_header(ws)
    processed = get_processed_rcept_set(ws)

    items = dart_list(bgn_de=bgn_de, end_de=end_de)
    candidates = [it for it in items if TARGET_REPORT_RE.search(it.get("report_nm", ""))]
    new_items = [it for it in candidates if it.get("rcept_no") not in processed]

    rows_to_append = []
    for it in new_items:
        rcept_no = it.get("rcept_no")
        corp_code = it.get("corp_code")
        rcept_dt = it.get("rcept_dt")
        
        report_type, structured_data = get_structured_dart_data(corp_code, rcept_dt, rcept_dt, rcept_no, it.get("report_nm"))
        doc_data = parse_html_for_investor_and_underwriter(rcept_no) if structured_data else {}
        
        if structured_data:
            rows_to_append.append(build_row(it, report_type, structured_data, doc_data))

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended rows: {len(rows_to_append)}")
    else:
        print("✅ No new rows to append.")

if __name__ == "__main__":
    main()
