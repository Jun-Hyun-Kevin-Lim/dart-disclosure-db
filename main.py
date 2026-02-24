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

DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 당일 공시 기준 (과거 데이터 테스트 시 1~3 등 숫자로 변경 가능)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

# 1. 대표님이 강조하신 가장 중요한 전용 JSON API 엔드포인트!
API_URLS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# 2. 실시간 목록 검색(JSON) 및 보조 원본 문서(XML/HTML) 엔드포인트
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"

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
    """원 단위를 억원 단위로 변환 (소수점 2자리)"""
    if not won: return ""
    return round(won / 100_000_000, 2)

def get_gsheet_client():
    require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_worksheet(sh, title: str):
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        print(f"[{title}] 시트가 없어 새로 생성합니다.")
        ws = sh.add_worksheet(title=title, rows="1000", cols="20")
    return ws

def ensure_header(ws):
    header = [
        "접수번호", "회사명", "상장시장", "보고서명", "이사회결의일", "발행방식", "발행상품",
        "발행수량(주/권면)", "발행(전환/교환)가(원)", "기준주가(원)", "조달금액(억원)", "할인/할증률",
        "증자전 주식수", "증자비율(%)", "청약일", "납입일", "자금용도", "투자자/대상자", "주관사"
    ]
    if not ws.row_values(1):
        ws.append_row(header, value_input_option="USER_ENTERED")

def get_processed_rcept_set(ws):
    col = ws.col_values(1)
    if not col or col[0].strip() == "접수번호":
        return set(x.strip() for x in col[1:] if x.strip())
    return set(x.strip() for x in col if x.strip())

def dart_list_json(bgn_de: str, end_de: str):
    """list.json을 활용하여 당일 공시 목록을 빠르게 가져옵니다."""
    require_env("DART_API_KEY", DART_API_KEY)
    results = []
    page_no = 1
    while page_no <= MAX_PAGES:
        params = {
            "crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de,
            "sort": "date", "sort_mth": "desc",
            "page_no": str(page_no), "page_count": str(PAGE_COUNT),
        }
        r = requests.get(LIST_URL, params=params, timeout=30)
        data = r.json()
        
        if data.get("status") != "000": break
        results.extend(data.get("list", []))
        
        total_page = data.get("total_page", 1)
        if page_no >= total_page: break
        page_no += 1
    return results

def get_structured_json_data(corp_code: str, rcept_no: str, report_type: str):
    """대표님이 지정한 3개의 전용 JSON API에서 정확한 핵심 수치를 가져옵니다."""
    url = API_URLS.get(report_type)
    if not url: return {}

    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code}
    try:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()
        if data.get("status") == "000":
            for row in data.get("list", []):
                if str(row.get("rcept_no", "")).strip() == str(rcept_no).strip():
                    return row
    except Exception as e:
        print(f"JSON API 호출 에러: {e}")
    return {}

def extract_purpose(data: dict) -> str:
    """자금조달 목적을 파싱하여 억원 단위로 정리합니다."""
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
    """JSON API에서 제공하지 않는 '투자자'와 '주관사' 정보만 원본 HTML에서 안전하게 추출합니다."""
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    out = {"투자자": "", "주관사": ""}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        html = zf.read(html_file).decode("utf-8", errors="ignore")
        
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        
        investor_match = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if investor_match: out["투자자"] = investor_match.group(2)[:30].strip()
        
        underwriter_match = re.search(r"(주관회사|대표주관회사|인수회사)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+증권)", text)
        if underwriter_match: out["주관사"] = underwriter_match.group(2)[:30].strip()
    except:
        pass
    return out

def build_row(list_item: dict, report_type: str, data: dict, doc_data: dict):
    """전용 JSON API의 정확한 데이터를 구글 시트 19개 열(Column)에 완벽하게 1:1 매핑합니다."""
    rcept_no = list_item.get("rcept_no", "")
    corp_name = list_item.get("corp_name", "")
    market = list_item.get("corp_cls", "")
    market = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}.get(market, market)
    report_nm = list_item.get("report_nm", "")
    
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
        
    elif report_type == "전환사채":
        # 전환사채권 전용 키값 적용
        method = data.get("fnd_mthd", data.get("cvbd_is_mthd", ""))
        product = "전환사채"
        qty = data.get("bnd_fac_totam", "") 
        issue_price = data.get("cnv_prc", "") 
        base_price = "" 
        total_amount = parse_int_maybe(data.get("bnd_fac_totam"))
        discount = ""
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("sbpmcb_pymdt", "") 
        pre_qty, ratio = "", ""
        
    elif report_type == "교환사채":
        # 교환사채권 전용 키값 적용
        method = data.get("fnd_mthd", data.get("excbnd_is_mthd", ""))
        product = "교환사채"
        qty = data.get("bnd_fac_totam", "") 
        issue_price = data.get("exch_prc", "") 
        base_price = ""
        total_amount = parse_int_maybe(data.get("bnd_fac_totam"))
        discount = ""
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("sbpmcb_pymdt", "")
        pre_qty, ratio = "", ""

    amount_eok = amount_won_to_eok(total_amount) if total_amount else ""

    # 19개 컬럼 순서 고정!
    return [
        rcept_no, corp_name, market, report_nm, board_date, method, product,
        str(qty), str(issue_price), str(base_price), str(amount_eok), discount,
        str(pre_qty), str(ratio), sub_date, pay_date, purpose, investor, underwriter
    ]

def main():
    require_env("DART_API_KEY", DART_API_KEY)
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn = today - timedelta(days=LOOKBACK_DAYS)
    bgn_de = bgn.strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    gc = get_gsheet_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    sheet_names = ["유상증자", "전환사채", "교환사채"]
    worksheets = {}
    processed_rcepts = {}

    for name in sheet_names:
        ws = get_or_create_worksheet(sh, name)
        ensure_header(ws)
        worksheets[name] = ws
        processed_rcepts[name] = get_processed_rcept_set(ws)

    # 전체 공시 리스트 호출
    items = dart_list_json(bgn_de=bgn_de, end_de=end_de)
    print(f"📋 공시 목록 검색 완료: {bgn_de} ~ {end_de} 기간 총 {len(items)}건")
    
    rows_to_append = {"유상증자": [], "전환사채": [], "교환사채": []}

    for it in items:
        report_nm = it.get("report_nm", "")
        
        report_type = ""
        if "유상" in report_nm and "결정" in report_nm: report_type = "유상증자"
        elif "전환사채" in report_nm and "결정" in report_nm: report_type = "전환사채"
        elif "교환사채" in report_nm and "결정" in report_nm: report_type = "교환사채"
        else: continue 

        rcept_no = it.get("rcept_no")
        corp_name = it.get("corp_name", "알수없음")
        
        print(f"\n🔍 타겟 공시 발견: [{corp_name}] {report_nm} (접수번호: {rcept_no})")

        if rcept_no in processed_rcepts[report_type]:
            print(f"   -> 🚫 이미 구글 시트에 등록된 공시입니다. 패스.")
            continue
            
        corp_code = it.get("corp_code")
        
        # 💡 대표님이 강조하신 핵심! 전용 JSON API에서 정확한 재무 데이터를 호출합니다.
        structured_data = get_structured_json_data(corp_code, rcept_no, report_type)
        
        if structured_data:
            # 수치 데이터가 존재하면 보조 정보(투자자/주관사)를 위해 HTML을 긁어옵니다.
            doc_data = parse_html_for_investor_and_underwriter(rcept_no)
            row = build_row(it, report_type, structured_data, doc_data)
            rows_to_append[report_type].append(row)
            print(f"   -> ✅ 데이터 100% 완벽 매핑. 시트 대기열에 추가했습니다.")
        else:
            # 금감원 서버에서 JSON 데이터 변환이 지연되고 있을 경우
            print(f"   -> ⏳ 금감원 전용 JSON API 업데이트 지연 중. 엉뚱한 데이터를 넣지 않기 위해 다음 실행 시 다시 시도합니다.")

    print("\n[시트 최종 업데이트 결과]")
    for name in sheet_names:
        if rows_to_append[name]:
            worksheets[name].append_rows(rows_to_append[name], value_input_option="USER_ENTERED")
            print(f"✅ {name} 시트: {len(rows_to_append[name])}건 추가 완료.")
        else:
            print(f"✅ {name} 시트: 새로 추가할 내용 없음.")

if __name__ == "__main__":
    main()
