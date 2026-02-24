import os
import re
import io
import json
import zipfile
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 당일 공시만 가져오려면 0으로 설정. 테스트를 원하시면 3~7 정도로 늘려보세요.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

# 타겟 공시명 정규식 (정정공시 포함 모두 캐치)
TARGET_REPORT_RE = re.compile(r"(유상\s*증자\s*결정|전환\s*사채\s*권\s*발행\s*결정|교환\s*사채\s*권\s*발행\s*결정)")

# DART OpenAPI 엔드포인트
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
    # 쉼표(,) 등 숫자와 소수점, 마이너스 기호 외의 문자 제거
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."): return None
    try:
        return int(float(t)) if "." in t else int(t)
    except:
        return None

def amount_won_to_eok(won: int):
    """원 단위 금액을 억원 단위로 변환 (소수점 2자리)"""
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
    # 3개 시트 모두 동일하게 적용되는 19개의 열(Column)
    header = [
        "접수번호", "회사명", "상장시장", "보고서명", "이사회결의일", "발행방식", "발행상품",
        "발행수량(주/권면)", "발행(전환/교환)가(원)", "기준주가(원)", "조달금액(억원)", "할인/할증률",
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
        # 카테고리 필터("pblntf_ty": "B")를 완전히 삭제하여 모든 공시를 검색하도록 수정!
        params = {
            "crtfc_key": DART_API_KEY, 
            "bgn_de": bgn_de, 
            "end_de": end_de,
            "sort": "date", 
            "sort_mth": "desc",
            "page_no": str(page_no), 
            "page_count": str(PAGE_COUNT),
        }
        r = requests.get(LIST_URL, params=params, timeout=30)
        root = ET.fromstring(r.content)
        
        # API 일일 한도 초과 등 에러 발생 시 중단
        if root.findtext("status") != "000": 
            break
        
        for node in root.findall("list"):
            results.append({child.tag: (child.text or "").strip() for child in list(node)})
            
        total_page = parse_int_maybe(root.findtext("total_page")) or 1
        if page_no >= total_page: 
            break
        page_no += 1
    return results

def get_structured_dart_data(corp_code: str, bgn_de: str, end_de: str, rcept_no: str, report_type: str):
    """공시별 전용 JSON API를 호출하여 정확한 필드값을 가져옵니다."""
    url = API_URLS.get(report_type)
    if not url: return {}

    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bgn_de": bgn_de, "end_de": end_de}
    r = requests.get(url, params=params, timeout=30)
    data = r.json()
    
    if data.get("status") == "000":
        for row in data.get("list", []):
            if str(row.get("rcept_no", "")).strip() == str(rcept_no).strip():
                return row
    return {}

def extract_purpose(data: dict) -> str:
    """조달된 자금(원)을 목적별로 파싱하여 억원 단위 문자열로 합칩니다."""
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
    """JSON으로 주지 않는 투자자, 주관사 정보만 문서 전문(HTML)에서 정규식으로 안전하게 추출합니다."""
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    out = {"투자자": "", "주관사": ""}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = next(n for n in zf.namelist() if n.lower().endswith((".html", ".htm")))
        html = zf.read(html_file).decode("utf-8", errors="ignore")
        
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        
        # 투자자/주관사 키워드 주변 텍스트 30자 이내 추출
        investor_match = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if investor_match: out["투자자"] = investor_match.group(2)[:30].strip()
        
        underwriter_match = re.search(r"(주관회사|대표주관회사|인수회사)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+증권)", text)
        if underwriter_match: out["주관사"] = underwriter_match.group(2)[:30].strip()
    except:
        pass
    return out

def build_row(list_item: dict, report_type: str, data: dict, doc_data: dict):
    """API 데이터를 바탕으로 19개의 컬럼에 정확히 1:1로 매핑합니다."""
    # 1. 공통 정보 세팅
    rcept_no = list_item.get("rcept_no", "")
    corp_name = list_item.get("corp_name", "")
    market = list_item.get("corp_cls", "")
    market = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}.get(market, market)
    report_nm = list_item.get("report_nm", "")
    
    board_date = data.get("bddd", "") # 이사회결의일
    purpose = extract_purpose(data) # 자금용도
    investor = doc_data.get("투자자", "")
    underwriter = doc_data.get("주관사", "")

    # 2. 공시 종류별 데이터 매핑
    if report_type == "유상증자":
        method = data.get("ic_mthn", "")
        product = "유상증자"
        # 발행수량 (보통주 + 기타주)
        qty = (parse_int_maybe(data.get("nstk_ostk_cnt")) or 0) + (parse_int_maybe(data.get("nstk_estk_cnt")) or 0)
        issue_price = data.get("tisstk_prc", "") # 발행가액
        base_price = data.get("bsstk_prc", "") # 기준주가
        total_amount = parse_int_maybe(data.get("fdpp_totam")) # 총 조달금액
        discount = data.get("drt", "") # 할인율
        sub_date = data.get("sbscpn_bgd", "") # 청약시작일
        pay_date = data.get("pymdt", "") # 납입일
        
        pre_qty = (parse_int_maybe(data.get("bfic_tisstk_ostk")) or 0) + (parse_int_maybe(data.get("bfic_tisstk_estk")) or 0)
        ratio = round((qty / pre_qty) * 100, 2) if pre_qty and qty else ""
        
    elif report_type == "전환사채":
        method = data.get("fnd_mthd", "")
        product = "전환사채"
        qty = data.get("bnd_fac_totam", "") # 사채 권면총액을 수량으로 대체
        issue_price = data.get("cnv_prc", "") # 전환가액
        base_price = "" 
        total_amount = parse_int_maybe(data.get("bnd_fac_totam"))
        discount = ""
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("sbpmcb_pymdt", "") # 사채 납입일
        pre_qty, ratio = "", ""
        
    elif report_type == "교환사채":
        method = data.get("fnd_mthd", "")
        product = "교환사채"
        qty = data.get("bnd_fac_totam", "") 
        issue_price = data.get("exch_prc", "") # 교환가액
        base_price = ""
        total_amount = parse_int_maybe(data.get("bnd_fac_totam"))
        discount = ""
        sub_date = data.get("sbscpn_bgd", "")
        pay_date = data.get("sbpmcb_pymdt", "")
        pre_qty, ratio = "", ""

    # 조달금액을 억원 단위로 변환
    amount_eok = amount_won_to_eok(total_amount) if total_amount else ""

    # 3. 배열 완성 (반드시 ensure_header에 선언된 19개 순서와 완벽히 일치해야 함)
    return [
        rcept_no,           # 1. 접수번호
        corp_name,          # 2. 회사명
        market,             # 3. 상장시장
        report_nm,          # 4. 보고서명
        board_date,         # 5. 이사회결의일
        method,             # 6. 발행방식
        product,            # 7. 발행상품
        str(qty),           # 8. 발행수량(주/권면)
        str(issue_price),   # 9. 발행(전환/교환)가(원)
        str(base_price),    # 10. 기준주가(원)
        str(amount_eok),    # 11. 조달금액(억원)
        discount,           # 12. 할인/할증률
        str(pre_qty),       # 13. 증자전 주식수
        str(ratio),         # 14. 증자비율(%)
        sub_date,           # 15. 청약일
        pay_date,           # 16. 납입일
        purpose,            # 17. 자금용도
        investor,           # 18. 투자자/대상자
        underwriter         # 19. 주관사
    ]

def main():
    require_env("DART_API_KEY", DART_API_KEY)
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    # LOOKBACK_DAYS 기간만큼 과거 데이터부터 오늘까지 검색
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

    # 1. DART 공시 전체 목록 가져오기
    items = dart_list(bgn_de=bgn_de, end_de=end_de)
    
    rows_to_append = {"유상증자": [], "전환사채": [], "교환사채": []}

    # 2. 타겟 공시 필터링 및 데이터 추출
    for it in items:
        report_nm = it.get("report_nm", "")
        
        report_type = ""
        if "유상" in report_nm and "결정" in report_nm: report_type = "유상증자"
        elif "전환사채" in report_nm: report_type = "전환사채"
        elif "교환사채" in report_nm: report_type = "교환사채"
        else: continue 

        rcept_no = it.get("rcept_no")
        # 중복 방지 (이미 시트에 있는 접수번호면 건너뜀)
        if rcept_no in processed_rcepts[report_type]:
            continue
            
        corp_code = it.get("corp_code")
        rcept_dt = it.get("rcept_dt")
        
        # 3. DART 전용 JSON API에서 정확한 상세 데이터 가져오기
        structured_data = get_structured_dart_data(corp_code, rcept_dt, rcept_dt, rcept_no, report_type)
        if structured_data:
            doc_data = parse_html_for_investor_and_underwriter(rcept_no)
            row = build_row(it, report_type, structured_data, doc_data)
            rows_to_append[report_type].append(row)

    # 4. 시트에 데이터 기록
    for name in sheet_names:
        if rows_to_append[name]:
            # 최신 데이터가 위로 오길 원하시면 시트 정렬 기능을 별도로 쓰시는 것이 좋습니다.
            worksheets[name].append_rows(rows_to_append[name], value_input_option="USER_ENTERED")
            print(f"✅ {name} 추가 완료: {len(rows_to_append[name])}건")
        else:
            print(f"✅ {name} 새로 추가할 내용 없음.")

if __name__ == "__main__":
    main()
