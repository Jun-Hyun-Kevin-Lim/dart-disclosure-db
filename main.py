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

# 당일 공시만 가져오기 (테스트 시 1~3으로 늘려보세요)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

# 타겟 공시명 정규식
TARGET_REPORT_RE = re.compile(r"(유상\s*증자\s*결정|전환\s*사채\s*권\s*발행\s*결정|교환\s*사채\s*권\s*발행\s*결정)")

# 대표님이 정리해주신 필수 URL 적용
LIST_URL = "https://opendart.fss.or.kr/api/list.json"      # 목록은 다루기 쉬운 json으로
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"    # 문서는 xml(zip)만 지원

def require_env(name: str, value: str):
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")

def clean_str(x) -> str:
    if x is None: return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def normalize_ws(s: str) -> str:
    s = clean_str(s)
    return re.sub(r"\s+", " ", s).strip()

def extract_number(s: str):
    s = clean_str(s)
    if not s: return ""
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."): return ""
    try:
        return str(int(float(t)) if "." in t else int(t))
    except:
        return ""

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
    """list.json을 활용하여 공시 목록을 가져옵니다."""
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

def get_document_html(rcept_no: str) -> str:
    """document.xml을 호출해 ZIP 파일을 다운받고 메인 HTML을 추출합니다."""
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        # HTML 파일 찾기 (보통 여러 개가 있지만 가장 용량이 큰 것이 본문입니다)
        html_files = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
        if not html_files: return ""
        largest_html = max(html_files, key=lambda n: zf.getinfo(n).file_size)
        raw = zf.read(largest_html)
        
        # 인코딩 처리
        for enc in ("utf-8", "cp949", "euc-kr"):
            try: return raw.decode(enc)
            except: continue
        return raw.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"HTML 추출 실패 ({rcept_no}): {e}")
        return ""

def parse_html_content(html: str, report_type: str) -> dict:
    """다운받은 HTML 표(Table)와 텍스트를 분석하여 필요한 19개 필드값을 긁어냅니다."""
    out = {
        "board_date": "", "method": "", "qty": "", "issue_price": "", 
        "base_price": "", "total_amount": "", "discount": "", "pre_qty": "", 
        "sub_date": "", "pay_date": "", "purpose": "", "investor": "", "underwriter": ""
    }
    
    if not html: return out
    
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ").replace("\n", " ")

    # 1. 정규식 텍스트 파싱 (투자자, 주관사, 이사회결의일 등)
    investor_match = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
    if investor_match: out["investor"] = investor_match.group(2)[:30].strip()
    
    underwriter_match = re.search(r"(주관회사|대표주관회사|인수회사)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+증권)", text)
    if underwriter_match: out["underwriter"] = underwriter_match.group(2)[:30].strip()
    
    board_match = re.search(r"이사회\s*결의일.*?(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
    if board_match:
        out["board_date"] = f"{board_match.group(1)}-{int(board_match.group(2)):02d}-{int(board_match.group(3)):02d}"

    # 2. Pandas를 이용한 표(Table) 데이터 파싱
    try:
        dfs = pd.read_html(io.StringIO(html))
        for df in dfs:
            df = df.fillna("").astype(str)
            for _, row in df.iterrows():
                row_vals = [normalize_ws(v) for v in row.tolist()]
                row_str = " ".join(row_vals)
                
                # 자금조달 목적 파싱 (억원 단위 변환)
                if "자금조달의 목적" in row_str or "시설자금" in row_str or "운영자금" in row_str:
                    purposes = []
                    total = 0
                    for k, label in [("시설자금", "시설"), ("운영자금", "운영"), ("영업양수자금", "영업양수"), 
                                     ("채무상환자금", "채무상환"), ("타법인 증권 취득자금", "타법인증권취득"), ("기타자금", "기타")]:
                        for i, cell in enumerate(row_vals):
                            if k in cell and i + 1 < len(row_vals):
                                val = extract_number(row_vals[i+1])
                                if val:
                                    eok = round(int(val) / 100_000_000, 2)
                                    if eok > 0:
                                        purposes.append(f"{label}:{eok}억")
                                        total += int(val)
                    if purposes: out["purpose"] = ", ".join(purposes)
                    if total > 0 and not out["total_amount"]: out["total_amount"] = str(round(total / 100_000_000, 2))

                # 기타 주요 항목 추출 로직
                for i, cell in enumerate(row_vals):
                    if not cell: continue
                    next_val = row_vals[i+1] if i + 1 < len(row_vals) else ""
                    
                    if any(x in cell for x in ["증자방식", "사채발행방법"]) and not out["method"]:
                        out["method"] = next_val
                    elif any(x in cell for x in ["신주발행가액", "전환가액", "교환가액"]) and not out["issue_price"]:
                        out["issue_price"] = extract_number(next_val)
                    elif "기준주가" in cell and not out["base_price"]:
                        out["base_price"] = extract_number(next_val)
                    elif any(x in cell for x in ["할인율", "할증율"]) and not out["discount"]:
                        out["discount"] = next_val
                    elif any(x in cell for x in ["청약기일", "청약시작일"]) and not out["sub_date"]:
                        out["sub_date"] = next_val.replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                    elif "납입기일" in cell and not out["pay_date"]:
                        out["pay_date"] = next_val.replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                    elif any(x in cell for x in ["사채의 권면총액", "신주의 수"]) and not out["qty"]:
                        out["qty"] = extract_number(next_val)
                    elif "증자전 발행주식총수" in cell and not out["pre_qty"]:
                        out["pre_qty"] = extract_number(next_val)
                        
    except Exception as e:
        print(f"표 분석 중 에러 (텍스트 기반으로만 진행): {e}")

    # 증자비율 계산
    if out["qty"] and out["pre_qty"] and report_type == "유상증자":
        try:
            out["ratio"] = str(round((int(out["qty"]) / int(out["pre_qty"])) * 100, 2))
        except:
            out["ratio"] = ""
    else:
        out["ratio"] = ""

    return out

def build_row(list_item: dict, report_type: str, parsed: dict):
    rcept_no = list_item.get("rcept_no", "")
    corp_name = list_item.get("corp_name", "")
    market = list_item.get("corp_cls", "")
    market = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}.get(market, market)
    report_nm = list_item.get("report_nm", "")

    return [
        rcept_no, corp_name, market, report_nm,
        parsed["board_date"], parsed["method"], report_type,
        parsed["qty"], parsed["issue_price"], parsed["base_price"], parsed["total_amount"], parsed["discount"],
        parsed["pre_qty"], parsed.get("ratio", ""), parsed["sub_date"], parsed["pay_date"],
        parsed["purpose"], parsed["investor"], parsed["underwriter"]
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

    # 1. list.json으로 전체 공시 검색
    items = dart_list_json(bgn_de=bgn_de, end_de=end_de)
    print(f"📋 DART 목록 검색(list.json) 완료: 총 {len(items)}건 확인됨.")
    
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
        print(f"\n🔍 타겟 공시 발견: [{corp_name}] {report_nm}")

        if rcept_no in processed_rcepts[report_type]:
            print("   -> 🚫 이미 기록된 공시입니다. 패스.")
            continue
            
        # 2. document.xml로 원본 HTML 실시간 다운로드 및 분석 (지연 없음!)
        print("   -> 📥 원본 HTML 문서 다운로드 및 데이터 추출 중...")
        html_content = get_document_html(rcept_no)
        parsed_data = parse_html_content(html_content, report_type)
        
        row = build_row(it, report_type, parsed_data)
        rows_to_append[report_type].append(row)
        print("   -> ✅ 데이터 추출 완료 및 시트 대기열 추가.")

    print("\n[시트 업데이트 결과]")
    for name in sheet_names:
        if rows_to_append[name]:
            worksheets[name].append_rows(rows_to_append[name], value_input_option="USER_ENTERED")
            print(f"✅ {name} 시트: {len(rows_to_append[name])}건 추가 완료.")
        else:
            print(f"✅ {name} 시트: 새로 추가할 내용 없음.")

if __name__ == "__main__":
    main()
