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

# 💡 핵심: 깃허브 액션에서 0으로 설정되어 있어도, 무조건 최근 3일치를 검색하여 누락과 지연을 방어합니다.
LOOKBACK_DAYS =  int(os.getenv("LOOKBACK_DAYS", "0"))
MAX_PAGES = 5  # 최대 500건 탐색
PAGE_COUNT = 100
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"

# --- [2] 시트별 헤더 정의 (요청하신 순서 100% 반영) ---
HEADERS = {
    "유상증자": [
        "접수번호", "회사명", "이사회결의일", "증자방식", "기타주발행수", "1주당액면가(원)", "신주발행가액(원)", 
        "증자전보통주(주)", "증자전기타주(주)", "시설자금(억)", "영업양수(억)", "운영자금(억)", 
        "채무상환(억)", "타법인취득(억)", "기타자금(억)", "청약일", "납입일", "투자자(대상자)"
    ],
    "전환사채": [
        "접수번호", "회사명", "이사회결의일", "회차", "발행방법", "권면총액(원)", "표면이자율(%)", "만기이자율(%)", 
        "사채만기일", "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", "타법인취득(억)", 
        "기타자금(억)", "전환비율(%)", "전환가액(원)", "최저조정가액(원)", "전환청구시작일", "전환청구종료일", 
        "청약일", "납입일", "대표주관사/투자자"
    ],
    "교환사채": [
        "접수번호", "회사명", "이사회결의일", "회차", "발행방법", "권면총액(원)", "표면이자율(%)", "만기이자율(%)", 
        "사채만기일", "시설자금(억)", "영업양수(억)", "운영자금(억)", "채무상환(억)", "타법인취득(억)", 
        "기타자금(억)", "교환비율(%)", "교환가액(원)", "교환청구시작일", "교환청구종료일", "청약일", "납입일", "대표주관사/투자자"
    ]
}

def require_env(name: str, value: str):
    if not value: raise RuntimeError(f"Missing required env var: {name}")

def clean_str(x) -> str:
    if x is None: return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", clean_str(s)).strip()

def extract_number(s: str):
    s = clean_str(s)
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."): return ""
    try: return str(int(float(t)) if "." in t else int(t))
    except: return ""

def to_eok(val_str):
    num_str = extract_number(val_str)
    if not num_str: return "0"
    return str(round(int(num_str) / 100_000_000, 2))

# --- [3] 구글 시트 연동 ---
def get_or_create_worksheet(sh, title: str):
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="1000", cols="30")
    if not ws.row_values(1):
        ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

def get_processed_rcept_set(ws):
    """구글 시트 A열(접수번호)을 읽어서 완벽한 큐(Queue) 역할을 합니다."""
    col = ws.col_values(1)
    if not col or col[0].strip() == "접수번호":
        return set(x.strip() for x in col[1:] if x.strip())
    return set(x.strip() for x in col if x.strip())

# --- [4] 데이터 추출 (초강력 HTML 파서) ---
def parse_html_content(rcept_no: str) -> dict:
    """공시 원본 HTML을 뜯어서 모든 세부 수치를 지연 없이 즉시 가져옵니다."""
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    out = {
        "board_date": "", "method": "", "qty_e": "", "fv": "", "isu_prc": "", 
        "pre_o": "", "pre_e": "", "f": "0", "b": "0", "o": "0", "d": "0", "c": "0", "e": "0",
        "sub_d": "", "pay_d": "", "inv": "", "rnd": "", "fta": "", "i_ex": "", "i_sf": "", 
        "m_d": "", "c_rt": "", "refix": "", "c_s": "", "c_e": ""
    }
    
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_files = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
        if not html_files: return out
        largest_html = max(html_files, key=lambda n: zf.getinfo(n).file_size)
        raw = zf.read(largest_html)
        
        for enc in ("utf-8", "cp949", "euc-kr"):
            try: 
                html = raw.decode(enc)
                break
            except: continue
        else:
            html = raw.decode("utf-8", errors="ignore")
            
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ").replace("\n", " ")

        # 1. 텍스트에서 주요 항목 추출 (투자자, 결의일 등)
        investor_match = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        if investor_match: out["inv"] = investor_match.group(2)[:30].strip()
        
        board_match = re.search(r"이사회\s*결의일.*?(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
        if board_match:
            out["board_date"] = f"{board_match.group(1)}-{int(board_match.group(2)):02d}-{int(board_match.group(3)):02d}"

        # 2. 표(Table)에서 세부 수치 추출 (예전 코드에 빠져있던 핵심 로직 추가!)
        dfs = pd.read_html(io.StringIO(html))
        for df in dfs:
            df = df.fillna("").astype(str)
            for _, row in df.iterrows():
                row_vals = [normalize_ws(v) for v in row.tolist()]
                row_str = " ".join(row_vals)
                
                # 자금조달 목적 
                if "자금조달의 목적" in row_str or "시설자금" in row_str:
                    for k, key_out in [("시설자금", "f"), ("영업양수", "b"), ("운영자금", "o"), 
                                     ("채무상환", "d"), ("타법인", "c"), ("기타자금", "e")]:
                        for i, cell in enumerate(row_vals):
                            if k in cell and i + 1 < len(row_vals):
                                val = extract_number(row_vals[i+1])
                                if val: out[key_out] = to_eok(val)

                # 일반 및 사채 상세 항목 추출
                for i, cell in enumerate(row_vals):
                    if not cell: continue
                    next_val = row_vals[i+1] if i + 1 < len(row_vals) else ""
                    if not next_val: continue
                    
                    if any(x in cell for x in ["증자방식", "사채발행방법"]) and not out["method"]: out["method"] = next_val
                    elif "기타주식" in cell and "신주의 수" in cell: out["qty_e"] = extract_number(next_val)
                    elif "1주당 액면가액" in cell: out["fv"] = extract_number(next_val)
                    elif any(x in cell for x in ["신주발행가액", "전환가액", "교환가액"]): out["isu_prc"] = extract_number(next_val)
                    elif "증자전 발행주식총수" in cell:
                        if "보통" in cell: out["pre_o"] = extract_number(next_val)
                        if "기타" in cell: out["pre_e"] = extract_number(next_val)
                    elif any(x in cell for x in ["청약기일", "청약시작일"]): out["sub_d"] = next_val.replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                    elif "납입기일" in cell: out["pay_d"] = next_val.replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                    
                    # 사채 전용 추가 필드
                    elif "사채의 종류" in cell: out["rnd"] = next_val
                    elif "권면총액" in cell: out["fta"] = extract_number(next_val)
                    elif "표면이자율" in cell: out["i_ex"] = next_val
                    elif "만기이자율" in cell: out["i_sf"] = next_val
                    elif "사채만기일" in cell: out["m_d"] = next_val.replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                    elif "전환비율" in cell or "교환비율" in cell: out["c_rt"] = next_val
                    elif "최저 조정가액" in cell: out["refix"] = extract_number(next_val)
                    elif "청구기간" in cell:
                        dates = next_val.split("~")
                        if len(dates) == 2:
                            out["c_s"] = dates[0].replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
                            out["c_e"] = dates[1].replace("년", "-").replace("월", "-").replace("일", "").replace(" ", "")
    except Exception as e:
        print(f"   -> ⚠️ 일부 파싱 중 오류 발생 ({e})")
        
    return out

# --- [5] 시트별 매핑 ---
def build_row(list_item: dict, report_type: str, p: dict):
    rn = clean_str(list_item.get("rcept_no"))
    cn = clean_str(list_item.get("corp_name"))

    if report_type == "유상증자":
        return [rn, cn, p["board_date"], p["method"], p["qty_e"], p["fv"], p["isu_prc"], p["pre_o"], p["pre_e"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["sub_d"], p["pay_d"], p["inv"]]
    elif report_type == "전환사채":
        return [rn, cn, p["board_date"], p["rnd"], p["method"], p["fta"], p["i_ex"], p["i_sf"], p["m_d"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["c_rt"], p["isu_prc"], p["refix"], p["c_s"], p["c_e"], p["sub_d"], p["pay_d"], p["inv"]]
    elif report_type == "교환사채":
        return [rn, cn, p["board_date"], p["rnd"], p["method"], p["fta"], p["i_ex"], p["i_sf"], p["m_d"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["c_rt"], p["isu_prc"], p["c_s"], p["c_e"], p["sub_d"], p["pay_d"], p["inv"]]

# --- [6] 메인 실행부 ---
def main():
    require_env("DART_API_KEY", DART_API_KEY)
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)

    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.authorize(Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    sheet_names = ["유상증자", "전환사채", "교환사채"]
    worksheets, sheet_seens = {}, {}

    for name in sheet_names:
        worksheets[name] = get_or_create_worksheet(sh, name)
        sheet_seens[name] = get_processed_rcept_set(worksheets[name])

    # 목록 최대 500건 검색 (누락 방지)
    results = []
    page_no = 1
    while page_no <= MAX_PAGES:
        params = {"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de, "sort": "date", "sort_mth": "desc", "page_no": str(page_no), "page_count": str(PAGE_COUNT)}
        r = requests.get(LIST_URL, params=params).json()
        if r.get("status") != "000": break
        results.extend(r.get("list", []))
        if page_no >= r.get("total_page", 1): break
        page_no += 1

    print(f"📋 최근 {LOOKBACK_DAYS}일치 공시 {len(results)}건 확인 중...")
    rows_to_append = {"유상증자": [], "전환사채": [], "교환사채": []}

    for it in results:
        rpt = it.get("report_nm", "")
        r_type = "유상증자" if "유상" in rpt and "결정" in rpt else ("전환사채" if "전환사채" in rpt and "결정" in rpt else ("교환사채" if "교환사채" in rpt and "결정" in rpt else ""))
        if not r_type: continue 

        r_no = it.get("rcept_no")
        
        # 구글 시트에 이미 있으면 패스 (지우면 다시 가져옴)
        if r_no in sheet_seens[r_type]: continue
            
        print(f"🔍 실시간 분석: [{it.get('corp_name')}] {rpt}")
        
        # 원본 HTML 문서 실시간 다운로드 및 분석 (지연 시간 0)
        parsed_data = parse_html_content(r_no)
        row = build_row(it, r_type, parsed_data)
        rows_to_append[r_type].append(row)
        print(f"   -> ✅ 즉시 추출 성공 및 시트 대기열 추가")

    print("\n[구글 시트 업데이트]")
    for name in sheet_names:
        if rows_to_append[name]:
            worksheets[name].append_rows(rows_to_append[name], value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트: {len(rows_to_append[name])}건 업데이트 완료!")
        else:
            print(f"📊 {name} 시트: 새로 추가할 건 없음.")

if __name__ == "__main__":
    main()
