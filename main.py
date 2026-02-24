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
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"

# --- [2] 시트별 독립 헤더 정의 (요청 스펙 반영) ---
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
def clean_str(x):
    if x is None: return ""
    return str(x).strip().replace("\n", " ")

def extract_num(s):
    """문자열에서 숫자만 추출 (억원 단위 변환용)"""
    s = clean_str(s)
    t = re.sub(r"[^\d]", "", s)
    if not t: return 0
    return int(t)

def to_eok(val):
    """원 단위를 억원 단위로 변환"""
    num = extract_num(val)
    if num == 0: return "0"
    return str(round(num / 100_000_000, 2))

# --- [4] 상태 관리 (Seen.json) ---
def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r") as f: return set(json.load(f))
        except: return set()
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen_set), f)

# --- [5] HTML 표 분석 (Pandas 활용 실시간 파싱) ---
def get_html_data(rcept_no, r_type):
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    res_data = {k: "" for k in ["bd_date", "method", "stk_o", "stk_e", "fv", "isu_prc", "pre_o", "pre_e", 
                               "f", "b", "o", "d", "c", "e", "sub_d", "pay_d", "inv", "rnd", "knd", 
                               "fta", "i_ex", "i_sf", "m_d", "c_rt", "c_prc", "refix", "c_s", "c_e"]}
    try:
        r = requests.get(DOC_URL, params=params, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
        html = zf.read(html_file).decode("utf-8", errors="ignore")
        
        # 1. 텍스트 정규식 (투자자, 이사회결의일 등)
        soup = BeautifulSoup(html, "lxml")
        full_text = soup.get_text(" ").replace("\n", " ")
        m_inv = re.search(r"(배정대상자|투자자|대표주관회사)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", full_text)
        if m_inv: res_data["inv"] = m_inv.group(2)[:40].strip()
        m_bd = re.search(r"이사회\s*결의일.*?(\d{4}-\d{2}-\d{2})", full_text)
        if m_bd: res_data["bd_date"] = m_bd.group(1)

        # 2. 표 데이터 분석
        dfs = pd.read_html(io.StringIO(html))
        for df in dfs:
            df = df.fillna("").astype(str)
            for _, row in df.iterrows():
                line = " ".join(row.tolist())
                vals = [clean_str(v) for v in row.tolist()]
                
                # 키워드 매칭 로직 (지연 없이 즉시 탐색)
                if "증자방식" in line or "발행방법" in line: res_data["method"] = vals[-1]
                elif "보통주식" in line and "신주의 수" in line: res_data["stk_o"] = vals[-1]
                elif "기타주식" in line and "신주의 수" in line: res_data["stk_e"] = vals[-1]
                elif "액면가액" in line: res_data["fv"] = vals[-1]
                elif "발행가액" in line or "전환가액" in line or "교환가액" in line: res_data["isu_prc"] = vals[-1]
                elif "증자전 발행주식총수" in line: 
                    if "보통" in line: res_data["pre_o"] = vals[-1]
                    if "기타" in line: res_data["pre_e"] = vals[-1]
                # 자금 용도
                if "시설자금" in line: res_data["f"] = to_eok(vals[-1])
                elif "영업양수" in line: res_data["b"] = to_eok(vals[-1])
                elif "운영자금" in line: res_data["o"] = to_eok(vals[-1])
                elif "채무상환" in line: res_data["d"] = to_eok(vals[-1])
                elif "타법인" in line: res_data["c"] = to_eok(vals[-1])
                elif "기타자금" in line: res_data["e"] = to_eok(vals[-1])
                # 날짜
                if "청약일" in line: res_data["sub_d"] = vals[-1]
                elif "납입일" in line: res_data["pay_d"] = vals[-1]
                # 사채 전용
                if "사채의 종류" in line: res_data["rnd"] = vals[-1]
                elif "권면총액" in line: res_data["fta"] = vals[-1]
                elif "표면이자율" in line: res_data["i_ex"] = vals[-1]
                elif "만기이자율" in line: res_data["i_sf"] = vals[-1]
                elif "사채만기일" in line: res_data["m_d"] = vals[-1]
                elif "전환비율" in line or "교환비율" in line: res_data["c_rt"] = vals[-1]
                elif "최저 조정가액" in line: res_data["refix"] = vals[-1]
                elif "청구기간" in line:
                    res_data["c_s"] = vals[-1].split("~")[0] if "~" in vals[-1] else vals[-1]
                    res_data["c_e"] = vals[-1].split("~")[-1] if "~" in vals[-1] else ""
    except: pass
    return res_data

# --- [6] 행 데이터 조립 (시트별 필드 구성) ---
def build_row(r_type, list_item, p):
    rn, cn, mr, rpt = [clean_str(list_item.get(k)) for k in ["rcept_no", "corp_name", "corp_cls", "report_nm"]]
    mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "기타"}.get(mr, mr)

    if r_type == "유상증자":
        return [rn, cn, mr, rpt, p["bd_date"], p["method"], p["stk_o"], p["stk_e"], p["fv"], p["isu_prc"], p["pre_o"], p["pre_e"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["sub_d"], p["pay_d"], p["inv"]]
    elif r_type == "전환사채":
        return [rn, cn, mr, rpt, p["bd_date"], p["rnd"], "전환사채", p["method"], p["fta"], p["i_ex"], p["i_sf"], p["m_d"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["c_rt"], p["isu_prc"], p["refix"], p["c_s"], p["c_e"], p["sub_d"], p["pay_d"], p["inv"]]
    elif r_type == "교환사채":
        return [rn, cn, mr, rpt, p["bd_date"], p["rnd"], "교환사채", p["method"], p["fta"], p["i_ex"], p["i_sf"], p["m_d"], p["f"], p["b"], p["o"], p["d"], p["c"], p["e"], p["c_rt"], p["isu_prc"], p["c_s"], p["c_e"], p["sub_d"], p["pay_d"], p["inv"]]

# --- [7] 메인 실행 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    seen = load_seen()
    worksheets = {}
    for name in HEADERS.keys():
        try: worksheets[name] = sh.worksheet(name)
        except WorksheetNotFound: 
            worksheets[name] = sh.add_worksheet(title=name, rows="1000", cols="30")
        if not worksheets[name].row_values(1): worksheets[name].append_row(HEADERS[name], value_input_option="USER_ENTERED")

    # 실시간 목록 검색
    list_res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "page_count": "100"}).json()
    items = list_res.get("list", [])
    print(f"📋 공시 목록 확인: {len(items)}건")

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
        # 구글 시트 A열 실시간 확인 (중복 방지)
        sheet_seen = set(worksheets[r_type].col_values(1)[1:])
        if r_no in seen or r_no in sheet_seen: continue

        print(f"🔍 [실시간 추출 시작] [{it.get('corp_name')}] {rpt}")
        # 지연 없는 HTML 직접 파싱 호출
        parsed = get_html_data(r_no, r_type)
        row = build_row(r_type, it, parsed)
        rows_to_add[r_type].append(row)
        newly_seen.add(r_no)

    for name, data in rows_to_add.items():
        if data:
            worksheets[name].append_rows(data, value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트: {len(data)}건 업데이트 완료.")

    if newly_seen:
        seen.update(newly_seen)
        save_seen(seen)

if __name__ == "__main__":
    main()
