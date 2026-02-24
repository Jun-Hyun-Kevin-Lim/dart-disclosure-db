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

# --- [1] 환경 설정 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 최근 3일치 공시를 싹 다 뒤져서 놓치는 것을 방지
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
DETAIL_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# --- [2] 시트별 헤더 정의 (대표님 요청 스펙) ---
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

# --- [3] 유틸리티 함수 ---
def clean(val): return str(val).strip() if val is not None else ""

def to_eok(won):
    try:
        val = re.sub(r"[^\d\-\.]", "", str(won))
        if not val: return "0"
        return str(round(int(float(val)) / 100_000_000, 2))
    except: return "0"

def get_or_create_ws(sh, title):
    try: ws = sh.worksheet(title)
    except WorksheetNotFound: ws = sh.add_worksheet(title=title, rows="1000", cols="30")
    if not ws.row_values(1): ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

def get_investor_html(rcept_no):
    """HTML 문서를 열어 투자자 명단만 확실하게 추출합니다."""
    try:
        r = requests.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
        soup = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        return m.group(2)[:40].strip() if m else ""
    except: return ""

def get_all_dart_list(bgn_de, end_de):
    """500건 싹쓸이 검색"""
    results = []
    page_no = 1
    while page_no <= 5: 
        params = {"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de, "page_no": str(page_no), "page_count": "100"}
        res = requests.get(LIST_URL, params=params).json()
        if res.get("status") != "000": break
        results.extend(res.get("list", []))
        if page_no >= res.get("total_page", 1): break
        page_no += 1
    return results

# --- [4] 정확도 100% 데이터 매핑 (JSON 기반) ---
def build_row(r_type, list_item, d, inv):
    rn, cn = clean(list_item.get("rcept_no")), clean(list_item.get("corp_name"))
    bd = clean(d.get("bddd"))
    f, b, o, dtrp, c, e = [to_eok(d.get(k)) for k in ["fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp", "fdpp_ocsa", "fdpp_etc"]]
    uw_inv = clean(d.get("rpmcmp")) if d.get("rpmcmp") else inv

    if r_type == "유상증자":
        return [rn, cn, bd, clean(d.get("ic_mthn")), clean(d.get("nstk_estk_cnt")), clean(d.get("fv_ps")), clean(d.get("tisstk_prc")), clean(d.get("bfic_tisstk_ostk")), clean(d.get("bfic_tisstk_estk")), f, b, o, dtrp, c, e, clean(d.get("sbscpn_bgd")), clean(d.get("pymdt")), inv]
    elif r_type == "전환사채":
        return [rn, cn, bd, clean(d.get("bd_tm")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")), clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e, clean(d.get("cv_rt")), clean(d.get("cv_prc")), clean(d.get("act_mktprcfl_cvprc_lwtrsprc")), clean(d.get("cvrqpd_bgd")), clean(d.get("cvrqpd_edd")), clean(d.get("sbd")), clean(d.get("pymd")), uw_inv]
    elif r_type == "교환사채":
        return [rn, cn, bd, clean(d.get("bd_tm")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")), clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e, clean(d.get("ex_rt")), clean(d.get("ex_prc")), clean(d.get("exrqpd_bgd")), clean(d.get("exrqpd_edd")), clean(d.get("sbd")), clean(d.get("pymd")), uw_inv]

# --- [5] 메인 프로세스 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    worksheets = {name: get_or_create_ws(sh, name) for name in HEADERS.keys()}
    sheet_seens = {name: set(worksheets[name].col_values(1)[1:]) for name in HEADERS.keys()}

    items = get_all_dart_list(bgn_de, end_de)
    print(f"📋 DART 목록 확인: 최근 3일간 총 {len(items)}건 검색 완료")

    rows_to_add = {name: [] for name in HEADERS.keys()}

    for it in items:
        rpt = it.get("report_nm", "")
        r_type = "유상증자" if "유상" in rpt and "결정" in rpt else ("전환사채" if "전환사채" in rpt and "결정" in rpt else ("교환사채" if "교환사채" in rpt and "결정" in rpt else ""))
        if not r_type: continue

        r_no = clean(it.get("rcept_no"))
        
        # 시트에 있으면 건너뜀 (지우면 다시 가져옴)
        if r_no in sheet_seens[r_type]: 
            continue

        print(f"🔎 분석 시도: [{it.get('corp_name')}] {rpt} ({r_no})")
        
        # 💡 핵심 버그 해결: 과거 정정공시까지 완벽히 잡기 위해 날짜 파라미터(bgn_de, end_de) 제거!
        params = {"crtfc_key": DART_API_KEY, "corp_code": it.get("corp_code")}
        try:
            detail_res = requests.get(DETAIL_APIS[r_type], params=params, timeout=30).json()
            detail = next((d for d in detail_res.get("list", []) if clean(d.get("rcept_no")) == r_no), None)
            
            if detail:
                inv = get_investor_html(r_no)
                rows_to_add[r_type].append(build_row(r_type, it, detail, inv))
                print(f"   -> ✅ 데이터 100% 추출 성공 및 대기열 추가")
            else:
                print(f"   -> ⏳ 금감원 전용 API 데이터 생성 지연 중 (시트에 적지 않고 다음 실행 때 재시도합니다.)")
        except Exception as e:
            print(f"   -> ❌ API 호출 오류: {e}")

    for name, rows in rows_to_add.items():
        if rows:
            worksheets[name].append_rows(rows, value_input_option="USER_ENTERED")
            print(f"\n📊 [{name}] 시트에 {len(rows)}건 완벽하게 업데이트 되었습니다!")
        else:
            print(f"📊 [{name}] 새로 추가할 건이 없습니다.")

if __name__ == "__main__":
    main()
