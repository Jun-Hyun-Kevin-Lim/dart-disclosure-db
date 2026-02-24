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

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0")) 
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

# 상태 관리 파일
SEEN_FILE = "seen.json"
QUEUE_FILE = "retry_queue.json"

LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
DETAIL_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# --- [2] 시트별 헤더 정의 ---
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

# --- [3] 유틸리티 및 파일 I/O ---
def clean(val): return str(val).strip() if val is not None else ""

def to_eok(won):
    try:
        val = re.sub(r"[^\d\-\.]", "", str(won))
        if not val: return "0"
        return str(round(int(float(val)) / 100_000_000, 2))
    except: return "0"

def load_json(filepath, default_type):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except: pass
    return default_type

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def get_or_create_ws(sh, title):
    try: ws = sh.worksheet(title)
    except WorksheetNotFound: ws = sh.add_worksheet(title=title, rows="1000", cols="40")
    if not ws.row_values(1): ws.append_row(HEADERS[title], value_input_option="USER_ENTERED")
    return ws

def get_investor_html(rcept_no):
    try:
        r = requests.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        html_file = max(zf.namelist(), key=lambda n: zf.getinfo(n).file_size)
        soup = BeautifulSoup(zf.read(html_file).decode("utf-8", errors="ignore"), "lxml")
        text = soup.get_text(" ").replace("\n", " ")
        m = re.search(r"(배정대상자|제3자\s*배정대상자|투자자)\s*[:：]?\s*([가-힣a-zA-Z0-9\s㈜]+)", text)
        return m.group(2)[:40].strip() if m else ""
    except: return ""

# --- [4] 데이터 매핑 ---
def build_row(r_type, queue_item, d, inv):
    rn, cn, mr, rpt = queue_item["rcept_no"], queue_item["corp_name"], queue_item["corp_cls"], queue_item["report_nm"]
    bd = clean(d.get("bddd"))
    f, b, o, dtrp, c, e = [to_eok(d.get(k)) for k in ["fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp", "fdpp_ocsa", "fdpp_etc"]]
    uw_inv = clean(d.get("rpmcmp")) if d.get("rpmcmp") else inv

    if r_type == "유상증자":
        return [rn, cn, mr, rpt, bd, clean(d.get("ic_mthn")), clean(d.get("nstk_ostk_cnt")), clean(d.get("nstk_estk_cnt")), clean(d.get("fv_ps")), clean(d.get("tisstk_prc")), clean(d.get("bfic_tisstk_ostk")), clean(d.get("bfic_tisstk_estk")), f, b, o, dtrp, c, e, clean(d.get("sbscpn_bgd")), clean(d.get("pymdt")), inv]
    elif r_type == "전환사채":
        return [rn, cn, mr, rpt, bd, clean(d.get("bd_tm")), clean(d.get("bd_knd")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")), clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e, clean(d.get("cv_rt")), clean(d.get("cv_prc")), clean(d.get("act_mktprcfl_cvprc_lwtrsprc")), clean(d.get("cvrqpd_bgd")), clean(d.get("cvrqpd_edd")), clean(d.get("sbd")), clean(d.get("pymd")), uw_inv]
    elif r_type == "교환사채":
        return [rn, cn, mr, rpt, bd, clean(d.get("bd_tm")), clean(d.get("bd_knd")), clean(d.get("bdis_mthn")), clean(d.get("bd_fta")), clean(d.get("bd_intr_ex")), clean(d.get("bd_intr_sf")), clean(d.get("bd_mtd")), f, b, o, dtrp, c, e, clean(d.get("ex_rt")), clean(d.get("ex_prc")), clean(d.get("exrqpd_bgd")), clean(d.get("exrqpd_edd")), clean(d.get("sbd")), clean(d.get("pymd")), uw_inv]

# --- [5] 메인 프로세스 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    # 1. 상태 로드
    seen_list = load_json(SEEN_FILE, [])
    seen = set(seen_list)
    retry_queue = load_json(QUEUE_FILE, {}) # {rcept_no: {item_data}}

    worksheets = {name: get_or_create_ws(sh, name) for name in HEADERS.keys()}
    sheet_seens = {name: set(worksheets[name].col_values(1)[1:]) for name in HEADERS.keys()}

    # 2. 신규 공시 검색 (목록)
    res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de, "page_count": "100"}).json()
    items = res.get("list", [])
    print(f"📋 DART 목록 확인: 오늘 총 {len(items)}건의 전체 공시가 있습니다.")

    # 3. 큐(Queue)에 타겟 공시 담기
    for it in items:
        rpt = it.get("report_nm", "")
        r_type = "유상증자" if "유상" in rpt and "결정" in rpt else ("전환사채" if "전환사채" in rpt and "결정" in rpt else ("교환사채" if "교환사채" in rpt and "결정" in rpt else ""))
        if not r_type: continue

        r_no = clean(it.get("rcept_no"))
        mr = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX"}.get(it.get("corp_cls"), "기타")
        
        # 이미 처리된 건 제외
        if r_no in seen or r_no in sheet_seens[r_type]: continue
        
        # 큐에 없으면 새로 추가
        if r_no not in retry_queue:
            retry_queue[r_no] = {
                "rcept_no": r_no, "corp_code": it.get("corp_code"), "corp_name": it.get("corp_name"), 
                "corp_cls": mr, "report_nm": rpt, "r_type": r_type, "added_at": datetime.now(tz).isoformat()
            }
            print(f"📥 큐에 추가됨: [{it.get('corp_name')}] {rpt}")

    # 4. 큐(Queue) 순회하며 상세 데이터 수집 시도
    rows_to_add = {name: [] for name in HEADERS.keys()}
    processed_this_run = []

    print(f"\n🔄 현재 대기열(Queue) 처리 시작 (총 {len(retry_queue)}건)")
    
    for r_no, q_item in list(retry_queue.items()):
        r_type = q_item["r_type"]
        print(f"🔎 분석 중: [{q_item['corp_name']}] {q_item['report_nm']} ({r_no})")
        
        # 💡 핵심 버그 수정: 상세 API 호출 시 날짜 파라미터(bgn_de, end_de) 제거!
        # 기재정정 공시는 원본 날짜가 몇 달 전일 수 있으므로 
        # 날짜를 지정하면 금감원 API가 아무것도 반환하지 않는 버그가 있었습니다.
        params = {"crtfc_key": DART_API_KEY, "corp_code": q_item["corp_code"]}
        
        try:
            detail_res = requests.get(DETAIL_APIS[r_type], params=params, timeout=30).json()
            details_list = detail_res.get("list", [])
            
            # 정확한 접수번호 매칭
            detail = next((d for d in details_list if clean(d.get("rcept_no")) == r_no), None)
            
            if detail:
                inv = get_investor_html(r_no)
                rows_to_add[r_type].append(build_row(r_type, q_item, detail, inv))
                processed_this_run.append(r_no)
                print(f"   -> ✅ 데이터 완벽 매핑 성공!")
            else:
                print(f"   -> ⏳ 금감원 전용 API 데이터 생성 지연 중 (Queue에 보관 후 다음 실행 때 재시도)")
                
        except Exception as e:
            print(f"   -> ❌ 통신 에러 발생: {e}")

    # 5. 시트 업데이트
    for name, rows in rows_to_add.items():
        if rows:
            worksheets[name].append_rows(rows, value_input_option="USER_ENTERED")
            print(f"\n📊 [시트 업데이트] {name}: {len(rows)}건 추가 완료")

    # 6. 상태 파일 갱신 (성공한 항목은 Queue에서 빼고 Seen으로 이동)
    for r_no in processed_this_run:
        del retry_queue[r_no]
        seen_list.append(r_no)

    save_json(SEEN_FILE, seen_list)
    save_json(QUEUE_FILE, retry_queue)

if __name__ == "__main__":
    main()
