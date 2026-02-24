import os
import re
import io
import json
import requests
import zipfile
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
SEEN_FILE = "seen.json"

# --- [2] API 엔드포인트 설정 (요청하신 목록 반영) ---
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
CORP_URL = "https://opendart.fss.or.kr/api/company.json"

# 결정 공시 API
REALTIME_APIS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# 기업 분석용 보조 API (공시 발생 시 추가 조회용)
ANALYSIS_APIS = {
    "최대주주": "https://opendart.fss.or.kr/api/hyslrSttus.json",
    "배당": "https://opendart.fss.or.kr/api/alotMatter.json",
    "임원": "https://opendart.fss.or.kr/api/exctvSttus.json",
    "타법인출자": "https://opendart.fss.or.kr/api/otrCprInvstmntSttus.json"
}

# --- [3] 시트별 헤더 정의 (제공해주신 필드 100% 반영) ---
HEADERS = {
    "유상증자": [
        "접수번호", "법인구분", "고유번호", "회사명", "신주(보통)", "신주(기타)", "액면가", 
        "증자전(보통)", "증자전(기타)", "시설자금", "영업양수", "운영자금", "채무상환", "타법인취득", "기타자금", 
        "증자방식", "공매도해당", "공매도시작", "공매도종료", "최대주주지분율", "최근배당수익률"
    ],
    "전환사채": [
        "접수번호", "법인구분", "고유번호", "회사명", "회차", "사채종류", "권면총액", "잔여발행한도", 
        "해외권면", "통화", "기준환율", "발행지역", "해외시장명", "시설자금", "영업양수", "운영자금", 
        "채무상환", "타법인취득", "기타자금", "표면이율", "만기이율", "사채만기일", "발행방법", 
        "전환비율", "전환가액", "주식종류", "주식수", "주식총수대비비율", "청구시작", "청구종료", 
        "최저조정가액", "조정근거", "70%미만가능한도", "합병관련", "청약일", "납입일", "대표주관사", 
        "보증기관", "이사회결의일", "사외참석(참)", "사외참석(불)", "감사참석", "신고서제출대상", 
        "면제사유", "대차거래내역", "공정위신고"
    ],
    "교환사채": [
        "접수번호", "법인구분", "고유번호", "회사명", "회차", "사채종류", "권면총액", "해외권면", 
        "통화", "기준환율", "발행지역", "해외시장명", "시설자금", "영업양수", "운영자금", "채무상환", 
        "타법인취득", "기타자금", "표면이율", "만기이율", "사채만기일", "발행방법", "교환비율", 
        "교환가액", "가액결정방법", "교환대상종류", "교환대상주식수", "주식총수대비비율", "청구시작", 
        "청구종료", "청약일", "납입일", "대표주관사", "보증기관", "이사회결의일", "사외참석(참)", 
        "사외참석(불)", "감사참석", "신고서제출대상", "면제사유", "대차거래내역", "공정위신고"
    ]
}

# --- [4] 유틸리티 함수 ---
def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r") as f: return set(json.load(f))
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen_set), f)

def get_or_create_ws(sh, name):
    try:
        ws = sh.worksheet(name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="1000", cols="60")
    if not ws.row_values(1):
        ws.append_row(HEADERS[name], value_input_option="USER_ENTERED")
    return ws

# --- [5] 분석 데이터 추출 (추가하신 API 활용) ---
def get_analysis_info(corp_code):
    """최근 사업보고서 기준 최대주주 지분율 등을 가져옵니다."""
    year = str(datetime.now().year - 1)
    res = requests.get(ANALYSIS_APIS["최대주주"], params={
        "crtfc_key": DART_API_KEY, "corp_code": corp_code, "bsns_year": year, "reprt_code": "11011"
    })
    data = res.json().get("list", [])
    share = next((i.get("thstrm_share_rt", "0") for i in data if "계" in i.get("nm", "")), "0")
    return share

# --- [6] 메인 실행 로직 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    
    # 목록 검색
    list_res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "page_count": "100"}).json()
    items = list_res.get("list", [])
    
    seen = load_seen()
    
    for name in ["유상증자", "전환사채", "교환사채"]:
        ws = get_or_create_ws(sh, name)
        sheet_rcepts = set(ws.col_values(1)[1:])
        
        keyword = "유상증자" if name == "유상증자" else name
        targets = [it for it in items if keyword in it.get("report_nm", "")]
        
        rows = []
        for t in targets:
            r_no = t.get("rcept_no")
            # 중복 체크 (시트에서 지우거나 seen.json에서 지우면 재수집)
            if r_no not in sheet_rcepts and r_no not in seen:
                print(f"🔍 분석 중: [{t.get('corp_name')}] {t.get('report_nm')}")
                
                # 상세 결정 정보 호출
                detail_res = requests.get(REALTIME_APIS[name], params={"crtfc_key": DART_API_KEY, "corp_code": t.get("corp_code")}).json()
                detail = next((d for d in detail_res.get("list", []) if d.get("rcept_no") == r_no), None)
                
                if detail:
                    # 추가 분석 정보 (최대주주 지분율 등)
                    share_rt = get_analysis_info(t.get("corp_code"))
                    
                    if name == "유상증자":
                        row = [
                            detail.get("rcept_no"), detail.get("corp_cls"), detail.get("corp_code"), detail.get("corp_name"),
                            detail.get("nstk_ostk_cnt"), detail.get("nstk_estk_cnt"), detail.get("fv_ps"),
                            detail.get("bfic_tisstk_ostk"), detail.get("bfic_tisstk_estk"), detail.get("fdpp_fclt"),
                            detail.get("fdpp_bsninh"), detail.get("fdpp_op"), detail.get("fdpp_dtrp"),
                            detail.get("fdpp_ocsa"), detail.get("fdpp_etc"), detail.get("ic_mthn"),
                            detail.get("ssl_at"), detail.get("ssl_bgd"), detail.get("ssl_edd"), share_rt
                        ]
                    elif name == "전환사채":
                        row = [
                            detail.get("rcept_no"), detail.get("corp_cls"), detail.get("corp_code"), detail.get("corp_name"),
                            detail.get("bd_tm"), detail.get("bd_knd"), detail.get("bd_fta"), detail.get("atcsc_rmislmt"),
                            detail.get("ovis_fta"), detail.get("ovis_fta_crn"), detail.get("ovis_ster"), detail.get("ovis_isar"),
                            detail.get("ovis_mktnm"), detail.get("fdpp_fclt"), detail.get("fdpp_bsninh"), detail.get("fdpp_op"),
                            detail.get("fdpp_dtrp"), detail.get("fdpp_ocsa"), detail.get("fdpp_etc"), detail.get("bd_intr_ex"),
                            detail.get("bd_intr_sf"), detail.get("bd_mtd"), detail.get("bdis_mthn"), detail.get("cv_rt"),
                            detail.get("cv_prc"), detail.get("cvisstk_knd"), detail.get("cvisstk_cnt"), detail.get("cvisstk_tisstk_vs"),
                            detail.get("cvrqpd_bgd"), detail.get("cvrqpd_edd"), detail.get("act_mktprcfl_cvprc_lwtrsprc"),
                            detail.get("act_mktprcfl_cvprc_lwtrsprc_bs"), detail.get("rmislmt_lt70p"), detail.get("abmg"),
                            detail.get("sbd"), detail.get("pymd"), detail.get("rpmcmp"), detail.get("grint"), detail.get("bddd"),
                            detail.get("od_a_at_t"), detail.get("od_a_at_b"), detail.get("adt_a_atn"), detail.get("rs_sm_atn"),
                            detail.get("ex_sm_r"), detail.get("ovis_ltdtl"), detail.get("ftc_stt_atn")
                        ]
                    elif name == "교환사채":
                        row = [
                            detail.get("rcept_no"), detail.get("corp_cls"), detail.get("corp_code"), detail.get("corp_name"),
                            detail.get("bd_tm"), detail.get("bd_knd"), detail.get("bd_fta"), detail.get("ovis_fta"),
                            detail.get("ovis_fta_crn"), detail.get("ovis_ster"), detail.get("ovis_isar"), detail.get("ovis_mktnm"),
                            detail.get("fdpp_fclt"), detail.get("fdpp_bsninh"), detail.get("fdpp_op"), detail.get("fdpp_dtrp"),
                            detail.get("fdpp_ocsa"), detail.get("fdpp_etc"), detail.get("bd_intr_ex"), detail.get("bd_intr_sf"),
                            detail.get("bd_mtd"), detail.get("bdis_mthn"), detail.get("ex_rt"), detail.get("ex_prc"),
                            detail.get("ex_prc_dmth"), detail.get("extg"), detail.get("extg_stkcnt"), detail.get("extg_tisstk_vs"),
                            detail.get("exrqpd_bgd"), detail.get("exrqpd_edd"), detail.get("sbd"), detail.get("pymd"),
                            detail.get("rpmcmp"), detail.get("grint"), detail.get("bddd"), detail.get("od_a_at_t"),
                            detail.get("od_a_at_b"), detail.get("adt_a_atn"), detail.get("rs_sm_atn"), detail.get("ex_sm_r"),
                            detail.get("ovis_ltdtl"), detail.get("ftc_stt_atn")
                        ]
                    rows.append(row)
                    seen.add(r_no)
        
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            print(f"✅ {name} {len(rows)}건 추가 완료")

    save_seen(seen)

if __name__ == "__main__":
    main()
