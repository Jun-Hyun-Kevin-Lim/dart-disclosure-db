import os
import re
import io
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

# --- [1] 환경 설정 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 당일 공시만 가져오기 (테스트 시 1~3으로 변경하면 과거 공시를 가져옵니다)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")
SEEN_FILE = "seen.json"

# API 엔드포인트 (대표님이 지정한 목록)
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
API_URLS = {
    "유상증자": "https://opendart.fss.or.kr/api/piicDecsn.json",
    "전환사채": "https://opendart.fss.or.kr/api/cvbdIsDecsn.json",
    "교환사채": "https://opendart.fss.or.kr/api/exbdIsDecsn.json"
}

# --- [2] 시트별 헤더 정의 (제시해주신 필드 목록 100% 반영) ---
HEADERS = {
    "유상증자": [
        "접수번호", "법인구분", "고유번호", "회사명", "신주(보통주)", "신주(기타주)", "액면가", 
        "증자전(보통주)", "증자전(기타주)", "시설자금", "영업양수", "운영자금", "채무상환", "타법인취득", "기타자금", 
        "증자방식", "공매도해당", "공매도시작", "공매도종료"
    ],
    "전환사채": [
        "접수번호", "법인구분", "고유번호", "회사명", "회차", "사채종류", "권면총액", "잔여발행한도", 
        "해외권면", "통화", "기준환율", "발행지역", "해외시장명", "시설자금", "영업양수", "운영자금", 
        "채무상환", "타법인취득", "기타자금", "표면이율", "만기이율", "사채만기일", "발행방법", 
        "전환비율", "전환가액", "주식종류", "주식수", "주식총수대비비율", "청구시작", "청구종료", 
        "최저조정가액", "조정근거", "70%미만조정가능한도", "합병관련", "청약일", "납입일", "대표주관사", 
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

# --- [3] 데이터 상태 관리 ---
def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r") as f: return set(json.load(f))
        except: return set()
    return set()

def save_seen(seen_set):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen_set), f)

def get_sheet_seen(ws):
    """구글 시트의 첫 번째 열(접수번호)을 읽어 중복을 체크합니다."""
    try:
        col = ws.col_values(1)
        return set(x.strip() for x in col[1:] if x.strip())
    except: return set()

# --- [4] 핵심 매핑 함수 (DART 응답키 1:1 매칭) ---
def map_fields(r_type, item):
    if r_type == "유상증자":
        return [
            item.get("rcept_no"), item.get("corp_cls"), item.get("corp_code"), item.get("corp_name"),
            item.get("nstk_ostk_cnt"), item.get("nstk_estk_cnt"), item.get("fv_ps"),
            item.get("bfic_tisstk_ostk"), item.get("bfic_tisstk_estk"), item.get("fdpp_fclt"),
            item.get("fdpp_bsninh"), item.get("fdpp_op"), item.get("fdpp_dtrp"),
            item.get("fdpp_ocsa"), item.get("fdpp_etc"), item.get("ic_mthn"),
            item.get("ssl_at"), item.get("ssl_bgd"), item.get("ssl_edd")
        ]
    elif r_type == "전환사채":
        return [
            item.get("rcept_no"), item.get("corp_cls"), item.get("corp_code"), item.get("corp_name"),
            item.get("bd_tm"), item.get("bd_knd"), item.get("bd_fta"), item.get("atcsc_rmislmt"),
            item.get("ovis_fta"), item.get("ovis_fta_crn"), item.get("ovis_ster"), item.get("ovis_isar"),
            item.get("ovis_mktnm"), item.get("fdpp_fclt"), item.get("fdpp_bsninh"), item.get("fdpp_op"),
            item.get("fdpp_dtrp"), item.get("fdpp_ocsa"), item.get("fdpp_etc"), item.get("bd_intr_ex"),
            item.get("bd_intr_sf"), item.get("bd_mtd"), item.get("bdis_mthn"), item.get("cv_rt"),
            item.get("cv_prc"), item.get("cvisstk_knd"), item.get("cvisstk_cnt"), item.get("cvisstk_tisstk_vs"),
            item.get("cvrqpd_bgd"), item.get("cvrqpd_edd"), item.get("act_mktprcfl_cvprc_lwtrsprc"),
            item.get("act_mktprcfl_cvprc_lwtrsprc_bs"), item.get("rmislmt_lt70p"), item.get("abmg"),
            item.get("sbd"), item.get("pymd"), item.get("rpmcmp"), item.get("grint"), item.get("bddd"),
            item.get("od_a_at_t"), item.get("od_a_at_b"), item.get("adt_a_atn"), item.get("rs_sm_atn"),
            item.get("ex_sm_r"), item.get("ovis_ltdtl"), item.get("ftc_stt_atn")
        ]
    elif r_type == "교환사채":
        return [
            item.get("rcept_no"), item.get("corp_cls"), item.get("corp_code"), item.get("corp_name"),
            item.get("bd_tm"), item.get("bd_knd"), item.get("bd_fta"), item.get("ovis_fta"),
            item.get("ovis_fta_crn"), item.get("ovis_ster"), item.get("ovis_isar"), item.get("ovis_mktnm"),
            item.get("fdpp_fclt"), item.get("fdpp_bsninh"), item.get("fdpp_op"), item.get("fdpp_dtrp"),
            item.get("fdpp_ocsa"), item.get("fdpp_etc"), item.get("bd_intr_ex"), item.get("bd_intr_sf"),
            item.get("bd_mtd"), item.get("bdis_mthn"), item.get("ex_rt"), item.get("ex_prc"),
            item.get("ex_prc_dmth"), item.get("extg"), item.get("extg_stkcnt"), item.get("extg_tisstk_vs"),
            item.get("exrqpd_bgd"), item.get("exrqpd_edd"), item.get("sbd"), item.get("pymd"),
            item.get("rpmcmp"), item.get("grint"), item.get("bddd"), item.get("od_a_at_t"),
            item.get("od_a_at_b"), item.get("adt_a_atn"), item.get("rs_sm_atn"), item.get("ex_sm_r"),
            item.get("ovis_ltdtl"), item.get("ftc_stt_atn")
        ]

# --- [5] 메인 실행 로직 ---
def main():
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn_de = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    # 1. 공시 목록 검색
    list_res = requests.get(LIST_URL, params={"crtfc_key": DART_API_KEY, "bgn_de": bgn_de, "end_de": end_de, "page_count": "100"}).json()
    items = list_res.get("list", [])
    print(f"📋 DART 목록 확인: 오늘 총 {len(items)}건의 공시가 검색되었습니다.")

    seen = load_seen()
    
    for name in ["유상증자", "전환사채", "교환사채"]:
        ws = get_or_create_ws = None
        try:
            ws = sh.worksheet(name)
        except WorksheetNotFound:
            ws = sh.add_worksheet(title=name, rows="1000", cols="60")
        
        if not ws.row_values(1):
            ws.append_row(HEADERS[name], value_input_option="USER_ENTERED")
            
        sheet_seen = get_sheet_seen(ws)
        
        # 이름별 타겟 필터링 (결정 공시만)
        targets = [it for it in items if name in it.get("report_nm", "")]
        
        rows_to_add = []
        for t in targets:
            r_no = str(t.get("rcept_no")).strip()
            
            # ✨ 지우면 다시 가져오는 핵심 로직: 시트와 seen.json 둘 다 없을 때만 수집
            if r_no not in sheet_seen and r_no not in seen:
                print(f"🔎 신규 공시 분석: [{t.get('corp_name')}] {t.get('report_nm')}")
                
                # 상세 API 호출
                api_res = requests.get(API_URLS[name], params={"crtfc_key": DART_API_KEY, "corp_code": t.get("corp_code")}).json()
                detail = next((d for d in api_res.get("list", []) if str(d.get("rcept_no")).strip() == r_no), None)
                
                if detail:
                    rows_to_add.append(map_fields(name, detail))
                    seen.add(r_no)
                    print(f"   -> ✅ 대기열 추가 완료")
                else:
                    print(f"   -> ⏳ 상세 수치 생성 대기 중 (다음 주기 재시도)")

        if rows_to_add:
            ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
            print(f"📊 {name} 시트: {len(rows_to_add)}건 업데이트 성공!")

    save_seen(seen)

if __name__ == "__main__":
    main()
