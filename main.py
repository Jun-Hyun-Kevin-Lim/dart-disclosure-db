import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import time

# 1. 환경 변수 설정
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 날짜 설정 (한국 시간 KST 기준 당일)
KST = timezone(timedelta(hours=9))
today_dt = datetime.now(KST)
today_str = today_dt.strftime('%Y%m%d')

DART_BASE_URL = "https://opendart.fss.or.kr/api"

# 꼼꼼한 키워드 필터 (제공해주신 모든 보고서명 대응)
PIIC_KEYWORDS = ["유상증자", "주요사항보고서(유상증자결정)"]
CVBD_KEYWORDS = ["전환사채", "주요사항보고서(전환사채권발행결정)"]
EXBD_KEYWORDS = ["교환사채", "주요사항보고서(교환사채권발행결정)"]

def get_google_sheets():
    """구글 시트 연동 및 권한 확인"""
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    print(f"📡 시트 연결 성공: {doc.title}")
    return {
        "유상증자": doc.worksheet("유상증자"),
        "전환사채": doc.worksheet("전환사채"),
        "교환사채": doc.worksheet("교환사채")
    }

def fetch_api(url, params):
    """DART API 호출 공통 함수"""
    try:
        res = requests.get(url, params=params, timeout=10).json()
        return res if res.get('status') == '000' else {}
    except: return {}

def won_to_uk(amount_str):
    """금액 단위를 '억' 원으로 변환"""
    if not amount_str or amount_str in ['-', '']: return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except: return amount_str

def main():
    print(f"[{datetime.now(KST)}] 🔍 {today_str} 공시 전수 조사 시작...")
    sheets = get_google_sheets()
    
    # 공시 목록(list.json) 가져오기
    list_data = fetch_api(f"{DART_BASE_URL}/list.json", {'crtfc_key': DART_API_KEY, 'bgn_de': today_str, 'end_de': today_str})
    if not list_data:
        print("📭 오늘 업데이트된 공시가 없습니다.")
        return

    all_disclosures = list_data.get('list', [])
    print(f"📋 오늘 전체 공시 건수: {len(all_disclosures)}건")

    for item in all_disclosures:
        rcept_no = item['rcept_no']
        corp_code = item['corp_code']
        report_nm = item['report_nm']
        corp_name = item['corp_name']

        # 시트 및 상세 API 매핑
        target_name, api_path = None, None
        if any(kw in report_nm for kw in PIIC_KEYWORDS): target_name, api_path = "유상증자", "piicDecsn.json"
        elif any(kw in report_nm for kw in CVBD_KEYWORDS): target_name, api_path = "전환사채", "cvbdIsDecsn.json"
        elif any(kw in report_nm for kw in EXBD_KEYWORDS): target_name, api_path = "교환사채", "exbdIsDecsn.json"

        if target_name:
            sheet = sheets[target_name]
            # 중복 체크 (A열 전체 확인)
            if rcept_no in sheet.col_values(1):
                print(f"⏩ 중복 스킵: {corp_name} - {rcept_no}")
                continue

            # 1. 상세 정보 가져오기
            detail_res = fetch_api(f"{DART_BASE_URL}/{api_path}", {'crtfc_key': DART_API_KEY, 'corp_code': corp_code})
            detail = {}
            if detail_res and 'list' in detail_res:
                detail = next((d for d in detail_res['list'] if d['rcept_no'] == rcept_no), {})
            
            # 2. 기업 개황(company.json) 가져오기
            company = fetch_api(f"{DART_BASE_URL}/company.json", {'crtfc_key': DART_API_KEY, 'corp_code': corp_code})

            if not detail:
                print(f"⚠️ 상세 데이터 없음: {corp_name}")
                continue

            # 3. 데이터 가공 및 행 구성
            dart_link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
            
            if target_name == "유상증자":
                row = [
                    rcept_no, corp_name, item.get('corp_cls'), report_nm,
                    detail.get("ic_mthn", ""), detail.get("nstk_ostk_cnt", ""), detail.get("nstk_estk_cnt", ""),
                    detail.get("fv_ps", ""), detail.get("nstk_isu_prc", ""),
                    detail.get("bfic_tisstk_ostk", ""), detail.get("bfic_tisstk_estk", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                    detail.get("sbd", ""), detail.get("pymd", ""), detail.get("tp_allo_cprpty", ""),
                    company.get("ceo_nm", ""), dart_link
                ]
            else: # 사채류
                row = [
                    rcept_no, corp_name, item.get('corp_cls'), report_nm, detail.get("bddd", ""),
                    detail.get("bd_tm", ""), detail.get("bd_knd", ""), detail.get("bdis_mthn", ""),
                    detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""), detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))
                ]
                if target_name == "전환사채":
                    row += [detail.get("cv_rt", ""), detail.get("cv_prc", ""), detail.get("act_mktprcfl_cvprc_lwtrsprc", ""),
                            detail.get("cvrqpd_bgd", ""), detail.get("cvrqpd_edd", ""), detail.get("sbd", ""),
                            detail.get("pymd", ""), detail.get("rpmcmp", ""), dart_link]
                else: # 교환사채
                    row += [detail.get("ex_rt", ""), detail.get("ex_prc", ""), detail.get("exrqpd_bgd", ""),
                            detail.get("exrqpd_edd", ""), detail.get("sbd", ""), detail.get("pymd", ""),
                            detail.get("rpmcmp", ""), dart_link]

            # 4. 시트 2행(제목 바로 아래)에 삽입
            sheet.insert_row(row, 2)
            print(f"✅ 저장 완료: {corp_name} ({target_name})")
            time.sleep(0.5)

if __name__ == "__main__":
    main()
