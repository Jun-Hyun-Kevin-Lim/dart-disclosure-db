import os
import urllib
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import time

# 1. 환경 변수 설정
DART_API_KEY = os.getenv("DART_API_KEY", "18d878b167bd1e9f2ec1f7534b543e79463a72ac").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1CiJSHmTdHGXD_665TcbEB6GEKJao0WJrzb3UGlsfMBo").strip()

# 날짜 설정
KST = timezone(timedelta(hours=9))
today_dt = datetime.now(KST)
today_str = today_dt.strftime('%Y%m%d')

# [수정 포인트] 시작일과 종료일을 변수로 명확히 정의
start_date_str = "20260220"  # 2월 20일부터
end_date_str = today_str     # 오늘까지 (24일 포함)

DART_BASE_URL = "https://opendart.fss.or.kr/api"

def get_google_sheets():
    """구글 시트 연동"""
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    
    return {
        "유상증자": doc.worksheet("유상증자"),
        "전환사채": doc.worksheet("전환사채"),
        "교환사채": doc.worksheet("교환사채")
    }

def won_to_uk(amount_str):
    if not amount_str or amount_str == '-': return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except:
        return amount_str

def fetch_detail_data(api_endpoint, rcept_no, corp_code):
    url = f"{DART_BASE_URL}/{api_endpoint}"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    try:
        response = requests.get(url, params=params).json()
        if response.get('status') == '000':
            for item in response.get('list', []):
                if item.get('rcept_no') == rcept_no:
                    return item
    except:
        pass
    return {}

def main():
    print(f"[{datetime.now(KST)}] DART 공시 수집 시작 ({start_date_str} ~ {end_date_str})...")
    sheets = get_google_sheets()
    
    # 중복 체크용 기존 데이터 가져오기
    existing_rcepts = {
        "유상증자": sheets["유상증자"].col_values(1),
        "전환사채": sheets["전환사채"].col_values(1),
        "교환사채": sheets["교환사채"].col_values(1)
    }
    
    rows_to_add = {"유상증자": [], "전환사채": [], "교환사채": []}
    
    # [핵심 수정] 페이지네이션 추가: 여러 페이지를 돌며 모든 공시 확인
    page_no = 1
    while True:
        list_url = f"{DART_BASE_URL}/list.json"
        list_params = {
            'crtfc_key': DART_API_KEY, 
            'bgn_de': start_date_str, 
            'end_de': end_date_str, 
            'page_no': page_no, 
            'page_count': 100  # 한 번에 100건씩
        }
        
        list_data = requests.get(list_url, params=list_params).json()
        
        if list_data.get('status') != '000':
            break

        for item in list_data.get('list', []):
            rcept_no = item.get('rcept_no')
            corp_code = item.get('corp_code')
            corp_name = item.get('corp_name')
            report_nm = item.get('report_nm')
            corp_cls = item.get('corp_cls')
            
            # 1. 유상증자
            if "유상증자결정" in report_nm and rcept_no not in existing_rcepts["유상증자"]:
                detail = fetch_detail_data("piicDecsn.json", rcept_no, corp_code)
                if detail:
                    rows_to_add["유상증자"].append([
                        rcept_no, corp_name, corp_cls, report_nm,
                        detail.get("ic_mthn", ""), detail.get("nstk_ostk_cnt", ""),
                        detail.get("nstk_estk_cnt", ""), detail.get("fv_ps", ""),
                        detail.get("bfic_tisstk_ostk", ""), detail.get("bfic_tisstk_estk", ""),
                        won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                        won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                        won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))
                    ])

            # 2. 전환사채
            elif "전환사채권발행결정" in report_nm and rcept_no not in existing_rcepts["전환사채"]:
                detail = fetch_detail_data("cvbdIsDecsn.json", rcept_no, corp_code)
                if detail:
                    rows_to_add["전환사채"].append([
                        rcept_no, corp_name, corp_cls, report_nm,
                        detail.get("bddd", ""), detail.get("bd_tm", ""), detail.get("bd_knd", ""),
                        detail.get("bdis_mthn", ""), detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""),
                        detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                        won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                        won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                        won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                        detail.get("cv_rt", ""), detail.get("cv_prc", ""), detail.get("act_mktprcfl_cvprc_lwtrsprc", ""),
                        detail.get("cvrqpd_bgd", ""), detail.get("cvrqpd_edd", ""),
                        detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")
                    ])

            # 3. 교환사채
            elif "교환사채권발행결정" in report_nm and rcept_no not in existing_rcepts["교환사채"]:
                detail = fetch_detail_data("exbdIsDecsn.json", rcept_no, corp_code)
                if detail:
                    rows_to_add["교환사채"].append([
                        rcept_no, corp_name, corp_cls, report_nm,
                        detail.get("bddd", ""), detail.get("bd_tm", ""), detail.get("bd_knd", ""),
                        detail.get("bdis_mthn", ""), detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""),
                        detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                        won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                        won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                        won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                        detail.get("ex_rt", ""), detail.get("ex_prc", ""),
                        detail.get("exrqpd_bgd", ""), detail.get("exrqpd_edd", ""),
                        detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")
                    ])

        # 전체 페이지 수에 도달하면 종료
        if page_no >= list_data.get('total_page', 1):
            break
        page_no += 1
        time.sleep(0.5) # DART API 제한 준수

    # 시트 업데이트
    for sheet_name, rows in rows_to_add.items():
        if rows:
            sheets[sheet_name].append_rows(rows)
            print(f"[{sheet_name}] {len(rows)}건 업데이트 완료!")
        else:
            print(f"[{sheet_name}] 새로 추가할 건이 없습니다.")

if __name__ == "__main__":
    main()
