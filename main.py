import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# 1. 환경 변수 설정
# 보안을 위해 실제 키는 GitHub Secrets에 넣고 여기는 비워두는 것을 추천합니다.
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 한국 시간(KST) 설정 및 당일 날짜 생성
KST = timezone(timedelta(hours=9))
today_dt = datetime.now(KST)
today_str = today_dt.strftime('%Y%m%d')

# DART API 기본 URL
DART_BASE_URL = "https://opendart.fss.or.kr/api"

# 키워드 리스트 (기존과 동일)
PIIC_KEYWORDS = ["유상증자1차발행가액결정", "유상증자결의", "유상증자결정", "주요사항보고서(유상증자결정)"] # 예시로 일부 축약, 실제는 전체 리스트 사용 권장
CVBD_KEYWORDS = ["전환사채권발행결정", "전환사채발행결정", "주요사항보고서(전환사채권발행결정)"]
EXBD_KEYWORDS = ["교환사채권발행결정", "교환사채발행결정", "주요사항보고서(교환사채권발행결정)"]

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
    """원 단위를 억 단위로 변환"""
    if not amount_str or amount_str in ['-', '']: return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except:
        return amount_str

def fetch_company_info(corp_code):
    """기업개황(company.json) API 호출"""
    url = f"{DART_BASE_URL}/company.json"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(url, params=params).json()
    return res if res.get('status') == '000' else {}

def fetch_detail_data(api_endpoint, rcept_no, corp_code):
    """주요사항 상세 API 호출"""
    url = f"{DART_BASE_URL}/{api_endpoint}"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(url, params=params).json()
    if res.get('status') == '000':
        for item in res['list']:
            if item.get('rcept_no') == rcept_no:
                return item
    return {}

def main():
    print(f"[{datetime.now(KST)}] DART 당일 공시 수집 시작 (날짜: {today_str})")
    sheets = get_google_sheets()
    
    # 중복 방지를 위한 기존 접수번호 로드
    existing_rcepts = {k: v.col_values(1) for k, v in sheets.items()}
    rows_to_add = {"유상증자": [], "전환사채": [], "교환사채": []}
    
    # 1. 공시검색(list.json) 호출
    list_url = f"{DART_BASE_URL}/list.json"
    list_params = {'crtfc_key': DART_API_KEY, 'bgn_de': today_str, 'end_de': today_str}
    list_data = requests.get(list_url, params=list_params).json()
    
    if list_data.get('status') != '000':
        print(f"🚨 DART API 응답: {list_data}")
        return

    for item in list_data['list']:
        rcept_no = item.get('rcept_no')
        corp_code = item.get('corp_code')
        report_nm = item.get('report_nm')
        
        # 필터링 및 시트별 처리
        target_sheet = None
        detail_api = None
        
        if any(kw in report_nm for kw in PIIC_KEYWORDS):
            target_sheet, detail_api = "유상증자", "piicDecsn.json"
        elif any(kw in report_nm for kw in CVBD_KEYWORDS):
            target_sheet, detail_api = "전환사채", "cvbdIsDecsn.json"
        elif any(kw in report_nm for kw in EXBD_KEYWORDS):
            target_sheet, detail_api = "교환사채", "exbdIsDecsn.json"
            
        if target_sheet and rcept_no not in existing_rcepts[target_sheet]:
            # 상세 데이터와 기업개황 데이터를 모두 가져옴
            detail = fetch_detail_data(detail_api, rcept_no, corp_code)
            company = fetch_company_info(corp_code)
            
            if not detail: continue
            
            # 공통 정보 (기업개황 활용)
            ceo_nm = company.get('ceo_nm', '') # 대표이사 이름 추가 활용 가능
            
            if target_sheet == "유상증자":
                row = [
                    rcept_no, item.get('corp_name'), item.get('corp_cls'), report_nm,
                    detail.get("ic_mthn", ""), detail.get("nstk_ostk_cnt", ""), detail.get("nstk_estk_cnt", ""),
                    detail.get("fv_ps", ""), detail.get("bfic_tisstk_ostk", ""), detail.get("bfic_tisstk_estk", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))
                ]
            else: # 사채 관련 (전환/교환)
                row = [
                    rcept_no, item.get('corp_name'), item.get('corp_cls'), report_nm, detail.get("bddd", ""),
                    detail.get("bd_tm", ""), detail.get("bd_knd", ""), detail.get("bdis_mthn", ""),
                    detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""), detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))
                ]
                # 사채 종류별 추가 필드
                if target_sheet == "전환사채":
                    row += [detail.get("cv_rt", ""), detail.get("cv_prc", ""), detail.get("act_mktprcfl_cvprc_lwtrsprc", ""),
                            detail.get("cvrqpd_bgd", ""), detail.get("cvrqpd_edd", ""), detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")]
                else: # 교환사채
                    row += [detail.get("ex_rt", ""), detail.get("ex_prc", ""), detail.get("exrqpd_bgd", ""), detail.get("exrqpd_edd", ""),
                            detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")]
            
            rows_to_add[target_sheet].append(row)

    # 데이터 업로드
    for name, rows in rows_to_add.items():
        if rows:
            sheets[name].append_rows(rows)
            print(f"✅ [{name}] {len(rows)}건 추가 완료")

if __name__ == "__main__":
    main()
