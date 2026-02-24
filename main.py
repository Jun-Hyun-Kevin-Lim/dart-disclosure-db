import os
import urllib
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# 1. 환경 변수 설정
DART_API_KEY = os.getenv("DART_API_KEY", "18d878b167bd1e9f2ec1f7534b543e79463a72ac").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1CiJSHmTdHGXD_665TcbEB6GEKJao0WJrzb3UGlsfMBo").strip()

week_ago_dt = today_dt - timedelta(days=7)
week_ago_str = week_ago_dt.strftime('%Y%m%d')
# 테스트용 특정 날짜: today_str = '20231025' 

# DART API 기본 URL
DART_BASE_URL = "https://opendart.fss.or.kr/api"

PIIC_KEYWORDS = [
    "유상증자1차발행가액결정", "유상증자결의", "유상증자결정", "유상증자결정(자율공시)(종속회사의주요경영사항)",
    "유상증자결정(종속회사의주요경영사항)", "유상증자또는주식관련사채등의발행결과", "유상증자또는주식관련사채등의청약결과",
    "유상증자신주발행가액", "유상증자실권주식의처리", "유상증자최종발행가액확정", "주요사항보고서(유상증자결정)",
    "투자회사의유상증자결의", "특수관계인의유상증자참여", "특수관계인이참여한유상증자"
]

CVBD_KEYWORDS = [
    "자기전환사채만기전취득결정", "자기전환사채매도결정", "전환사채(해외전환사채포함)발행후만기전사채취득",
    "전환사채권발행결정", "전환사채발행결의", "전환사채발행결정", "전환사채전환가액결정",
    "주요사항보고서(자기전환사채만기전취득결정)", "주요사항보고서(자기전환사채매도결정)",
    "주요사항보고서(전환사채권발행결정)", "주요사항보고서(전환사채매수선택권행사자지정)",
    "주요사항보고서(제3자의전환사채매수선택권행사)", "특수관계인에대한전환사채발행의결",
    "해외전환사채발행결정", "해외전환사채발행계약체결", "해외전환사채발행완료"
]

EXBD_KEYWORDS = [
    "교환사채(해외교환사채포함)발행후만기전사채취득", "교환사채교환가액결정", "교환사채권발행결정",
    "교환사채권발행결정(자율공시)(종속회사의주요경영사항)", "교환사채권발행결정(종속회사의주요경영사항)",
    "교환사채발행결의", "교환사채발행결정", "자기교환사채만기전취득결정", "자기교환사채매도결정",
    "주요사항보고서(교환사채권발행결정)", "특수관계인에대한교환사채발행의결",
    "해외교환사채발행결정", "해외교환사채발행계약체결", "해외교환사채발행완료"
]

def get_google_sheets():
    """구글 시트 연동 및 3개의 시트 객체 반환"""
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    
    # 시트 이름에 맞춰 매핑 (시트 이름이 다르면 여기서 수정하세요)
    return {
        "유상증자": doc.worksheet("유상증자"),
        "전환사채": doc.worksheet("전환사채"),
        "교환사채": doc.worksheet("교환사채")
    }

def won_to_uk(amount_str):
    """원 단위의 문자열을 억 단위로 변환 (소수점 1자리까지)"""
    if not amount_str or amount_str == '-': return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except:
        return amount_str

def get_market_type(corp_cls):
    """시장구분 코드 변환"""
    mapping = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    return mapping.get(corp_cls, corp_cls)

def fetch_detail_data(api_endpoint, rcept_no, corp_code):
    """특정 공시의 상세 정보를 가져오는 함수"""
    url = f"{DART_BASE_URL}/{api_endpoint}"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    response = requests.get(url, params=params).json()
    
    if response.get('status') == '000':
        for item in response['list']:
            if item.get('rcept_no') == rcept_no:
                return item
    return {}

def main():
    print(f"[{datetime.now(KST)}] DART 공시 수집 시작...")
    sheets = get_google_sheets()
    
    # 각 시트별 기존 접수번호(A열) 가져오기 (중복 방지)
    existing_rcepts = {
        "유상증자": sheets["유상증자"].col_values(1),
        "전환사채": sheets["전환사채"].col_values(1),
        "교환사채": sheets["교환사채"].col_values(1)
    }
    
    # 새로 추가할 데이터 리스트
    rows_to_add = {"유상증자": [], "전환사채": [], "교환사채": []}
    
    # 일주일치 공시 목록 조회
    list_url = f"{DART_BASE_URL}/list.json"
    # 시작일(bgn_de)을 week_ago_str로 변경 👇
    list_params = {'crtfc_key': DART_API_KEY, 'bgn_de': week_ago_str, 'end_de': today_str}
    list_data = requests.get(list_url, params=list_params).json()
    
    if list_data.get('status') != '000':
        print("조회된 공시가 없거나 오류가 발생했습니다.")
        print(f"DART API 상세 응답: {list_data}") # 이 줄을 추가합니다!
        return

    for item in list_data['list']:
        rcept_no = item.get('rcept_no')
        corp_code = item.get('corp_code')
        corp_name = item.get('corp_name')
        report_nm = item.get('report_nm')
        market = get_market_type(item.get('corp_cls'))
        
        # 1. 유상증자결정
        if "유상증자" in report_nm and rcept_no not in existing_rcepts["유상증자"]:
            detail = fetch_detail_data("piicDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no,                               # 접수번호
                    corp_name,                              # 회사명
                    corp_cls,                               # 법인구분
                    report_nm,                              # 보고서명
        
                    detail.get("ic_mthn", ""),              # 증자방식
                    detail.get("nstk_ostk_cnt", ""),        # 보통주발행수
                    detail.get("nstk_estk_cnt", ""),        # 기타주발행수
                    detail.get("fv_ps", ""),                # 1주당액면가(원)
        
                    detail.get("bfic_tisstk_ostk", ""),     # 증자전보통주(주)
                    detail.get("bfic_tisstk_estk", ""),     # 증자전기타주(주)
        
                    won_to_uk(detail.get("fdpp_fclt", "")),     # 시설자금(억)
                    won_to_uk(detail.get("fdpp_bsnhinh", "")),  # 영업양수(억)
                    won_to_uk(detail.get("fdpp_op", "")),       # 운영자금(억)
                    won_to_uk(detail.get("fdpp_dtrp", "")),     # 채무상환(억)
                    won_to_uk(detail.get("fdpp_ocsa", "")),     # 타법인취득(억)
                    won_to_uk(detail.get("fdpp_etc", "")),      # 기타자금(억)
                ]
                rows_to_add["유상증자"].append(row)

        # 2. 전환사채권발행결정
        elif "전환사채" in report_nm and rcept_no not in existing_rcepts["전환사채"]:
            detail = fetch_detail_data("cvbdIsDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no,                                  # 접수번호
                    corp_name,                                 # 회사명
                    corp_cls,                                  # 법인구분
                    report_nm,                                 # 보고서명
        
                    detail.get("bddd", ""),                     # 이사회결의일(결정일)
        
                    detail.get("bd_tm", ""),                    # 회차
                    detail.get("bd_knd", ""),                   # 사채종류
                    detail.get("bdis_mthn", ""),                # 발행방법
        
                    detail.get("bd_fta", ""),                   # 권면총액(원)
                    detail.get("bd_intr_ex", ""),               # 표면이자율(%)
                    detail.get("bd_intr_sf", ""),               # 만기이자율(%)
                    detail.get("bd_mtd", ""),                   # 사채만기일
        
                    won_to_uk(detail.get("fdpp_fclt", "")),     # 시설자금(억)
                    won_to_uk(detail.get("fdpp_bsnhinh", "")),  # 영업양수(억)
                    won_to_uk(detail.get("fdpp_op", "")),       # 운영자금(억)
                    won_to_uk(detail.get("fdpp_dtrp", "")),     # 채무상환(억)
                    won_to_uk(detail.get("fdpp_ocsa", "")),     # 타법인취득(억)
                    won_to_uk(detail.get("fdpp_etc", "")),      # 기타자금(억)
        
                    detail.get("cv_rt", ""),                    # 전환비율(%)
                    detail.get("cv_prc", ""),                   # 전환가액(원)
                    detail.get("act_mktprcfl_cvprc_lwtrsprc", ""),  # 최저조정가액(원)
        
                    detail.get("cvrqpd_bgd", ""),               # 전환청구시작일
                    detail.get("cvrqpd_edd", ""),               # 전환청구종료일
        
                    detail.get("sbd", ""),                      # 청약일
                    detail.get("pymd", ""),                     # 납입일
        
                    detail.get("rpmcmp", ""),                   # 대표주관사/투자자
                ]
                rows_to_add["전환사채"].append(row)

        # 3. 교환사채권발행결정
        elif "교환사채" in report_nm and rcept_no not in existing_rcepts["교환사채"]:
            detail = fetch_detail_data("exbdIsDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no,                                  # 접수번호
                    corp_name,                                 # 회사명
                    corp_cls,                                  # 법인구분
                    report_nm,                                 # 보고서명
        
                    detail.get("bddd", ""),                     # 이사회결의일(결정일)
                    detail.get("bd_tm", ""),                    # 회차
                    detail.get("bd_knd", ""),                   # 사채종류
                    detail.get("bdis_mthn", ""),                # 발행방법
        
                    detail.get("bd_fta", ""),                   # 권면총액(원)
                    detail.get("bd_intr_ex", ""),               # 표면이자율(%)
                    detail.get("bd_intr_sf", ""),               # 만기이자율(%)
                    detail.get("bd_mtd", ""),                   # 사채만기일
        
                    won_to_uk(detail.get("fdpp_fclt", "")),     # 시설자금(억)
                    won_to_uk(detail.get("fdpp_bsnhinh", "")),  # 영업양수(억)
                    won_to_uk(detail.get("fdpp_op", "")),       # 운영자금(억)
                    won_to_uk(detail.get("fdpp_dtrp", "")),     # 채무상환(억)
                    won_to_uk(detail.get("fdpp_ocsa", "")),     # 타법인취득(억)
                    won_to_uk(detail.get("fdpp_etc", "")),      # 기타자금(억)
        
                    detail.get("ex_rt", ""),                    # 교환비율(%)
                    detail.get("ex_prc", ""),                   # 교환가액(원)
        
                    detail.get("exrqpd_bgd", ""),               # 교환청구시작일
                    detail.get("exrqpd_edd", ""),               # 교환청구종료일
        
                    detail.get("sbd", ""),                      # 청약일
                    detail.get("pymd", ""),                     # 납입일
        
                    detail.get("rpmcmp", ""),                   # 대표주관사/투자자 
                ]
                rows_to_add["교환사채"].append(row)

    # 각 시트별로 모인 데이터를 한 번에 업데이트 (API 호출 최소화)
    for sheet_name, rows in rows_to_add.items():
        if rows:
            sheets[sheet_name].append_rows(rows)
            print(f"[{sheet_name}] {len(rows)}건 업데이트 완료!")
        else:
            print(f"[{sheet_name}] 새로 추가할 건이 없습니다.")

if __name__ == "__main__":
    main()
