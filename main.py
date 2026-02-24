import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# 1. 환경 변수 설정 (GitHub Secrets에 등록된 값을 가져옵니다)
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 날짜 설정 (한국 시간 KST 기준 당일)
KST = timezone(timedelta(hours=9))
today_dt = datetime.now(KST)
today_str = today_dt.strftime('%Y%m%d')

# DART API 기본 URL
DART_BASE_URL = "https://opendart.fss.or.kr/api"

# 보고서명 필터링 키워드 리스트
PIIC_KEYWORDS = ["유상증자1차발행가액결정", "유상증자결의", "유상증자결정", "유상증자결정(자율공시)(종속회사의주요경영사항)", "유상증자결정(종속회사의주요경영사항)", "유상증자또는주식관련사채등의발행결과", "유상증자또는주식관련사채등의청약결과", "유상증자신주발행가액", "유상증자실권주식의처리", "유상증자최종발행가액확정", "주요사항보고서(유상증자결정)", "투자회사의유상증자결의", "특수관계인의유상증자참여", "특수관계인이참여한유상증자"]
CVBD_KEYWORDS = ["자기전환사채만기전취득결정", "자기전환사채매도결정", "전환사채(해외전환사채포함)발행후만기전사채취득", "전환사채권발행결정", "전환사채발행결의", "전환사채발행결정", "전환사채전환가액결정", "주요사항보고서(자기전환사채만기전취득결정)", "주요사항보고서(자기전환사채매도결정)", "주요사항보고서(전환사채권발행결정)", "주요사항보고서(전환사채매수선택권행사자지정)", "주요사항보고서(제3자의전환사채매수선택권행사)", "특수관계인에대한전환사채발행의결", "해외전환사채발행결정", "해외전환사채발행계약체결", "해외전환사채발행완료"]
EXBD_KEYWORDS = ["교환사채(해외교환사채포함)발행후만기전사채취득", "교환사채교환가액결정", "교환사채권발행결정", "교환사채권발행결정(자율공시)(종속회사의주요경영사항)", "교환사채권발행결정(종속회사의주요경영사항)", "교환사채발행결의", "교환사채발행결정", "자기교환사채만기전취득결정", "자기교환사채매도결정", "주요사항보고서(교환사채권발행결정)", "특수관계인에대한교환사채발행의결", "해외교환사채발행결정", "해외교환사채발행계약체결", "해외교환사채발행완료"]

def get_google_sheets():
    """구글 시트 인증 및 시트 객체 반환"""
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    return {
        "유상증자": doc.worksheet("유상증자결정"),
        "전환사채": doc.worksheet("전환사채"),
        "교환사채": doc.worksheet("교환사채")
    }

def won_to_uk(amount_str):
    """원 단위 문자열을 '억' 단위로 변환"""
    if not amount_str or amount_str in ['-', '']: return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except: return amount_str

def get_market_type(corp_cls):
    """DART 시장구분 코드(Y/K/N/E)를 한글로 변환"""
    mapping = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    return mapping.get(corp_cls, corp_cls)

def fetch_company_info(corp_code):
    """기업개황(company.json) API 호출"""
    url = f"{DART_BASE_URL}/company.json"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(url, params=params).json()
    return res if res.get('status') == '000' else {}

def fetch_detail_data(api_endpoint, rcept_no, corp_code):
    """주요사항보고서 상세 정보를 가져오는 함수"""
    url = f"{DART_BASE_URL}/{api_endpoint}"
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(url, params=params).json()
    if res.get('status') == '000' and 'list' in res:
        for item in res['list']:
            if item.get('rcept_no') == rcept_no: return item
    return {}

def main():
    print(f"[{datetime.now(KST)}] DART 공시 자동 수집 시작 (조회일: {today_str})")
    sheets = get_google_sheets()
    
    # 중복 방지를 위해 기존 A열(접수번호) 데이터 로드
    existing_rcepts = {k: v.col_values(1) for k, v in sheets.items()}
    rows_to_add = {"유상증자": [], "전환사채": [], "교환사채": []}
    
    # 1. 공시검색 (list.json) 호출
    list_url = f"{DART_BASE_URL}/list.json"
    list_params = {'crtfc_key': DART_API_KEY, 'bgn_de': today_str, 'end_de': today_str}
    list_data = requests.get(list_url, params=list_params).json()
    
    if list_data.get('status') != '000':
        print(f"DART API 응답: {list_data}")
        return

    for item in list_data['list']:
        rcept_no = item.get('rcept_no')
        corp_code = item.get('corp_code')
        corp_name = item.get('corp_name')
        report_nm = item.get('report_nm')
        corp_cls = get_market_type(item.get('corp_cls'))
        dart_link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

        # --- 1. 유상증자 처리 ---
        if any(kw in report_nm for kw in PIIC_KEYWORDS) and rcept_no not in existing_rcepts["유상증자"]:
            detail = fetch_detail_data("piicDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no, corp_name, corp_cls, report_nm,
                    detail.get("ic_mthn", ""), detail.get("nstk_ostk_cnt", ""), detail.get("nstk_estk_cnt", ""),
                    detail.get("fv_ps", ""), detail.get("nstk_isu_prc", ""), # 신주발행가액 추가
                    detail.get("bfic_tisstk_ostk", ""), detail.get("bfic_tisstk_estk", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                    detail.get("sbd", ""), detail.get("pymd", ""), detail.get("tp_allo_cprpty", ""), dart_link
                ]
                rows_to_add["유상증자"].append(row)

        # --- 2. 전환사채 처리 ---
        elif any(kw in report_nm for kw in CVBD_KEYWORDS) and rcept_no not in existing_rcepts["전환사채"]:
            detail = fetch_detail_data("cvbdIsDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no, corp_name, corp_cls, report_nm, detail.get("bddd", ""),
                    detail.get("bd_tm", ""), detail.get("bd_knd", ""), detail.get("bdis_mthn", ""),
                    detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""), detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                    detail.get("cv_rt", ""), detail.get("cv_prc", ""), detail.get("act_mktprcfl_cvprc_lwtrsprc", ""),
                    detail.get("cvrqpd_bgd", ""), detail.get("cvrqpd_edd", ""), detail.get("sbd", ""), detail.get("pymd", ""),
                    detail.get("rpmcmp", ""), dart_link
                ]
                rows_to_add["전환사채"].append(row)

        # --- 3. 교환사채 처리 ---
        elif any(kw in report_nm for kw in EXBD_KEYWORDS) and rcept_no not in existing_rcepts["교환사채"]:
            detail = fetch_detail_data("exbdIsDecsn.json", rcept_no, corp_code)
            if detail:
                row = [
                    rcept_no, corp_name, corp_cls, report_nm, detail.get("bddd", ""),
                    detail.get("bd_tm", ""), detail.get("bd_knd", ""), detail.get("bdis_mthn", ""),
                    detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""), detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""),
                    won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")),
                    won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")),
                    won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", "")),
                    detail.get("ex_rt", ""), detail.get("ex_prc", ""), detail.get("exrqpd_bgd", ""), detail.get("exrqpd_edd", ""),
                    detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", ""), dart_link
                ]
                rows_to_add["교환사채"].append(row)

    # 구글 시트에 일괄 업데이트
    for sheet_name, rows in rows_to_add.items():
        if rows:
            sheets[sheet_name].append_rows(rows)
            print(f"✅ [{sheet_name}] {len(rows)}건 새 공시 추가 완료")
        else:
            print(f"➖ [{sheet_name}] 새로 추가할 데이터가 없습니다.")

if __name__ == "__main__":
    main()
