import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# 1. 환경 변수 설정
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# 날짜 설정
KST = timezone(timedelta(hours=9))
today_dt = datetime.now(KST)
today_str = today_dt.strftime('%Y%m%d')

# DART API 기본 URL
DART_BASE_URL = "https://opendart.fss.or.kr/api"

# 💡 사용자가 주신 모든 키워드 리스트 복구 (이게 없으면 데이터가 누락됩니다)
PIIC_KEYWORDS = ["유상증자1차발행가액결정", "유상증자결의", "유상증자결정", "유상증자결정(자율공시)(종속회사의주요경영사항)", "유상증자결정(종속회사의주요경영사항)", "유상증자또는주식관련사채등의발행결과", "유상증자또는주식관련사채등의청약결과", "유상증자신주발행가액", "유상증자실권주식의처리", "유상증자최종발행가액확정", "주요사항보고서(유상증자결정)", "투자회사의유상증자결의", "특수관계인의유상증자참여", "특수관계인이참여한유상증자"]
CVBD_KEYWORDS = ["자기전환사채만기전취득결정", "자기전환사채매도결정", "전환사채(해외전환사채포함)발행후만기전사채취득", "전환사채권발행결정", "전환사채발행결의", "전환사채발행결정", "전환사채전환가액결정", "주요사항보고서(자기전환사채만기전취득결정)", "주요사항보고서(자기전환사채매도결정)", "주요사항보고서(전환사채권발행결정)", "주요사항보고서(전환사채매수선택권행사자지정)", "주요사항보고서(제3자의전환사채매수선택권행사)", "특수관계인에대한전환사채발행의결", "해외전환사채발행결정", "해외전환사채발행계약체결", "해외전환사채발행완료"]
EXBD_KEYWORDS = ["교환사채(해외교환사채포함)발행후만기전사채취득", "교환사채교환가액결정", "교환사채권발행결정", "교환사채권발행결정(자율공시)(종속회사의주요경영사항)", "교환사채권발행결정(종속회사의주요경영사항)", "교환사채발행결의", "교환사채발행결정", "자기교환사채만기전취득결정", "자기교환사채매도결정", "주요사항보고서(교환사채권발행결정)", "특수관계인에대한교환사채발행의결", "해외교환사채발행결정", "해외교환사채발행계약체결", "해외교환사채발행완료"]

def get_google_sheets():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    return {"유상증자": doc.worksheet("유상증자"), "전환사채": doc.worksheet("전환사채"), "교환사채": doc.worksheet("교환사채")}

def won_to_uk(amount_str):
    if not amount_str or amount_str in ['-', '']: return "0"
    try:
        amount = int(amount_str.replace(',', ''))
        return str(round(amount / 100000000, 1))
    except: return amount_str

def fetch_company_info(corp_code):
    """기업개황 API 연동"""
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(f"{DART_BASE_URL}/company.json", params=params).json()
    return res if res.get('status') == '000' else {}

def fetch_detail_data(api_endpoint, rcept_no, corp_code):
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    res = requests.get(f"{DART_BASE_URL}/{api_endpoint}", params=params).json()
    if res.get('status') == '000':
        for item in res['list']:
            if item.get('rcept_no') == rcept_no: return item
    return {}

def main():
    print(f"[{datetime.now(KST)}] DART 당일 공시 수집 시작 (날짜: {today_str})")
    sheets = get_google_sheets()
    existing_rcepts = {k: v.col_values(1) for k, v in sheets.items()}
    rows_to_add = {"유상증자": [], "전환사채": [], "교환사채": []}
    
    # 공시검색(list.json) 연동
    list_params = {'crtfc_key': DART_API_KEY, 'bgn_de': today_str, 'end_de': today_str}
    list_data = requests.get(f"{DART_BASE_URL}/list.json", params=list_params).json()
    
    if list_data.get('status') != '000':
        print(f"🚨 DART API 응답: {list_data}")
        return

    print(f"검색된 총 공시 수: {len(list_data.get('list', []))}건")

    for item in list_data['list']:
        rcept_no, corp_code, report_nm = item.get('rcept_no'), item.get('corp_code'), item.get('report_nm')
        
        target_sheet, detail_api = None, None
        if any(kw in report_nm for kw in PIIC_KEYWORDS): target_sheet, detail_api = "유상증자", "piicDecsn.json"
        elif any(kw in report_nm for kw in CVBD_KEYWORDS): target_sheet, detail_api = "전환사채", "cvbdIsDecsn.json"
        elif any(kw in report_nm for kw in EXBD_KEYWORDS): target_sheet, detail_api = "교환사채", "exbdIsDecsn.json"
            
        if target_sheet and rcept_no not in existing_rcepts[target_sheet]:
            detail = fetch_detail_data(detail_api, rcept_no, corp_code)
            company = fetch_company_info(corp_code) # 기업개황 호출
            if not detail: continue
            
            # 💡 시트 구조에 맞게 데이터 배열 생성 (기업개황 정보 활용 가능)
            if target_sheet == "유상증자":
                row = [rcept_no, item.get('corp_name'), item.get('corp_cls'), report_nm, detail.get("ic_mthn", ""), detail.get("nstk_ostk_cnt", ""), detail.get("nstk_estk_cnt", ""), detail.get("fv_ps", ""), detail.get("bfic_tisstk_ostk", ""), detail.get("bfic_tisstk_estk", ""), won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")), won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")), won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))]
            else:
                row = [rcept_no, item.get('corp_name'), item.get('corp_cls'), report_nm, detail.get("bddd", ""), detail.get("bd_tm", ""), detail.get("bd_knd", ""), detail.get("bdis_mthn", ""), detail.get("bd_fta", ""), detail.get("bd_intr_ex", ""), detail.get("bd_intr_sf", ""), detail.get("bd_mtd", ""), won_to_uk(detail.get("fdpp_fclt", "")), won_to_uk(detail.get("fdpp_bsnhinh", "")), won_to_uk(detail.get("fdpp_op", "")), won_to_uk(detail.get("fdpp_dtrp", "")), won_to_uk(detail.get("fdpp_ocsa", "")), won_to_uk(detail.get("fdpp_etc", ""))]
                if target_sheet == "전환사채": row += [detail.get("cv_rt", ""), detail.get("cv_prc", ""), detail.get("act_mktprcfl_cvprc_lwtrsprc", ""), detail.get("cvrqpd_bgd", ""), detail.get("cvrqpd_edd", ""), detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")]
                else: row += [detail.get("ex_rt", ""), detail.get("ex_prc", ""), detail.get("exrqpd_bgd", ""), detail.get("exrqpd_edd", ""), detail.get("sbd", ""), detail.get("pymd", ""), detail.get("rpmcmp", "")]
            rows_to_add[target_sheet].append(row)

    for name, rows in rows_to_add.items():
        if rows:
            sheets[name].append_rows(rows)
            print(f"✅ [{name}] {len(rows)}건 추가 완료")

if __name__ == "__main__":
    main()
