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

KST = timezone(timedelta(hours=9))
today_str = datetime.now(KST).strftime('%Y%m%d')
DART_BASE_URL = "https://opendart.fss.or.kr/api"

# 키워드 리스트
PIIC_KEYWORDS = ["유상증자"]
CVBD_KEYWORDS = ["전환사채"]
EXBD_KEYWORDS = ["교환사채"]

def get_google_sheets():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    return {"유상증자": doc.worksheet("유상증자"), "전환사채": doc.worksheet("전환사채"), "교환사채": doc.worksheet("교환사채")}

def fetch_api(url, params):
    res = requests.get(url, params=params).json()
    return res if res.get('status') == '000' else {}

def main():
    print(f"[{datetime.now(KST)}] 모든 데이터 자동 수집 시작...")
    sheets = get_google_sheets()
    
    # 공시 목록 가져오기
    list_data = fetch_api(f"{DART_BASE_URL}/list.json", {'crtfc_key': DART_API_KEY, 'bgn_de': today_str, 'end_de': today_str})
    if not list_data: return

    for item in list_data.get('list', []):
        rcept_no = item['rcept_no']
        corp_code = item['corp_code']
        report_nm = item['report_nm']

        # 시트 결정 및 상세 API 설정
        target_name = None
        api_path = None
        if any(kw in report_nm for kw in PIIC_KEYWORDS): target_name, api_path = "유상증자", "piicDecsn.json"
        elif any(kw in report_nm for kw in CVBD_KEYWORDS): target_name, api_path = "전환사채", "cvbdIsDecsn.json"
        elif any(kw in report_nm for kw in EXBD_KEYWORDS): target_name, api_path = "교환사채", "exbdIsDecsn.json"

        if target_name:
            sheet = sheets[target_name]
            # 중복 체크 (A열: 접수번호)
            if rcept_no in sheet.col_values(1): continue

            # 상세 정보 및 기업 개황 정보 가져오기
            detail_res = fetch_api(f"{DART_BASE_URL}/{api_path}", {'crtfc_key': DART_API_KEY, 'corp_code': corp_code})
            # 상세 리스트 중 현재 접수번호와 맞는 것 추출
            detail = {}
            if detail_res and 'list' in detail_res:
                detail = next((d for d in detail_res['list'] if d['rcept_no'] == rcept_no), {})
            
            company = fetch_api(f"{DART_BASE_URL}/company.json", {'crtfc_key': DART_API_KEY, 'corp_code': corp_code})

            # 💡 모든 데이터 합치기 (목록 + 상세 + 기업정보 + 바로가기링크)
            full_data = {**item, **detail, **company}
            full_data['dart_link'] = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

            # 💡 시트 헤더 처리 및 데이터 입력
            headers = sheet.row_values(1)
            if not headers:
                headers = list(full_data.keys())
                sheet.append_row(headers) # 시트가 비었으면 제목 생성
            
            # 헤더 순서에 맞춰서 데이터 나열
            row = [str(full_data.get(h, "")) for h in headers]
            sheet.append_row(row)
            print(f"✅ {item['corp_name']} - {target_name} 데이터 추가 완료")

if __name__ == "__main__":
    main()
