import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta

# --- 1. 설정 및 환경 변수 로드 ---
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

KST = timezone(timedelta(hours=9))
today_str = datetime.now(KST).strftime('%Y%m%d')
DART_BASE_URL = "https://opendart.fss.or.kr/api"

# --- 2. 구글 시트 현대식 인증 함수 ---
def connect_spreadsheet():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(GOOGLE_SHEET_ID)
    print(f"📡 시트 연결 성공: {doc.title}")
    return doc

# --- 3. DART 데이터 수집 유틸리티 ---
def call_dart_api(endpoint, params):
    """ 가이드에 따른 API 호출 함수"""
    url = f"{DART_BASE_URL}/{endpoint}"
    params['crtfc_key'] = DART_API_KEY
    try:
        response = requests.get(url, params=params, timeout=15)
        res_json = response.json()
        if res_json.get('status') == '000':
            return res_json
        return None
    except Exception as e:
        print(f"❌ API 에러 ({endpoint}): {e}")
        return None

def main():
    print(f"[{datetime.now(KST)}] 🚀 시스템 가동: {today_str} 공시 전수 조사")
    doc = connect_spreadsheet()
    
    # 워크시트 객체 미리 생성
    sheets = {
        "유상증자": doc.worksheet("유상증자"),
        "전환사채": doc.worksheet("전환사채"),
        "교환사채": doc.worksheet("교환사채")
    }

    # 당일 공시 목록 가져오기
    list_res = call_dart_api("list.json", {'bgn_de': today_str, 'end_de': today_str})
    if not list_res or 'list' not in list_res:
        print("📭 오늘 업데이트된 공시 목록이 없습니다.")
        return

    all_items = list_res['list']
    print(f"📋 총 {len(all_items)}건의 공시 확인 중...")

    for item in all_items:
        report_nm = item['report_nm']
        rcept_no = item['rcept_no']
        corp_code = item['corp_code']
        corp_name = item['corp_name']

        # 1. 보고서 유형 분류
        target_name = None
        detail_endpoint = None
        
        if "유상증자" in report_nm:
            target_name, detail_endpoint = "유상증자", "piicDecsn.json"
        elif "전환사채" in report_nm:
            target_name, detail_endpoint = "전환사채", "cvbdIsDecsn.json"
        elif "교환사채" in report_nm:
            target_name, detail_endpoint = "교환사채", "exbdIsDecsn.json"

        if target_name:
            ws = sheets[target_name]
            
            # 2. 중복 체크 (A열: 접수번호)
            if rcept_no in ws.col_values(1):
                print(f"⏩ 스킵 (중복): {corp_name} - {rcept_no}")
                continue

            # 3. 상세 정보(Decsn API) 및 기업개황(company API) 통합 수집
            detail_res = call_dart_api(detail_endpoint, {'corp_code': corp_code})
            company_res = call_dart_api("company.json", {'corp_code': corp_code})
            
            # 해당 접수번호에 맞는 상세 내역 필터링
            detail_data = {}
            if detail_res and 'list' in detail_res:
                detail_data = next((d for d in detail_res['list'] if d['rcept_no'] == rcept_no), {})
            
            if not detail_data:
                print(f"⚠️ 상세 정보 없음: {corp_name} ({report_nm})")
                continue

            # 4. 데이터 병합 (모든 필드 하나도 빠짐없이)
            # 목록 정보 + 상세 정보 + 기업 개황 정보를 하나로 합칩니다.
            combined_data = {**item, **detail_data, **(company_res or {})}
            combined_data['dart_link'] = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
            combined_data['collected_at'] = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')

            # 5. 시트 헤더 및 데이터 삽입 (모든 데이터 자동 매핑)
            headers = ws.row_values(1)
            if not headers:
                headers = list(combined_data.keys())
                ws.append_row(headers)
                print(f"🆕 [{target_name}] 헤더 자동 생성")

            # 헤더 순서에 맞게 데이터 행 생성
            row_to_insert = [str(combined_data.get(h, "")) for h in headers]
            
            # 최상단(2행)에 삽입
            ws.insert_row(row_to_insert, 2)
            print(f"✅ 저장 완료: {corp_name} ({target_name})")
            
            # API 호출 간격 조절
            time.sleep(0.3)

if __name__ == "__main__":
    main()
