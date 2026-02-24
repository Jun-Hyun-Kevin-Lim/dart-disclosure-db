import os
import json
import gspread
import OpenDartReader
import pandas as pd
import requests
from datetime import datetime, timedelta

# 1. GitHub Secrets 설정값 불러오기
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

# 2. 인증 및 초기화
dart = OpenDartReader(dart_key)
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# DART 상세 API 호출 함수 (에러 해결의 핵심 부분)
def get_detailed_data(api_key, endpoint, corp_code, bgn_de, end_de):
    url = f"https://opendart.fss.or.kr/api/{endpoint}.json"
    params = {'crtfc_key': api_key, 'corp_code': corp_code, 'bgn_de': bgn_de, 'end_de': end_de}
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"API Request Error: {e}")
    return pd.DataFrame()

def get_and_update():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 주요 공시 목록을 가져오는 중...")
    try:
        # pblntf_ty='B'는 '주요사항보고서'만 필터링해서 가져옵니다 (속도 향상)
        all_filings = dart.list(start=start_date, end=end_date, pblntf_ty='B')
    except Exception as e:
        print(f"목록 호출 오류: {e}")
        return

    if all_filings is None or all_filings.empty:
        print("최근 7일간 발행된 주요사항보고서가 없습니다.")
        return

    # 실제 DART API 필드명에 맞게 컬럼 수정
    tasks = [
        {
            'name': '유상증자',
            'keyword': '유상증자결정',
            'endpoint': 'piicDecsn',
            'cols': ['corp_name', 'report_nm', 'nstk_astk_cnt', 'fnd_am_tamt', 'rcept_no'] # 보통주수, 총조달금액
        },
        {
            'name': '전환사채',
            'keyword': '전환사채권발행결정',
            'endpoint': 'cvbdIssDecsn',
            'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'cv_prc', 'rcept_no'] # 권면총액, 전환가액
        },
        {
            'name': '교환사채',
            'keyword': '교환사채권발행결정',
            'endpoint': 'exbdIssDecsn',
            'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'ex_prc', 'rcept_no'] # 권면총액, 교환가액
        }
    ]

    for task in tasks:
        print(f"\n[{task['name']}] 데이터 확인 중...")
        # 해당 키워드가 들어간 공시만 필터링
        df_filtered = all_filings[all_filings['report_nm'].str.contains(task['keyword'], na=False)]
        
        if df_filtered.empty:
            print(f"ℹ️ {task['name']}: 해당 기간 내 공시가 없습니다.")
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        # 필터링된 기업들에 대해서만 상세 내역 호출
        for code in corp_codes:
            df_detail = get_detailed_data(dart_key, task['endpoint'], code, start_date, end_date)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
        
        if not detail_dfs:
            print(f"ℹ️ {task['name']}: 상세 데이터를 불러올 수 없습니다.")
            continue
            
        # 수집된 상세 데이터를 하나로 합치기
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        
        # '보고서명(report_nm)'을 가져오기 위해 목록 데이터와 접수번호(rcept_no)를 기준으로 병합
        df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'report_nm']], on='rcept_no', how='inner')
        
        # 구글 시트에 업데이트
        worksheet = sh.worksheet(task['name'])
        col_index = len(task['cols'])
        existing_rcept_nos = worksheet.col_values(col_index)
        
        # 에러 방지: API에서 특정 컬럼이 누락된 경우 빈 값으로 채움
        for col in task['cols']:
            if col not in df_merged.columns:
                df_merged[col] = ''
        
        df_final = df_merged[task['cols']].copy()
        
        # 중복 제거 (기존 시트에 없는 접수번호만 추출)
        new_data_df = df_final[~df_final['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        if not new_data_df.empty:
            data_to_add = new_data_df.values.tolist()
            worksheet.append_rows(data_to_add)
            print(f"✅ {task['name']}: 신규 데이터 {len(data_to_add)}건 추가 완료")
        else:
            print(f"ℹ️ {task['name']}: 새로 추가할 공시가 없습니다.")

if __name__ == "__main__":
    get_and_update()
