import os
import json
import gspread
import pandas as pd
import requests
from datetime import datetime, timedelta

# 1. GitHub Secrets 설정값 불러오기
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

# 2. 구글 시트 인증
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# DART API 공통 호출 함수 (JSON 전용)
def fetch_dart_data(endpoint, params):
    url = f"https://opendart.fss.or.kr/api/{endpoint}"
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"API 호출 에러 ({endpoint}): {e}")
    return pd.DataFrame()

def get_and_update():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 주요 공시 목록을 가져오는 중...")
    
    # 1. 공시 목록 조회 (공식 API list.json 사용)
    list_params = {
        'crtfc_key': dart_key,
        'bgn_de': start_date,
        'end_de': end_date,
        'pblntf_ty': 'B' # B: 주요사항보고서
    }
    all_filings = fetch_dart_data('list.json', list_params)

    if all_filings.empty:
        print("최근 7일간 발행된 주요사항보고서가 없습니다.")
        return

    # 2. 탭별 상세 API 설정 (올려주신 공식 엔드포인트 적용)
    tasks = [
        {
            'name': '유상증자',
            'keyword': '유상증자결정',
            'endpoint': 'piicDecsn.json',
            'cols': ['corp_name', 'report_nm', 'nstk_astk_cnt', 'fnd_am_tamt', 'rcept_no']
        },
        {
            'name': '전환사채',
            'keyword': '전환사채권발행결정',
            'endpoint': 'cvbdIsDecsn.json', # 스펠링 수정 완료
            'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'cv_prc', 'rcept_no']
        },
        {
            'name': '교환사채',
            'keyword': '교환사채권발행결정',
            'endpoint': 'exbdIsDecsn.json', # 스펠링 수정 완료
            'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'ex_prc', 'rcept_no']
        }
    ]

    for task in tasks:
        print(f"\n[{task['name']}] 데이터 작업 시작...")
        
        # 목록에서 해당 키워드가 포함된 공시만 필터링
        df_filtered = all_filings[all_filings['report_nm'].str.contains(task['keyword'], na=False)]
        
        if df_filtered.empty:
            print(f"ℹ️ {task['name']}: 해당 기간 내 대상 공시가 없습니다.")
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        # 필터링된 회사들의 상세 정보(발행가액, 조달금액 등) 가져오기
        for code in corp_codes:
            detail_params = {
                'crtfc_key': dart_key,
                'corp_code': code,
                'bgn_de': start_date,
                'end_de': end_date
            }
            df_detail = fetch_dart_data(task['endpoint'], detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
        
        if not detail_dfs:
            print(f"ℹ️ {task['name']}: 상세 데이터를 불러올 수 없습니다.")
            continue
            
        # 상세 데이터 합치기
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        
        # 목록 데이터(보고서명 등)와 상세 데이터(발행가 등)를 접수번호 기준으로 합치기
        df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'report_nm']], on='rcept_no', how='inner')
        
        # 3. 구글 시트 업데이트
        worksheet = sh.worksheet(task['name'])
        col_index = len(task['cols'])
        existing_rcept_nos = worksheet.col_values(col_index)
        
        # 누락된 컬럼 빈칸 처리
        for col in task['cols']:
            if col not in df_merged.columns:
                df_merged[col] = ''
        
        df_final = df_merged[task['cols']].copy()
        
        # 중복 데이터 제외 (새로운 접수번호만 필터링)
        new_data_df = df_final[~df_final['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        if not new_data_df.empty:
            data_to_add = new_data_df.values.tolist()
            worksheet.append_rows(data_to_add)
            print(f"✅ {task['name']}: 신규 데이터 {len(data_to_add)}건 구글 시트 추가 완료!")
        else:
            print(f"ℹ️ {task['name']}: 새로 추가할 공시가 없습니다 (모두 이미 저장됨).")

if __name__ == "__main__":
    get_and_update()
