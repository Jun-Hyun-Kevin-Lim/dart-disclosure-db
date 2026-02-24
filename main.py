import os
import json
import gspread
import OpenDartReader
import pandas as pd
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

def get_and_update():
    # 수집 기간 설정 (최근 7일치 탐색)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
    
    # 탭별 설정 (접수번호 rcept_no가 반드시 마지막에 있어야 중복 검사가 원활합니다)
    tasks = [
        {'name': '유상증자', 'func': dart.pub_off_invst_re_dec, 'cols': ['corp_name', 'report_nm', 'nstk_isn_rt', 'fnd_usa_prp', 'rcept_no']},
        {'name': '전환사채', 'func': dart.cvbd_iss_dec, 'cols': ['corp_name', 'report_nm', 'bd_inst_tamt', 'fnd_usa_prp', 'cv_prc', 'rcept_no']},
        {'name': '교환사채', 'func': dart.exbd_iss_dec, 'cols': ['corp_name', 'report_nm', 'bd_inst_tamt', 'fnd_usa_prp', 'ex_prc', 'rcept_no']}
    ]

    for task in tasks:
        print(f"[{task['name']}] 데이터 확인 중...")
        try:
            # DART에서 신규 데이터 가져오기
            df = task['func'](start=start_date, end=end_date)
            
            if df is not None and not df.empty:
                worksheet = sh.worksheet(task['name'])
                
                # 시트에 저장된 기존 접수번호 목록 가져오기 (마지막 열 기준)
                col_index = len(task['cols']) 
                existing_rcept_nos = worksheet.col_values(col_index)
                
                # 사용할 컬럼만 필터링 (결측 방지)
                available_cols = [c for c in task['cols'] if c in df.columns]
                df_filtered = df[available_cols].copy()
                
                # 중복 제거: 기존 시트에 없는 접수번호(새로운 공시)만 추출
                new_data_df = df_filtered[~df_filtered['rcept_no'].astype(str).isin(existing_rcept_nos)]
                
                if not new_data_df.empty:
                    data_to_add = new_data_df.values.tolist()
                    worksheet.append_rows(data_to_add)
                    print(f"✅ {task['name']}: 신규 데이터 {len(data_to_add)}건 추가 완료")
                else:
                    print(f"ℹ️ {task['name']}: 새로 추가할 공시가 없습니다 (모두 이미 저장됨).")
            else:
                print(f"ℹ️ {task['name']}: 최근 7일 내 발생한 공시가 없습니다.")
        
        except Exception as e:
            print(f"❌ {task['name']} 업데이트 중 오류 발생: {e}")

if __name__ == "__main__":
    get_and_update()
