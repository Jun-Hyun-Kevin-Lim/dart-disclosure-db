import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 1. GitHub Secrets 설정값
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

# 2. 구글 시트 인증
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# --- [JSON용] DART API 공통 호출 함수 ---
def fetch_dart_json(endpoint, params):
    url = f"https://opendart.fss.or.kr/api/{endpoint}"
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러 ({endpoint}): {e}")
    return pd.DataFrame()

# --- [XML용] DART 원문 다운로드 및 텍스트 추출 함수 ---
def fetch_dart_xml_text(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    try:
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            # 1. ZIP 파일 메모리에서 열기
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                # 2. 압축 안의 xml 파일 찾기
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    
                    # 3. BeautifulSoup으로 HTML/XML 태그 모두 제거하고 텍스트만 추출
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    raw_text = soup.get_text(separator=' ', strip=True)
                    
                    # 구글 시트 셀 용량 한계 방지를 위해 앞부분 500자만 리턴
                    return raw_text[:500] + " ...[이하 원문 생략]"
    except Exception as e:
        print(f"XML 처리 에러 ({rcept_no}): {e}")
    return "XML 원문 추출 실패"

def get_and_update():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 주요 공시 목록(JSON) 가져오는 중...")
    list_params = {'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 'pblntf_ty': 'B'}
    all_filings = fetch_dart_json('list.json', list_params)

    if all_filings.empty:
        print("최근 7일간 발행된 주요사항보고서가 없습니다.")
        return

    tasks = [
        {'name': '유상증자', 'keyword': '유상증자결정', 'endpoint': 'piicDecsn.json', 'cols': ['corp_name', 'report_nm', 'nstk_astk_cnt', 'fnd_am_tamt', 'rcept_no']},
        {'name': '전환사채', 'keyword': '전환사채권발행결정', 'endpoint': 'cvbdIsDecsn.json', 'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'cv_prc', 'rcept_no']},
        {'name': '교환사채', 'keyword': '교환사채권발행결정', 'endpoint': 'exbdIsDecsn.json', 'cols': ['corp_name', 'report_nm', 'bd_ftnd_tamt', 'ex_prc', 'rcept_no']}
    ]

    for task in tasks:
        print(f"\n[{task['name']}] 작업 시작...")
        df_filtered = all_filings[all_filings['report_nm'].str.contains(task['keyword'], na=False)]
        
        if df_filtered.empty:
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        for code in corp_codes:
            detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
            df_detail = fetch_dart_json(task['endpoint'], detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
        
        if not detail_dfs:
            continue
            
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'report_nm']], on='rcept_no', how='inner')
        
        worksheet = sh.worksheet(task['name'])
        col_index = len(task['cols'])
        existing_rcept_nos = worksheet.col_values(col_index)
        
        for col in task['cols']:
            if col not in df_merged.columns:
                df_merged[col] = ''
        
        df_final = df_merged[task['cols']].copy()
        new_data_df = df_final[~df_final['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        if not new_data_df.empty:
            data_to_add = []
            # 신규 공시가 있을 때만 한 줄씩 XML(원문)을 다운로드하여 붙입니다.
            for index, row in new_data_df.iterrows():
                row_list = row.tolist()
                rcept_no = row['rcept_no']
                
                print(f" -> {row['corp_name']} XML 원본 다운로드 및 파싱 중 ({rcept_no})...")
                xml_text = fetch_dart_xml_text(dart_key, rcept_no)
                
                # 기존 JSON 데이터 뒤에 XML 텍스트 추가
                row_list.append(xml_text)
                data_to_add.append(row_list)
                
            worksheet.append_rows(data_to_add)
            print(f"✅ {task['name']}: 신규 데이터 {len(data_to_add)}건 구글 시트 추가 완료 (XML 포함)!")
        else:
            print(f"ℹ️ {task['name']}: 새로 추가할 공시가 없습니다.")

if __name__ == "__main__":
    get_and_update()
