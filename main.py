import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
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

# --- [JSON 파싱] 공통 호출 함수 ---
def fetch_dart_json(url, params):
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame()

# --- [XML 파싱 1] 기업개황 (대표자명 추출) ---
def fetch_company_ceo_xml(api_key, corp_code):
    url = "https://opendart.fss.or.kr/api/company.xml" # XML 엔드포인트 사용
    params = {'crtfc_key': api_key, 'corp_code': corp_code}
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            root = ET.fromstring(res.content)
            ceo_nm = root.findtext('ceo_nm') # XML에서 ceo_nm 태그 추출
            return ceo_nm if ceo_nm else "정보없음"
    except Exception as e:
        print(f"기업개황 XML 에러: {e}")
    return "오류"

# --- [XML 파싱 2] 공시서류원본 (원문 텍스트 요약 추출) ---
def fetch_document_xml_text(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    try:
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            # Zip 파일로 리턴되므로 압축 해제 처리
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    # XML 태그 제거 및 순수 텍스트 추출
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    raw_text = soup.get_text(separator=' ', strip=True)
                    return raw_text[:200] + "..." # 엑셀 셀 용량을 위해 200자 요약
    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
    return "원문 추출 실패"

def get_and_update():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 주요 공시 목록 가져오는 중...")
    
    # 1. 공시 목록 (JSON)
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 
        'bgn_de': start_date, 
        'end_de': end_date, 
        'pblntf_ty': 'B',           # 주요사항보고
        'pblntf_detail_ty': 'B001', # 주요사항보고서 상세
        'page_count': '100'         # 최대 건수
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 7일간 발행된 주요사항보고서가 없습니다.")
        return

    # 2. 주요사항보고서 상세 가이드에 맞춘 항목 매핑
    tasks = [
        {
            'name': '유상증자',
            'keyword': '유상증자결정',
            'url': 'https://opendart.fss.or.kr/api/piicDecsn.json',
            # 가이드 기준: nstk_ostk_cnt(보통주식수)
            'cols': ['corp_name', 'report_nm', 'nstk_ostk_cnt', 'rcept_no'] 
        },
        {
            'name': '전환사채',
            'keyword': '전환사채권발행결정',
            'url': 'https://opendart.fss.or.kr/api/cvbdIsDecsn.json',
            # 가이드 기준: bd_fta(권면총액), cv_prc(전환가액)
            'cols': ['corp_name', 'report_nm', 'bd_fta', 'cv_prc', 'rcept_no']
        },
        {
            'name': '교환사채',
            'keyword': '교환사채권발행결정',
            'url': 'https://opendart.fss.or.kr/api/exbdIsDecsn.json',
            # 가이드 기준: bd_fta(권면총액), ex_prc(교환가액)
            'cols': ['corp_name', 'report_nm', 'bd_fta', 'ex_prc', 'rcept_no']
        }
    ]

    for task in tasks:
        print(f"\n[{task['name']}] 데이터 확인 중...")
        df_filtered = all_filings[all_filings['report_nm'].str.contains(task['keyword'], na=False)]
        
        if df_filtered.empty:
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        # 상세 데이터 가져오기 (JSON)
        for code in corp_codes:
            detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
            df_detail = fetch_dart_json(task['url'], detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
        
        if not detail_dfs:
            continue
            
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        # 공시 목록의 '접수번호'와 합치기
        df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'report_nm']], on='rcept_no', how='inner')
        
        worksheet = sh.worksheet(task['name'])
        existing_rcept_nos = worksheet.col_values(len(task['cols'])) # 접수번호 위치
        
        for col in task['cols']:
            if col not in df_merged.columns:
                df_merged[col] = ''
        
        df_final = df_merged[task['cols']].copy()
        
        # 중복 방지 로직 (신규 접수번호만 필터링)
        new_data_df = df_final[~df_final['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        if not new_data_df.empty:
            data_to_add = []
            
            # 신규 데이터가 있을 때만 기업개황(XML) & 원문(XML) 파싱 실행
            for index, row in new_data_df.iterrows():
                row_list = row.tolist()
                rcept_no = row['rcept_no']
                
                # 공시 목록(all_filings)에서 해당 접수번호의 corp_code 찾기
                corp_code = df_filtered[df_filtered['rcept_no'] == rcept_no]['corp_code'].values[0]
                
                print(f" -> {row['corp_name']} XML 추가 데이터(대표자/원문) 수집 중...")
                
                # XML 데이터 수집
                ceo_name_xml = fetch_company_ceo_xml(dart_key, corp_code)
                raw_text_xml = fetch_document_xml_text(dart_key, rcept_no)
                
                # 기존 JSON 데이터 뒤에 XML 데이터 2개(대표자명, 원문요약) 추가
                row_list.extend([ceo_name_xml, raw_text_xml])
                data_to_add.append(row_list)
                
            worksheet.append_rows(data_to_add)
            print(f"✅ {task['name']}: 신규 데이터 {len(data_to_add)}건 구글 시트 추가 완료 (JSON+XML 하이브리드)!")
        else:
            print(f"ℹ️ {task['name']}: 새로 추가할 공시가 없습니다.")

if __name__ == "__main__":
    get_and_update()
