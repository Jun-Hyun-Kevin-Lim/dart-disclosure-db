import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
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

# --- [JSON 파싱] ---
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

# --- [XML 원문 파싱 및 정규식(Regex) 데이터 추출] ---
def extract_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 기본값 설정
    extracted = {
        'board_date': '-', 'issue_price': '-', 'base_price': '-', 'discount': '-',
        'pay_date': '-', 'div_date': '-', 'list_date': '-', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    raw_text = soup.get_text(separator=' ', strip=True)
                    
                    # 1. 이사회결의일 추출
                    board = re.search(r'이사회결의일[^:\d]*[:\s]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if board: extracted['board_date'] = board.group(1).strip()
                    
                    # 2. 확정발행가 추출
                    issue = re.search(r'발행가액[^:\d]*[:\s]*([0-9,]+)\s*원', raw_text)
                    if issue: extracted['issue_price'] = issue.group(1).strip()
                    
                    # 3. 기준주가 추출
                    base = re.search(r'기준주가[^:\d]*[:\s]*([0-9,]+)\s*원', raw_text)
                    if base: extracted['base_price'] = base.group(1).strip()
                    
                    # 4. 할인(할증)율 추출
                    disc = re.search(r'할인율[^:\d]*[:\s]*([0-9\.]+)\s*%', raw_text)
                    if disc: extracted['discount'] = disc.group(1).strip() + "%"
                    
                    # 5. 납입일 추출
                    pay = re.search(r'납\s*입\s*일[^:\d]*[:\s]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if pay: extracted['pay_date'] = pay.group(1).strip()
                    
                    # 6. 배당기산일 추출
                    div = re.search(r'배당기산일[^:\d]*[:\s]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if div: extracted['div_date'] = div.group(1).strip()
                    
                    # 7. 상장예정일 추출
                    list_d = re.search(r'상장\s*예정일[^:\d]*[:\s]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if list_d: extracted['list_date'] = list_d.group(1).strip()
                    
                    # 8. 투자자 (제3자배정일 경우 텍스트 명시)
                    if "제3자배정" in raw_text: extracted['investor'] = "제3자배정 대상자 (원문/링크 참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 쉼표(,) 포함된 문자열을 정수로 안전하게 변환하는 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def get_and_update_yusang():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 유상증자 공시 탐색 중...")
    
    # 1. 공시 목록(list.json)에서 시장 구분(유가, 코스닥 등) 가져오기
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 7일간 주요사항보고서가 없습니다.")
        return

    # 유상증자결정만 필터링
    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)]
    if df_filtered.empty:
        print("ℹ️ 유상증자: 해당 기간 내 공시가 없습니다.")
        return
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    # 2. 유상증자 상세 데이터(piicDecsn.json) 수집
    for code in corp_codes:
        detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
        df_detail = fetch_dart_json('https://opendart.fss.or.kr/api/piicDecsn.json', detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("ℹ️ 유상증자: 상세 데이터를 불러올 수 없습니다.")
        return
        
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    
    # 3. 데이터 병합 (JSON + 목록 정보)
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'corp_cls']], on='rcept_no', how='inner')
    
    # 구글 시트 및 중복 방지 (20번째 컬럼인 접수번호 확인)
    worksheet = sh.worksheet('유상증자')
    existing_rcept_nos = worksheet.col_values(20) 
    new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
    
    if new_data_df.empty:
        print("ℹ️ 새로 추가할 유상증자 공시가 없습니다 (모두 최신 상태).")
        return
        
    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    for _, row in new_data_df.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        print(f" -> {corp_name} 데이터 파싱 및 계산 중...")
        
        # XML 원문 파싱으로 숨겨진 날짜/금액 데이터 추출
        xml_data = extract_xml_details(dart_key, rcept_no)
        
        # 상장시장 및 증자방식
        market = cls_map.get(row.get('corp_cls', ''), '')
        method = row.get('ic_mthn', '')
        
        # 주식수 및 증자비율 계산
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk
        product = "보통주" if ostk > 0 else "기타주"
        
        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk
        
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"
        
        # 확정발행금액 (억원 단위 변환) 및 자금용도 추출
        fclt = to_int(row.get('fdpp_fclt'))
        bsninh = to_int(row.get('fdpp_bsninh'))
        op = to_int(row.get('fdpp_op'))
        dtrp = to_int(row.get('fdpp_dtrp'))
        ocsa = to_int(row.get('fdpp_ocsa'))
        etc = to_int(row.get('fdpp_etc'))
        
        total_amt = fclt + bsninh + op + dtrp + ocsa + etc
        total_amt_uk = f"{(total_amt / 100000000):.1f}" if total_amt > 0 else "0" # 억원 단위
        
        purposes = []
        if fclt > 0: purposes.append("시설")
        if bsninh > 0: purposes.append("영업양수")
        if op > 0: purposes.append("운영")
        if dtrp > 0: purposes.append("채무상환")
        if ocsa > 0: purposes.append("타법인증권")
        if etc > 0: purposes.append("기타")
        purpose_str = ", ".join(purposes)
        
        # 다트 공시 원문 링크 생성
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        # 19개 요청 열 + 1개 식별 열
        new_row = [
            corp_name,                  # 1. 회사명
            market,                     # 2. 상장시장
            xml_data['board_date'],     # 3. 최초 이사회결의일
            method,                     # 4. 증자방식
            product,                    # 5. 발행상품
            new_shares,                 # 6. 신규발행주식수
            xml_data['issue_price'],    # 7. 확정발행가(원)
            xml_data['base_price'],     # 8. 기준주가
            total_amt_uk,               # 9. 확정발행금액(억원)
            xml_data['discount'],       # 10. 할인(할증률)
            old_shares,                 # 11. 증자전 주식수
            ratio,                      # 12. 증자비율
            xml_data['pay_date'],       # 13. 납입일
            xml_data['div_date'],       # 14. 신주의배당기산일
            xml_data['list_date'],      # 15. 신주의상장 예정일
            xml_data['board_date'],     # 16. 이사회결의일
            purpose_str,                # 17. 자금용도
            xml_data['investor'],       # 18. 투자자
            link,                       # 19. 링크
            rcept_no                    # 20. 접수번호 (숨김 권장)
        ]
        
        data_to_add.append(new_row)
        
    worksheet.append_rows(data_to_add)
    print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 구글 시트 추가 완료!")

if __name__ == "__main__":
    get_and_update_yusang()
