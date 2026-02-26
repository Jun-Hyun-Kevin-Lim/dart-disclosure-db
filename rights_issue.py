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

# --- [XML 원문 족집게 파싱 (할인율 집중 공략)] ---
def extract_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
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
                    
                    for tag in soup.find_all(['td', 'th', 'p', 'div']):
                        tag.append(' ')
                        
                    raw_text = soup.get_text(separator=' ', strip=True)
                    # 여백을 1칸으로만 압축 (다른 값이 망가지지 않게 유지)
                    clean_text = re.sub(r'\s+', ' ', raw_text)
                    
                    # 1. 가격 추출 (안정적인 기존 코드 유지)
                    def get_price(keyword):
                        for match in re.finditer(keyword, clean_text):
                            window = clean_text[match.end():match.end()+150]
                            nums = re.findall(r'(?<![\d\.])(?:[1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{2,})(?![\d\.])', window)
                            for n in nums:
                                val = int(n.replace(',', ''))
                                if val >= 100 and val not in [2023, 2024, 2025, 2026, 2027]:
                                    return f"{val:,}"
                        return '-'
                        
                    extracted['issue_price'] = get_price(r'(?:확\s*정|예\s*정)?\s*발\s*행\s*가\s*(?:액)?')
                    extracted['base_price'] = get_price(r'기\s*준\s*(?:주\s*가|발\s*행\s*가\s*(?:액)?|가\s*액)')
                    
                    # 2. 💡 할인/할증률 추출 (할인율 집중 고도화 엔진)
                    def get_discount(issue_p, base_p):
                        # 긴 문구까지 완벽하게 잡아내는 패턴
                        pattern = r'(기\s*준\s*주\s*가\s*에\s*대\s*한\s*)?(할\s*인\s*[률율]\s*또\s*는\s*할\s*증\s*[률율]|할\s*인\s*[\(\[\{]?\s*할\s*증\s*[\)\]\}]?\s*[률율]|할\s*인\s*[률율]|할\s*증\s*[률율])'
                        
                        extracted_val = None
                        val_str = ""
                        keyword = ""
                        
                        for match in re.finditer(pattern, clean_text):
                            keyword = match.group(0).replace(' ', '')
                            # 숫자 찾을 때만 해당 구간의 띄어쓰기를 없애서 정확히 캐치
                            window = clean_text[match.end():match.end()+150].replace(' ', '')
                            
                            # 소수점이 있는 숫자 우선 탐색 (-2.80)
                            m = re.search(r'([\-\+]?\d+\.\d+)', window)
                            if not m:
                                # 없으면 %가 붙어있는 정수 탐색 (10%)
                                m = re.search(r'([\-\+]?\d+)%', window)
                                
                            if m:
                                val_str = m.group(1)
                                extracted_val = float(val_str)
                                break
                                
                            if re.search(r'(해당\s*사항\s*없음|해당\s*없음|-)', window[:20]):
                                return "0.00%"

                        if extracted_val is None:
                            return '-'
                            
                        if extracted_val == 0:
                            return "0.00%"

                        # --- 부호(+, -) 결정 로직 ---
                        val_abs = abs(extracted_val)
                        final_sign = 0 
                        
                        # 1단계: 문자열 자체에 기호가 있으면 우선 적용
                        if '-' in val_str: final_sign = -1
                        elif '+' in val_str: final_sign = 1
                            
                        # 2단계: 💡 수학적 크로스체크 (발행가 vs 기준주가 비교로 명확한 부호 확정)
                        if issue_p != '-' and base_p != '-':
                            try:
                                i_v = float(issue_p.replace(',', ''))
                                b_v = float(base_p.replace(',', ''))
                                if b_v > 0:
                                    if i_v > b_v: final_sign = 1    # 비싸게 팔면 무조건 할증(+)
                                    elif i_v < b_v: final_sign = -1 # 싸게 팔면 무조건 할인(-)
                            except:
                                pass
                                
                        # 3단계: 수학 체크 불가 & 부호 없을 때 글자로 유추
                        if final_sign == 0:
                            if '할증' in keyword and '할인' not in keyword:
                                final_sign = 1
                            else:
                                final_sign = -1 # 기본값은 할인(-)
                                
                        return f"{val_abs * final_sign:+.2f}%"

                    extracted['discount'] = get_discount(extracted['issue_price'], extracted['base_price'])
                    
                    # 3. 날짜 추출 (안정적인 기존 코드 유지)
                    def get_date(keyword):
                        for match in re.finditer(keyword, clean_text):
                            window = clean_text[match.end():match.end()+150]
                            m = re.search(r'(20[2-3][0-9])\s*[\-\.년]\s*([0-1]?[0-9])\s*[\-\.월]\s*([0-3]?[0-9])', window)
                            if m:
                                y, m_num, d_num = m.groups()
                                return f"{y}년 {m_num.zfill(2)}월 {d_num.zfill(2)}일"
                        return '-'
                        
                    extracted['board_date'] = get_date(r'(?:최\s*초\s*)?이\s*사\s*회\s*결\s*의\s*일')
                    extracted['pay_date'] = get_date(r'(납\s*입\s*일|주\s*금\s*납\s*입\s*기\s*일)')
                    extracted['div_date'] = get_date(r'(?:신\s*주\s*의\s*)?배\s*당\s*기\s*산\s*일')
                    extracted['list_date'] = get_date(r'(?:신\s*주\s*권\s*교\s*부\s*예\s*정\s*일|상\s*장\s*예\s*정\s*일)')
                    
                    # 4. 투자자
                    if "제3자배정" in clean_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def get_and_update_yusang():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 유상증자 공시 탐색 중 (할인율 집중 치료 버전)...")
    
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100',
        'last_reprt_at': 'Y'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)].copy()
    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return
        
    df_filtered['corp_cls'] = df_filtered['corp_cls'].fillna('')
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    for code in corp_codes:
        detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
        df_detail = fetch_dart_json('https://opendart.fss.or.kr/api/piicDecsn.json', detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return
        
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    df_combined = df_combined.drop(columns=['corp_cls'], errors='ignore')
    
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'corp_cls', 'report_nm']], on='rcept_no', how='left')
    
    worksheet = sh.worksheet('유상증자')
    existing_rcept_nos = worksheet.col_values(21) 
    
    new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
    
    if new_data_df.empty:
        print("ℹ️ 새로 추가할 데이터가 없습니다.")
        return
        
    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    for _, row in new_data_df.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        report_nm = row.get('report_nm', '') 
        
        print(f" -> {corp_name} 데이터 추출 및 포매팅 적용 중...")
        
        xml_data = extract_xml_details(dart_key, rcept_no)
        
        market = cls_map.get(row.get('corp_cls', ''), '기타')
        method = row.get('ic_mthn', '')
        
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk
        product = "보통주" if ostk > 0 else "기타주"
        
        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk
        
        new_shares_str = f"{new_shares:,}"
        old_shares_str = f"{old_shares:,}"
        
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"
        
        fclt = to_int(row.get('fdpp_fclt'))
        bsninh = to_int(row.get('fdpp_bsninh'))
        op = to_int(row.get('fdpp_op'))
        dtrp = to_int(row.get('fdpp_dtrp'))
        ocsa = to_int(row.get('fdpp_ocsa'))
        etc = to_int(row.get('fdpp_etc'))
        
        total_amt = fclt + bsninh + op + dtrp + ocsa + etc
        total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "0.00"
        
        purposes = []
        if fclt > 0: purposes.append("시설")
        if bsninh > 0: purposes.append("영업양수")
        if op > 0: purposes.append("운영")
        if dtrp > 0: purposes.append("채무상환")
        if ocsa > 0: purposes.append("타법인증권")
        if etc > 0: purposes.append("기타")
        purpose_str = ", ".join(purposes)
        
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        new_row = [
            corp_name,                  
            report_nm,                  
            market,                     
            xml_data['board_date'],     
            method,                     
            product,                    
            new_shares_str,             
            xml_data.get('issue_price', '-'),    
            xml_data.get('base_price', '-'),     
            total_amt_uk,               
            xml_data.get('discount', '-'),       
            old_shares_str,             
            ratio,                      
            xml_data['pay_date'],       
            xml_data['div_date'],       
            xml_data['list_date'],      
            xml_data['board_date'],     
            purpose_str,                
            xml_data['investor'],       
            link,                       
            rcept_no                    
        ]
        
        data_to_add.append(new_row)
        
    worksheet.append_rows(data_to_add)
    print(f"✅ 유상증자: 안정화 및 할인율 집중 개선 완료! 신규 데이터 {len(data_to_add)}건 추가됨!")

if __name__ == "__main__":
    get_and_update_yusang()
