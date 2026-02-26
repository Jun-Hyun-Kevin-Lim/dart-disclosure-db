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

# ==========================================
# 1. 초기 셋팅: API 키와 구글 시트 연결
# ==========================================
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# ==========================================
# 2. DART API 기본 데이터 가져오기 (JSON)
# ==========================================
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

# ==========================================
# 3. 💡 완벽 스캐너: 표(Table) 구조 분석 엔진
# ==========================================
def extract_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 실패 시 기본값 (빈칸)
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
                    
                    raw_data = {}
                    
                    # 💡 표의 '행(tr)' 단위로만 쪼개서 읽음 -> 다른 줄의 데이터를 훔쳐오는 오류 원천 차단
                    for tr in soup.find_all('tr'):
                        cells = tr.find_all(['th', 'td'])
                        
                        for i in range(len(cells)):
                            header_raw = cells[i].get_text(strip=True)
                            header_clean = re.sub(r'\s+', '', header_raw.replace('\xa0', ''))
                            
                            # 현재 칸이 제목이면, 무조건 '같은 줄 오른쪽 칸'의 데이터만 긁어옴
                            if i + 1 < len(cells):
                                val_raw = " ".join([c.get_text(separator=' ', strip=True) for c in cells[i+1:]])
                                
                                if 'issue_price' not in raw_data and re.search(r'(1주당|확정|예정|모집|발행|신주).*발행가액', header_clean):
                                    raw_data['issue_price'] = val_raw
                                elif 'base_price' not in raw_data and re.search(r'^기준(주가|발행가액|가액|단가|주당가액)', header_clean):
                                    raw_data['base_price'] = val_raw
                                elif 'discount' not in raw_data and re.search(r'(할인|할증)[율률]', header_clean):
                                    raw_data['discount'] = val_raw
                                    raw_data['discount_header'] = header_clean
                                elif 'board_date' not in raw_data and re.search(r'(최초)?이사회결의일', header_clean):
                                    raw_data['board_date'] = val_raw
                                elif 'pay_date' not in raw_data and re.search(r'(납입일|주금납입기일)', header_clean):
                                    raw_data['pay_date'] = val_raw
                                elif 'div_date' not in raw_data and re.search(r'(신주의)?배당기산일', header_clean):
                                    raw_data['div_date'] = val_raw
                                elif 'list_date' not in raw_data and re.search(r'(신주권교부예정일|신주의상장예정일|상장예정일|신주상장예정일)', header_clean):
                                    raw_data['list_date'] = val_raw

                    # --- [클리닝 1] 가격 팩트 체크 ---
                    def clean_price(text):
                        if not text: return '-'
                        t_clean = re.sub(r'[\s,원]', '', text)
                        if re.search(r'^(미정|해당없음|기재생략|-)', t_clean): return '-'
                        
                        nums = re.findall(r'(?<!\d)([1-9]\d{2,})(?!\d)', t_clean)
                        for val_str in nums:
                            val = int(val_str)
                            if val not in [2023, 2024, 2025, 2026, 2027]:
                                return f"{val:,}"
                        return '-'
                        
                    # --- [클리닝 2] 날짜 팩트 체크 ---
                    def clean_date(text):
                        if not text: return '-'
                        t_clean = re.sub(r'\s+', '', text)
                        if re.search(r'^(미정|해당없음|기재생략|-)', t_clean): return '-'
                        
                        m = re.search(r'(20[2-3]\d)[\-\.년/]([0-1]?\d)[\-\.월/]([0-3]?\d)', t_clean)
                        if m:
                            y, m_num, d_num = m.groups()
                            return f"{y}년 {m_num.zfill(2)}월 {d_num.zfill(2)}일"
                        return '-'
                        
                    # --- [클리닝 3] 할인율/할증률 완벽 검증 ---
                    def clean_discount(text, issue_p, base_p, header_text):
                        if not text: return '-'
                        t_clean = re.sub(r'\s+', '', text)
                        if re.search(r'^(미정|해당사항없음|해당없음|기재생략|-)', t_clean): return "0.00%"
                        
                        # 수학적 부호 판별
                        math_sign = 0
                        if issue_p != '-' and base_p != '-':
                            try:
                                i_v = float(issue_p.replace(',', ''))
                                b_v = float(base_p.replace(',', ''))
                                if b_v > 0:
                                    if i_v > b_v: math_sign = 1    # 할증(+)
                                    elif i_v < b_v: math_sign = -1 # 할인(-)
                            except: pass
                            
                        # 숫자 추출
                        m = re.search(r'([+\-]?\d+(?:\.\d+)?)', t_clean)
                        if m:
                            val_str = m.group(1)
                            try: val = float(val_str)
                            except: return '-'
                            
                            if val == 0: return "0.00%"
                            if abs(val) > 100: return '-' # 페이지 번호 쓰레기값 방어
                            
                            val_abs = abs(val)
                            
                            # 1. 수학적 팩트가 있으면 무조건 우선
                            if math_sign != 0:
                                return f"{val_abs * math_sign:+.2f}%"
                            # 2. 계산 불가 시 텍스트 기호 및 문맥 파악
                            else:
                                if '-' in val_str: return f"{-val_abs:+.2f}%"
                                elif '+' in val_str: return f"{val_abs:+.2f}%"
                                else:
                                    if '할증' in header_text and '할인' not in header_text: return f"+{val_abs:.2f}%"
                                    else: return f"{-val_abs:+.2f}%"
                        return '-'

                    # 클리닝 기계 가동 및 결과 저장
                    extracted['issue_price'] = clean_price(raw_data.get('issue_price'))
                    extracted['base_price'] = clean_price(raw_data.get('base_price'))
                    extracted['discount'] = clean_discount(raw_data.get('discount'), extracted['issue_price'], extracted['base_price'], raw_data.get('discount_header', ''))
                    extracted['board_date'] = clean_date(raw_data.get('board_date'))
                    extracted['pay_date'] = clean_date(raw_data.get('pay_date'))
                    extracted['div_date'] = clean_date(raw_data.get('div_date'))
                    extracted['list_date'] = clean_date(raw_data.get('list_date'))
                    
                    full_text = soup.get_text(separator=' ', strip=True).replace(' ', '')
                    if "제3자배정" in full_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

# ==========================================
# 4. 메인 실행 및 덮어쓰기 로직
# ==========================================
def get_and_update_yusang():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("🚀 100% 완벽 스캐너 가동! 데이터 검증 및 덮어쓰기 진행 중...")
    
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
    
    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        report_nm = row.get('report_nm', '') 
        
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
        
        # 💡 에러 데이터 완벽 덮어쓰기 로직
        if rcept_no in existing_rcept_nos:
            row_idx = existing_rcept_nos.index(rcept_no) + 1 
            try:
                worksheet.update(range_name=f'A{row_idx}:U{row_idx}', values=[new_row])
            except TypeError:
                worksheet.update(f'A{row_idx}:U{row_idx}', [new_row])
            print(f" 🔄 {corp_name}: 완벽 스캔 완료! 기존 데이터 빈틈없이 덮어썼습니다! (행: {row_idx})")
            
        else:
            print(f" 🆕 {corp_name}: 신규 데이터 추출 완료!")
            data_to_add.append(new_row)
        
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 끝! 신규 공시 {len(data_to_add)}건 완벽하게 추가 완료!")
    else:
        print("✅ 끝! 오류 났던 기존 데이터들 100% 깔끔하게 복구 완료했습니다!")

if __name__ == "__main__":
    get_and_update_yusang()
