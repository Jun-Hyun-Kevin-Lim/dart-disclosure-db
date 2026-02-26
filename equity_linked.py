import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# ==========================================
# 1. 초기 설정 및 인증 (GitHub Secrets 연동)
# ==========================================
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# ==========================================
# 2. 공통 도우미 함수 모음
# ==========================================

# --- [1] DART API JSON 호출 함수 (수치/날짜 등 규격 데이터용) ---
def fetch_dart_json(url, params):
    try:
        res = requests.get(url, params=params, timeout=10) # API 무한 대기 방지
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame()


# --- [2] 채권 전용 XML 원문 족집게 파싱 함수 (콜/풋옵션 서술형 데이터용) ---
def extract_bond_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 기본값 세팅 (나중에 'X' 여부로 업데이트 필요성을 판단합니다)
    extracted = {
        'put_option': 'X', 'call_option': 'X', 
        'call_ratio': 'X', 'ytc': 'X', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True, timeout=15)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    
                    # 표(Table) 안의 데이터가 스크래핑될 때 글자가 엉겨 붙는 현상 완벽 방지
                    raw_text = soup.get_text(separator=' | ', strip=True)
                    clean_text = re.sub(r'\s*\|\s*', ' ', raw_text)
                    clean_text = re.sub(r'\s+', ' ', clean_text)
                    
                    def get_option_text(text, keyword_regex, stop_regex, max_chars=600):
                        matches = list(re.finditer(keyword_regex, text, re.IGNORECASE))
                        if not matches: return "X"
                            
                        last_match = matches[-1] # 목차를 피하고 본문을 잡기 위해 마지막 탐색 지점 사용
                        start_idx = last_match.end()
                        window_text = text[start_idx : start_idx + max_chars]
                        
                        stop_match = re.search(stop_regex, window_text, re.IGNORECASE)
                        if stop_match:
                            content = window_text[:stop_match.start()]
                        else:
                            content = window_text
                            
                        content = content.strip()
                        content = re.sub(r'^(?:\s*에\s*관한\s*사항\s*)?[\:\-\|\>\s]*', '', content)
                        
                        # 완벽한 필터링: 내용이 비어있거나 기재생략인 경우 'X' 처리
                        clean_check = re.sub(r'[\s\-\.\:\(\)]', '', content)
                        if not clean_check or clean_check in ['해당사항없음', '해당없음', '없음', '해당사항없음입니다', 'X', '기재생략']:
                            return "X"
                            
                        if len(content) > 400: content = content[:400] + "..."
                        return content

                    # 1 & 2. Put / Call Option 내용 추출 (발생할 수 있는 모든 다음 목차를 방어벽으로 설정)
                    stop_keywords = r'(매도\s*청구권|조기상환\s*청구권|Call\s*Option|Put\s*Option|기타\s*투자판단|당해\s*사채|합병\s*관련|발행회사|신주인수권|교환권|청약|납입|보증)'
                    extracted['put_option'] = get_option_text(clean_text, r'(조기상환\s*청구권|Put\s*Option)', stop_keywords)
                    extracted['call_option'] = get_option_text(clean_text, r'(매도\s*청구권|Call\s*Option)', stop_keywords)
                    
                    # 3. Call 비율 추출
                    if extracted['call_option'] != 'X':
                        ratio_match = re.search(r'([0-9]{1,3}(?:\.[0-9]+)?)\s*%', extracted['call_option'])
                        if ratio_match: extracted['call_ratio'] = ratio_match.group(1) + '%'
                            
                    # 4. YTC (매도청구권 수익률) 추출
                    if extracted['call_option'] != 'X':
                        ytc_match = re.search(r'매도청구권.*?수익률.{0,50}?([0-9]{1,2}(?:\.[0-9]+)?)\s*%', clean_text)
                        if ytc_match: extracted['ytc'] = ytc_match.group(1) + '%'
                            
                    # 5. 투자자(대상자) 추출
                    inv_match = re.search(r'배정\s*대상자.{0,100}?(주식회사\s*\S+|\S+\s*투자조합|\S+\s*펀드|[가-힣]{2,4})', clean_text)
                    if inv_match: extracted['investor'] = inv_match.group(1).strip()
                    elif "제3자배정" in clean_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"채권 XML 에러 ({rcept_no}): {e}")
        
    return extracted


# --- [3] 안전한 숫자 변환 함수 ---
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0


# --- [4] 구글 시트 25개 컬럼 데이터 생성기 (DART API 공식 가이드 100% 매핑) ---
def format_bond_row(row, xml_data, config, cls_map):
    f_map = config['fields'] 
    rcept_no = str(row.get('rcept_no', ''))
    corp_name = row.get('corp_name', '')
    
    # 자금조달의 목적 합산
    fclt = to_int(row.get('fdpp_fclt'))
    bsninh = to_int(row.get('fdpp_bsninh'))
    op = to_int(row.get('fdpp_op'))
    dtrp = to_int(row.get('fdpp_dtrp'))
    ocsa = to_int(row.get('fdpp_ocsa'))
    etc = to_int(row.get('fdpp_etc'))
    
    purposes = []
    if fclt > 0: purposes.append("시설")
    if bsninh > 0: purposes.append("영업양수")
    if op > 0: purposes.append("운영")
    if dtrp > 0: purposes.append("채무상환")
    if ocsa > 0: purposes.append("타법인증권")
    if etc > 0: purposes.append("기타")
    purpose_str = ", ".join(purposes) if purposes else "-"

    face_value = to_int(row.get('bd_fta'))
    face_value_str = f"{face_value:,}" if face_value > 0 else "-"
    
    bd_tm = str(row.get('bd_tm', '')).strip()
    bd_knd = str(row.get('bd_knd', '')).strip()
    product_name = f"제{bd_tm}회차 {bd_knd}" if bd_tm else bd_knd

    shares = to_int(row.get(f_map['shares']))
    shares_str = f"{shares:,}" if shares > 0 else "-"
    
    refix_val = to_int(row.get(f_map['refix'])) if f_map['refix'] else 0
    refix_str = f"{refix_val:,}" if refix_val > 0 else "-"
    
    price_val = to_int(row.get(f_map['price']))
    price_str = f"{price_val:,}" if price_val > 0 else "-"

    link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
    
    return [
        config['type'], corp_name, cls_map.get(row.get('corp_cls', ''), '기타'),
        str(row.get('bddd', '-')), face_value_str, str(row.get('bd_intr_ex', '-')),
        str(row.get('bd_intr_sf', '-')), str(row.get('bd_mtd', '-')), 
        str(row.get(f_map['start'], '-')), str(row.get(f_map['end'], '-')),
        xml_data['put_option'], xml_data['call_option'], xml_data['call_ratio'],
        xml_data['ytc'], str(row.get('bdis_mthn', '-')), product_name,
        price_str, shares_str, str(row.get(f_map['ratio'], '-')), refix_str,
        str(row.get('pymd', '-')), purpose_str, xml_data['investor'], link, rcept_no
    ]


# ==========================================
# 3. 메인 실행 함수
# ==========================================
def get_and_update_bonds():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 주식연계채권(CB, BW, EB) 공시 탐색 중...")
    
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    # DART API 가이드 완벽 매핑
    bond_configs = [
        {'type': 'CB', 'keyword': '전환사채권발행결정', 'endpoint': 'cvbdIsDecsn', 'fields': {'price': 'cv_prc', 'shares': 'cvisstk_cnt', 'ratio': 'cvisstk_tisstk_vs', 'start': 'cvrqpd_bgd', 'end': 'cvrqpd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'BW', 'keyword': '신주인수권부사채권발행결정', 'endpoint': 'bdwtIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'nstk_isstk_cnt', 'ratio': 'nstk_isstk_tisstk_vs', 'start': 'expd_bgd', 'end': 'expd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'EB', 'keyword': '교환사채권발행결정', 'endpoint': 'exbdIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'extg_stkcnt', 'ratio': 'extg_tisstk_vs', 'start': 'exrqpd_bgd', 'end': 'exrqpd_edd', 'refix': ''}} 
    ]

    worksheet = sh.worksheet('주식연계채권')
    # 구글 API 500 에러 완벽 차단 로직 (전체 읽어와서 파이썬으로 인덱싱)
    all_sheet_data = worksheet.get_all_values()
    rcept_row_map = {row[24]: i + 1 for i, row in enumerate(all_sheet_data) if len(row) > 24}
    existing_rcept_nos = list(rcept_row_map.keys()) 
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'} 

    for config in bond_configs:
        print(f"\n[{config['type']}] 데이터 확인 중...")
        df_filtered = all_filings[all_filings['report_nm'].str.contains(config['keyword'], na=False)]
        
        if df_filtered.empty:
            print(f"ℹ️ {config['type']} 공시가 없습니다.")
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        for code in corp_codes:
            time.sleep(0.1) # DART 서버 과부하 방지
            detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
            df_detail = fetch_dart_json(f"https://opendart.fss.or.kr/api/{config['endpoint']}.json", detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
                
        if not detail_dfs: continue
            
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        target_rcept_nos = df_filtered['rcept_no'].unique()
        df_merged = df_combined[df_combined['rcept_no'].isin(target_rcept_nos)]
        
        # ==========================================
        # 🟢 [기능 1] 새로운 공시 데이터 무결점 삽입
        # ==========================================
        new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        data_to_add = []
        
        for _, row in new_data_df.iterrows():
            print(f" -> [신규] {row.get('corp_name', '')} 포매팅 중...")
            time.sleep(0.3)
            xml_data = extract_bond_xml_details(dart_key, str(row.get('rcept_no', '')))
            new_row = format_bond_row(row, xml_data, config, cls_map) 
            data_to_add.append(new_row)
            
        if data_to_add:
            worksheet.append_rows(data_to_add)
            print(f"✅ {config['type']}: 신규 데이터 {len(data_to_add)}건 추가 완료!")

        # ==========================================
        # 🔄 [기능 2] 기존 시트 빈칸(X, -) 스마트 재검토 및 업데이트
        # ==========================================
        existing_data_df = df_merged[df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        update_count = 0
        
        for _, row in existing_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            row_idx = rcept_row_map.get(rcept_no)
            if not row_idx: continue
            
            sheet_row = all_sheet_data[row_idx - 1] 
            needs_update = False
            
            # 파이썬 인덱스 타겟: 10(Put), 11(Call), 12(Call비율), 16(행사가액)
            check_indices = [10, 11, 12, 16] 
            for check_idx in check_indices:
                if len(sheet_row) > check_idx and sheet_row[check_idx] in ['X', '-', '', '없음']:
                    needs_update = True
                    break 
                    
            if needs_update:
                time.sleep(0.3)
                xml_data = extract_bond_xml_details(dart_key, rcept_no)
                updated_row = format_bond_row(row, xml_data, config, cls_map)
                
                # 방어 로직: "기존 칸이 비어있고, 새로 긁어온 값은 진짜 의미 있는 데이터일 때만" 덮어씀
                is_meaningful_update = False
                for check_idx in check_indices:
                    old_val = sheet_row[check_idx] if len(sheet_row) > check_idx else ""
                    new_val = updated_row[check_idx]
                    
                    if old_val in ['X', '-', '', '없음'] and new_val not in ['X', '-', '', '없음']:
                        is_meaningful_update = True
                        break
                
                if is_meaningful_update:
                    print(f" 🔄 [스마트 업데이트] {row.get('corp_name', '')} 완벽한 데이터로 덮어씁니다.")
                    worksheet.update(values=[updated_row], range_name=f'A{row_idx}')
                    update_count += 1
                    time.sleep(1) # 구글 API 쓰기 할당량 초과 100% 방지
                    
        if update_count > 0:
            print(f"✅ {config['type']}: 누락 데이터 {update_count}건 완벽 업데이트 완료!")

if __name__ == "__main__":
    get_and_update_bonds()
