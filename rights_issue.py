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
# 1. 초기 설정 및 API 인증
# ==========================================
# GitHub Secrets 등에 저장된 환경변수(API 키, 인증서 등)를 불러옵니다.
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

# 구글 시트 API에 로그인하고 연동할 시트(파일)를 엽니다.
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# ==========================================
# 2. DART API JSON 데이터 호출 함수
# ==========================================
def fetch_dart_json(url, params):
    """주어진 URL과 파라미터로 DART API를 호출해 JSON 결과를 판다스 데이터프레임으로 반환합니다."""
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame() # 에러 시 빈 데이터 반환

# ==========================================
# 3. 핵심 엔진: 공시 원문(XML) 족집게 스캐너
# ==========================================
def extract_xml_details(api_key, rcept_no):
    """
    접수번호(rcept_no)를 받아 해당 공시의 압축파일(ZIP)을 다운로드하고,
    그 안의 XML 문서를 까서 '가격, 할인율, 날짜, 투자자'를 정밀하게 추출합니다.
    """
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 데이터를 못 찾았을 때 기본으로 출력할 값들 (빈칸)
    extracted = {
        'board_date': '-', 'issue_price': '-', 'base_price': '-', 'discount': '-',
        'pay_date': '-', 'div_date': '-', 'list_date': '-', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            # ZIP 파일 메모리상에서 압축 해제 후 XML 파일 찾기
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    
                    # 표(Table) 안의 데이터가 텍스트로 변환될 때 다닥다닥 붙는 것을 방지하기 위해 강제 띄어쓰기 삽입
                    for tag in soup.find_all(['td', 'th', 'p', 'div', 'span']):
                        tag.append(' ')
                        
                    raw_text = soup.get_text(separator=' ', strip=True)
                    # 정규화된 텍스트: 여러 개의 띄어쓰기를 하나로 압축
                    clean_text = re.sub(r'\s+', ' ', raw_text)
                    # 완전 무공백 텍스트: 보이지 않는 특수공백까지 박살내어 할인율 등 정밀 검색에 사용
                    text_no_space = re.sub(r'\s+', '', raw_text.replace('\xa0', '').replace('\u200b', ''))
                    
                    # --- [방어막 로직] 해당 값이 진짜 '빈칸(미정)'인지 검증 ---
                    def is_empty_value(text_window):
                        # 탐색한 단어 바로 뒤에 '미정', '해당없음' 등이 있으면 엉뚱한 값을 찾지 않도록 차단
                        check_win = re.sub(r'[\s,]', '', text_window)[:15]
                        return bool(re.match(r'^(미정|해당사항없음|해당없음|기재생략)', check_win))

                    # --- 3-1. 가격/금액 추출기 ---
                    def get_price(keyword):
                        for match in re.finditer(keyword, clean_text):
                            window = clean_text[match.end():match.end()+200] # 키워드 뒤 200글자 스캔
                            
                            if is_empty_value(window): return '-' # 빈칸이면 탐색 포기
                                
                            win_clean = re.sub(r'[\s,]', '', window) # '5, 0 0 0' 같은 악랄한 띄어쓰기 복구
                            # 숫자만 쏙 골라내기 (100 미만의 숫자는 연도/페이지번호일 수 있어 제외)
                            nums = re.findall(r'(?<!\d)([1-9]\d{2,})(?!\d)', win_clean)
                            for val_str in nums:
                                val = int(val_str)
                                if val not in [2023, 2024, 2025, 2026, 2027]: # 연도가 아니면 콤마 찍어서 반환
                                    return f"{val:,}"
                        return '-'
                        
                    extracted['issue_price'] = get_price(r'(?:1\s*주\s*당|확\s*정|예\s*정|모\s*집|발\s*행|신\s*주).{0,10}?발\s*행\s*가\s*(?:액)?')
                    extracted['base_price'] = get_price(r'기\s*준\s*(?:주\s*가|발\s*행\s*가\s*액|가\s*액|단\s*가|주\s*당\s*가\s*액)')
                    
                    # --- 3-2. 할인율(할증률) 팩트 추출기 ---
                    def get_discount():
                        # '할인율 또는 할증율(%)' 등의 단어를 찾음
                        pattern = r'(?:할인|할증)[율률](?:또는할증[율률]|또는할인[율률])?(?:\(%\))?'
                        for match in re.finditer(pattern, text_no_space):
                            window = text_no_space[match.end():match.end()+100] # 뒤 100글자 이내 탐색
                            
                            if is_empty_value(window): return "0.00%"
                                
                            # 숫자에 마이너스(-), 플러스(+)가 붙어있든 안 붙어있든 그대로 추출
                            m = re.search(r'^([^\d]{0,15})([+\-]?\d+(?:\.\d+)?)', window)
                            if m:
                                val_str = m.group(2)
                                try: val = float(val_str)
                                except: return '-'
                                
                                if val == 0: return "0.00%"
                                if abs(val) > 100: continue # 비정상적으로 큰 숫자는 무시
                                
                                # 원문 팩트에 맞춰 부호 결정
                                if '-' in val_str: return f"{val:.2f}%"
                                elif '+' in val_str: return f"{val:+.2f}%"
                                else:
                                    # 부호가 아예 안 적힌 경우 텍스트 문맥(할증/할인)으로 유추
                                    if '할증' in match.group(0) and '할인' not in match.group(0): return f"+{val:.2f}%"
                                    else: return f"-{abs(val):.2f}%"
                        return '-'
                        
                    extracted['discount'] = get_discount()
                    
                    # --- 3-3. 날짜 추출기 ---
                    def get_date(keyword):
                        for match in re.finditer(keyword, clean_text):
                            window = clean_text[match.end():match.end()+200]
                            
                            if is_empty_value(window): return '-'
                                
                            win_clean = window.replace(' ', '')
                            # 2026.04.17 또는 2026/04/17 포맷 모두 캐치
                            m = re.search(r'(20[2-3]\d)[\-\.년/]([0-1]?\d)[\-\.월/]([0-3]?\d)', win_clean)
                            if m:
                                y, m_num, d_num = m.groups()
                                return f"{y}년 {m_num.zfill(2)}월 {d_num.zfill(2)}일"
                        return '-'
                        
                    extracted['board_date'] = get_date(r'(?:최\s*초\s*)?이\s*사\s*회\s*결\s*의\s*일')
                    extracted['pay_date'] = get_date(r'(납\s*입\s*일|주\s*금\s*납\s*입\s*기\s*일)')
                    extracted['div_date'] = get_date(r'(?:신\s*주\s*의\s*)?배\s*당\s*기\s*산\s*일')
                    # '상장 예정일' 뿐만 아니라 '교부 예정일' 같은 변칙 표현도 대응
                    extracted['list_date'] = get_date(r'(?:신\s*주\s*의\s*)?(?:상\s*장|교\s*부)\s*예\s*정\s*일')
                    
                    # --- 3-4. 투자자 추출 ---
                    if "제3자배정" in clean_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# ==========================================
# 4. 유틸리티 함수
# ==========================================
def to_int(val):
    """빈칸이나 문자가 섞인 값을 안전하게 정수형으로 변환합니다."""
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

# ==========================================
# 5. 메인 로직: 유상증자 데이터 수집 및 업데이트
# ==========================================
def get_and_update_yusang():
    # 최근 12일간의 데이터를 조회합니다.
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 유상증자 공시 탐색 중 (팩트 스캐너 & 자가 치유 모드 작동)...")
    
    # DART 목록 API 호출 (last_reprt_at: 'Y' 로 정정공시의 최종본만 가져옴)
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

    # '유상증자결정' 키워드가 들어간 보고서만 필터링
    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)].copy()
    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return
        
    df_filtered['corp_cls'] = df_filtered['corp_cls'].fillna('')
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    # 필터링된 회사들의 고유번호를 이용해 '상세 정보 JSON' 호출
    for code in corp_codes:
        detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
        df_detail = fetch_dart_json('https://opendart.fss.or.kr/api/piicDecsn.json', detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return
        
    # 데이터프레임 병합: 상세 데이터 + (접수번호, 상장시장, 보고서명)
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    df_combined = df_combined.drop(columns=['corp_cls'], errors='ignore')
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'corp_cls', 'report_nm']], on='rcept_no', how='left')
    
    # 구글 시트에 연결
    worksheet = sh.worksheet('유상증자')
    existing_rcept_nos = worksheet.col_values(21) # 21번째 U열(접수번호)를 기준으로 기존 여부 확인
    
    data_to_add = [] # 구글 시트에 새롭게 추가될 행들을 담는 리스트
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    # --------------------------------------------------
    # 데이터 추출 및 가공 시작
    # --------------------------------------------------
    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        report_nm = row.get('report_nm', '') 
        
        # 앞서 만든 XML 스캐너(extract_xml_details)를 가동
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
        
        # 자금조달 목적(용도)을 콤마로 연결
        purposes = []
        if fclt > 0: purposes.append("시설")
        if bsninh > 0: purposes.append("영업양수")
        if op > 0: purposes.append("운영")
        if dtrp > 0: purposes.append("채무상환")
        if ocsa > 0: purposes.append("타법인증권")
        if etc > 0: purposes.append("기타")
        purpose_str = ", ".join(purposes)
        
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        # 최종적으로 구글 시트 1행을 구성할 21칸의 데이터 리스트
        new_row = [
            corp_name,                           # 1. 회사명
            report_nm,                           # 2. 보고서명
            market,                              # 3. 상장시장
            xml_data['board_date'],              # 4. 최초 이사회결의일
            method,                              # 5. 증자방식
            product,                             # 6. 발행상품
            new_shares_str,                      # 7. 신규발행주식수
            xml_data.get('issue_price', '-'),    # 8. 확정발행가(원)
            xml_data.get('base_price', '-'),     # 9. 기준주가
            total_amt_uk,                        # 10. 확정발행금액(억원)
            xml_data.get('discount', '-'),       # 11. 할인(할증률)
            old_shares_str,                      # 12. 증자전 주식수
            ratio,                               # 13. 증자비율
            xml_data['pay_date'],                # 14. 납입일
            xml_data['div_date'],                # 15. 배당기산일
            xml_data['list_date'],               # 16. 신주의 상장 예정일
            xml_data['board_date'],              # 17. 이사회결의일
            purpose_str,                         # 18. 자금용도
            xml_data['investor'],                # 19. 투자자
            link,                                # 20. 링크
            rcept_no                             # 21. 접수번호 (기준 식별자)
        ]
        
        # --------------------------------------------------
        # 구글 시트 업로드 (자가 치유 및 신규 추가)
        # --------------------------------------------------
        # 1. 시트에 동일한 접수번호가 이미 존재하는 경우 -> 해당 행을 최신 데이터로 '덮어쓰기'
        if rcept_no in existing_rcept_nos:
            row_idx = existing_rcept_nos.index(rcept_no) + 1 # 1번 행부터 시작하므로 +1
            try:
                # gspread 버전에 맞게 유연한 업데이트 처리
                worksheet.update(range_name=f'A{row_idx}:U{row_idx}', values=[new_row])
            except TypeError:
                worksheet.update(f'A{row_idx}:U{row_idx}', [new_row])
            print(f" 🔄 {corp_name}: 기존 오류 데이터 재스캔 및 완벽 덮어쓰기 완료! (행: {row_idx})")
            
        # 2. 시트에 없는 새로운 접수번호인 경우 -> 신규 추가 대기열에 담기
        else:
            print(f" 🆕 {corp_name}: 신규 데이터 추출 완료!")
            data_to_add.append(new_row)
        
    # 루프 종료 후, 신규 추가할 데이터가 있다면 한꺼번에 맨 밑에 삽입
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 공시 {len(data_to_add)}건 추가 완료!")
    else:
        print("✅ 유상증자: 새 공시는 없으며, 기존 공시들의 재검토 및 오류 수정을 완료했습니다!")

if __name__ == "__main__":
    get_and_update_yusang()
