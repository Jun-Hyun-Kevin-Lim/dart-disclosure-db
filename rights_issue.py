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
# 1. 초기 셋팅: API 키와 구글 시트 연결 준비
# ==========================================
# 깃허브 시크릿(비밀창고)에 숨겨둔 DART API 키와 구글 시트 접속 비밀번호를 가져옵니다.
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID'] # 우리가 저장할 구글 엑셀 파일의 고유 주소

# 가져온 비밀번호를 이용해 구글 시트에 로그인(인증)을 합니다.
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id) # 구글 시트 파일을 엽니다.

# ==========================================
# 2. DART 목록 가져오기 함수 (기본 정보 스캐너)
# ==========================================
def fetch_dart_json(url, params):
    """
    DART(전자공시시스템)에 "최근 유상증자 목록 좀 줘!" 라고 요청해서
    받아온 결과물(JSON 형식)을 엑셀 표(데이터프레임)처럼 예쁘게 만들어주는 함수입니다.
    """
    try:
        res = requests.get(url, params=params) # 인터넷 주소로 데이터 달라고 요청!
        if res.status_code == 200: # 200은 '정상적으로 잘 받았음'을 뜻합니다.
            data = res.json()
            # 데이터가 비어있지 않고 잘 왔으면 표 형태로 바꿔서 돌려줍니다.
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}") # 인터넷이 끊기거나 에러가 나면 이유를 출력합니다.
    return pd.DataFrame() # 실패하면 빈 표를 돌려줍니다.

# ==========================================
# 3. 💡 핵심 엔진: 공시 원문(표 구조) 족집게 스캐너
# ==========================================
def extract_xml_details(api_key, rcept_no):
    """
    접수번호를 받아서 해당 공시의 진짜 원본 문서(XML) 압축파일을 다운받은 뒤,
    우리가 원하는 '가격, 할인율, 날짜'를 표(Table) 구조에 맞춰서 쏙쏙 뽑아내는 함수입니다.
    """
    # 원문 다운로드 주소
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 데이터를 못 찾았을 때 기본으로 출력할 값들 (기본값은 빈칸인 '-'로 설정)
    extracted = {
        'board_date': '-', 'issue_price': '-', 'base_price': '-', 'discount': '-',
        'pay_date': '-', 'div_date': '-', 'list_date': '-', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True) # 압축파일 다운로드
        if res.status_code == 200:
            # ZIP 압축파일을 풀어서 안에 있는 XML 문서만 꺼냅니다.
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    # 컴퓨터가 읽기 편하게 BeautifulSoup 이라는 도구로 문서를 정리합니다.
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    
                    # 뽑아낸 날것의 데이터를 임시로 담아둘 바구니
                    raw_data = {}
                    
                    # 💡 [표 구조 분석] 문서 안에 있는 모든 표의 '줄(tr)'을 하나씩 읽어봅니다.
                    for tr in soup.find_all('tr'):
                        cells = tr.find_all(['th', 'td']) # 한 줄에 있는 모든 '칸(셀)'들
                        
                        # 각 칸을 왼쪽부터 오른쪽으로 하나씩 확인합니다.
                        for i in range(len(cells)):
                            # 현재 칸의 글자를 가져옵니다. (예: "기준주가")
                            header_raw = cells[i].get_text(strip=True)
                            # 띄어쓰기를 전부 없애서 빽빽하게 만듭니다. (검색을 쉽게 하기 위해)
                            header_clean = re.sub(r'\s+', '', header_raw.replace('\xa0', ''))
                            
                            # 만약 현재 칸 오른쪽에 데이터가 들어갈 칸이 남아있다면?
                            if i + 1 < len(cells):
                                # 무조건 '현재 칸의 오른쪽 칸'에 있는 글자만 긁어옵니다! (다른 줄 침범 방지)
                                val_raw = " ".join([c.get_text(separator=' ', strip=True) for c in cells[i+1:]])
                                
                                # 만약 왼쪽 칸 제목이 '발행가액'과 관련된 단어라면, 오른쪽 칸의 값을 바구니에 담습니다.
                                if 'issue_price' not in raw_data and re.search(r'(1주당|확정|예정|모집|발행|신주).*발행가액', header_clean):
                                    raw_data['issue_price'] = val_raw
                                # 제목이 '기준주가'나 '기준단가'라면?
                                elif 'base_price' not in raw_data and re.search(r'^기준(주가|발행가액|가액|단가|주당가액)', header_clean):
                                    raw_data['base_price'] = val_raw
                                # 제목이 '할인율'이나 '할증률'이라면?
                                elif 'discount' not in raw_data and re.search(r'(할인|할증)[율률]', header_clean):
                                    raw_data['discount'] = val_raw
                                    raw_data['discount_header'] = header_clean # 나중에 부호 판별을 위해 제목도 같이 저장
                                # 제목이 '이사회결의일' 이라면?
                                elif 'board_date' not in raw_data and re.search(r'(최초)?이사회결의일', header_clean):
                                    raw_data['board_date'] = val_raw
                                # 제목이 '납입일' 이라면?
                                elif 'pay_date' not in raw_data and re.search(r'(납입일|주금납입기일)', header_clean):
                                    raw_data['pay_date'] = val_raw
                                # 제목이 '배당기산일' 이라면?
                                elif 'div_date' not in raw_data and re.search(r'(신주의)?배당기산일', header_clean):
                                    raw_data['div_date'] = val_raw
                                # 제목이 '상장 예정일' 이라면?
                                elif 'list_date' not in raw_data and re.search(r'(신주권교부예정일|신주의상장예정일|상장예정일|신주상장예정일)', header_clean):
                                    raw_data['list_date'] = val_raw

                    # --- [클리닝 1] 뽑아온 가격 데이터 예쁘게 다듬기 ---
                    def clean_price(text):
                        if not text: return '-' # 비어있으면 빈칸(-) 반환
                        text_clean = re.sub(r'[\s,원]', '', text) # 띄어쓰기, 쉼표, '원' 글자 전부 제거
                        # '미정'이나 '해당없음'이라고 적혀있으면 억지로 숫자 안 찾고 빈칸 처리!
                        if re.search(r'^(미정|해당없음|기재생략|-)', text_clean): return '-'
                        
                        # 100 이상의 진짜 숫자만 찾아내기
                        nums = re.findall(r'(?<!\d)([1-9]\d{2,})(?!\d)', text_clean)
                        for val_str in nums:
                            val = int(val_str)
                            # 2024, 2025 같은 연도는 가격이 아니니까 무시하고, 진짜 가격이면 콤마(,) 찍어서 반환
                            if val not in [2023, 2024, 2025, 2026, 2027]:
                                return f"{val:,}"
                        return '-'
                        
                    # --- [클리닝 2] 뽑아온 날짜 데이터 예쁘게 다듬기 ---
                    def clean_date(text):
                        if not text: return '-'
                        text_clean = re.sub(r'\s+', '', text)
                        if re.search(r'^(미정|해당없음|기재생략|-)', text_clean): return '-'
                        
                        # 2026.04.17 이나 2026/04/17 같은 모양의 날짜를 찾아서 '2026년 04월 17일' 모양으로 통일
                        m = re.search(r'(20[2-3]\d)[\-\.년/]([0-1]?\d)[\-\.월/]([0-3]?\d)', text_clean)
                        if m:
                            y, m_num, d_num = m.groups()
                            return f"{y}년 {m_num.zfill(2)}월 {d_num.zfill(2)}일" # 1월을 01월로 맞춰줌(zfill)
                        return '-'
                        
                    # --- [클리닝 3] 뽑아온 할인율(할증률) 팩트 체크 및 다듬기 ---
                    def clean_discount(text, issue_p, base_p, header_text):
                        if not text: return '-'
                        text_clean = re.sub(r'\s+', '', text)
                        if re.search(r'^(미정|해당사항없음|해당없음|기재생략)', text_clean): return "0.00%"
                        
                        # 팩트 체크를 위한 보조 수단: 발행가와 기준주가를 살짝 비교해봅니다.
                        math_sign = 0
                        if issue_p != '-' and base_p != '-':
                            try:
                                i_v = float(issue_p.replace(',', ''))
                                b_v = float(base_p.replace(',', ''))
                                if b_v > 0:
                                    if i_v > b_v: math_sign = 1    # 비싸면 할증(+)
                                    elif i_v < b_v: math_sign = -1 # 싸면 할인(-)
                            except: pass
                            
                        # 오른쪽 칸에 적힌 숫자와 부호(+,-)를 쏙 뽑아옵니다.
                        m = re.search(r'([+\-]?\d+(?:\.\d+)?)', text_clean)
                        if m:
                            val_str = m.group(1)
                            try: val = float(val_str)
                            except: return '-'
                            
                            if val == 0: return "0.00%"
                            if abs(val) > 100: return '-' # 페이지 번호 같은 쓰레기값 무시
                            
                            # 부호 판별!
                            if math_sign != 0:
                                return f"{abs(val) * math_sign:+.2f}%" # 확실하면 수학적 팩트 따르기
                            else:
                                if '-' in val_str: return f"{val:.2f}%" # 마이너스 있으면 그대로 출력
                                elif '+' in val_str: return f"{val:+.2f}%" # 플러스 있으면 그대로 출력
                                else:
                                    # 기호가 없으면 표 왼쪽 제목에 적힌 '할증'/'할인' 단어 보고 유추
                                    if '할증' in header_text and '할인' not in header_text: return f"+{val:.2f}%"
                                    else: return f"-{abs(val):.2f}%" # 웬만하면 할인(-) 처리
                        
                        if text_clean == '-': return "0.00%"
                        return '-'

                    # 위에서 만든 클리닝 기계를 돌려서 최종 데이터를 깔끔하게 추출 바구니(extracted)에 넣습니다.
                    extracted['issue_price'] = clean_price(raw_data.get('issue_price'))
                    extracted['base_price'] = clean_price(raw_data.get('base_price'))
                    extracted['discount'] = clean_discount(raw_data.get('discount'), extracted['issue_price'], extracted['base_price'], raw_data.get('discount_header', ''))
                    extracted['board_date'] = clean_date(raw_data.get('board_date'))
                    extracted['pay_date'] = clean_date(raw_data.get('pay_date'))
                    extracted['div_date'] = clean_date(raw_data.get('div_date'))
                    extracted['list_date'] = clean_date(raw_data.get('list_date'))
                    
                    # 투자자 추출: 문서 전체에 '제3자배정'이라는 말이 있으면 무조건 적어줌
                    full_text = soup.get_text(separator=' ', strip=True).replace(' ', '')
                    if "제3자배정" in full_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}") # 중간에 문서 열다가 터지면 에러 표시
        
    return extracted # 꽉꽉 채워진 8개 데이터를 돌려줍니다.

# ==========================================
# 4. 숫자 변환 보조 함수
# ==========================================
def to_int(val):
    """표에 있는 글자(예: 1,000)를 진짜 컴퓨터 숫자(1000)로 바꿔서 덧셈 뺄셈이 가능하게 해줍니다."""
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0 # 실패하면 그냥 0

# ==========================================
# 5. 메인 로직: 유상증자 데이터 수집 & 구글 시트 업데이트
# ==========================================
def get_and_update_yusang():
    # 1. 날짜 설정: 오늘부터 12일 전까지의 공시만 뒤져봅니다.
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 유상증자 공시 탐색 중 (표 구조 분석 & 덮어쓰기 모드 작동)...")
    
    # 2. DART에 "12일치 공시 목록 다 가져와!" 라고 요청합니다.
    # last_reprt_at: 'Y' 설정으로 옛날 꺼 말고 '가장 최신 정정보고서'만 가져오게 합니다.
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100',
        'last_reprt_at': 'Y' 
    }
    all_filings = fetch_dart_json(list_url, list_params)

    # 목록이 하나도 없으면 여기서 프로그램 종료
    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    # 3. 가져온 목록 중에서 제목에 '유상증자결정'이 들어간 것만 체에 걸러냅니다.
    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)].copy()
    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return
        
    df_filtered['corp_cls'] = df_filtered['corp_cls'].fillna('')
        
    # 4. 걸러진 회사들의 고유번호를 모아서, 이번엔 "유상증자 상세 데이터 줘!" 라고 다시 요청합니다.
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
        
    # 상세 데이터들을 다 모아서 하나로 큼직하게 합칩니다.
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    df_combined = df_combined.drop(columns=['corp_cls'], errors='ignore')
    
    # 아까 가져온 목록 데이터(보고서명 등)와 상세 데이터를 '접수번호'를 기준으로 짝맞춰 합칩니다.
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'corp_cls', 'report_nm']], on='rcept_no', how='left')
    
    # 5. 구글 시트의 '유상증자' 탭을 엽니다.
    worksheet = sh.worksheet('유상증자')
    # 21번째 칸(U열)에 적혀있는 기존 공시들의 접수번호를 전부 가져와서 기억해둡니다. (중복 방지 & 덮어쓰기 용도)
    existing_rcept_nos = worksheet.col_values(21) 
    
    data_to_add = [] # 완전 새로운 공시를 담을 대기열
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    # 6. 합쳐진 데이터를 한 줄씩 읽으면서 구글 시트에 넣을 준비를 합니다.
    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        report_nm = row.get('report_nm', '') 
        
        # 💡 [필살기 출동] 표 구조(DOM) 스캐너를 보내서 원본 팩트 8개를 캐옵니다!
        xml_data = extract_xml_details(dart_key, rcept_no)
        
        # 남은 데이터들 계산하기 (보통주, 기타주 합치기 등)
        market = cls_map.get(row.get('corp_cls', ''), '기타')
        method = row.get('ic_mthn', '')
        
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk # 신규발행주식수 = 보통주 + 기타주
        product = "보통주" if ostk > 0 else "기타주"
        
        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk # 증자전 주식수 = 보통주 + 기타주
        
        new_shares_str = f"{new_shares:,}"
        old_shares_str = f"{old_shares:,}"
        
        # 증자비율 계산: (신규발행주식수 / 증자전 주식수) * 100
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"
        
        # 자금 용도별 금액들 가져오기
        fclt = to_int(row.get('fdpp_fclt'))
        bsninh = to_int(row.get('fdpp_bsninh'))
        op = to_int(row.get('fdpp_op'))
        dtrp = to_int(row.get('fdpp_dtrp'))
        ocsa = to_int(row.get('fdpp_ocsa'))
        etc = to_int(row.get('fdpp_etc'))
        
        # 확정발행금액 계산 (단위: 억원으로 맞춤)
        total_amt = fclt + bsninh + op + dtrp + ocsa + etc
        total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "0.00"
        
        # 금액이 0보다 큰 용도만 골라서 글자로 예쁘게 이어붙임 (예: "시설, 운영")
        purposes = []
        if fclt > 0: purposes.append("시설")
        if bsninh > 0: purposes.append("영업양수")
        if op > 0: purposes.append("운영")
        if dtrp > 0: purposes.append("채무상환")
        if ocsa > 0: purposes.append("타법인증권")
        if etc > 0: purposes.append("기타")
        purpose_str = ", ".join(purposes)
        
        # 공시 원문 링크 주소 만들기
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        # 7. 구글 시트 1줄(21칸) 세팅 완료!
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
            rcept_no                             # 21. 접수번호 (기준표)
        ]
        
        # 8. 💡 구글 시트에 넣기 (덮어쓰기 vs 새로 추가)
        if rcept_no in existing_rcept_nos:
            # 시트에 이미 있다면? 예전에 틀리거나 비어있던 값일 수 있으니 '완벽한 최신 데이터로 덮어쓰기' 합니다.
            row_idx = existing_rcept_nos.index(rcept_no) + 1 # 몇 번째 줄인지 찾기 (+1은 엑셀은 1줄부터 시작하니까)
            try:
                worksheet.update(range_name=f'A{row_idx}:U{row_idx}', values=[new_row])
            except TypeError:
                worksheet.update(f'A{row_idx}:U{row_idx}', [new_row])
            print(f" 🔄 {corp_name}: 기존 오류 데이터 검증 및 완벽 덮어쓰기 완료! (행: {row_idx})")
            
        else:
            # 시트에 없다면? 신규 데이터니까 대기열 바구니에 차곡차곡 담습니다.
            print(f" 🆕 {corp_name}: 신규 데이터 추출 완료!")
            data_to_add.append(new_row)
        
    # 9. 마지막 작업: 신규 대기열에 담긴 데이터가 있다면 맨 밑에 한방에 추가!
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 공시 {len(data_to_add)}건 추가 완료!")
    else:
        print("✅ 유상증자: 새 공시는 없으며, 완벽한 표 구조 분석으로 기존 데이터 수정을 완료했습니다!")

# 파이썬 프로그램이 시작되는 곳
if __name__ == "__main__":
    get_and_update_yusang()
