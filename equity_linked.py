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
# 1. 초기 설정 및 인증 (GitHub Secrets 연동)
# ==========================================
# GitHub 저장소에 안전하게 숨겨둔 환경변수(비밀번호/키)를 불러옵니다.
dart_key = os.environ['DART_API_KEY']                # DART API 발급 키
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON'] # 구글 서비스 계정 JSON (문자열 형태)
sheet_id = os.environ['GOOGLE_SHEET_ID']             # 데이터를 넣을 구글 시트의 고유 ID

# 문자열로 된 구글 인증 정보를 딕셔너리 형태로 변환하여 인증을 진행합니다.
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id) # 구글 시트 파일 열기

# ==========================================
# 2. 공통 도우미 함수 모음
# ==========================================

# --- [1] DART API JSON 호출 함수 ---
def fetch_dart_json(url, params):
    """주어진 URL과 파라미터로 DART API를 호출하고, 결과를 Pandas DataFrame으로 반환합니다."""
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            # API 응답 상태가 '000'(정상)이고 데이터 리스트가 존재할 때만 데이터프레임으로 변환
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    # 에러가 나거나 데이터가 없으면 빈 데이터프레임 반환 (코드 멈춤 방지)
    return pd.DataFrame()


# --- [2] 채권 전용 XML 원문 족집게 파싱 함수 ---
def extract_bond_xml_details(api_key, rcept_no):
    """공시 원문(XML) 문서를 다운받아 압축을 풀고, 콜/풋옵션 및 투자자 정보를 텍스트 마이닝으로 찾아냅니다."""
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    # 찾지 못했을 때 들어갈 기본값 세팅 (나중에 이 'X'를 기준으로 업데이트를 판단합니다)
    extracted = {
        'put_option': 'X', 'call_option': 'X', 
        'call_ratio': 'X', 'ytc': 'X', 'investor': '원문참조'
    }
    
    try:
        # stream=True로 대용량 파일(ZIP)을 메모리에 조각조각 받습니다.
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                # 압축 풀기 후 .xml 확장자를 가진 파일의 이름만 찾습니다.
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    
                    # HTML 테이블 태그 안에서 글자들이 엉겨 붙는 것을 막기 위해 공백을 강제로 하나씩 넣어줍니다.
                    for tag in soup.find_all(['td', 'th', 'p', 'div']):
                        tag.append(' ')
                        
                    raw_text = soup.get_text(separator=' ', strip=True)
                    clean_text = re.sub(r'\s+', ' ', raw_text) # 불필요한 연속 공백을 1칸으로 압축
                    
                    # 내부 함수: 시작 키워드부터 다음 목차(정지 키워드) 직전까지만 정확히 잘라오는 정규식 로직
                    def get_option_text(text, keyword_regex, stop_regex, max_chars=500):
                        matches = list(re.finditer(keyword_regex, text, re.IGNORECASE))
                        if not matches: return "X" # 시작 키워드가 없으면 무조건 X
                            
                        last_match = matches[-1] # 문서 후반부의 진짜 설명 파트를 타겟팅하기 위해 마지막 매치 사용
                        start_idx = last_match.end()
                        window_text = text[start_idx : start_idx + max_chars] # 일단 넉넉하게 뒤로 잘라옴
                        
                        # 잘라온 텍스트 안에서 '다음 항목'의 제목이 나오면 그 직전까지만 다시 자름
                        stop_match = re.search(stop_regex, window_text, re.IGNORECASE)
                        if stop_match:
                            content = window_text[:stop_match.start()]
                        else:
                            content = window_text
                            
                        # 문자열 양끝 공백 제거 및 서론에 붙는 쓸데없는 기호 청소
                        content = content.strip()
                        content = re.sub(r'^(?:\s*에\s*관한\s*사항\s*)?[\:\-\|\>\s]*', '', content)
                        
                        # 💡 핵심: 기호를 다 떼어내고 진짜 내용이 있는지 검사. '해당사항없음' 등 허수 데이터 완벽 필터링
                        clean_check = re.sub(r'[\s\-\.\:\(\)]', '', content)
                        if not clean_check or clean_check in ['해당사항없음', '해당없음', '없음', '해당사항없음입니다', 'X']:
                            return "X"
                            
                        # 내용이 너무 길면 시트가 깨지므로 400자 언저리에서 요약 처리
                        if len(content) > 400: content = content[:400] + "..."
                        return content

                    # 1. Put Option (조기상환청구권) 마이닝
                    put_stop = r'(매도\s*청구권|Call\s*Option|기타\s*투자판단|당해\s*사채|합병\s*관련|발행회사)'
                    extracted['put_option'] = get_option_text(clean_text, r'(조기상환\s*청구권|Put\s*Option)', put_stop)
                    
                    # 2. Call Option (매도청구권) 마이닝
                    call_stop = r'(조기상환\s*청구권|Put\s*Option|기타\s*투자판단|당해\s*사채|합병\s*관련|발행회사)'
                    extracted['call_option'] = get_option_text(clean_text, r'(매도\s*청구권|Call\s*Option)', call_stop)
                    
                    # 3. Call 비율 (콜옵션 내용 안에 있는 % 숫자 찾기)
                    if extracted['call_option'] != 'X':
                        ratio_match = re.search(r'([0-9]{1,3}(?:\.[0-9]+)?)\s*%', extracted['call_option'])
                        if ratio_match: extracted['call_ratio'] = ratio_match.group(1) + '%'
                            
                    # 4. YTC (수익률 마이닝)
                    if extracted['call_option'] != 'X':
                        ytc_match = re.search(r'매도청구권.*?수익률.{0,50}?([0-9]{1,2}(?:\.[0-9]+)?)\s*%', clean_text)
                        if ytc_match: extracted['ytc'] = ytc_match.group(1) + '%'
                            
                    # 5. 투자자 마이닝 (특정 명칭 패턴을 찾거나 제3자배정을 감지)
                    inv_match = re.search(r'배정\s*대상자.{0,100}?(주식회사\s*\S+|\S+\s*투자조합|\S+\s*펀드|[가-힣]{2,4})', clean_text)
                    if inv_match: extracted['investor'] = inv_match.group(1).strip()
                    elif "제3자배정" in clean_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"채권 XML 에러 ({rcept_no}): {e}")
        
    return extracted


# --- [3] 안전한 숫자 변환 함수 ---
def to_int(val):
    """콤마가 포함된 문자열이나 NaN 값을 안전하게 정수(int)로 변환해주는 방어용 함수입니다."""
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0


# --- [4] 25개 구글 시트 컬럼 양식에 맞게 포매팅하는 함수 ---
def format_bond_row(row, xml_data, config, cls_map):
    """판다스 한 줄(row)과 파싱한 XML 데이터를 합쳐서 구글 시트에 넣을 25칸짜리 리스트 1줄을 만듭니다."""
    f_map = config['fields'] # CB, BW, EB 종류에 따라 다른 DART API 필드명을 가져옴
    rcept_no = str(row.get('rcept_no', ''))
    corp_name = row.get('corp_name', '')
    
    # 자금 조달 목적 6가지를 가져와서 합산 (예: 시설자금, 운영자금 등)
    fclt = to_int(row.get('fdpp_fclt'))
    bsninh = to_int(row.get('fdpp_bsninh'))
    op = to_int(row.get('fdpp_op'))
    dtrp = to_int(row.get('fdpp_dtrp'))
    ocsa = to_int(row.get('fdpp_ocsa'))
    etc = to_int(row.get('fdpp_etc'))
    
    # 0보다 큰 자금용도만 찾아내서 글자로 변환
    purposes = []
    if fclt > 0: purposes.append("시설")
    if bsninh > 0: purposes.append("영업양수")
    if op > 0: purposes.append("운영")
    if dtrp > 0: purposes.append("채무상환")
    if ocsa > 0: purposes.append("타법인증권")
    if etc > 0: purposes.append("기타")
    purpose_str = ", ".join(purposes) if purposes else "-"

    # 권면총액 포매팅 (천 단위 콤마)
    face_value = to_int(row.get('bd_fta'))
    face_value_str = f"{face_value:,}" if face_value > 0 else "-"
    
    # 발행상품명 조합 (예: 제3회차 + 무기명식 이권부 무보증 사모 전환사채)
    bd_tm = str(row.get('bd_tm', '')).strip()
    bd_knd = str(row.get('bd_knd', '')).strip()
    product_name = f"제{bd_tm}회차 {bd_knd}" if bd_tm else bd_knd

    # 전환/행사 주식수 포매팅
    shares = to_int(row.get(f_map['shares']))
    shares_str = f"{shares:,}" if shares > 0 else "-"
    
    # 리픽싱(조정가액) 포매팅
    refix_val = to_int(row.get(f_map['refix'])) if f_map['refix'] else 0
    refix_str = f"{refix_val:,}" if refix_val > 0 else "-"
    
    # 단가(행사가액/전환가액) 포매팅
    price_val = to_int(row.get(f_map['price']))
    price_str = f"{price_val:,}" if price_val > 0 else "-"

    # 다트 공시 원문 링크 생성
    link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
    
    # 구글 시트에 들어갈 최종 순서 (25개 컬럼)
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
    # 최근 12일 기간 설정
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 주식연계채권(CB, BW, EB) 공시 탐색 중...")
    
    # 1차 작업: DART에 등록된 기간 내 모든 주요사항보고서 리스트를 한 번에 가져옴
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    # 각 채권 종류별로 다르게 쓰이는 DART API endpoint 및 내부 키값 매핑 사전
    bond_configs = [
        {'type': 'CB', 'keyword': '전환사채권발행결정', 'endpoint': 'cvbdIsDecsn', 'fields': {'price': 'cv_prc', 'shares': 'cvisstk_cnt', 'ratio': 'cvisstk_tisstk_vs', 'start': 'cvrqpd_bgd', 'end': 'cvrqpd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'BW', 'keyword': '신주인수권부사채권발행결정', 'endpoint': 'bdwtIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'nstk_isstk_cnt', 'ratio': 'nstk_isstk_tisstk_vs', 'start': 'expd_bgd', 'end': 'expd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'EB', 'keyword': '교환사채권발행결정', 'endpoint': 'exbdIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'extg_stkcnt', 'ratio': 'extg_tisstk_vs', 'start': 'exrqpd_bgd', 'end': 'exrqpd_edd', 'refix': ''}} # EB는 보통 리픽싱이 없음
    ]

    # 구글 시트 연결
    worksheet = sh.worksheet('주식연계채권')
    
    # 💡 500 에러 우회: 단일 열(.col_values)만 요구하지 않고, 시트 전체 데이터를 가져와서 파이썬으로 가공
    all_sheet_data = worksheet.get_all_values()
    
    # 💡 업데이트 위치 파악: 접수번호(25번째 열, 인덱스 24)를 Key로, 엑셀 행 번호(Index + 1)를 Value로 저장해둡니다.
    # 이렇게 해두면 나중에 특정 접수번호를 찾았을 때 엑셀 몇 번째 줄(Row)을 덮어써야 할지 바로 알 수 있습니다.
    rcept_row_map = {row[24]: i + 1 for i, row in enumerate(all_sheet_data) if len(row) > 24}
    existing_rcept_nos = list(rcept_row_map.keys()) # 이미 시트에 존재하는 접수번호 리스트 (중복 방지용)
    
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'} # 상장시장 맵핑표

    # CB, BW, EB 순서대로 루프를 돕니다.
    for config in bond_configs:
        print(f"\n[{config['type']}] 데이터 확인 중...")
        
        # 전체 리스트에서 해당 채권 키워드(예: 전환사채권발행결정)가 들어간 보고서만 필터링
        df_filtered = all_filings[all_filings['report_nm'].str.contains(config['keyword'], na=False)]
        
        if df_filtered.empty:
            print(f"ℹ️ {config['type']} 공시가 없습니다.")
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        # 필터링된 회사들의 상세 JSON 데이터를 DART에서 각각 호출
        for code in corp_codes:
            detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
            df_detail = fetch_dart_json(f"https://opendart.fss.or.kr/api/{config['endpoint']}.json", detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
                
        if not detail_dfs: continue
            
        # 가져온 상세 데이터를 하나로 합침
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        target_rcept_nos = df_filtered['rcept_no'].unique()
        
        # 합친 상세 데이터 중에서, 현재 채권 종류에 맞는 타겟 접수번호만 솎아냄 (상장시장 누락 버그 방지)
        df_merged = df_combined[df_combined['rcept_no'].isin(target_rcept_nos)]
        
        # ==========================================
        # 🟢 [로직 A] 신규 데이터 판별 및 추가
        # ==========================================
        # 현재 처리 중인 데이터 중 구글 시트에 없는 녀석들만 골라냄
        new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        data_to_add = []
        
        for _, row in new_data_df.iterrows():
            print(f" -> [신규] {row.get('corp_name', '')} 포매팅 중...")
            xml_data = extract_bond_xml_details(dart_key, str(row.get('rcept_no', ''))) # XML 파싱
            new_row = format_bond_row(row, xml_data, config, cls_map) # 25열 데이터 포매팅
            data_to_add.append(new_row)
            
        # 새로 추가할 묶음이 있으면 시트 맨 밑줄에 일괄 삽입
        if data_to_add:
            worksheet.append_rows(data_to_add)
            print(f"✅ {config['type']}: 신규 데이터 {len(data_to_add)}건 추가 완료!")

        # ==========================================
        # 🔄 [로직 B] 기존 데이터 '빈칸' 업데이트 점검
        # ==========================================
        # 현재 처리 중인 데이터 중 구글 시트에 이미 존재하는 녀석들 골라냄
        existing_data_df = df_merged[df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        update_count = 0
        
        for _, row in existing_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            row_idx = rcept_row_map.get(rcept_no) # 엑셀에서 해당 데이터가 위치한 행(Row) 번호 추적
            if not row_idx: continue
            
            # 구글 시트에 이미 적혀있는 해당 줄의 전체 데이터를 가져옴
            sheet_row = all_sheet_data[row_idx - 1] 
            needs_update = False
            
            # 검사할 타겟 인덱스: 10(Put옵션), 11(Call옵션), 12(Call비율), 16(단가/행사가액)
            # 파이썬 리스트는 0부터 시작하므로 인덱스 숫자에 유의합니다.
            check_indices = [10, 11, 12, 16] 
            for check_idx in check_indices:
                # 해당 칸이 'X', '-', 빈칸, '없음'으로 방치되어 있다면 업데이트 대상(needs_update)으로 체크
                if len(sheet_row) > check_idx and sheet_row[check_idx] in ['X', '-', '', '없음']:
                    needs_update = True
                    break # 하나라도 빈칸이 발견되면 더 검사할 필요 없이 루프 탈출
                    
            if needs_update:
                # 빈칸이 감지되었으므로 DART 원문을 다시 파싱하여 최신 상태를 불러옴
                xml_data = extract_bond_xml_details(dart_key, rcept_no)
                updated_row = format_bond_row(row, xml_data, config, cls_map)
                
                # 시트에 적혀있는 기존 내용과 방금 DART에서 다시 가져온 최신 내용이 다르면 덮어쓰기 실행
                # (업데이트할 게 없다면 API 낭비를 막기 위해 패스)
                if updated_row[:len(sheet_row)] != sheet_row[:len(updated_row)]:
                    print(f" 🔄 [업데이트] {row.get('corp_name', '')} 빈칸 감지! 새로운 데이터로 덮어씁니다.")
                    # 특정 범위(예: A15 셀부터 시작)를 지정하여 1줄만 깔끔하게 덮어씀
                    worksheet.update(values=[updated_row], range_name=f'A{row_idx}')
                    update_count += 1
                    
        if update_count > 0:
            print(f"✅ {config['type']}: 누락 데이터 {update_count}건 업데이트 완료!")

# ==========================================
# 4. 스크립트 실행 트리거
# ==========================================
if __name__ == "__main__":
    get_and_update_bonds()
