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

# --- [유상증자 전용 XML 원문 파싱 (상세 일정, 가격, 투자자 추출)] ---
def extract_piic_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    extracted = {
        'issue_price': '-', 'base_price': '-', 'discount_rate': '-',
        'pay_date': '-', 'dividend_date': '-', 'listing_date': '-', 
        'investor': '-', 'board_date': '-'
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
                    clean_text = re.sub(r'\s+', ' ', raw_text)
                    
                    # 1. 확정(예정) 발행가액
                    price_match = re.search(r'(?:예정|확정)?발행가액.*?(\d{1,3}(?:,\d{3})*)\s*원', clean_text)
                    if price_match: extracted['issue_price'] = price_match.group(1)
                    
                    # 2. 기준주가
                    base_match = re.search(r'기준주가.*?(\d{1,3}(?:,\d{3})*)\s*원', clean_text)
                    if base_match: extracted['base_price'] = base_match.group(1)
                    
                    # 3. 할인율 (또는 할증률)
                    discount_match = re.search(r'할인율.*?([0-9]{1,2}(?:\.[0-9]+)?)\s*%', clean_text)
                    if discount_match: extracted['discount_rate'] = discount_match.group(1) + '%'
                    
                    # 4. 납입일
                    pay_match = re.search(r'납입일.*?(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', clean_text)
                    if pay_match: extracted['pay_date'] = pay_match.group(1)
                    
                    # 5. 신주의 배당기산일
                    div_match = re.search(r'배당기산일.*?(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', clean_text)
                    if div_match: extracted['dividend_date'] = div_match.group(1)
                    
                    # 6. 신주의 상장 예정일
                    list_match = re.search(r'상장\s*예정일.*?(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', clean_text)
                    if list_match: extracted['listing_date'] = list_match.group(1)
                    
                    # 7. 이사회결의일
                    board_match = re.search(r'이사회결의일.*?(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', clean_text)
                    if board_match: extracted['board_date'] = board_match.group(1)
                    
                    # 8. 투자자 (제3자배정 대상자)
                    inv_match = re.search(r'배정\s*대상자.{0,100}?(주식회사\s*\S+|\S+\s*투자조합|\S+\s*펀드|[가-힣]{2,4})', clean_text)
                    if inv_match:
                        extracted['investor'] = inv_match.group(1).strip()
                    elif "제3자배정" in clean_text:
                        extracted['investor'] = "제3자배정 (원문참조)"
                    elif "주주배정" in clean_text:
                        extracted['investor'] = "기존주주"

    except Exception as e:
        print(f"유상증자 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def get_and_update_piic():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 유상증자 결정 공시 탐색 중...")
    
    # 공시 목록 호출
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    # 구글 시트 탭 연결
    worksheet = sh.worksheet('유상증자_1')
    existing_rcept_nos = worksheet.col_values(21) # 21번째 열이 접수번호
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}

    # 유상증자결정 보고서만 필터링
    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)]
    
    if df_filtered.empty:
        print("ℹ️ 유상증자결정 공시가 없습니다.")
        return
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    for code in corp_codes:
        detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
        df_detail = fetch_dart_json("https://opendart.fss.or.kr/api/piicDecsn.json", detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("상세 데이터를 불러올 수 없습니다.")
        return
        
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no', 'corp_cls', 'report_nm']], on='rcept_no', how='inner')
    new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
    
    if new_data_df.empty:
        print("ℹ️ 새로 추가할 유상증자 데이터가 없습니다.")
        return

    data_to_add = []
    for _, row in new_data_df.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        print(f" -> {corp_name} (유상증자) 세밀한 데이터 포매팅 적용 중...")
        
        # XML에서 부족한 일정 및 단가 정보 추출
        xml_data = extract_piic_xml_details(dart_key, rcept_no)
        
        # 자금용도 합산 및 텍스트화
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

        # 총 자금(확정발행금액) 계산 (단위: 억원)
        total_fund = fclt + bsninh + op + dtrp + ocsa + etc
        fund_100m_str = f"{total_fund / 100000000:,.1f}" if total_fund > 0 else "-"

        # 주식수 계산
        nstk_o = to_int(row.get('nstk_ostk_cnt'))
        nstk_e = to_int(row.get('nstk_estk_cnt'))
        total_new_shares = nstk_o + nstk_e
        
        bfic_o = to_int(row.get('bfic_tisstk_ostk'))
        bfic_e = to_int(row.get('bfic_tisstk_estk'))
        total_old_shares = bfic_o + bfic_e
        
        # 증자비율
        increase_ratio = f"{(total_new_shares / total_old_shares) * 100:.2f}%" if total_old_shares > 0 else "-"
        
        # 발행상품명 판별
        product_name = "보통주" if nstk_o > 0 and nstk_e == 0 else ("기타주" if nstk_o == 0 and nstk_e > 0 else "보통주+기타주")
        
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        new_row = [
            corp_name,                                      # 1. 회사명
            row.get('report_nm', '유상증자결정'),           # 2. 보고서명
            cls_map.get(row.get('corp_cls', ''), '기타'),   # 3. 상장시장
            xml_data['board_date'],                         # 4. 최초 이사회결의일
            str(row.get('ic_mthn', '-')),                   # 5. 증자방식
            product_name,                                   # 6. 발행상품
            f"{total_new_shares:,}" if total_new_shares > 0 else "-", # 7. 신규발행주식수
            xml_data['issue_price'],                        # 8. 확정발행가(원)
            xml_data['base_price'],                         # 9. 기준주가
            fund_100m_str,                                  # 10. 확정발행금액(억원)
            xml_data['discount_rate'],                      # 11. 할인(할증률)
            f"{total_old_shares:,}" if total_old_shares > 0 else "-", # 12. 증자전 주식수
            increase_ratio,                                 # 13. 증자비율
            xml_data['pay_date'],                           # 14. 납입일
            xml_data['dividend_date'],                      # 15. 신주의 배당기산일
            xml_data['listing_date'],                       # 16. 신주의 상장 예정일
            xml_data['board_date'],                         # 17. 이사회결의일
            purpose_str,                                    # 18. 자금용도
            xml_data['investor'],                           # 19. 투자자
            link,                                           # 20. 링크
            rcept_no                                        # 21. 접수번호
        ]
        data_to_add.append(new_row)
        
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자결정: 신규 데이터 {len(data_to_add)}건 추가 완료!")

if __name__ == "__main__":
    get_and_update_piic()
