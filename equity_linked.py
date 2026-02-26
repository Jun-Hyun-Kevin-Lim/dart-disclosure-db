import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
import time  # 업데이트 시 구글 API 과부하 방지용으로 추가
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

# --- [채권 전용 XML 원문 족집게 파싱 (콜/풋옵션 내용 추출 500자로 대폭 확장)] ---
# (작성해주신 원본 함수 그대로 유지)
def extract_bond_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    extracted = {
        'put_option': '없음', 'call_option': '없음', 
        'call_ratio': '-', 'ytc': '-', 'investor': '원문참조'
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
                    
                    # 💡 1. Put Option (조기상환청구권) : 500자로 넉넉하게 추출
                    put_match = re.search(r'(조기상환\s*청구권.{0,500})', clean_text)
                    if put_match:
                        extracted['put_option'] = put_match.group(1).strip() + "..."
                        
                    # 💡 2. Call Option (매도청구권) : 500자로 넉넉하게 추출
                    call_match = re.search(r'(매도\s*청구권.{0,500})', clean_text)
                    if call_match:
                        extracted['call_option'] = call_match.group(1).strip() + "..."
                        
                        # Call 비율 추출
                        ratio_match = re.search(r'([0-9]{1,3}(?:\.[0-9]+)?)\s*%', call_match.group(0))
                        if ratio_match:
                            extracted['call_ratio'] = ratio_match.group(1) + '%'
                            
                    # 3. YTC (매도청구권 수익률)
                    ytc_match = re.search(r'매도청구권.*?수익률.{0,50}?([0-9]{1,2}(?:\.[0-9]+)?)\s*%', clean_text)
                    if ytc_match:
                        extracted['ytc'] = ytc_match.group(1) + '%'
                        
                    # 4. 투자자 (대상자) 추출 시도
                    inv_match = re.search(r'배정\s*대상자.{0,100}?(주식회사\s*\S+|\S+\s*투자조합|\S+\s*펀드|[가-힣]{2,4})', clean_text)
                    if inv_match:
                        extracted['investor'] = inv_match.group(1).strip()
                    elif "제3자배정" in clean_text:
                        extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"채권 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0


# 💡 [추가] 신규 추가 & 업데이트 양쪽에서 똑같이 쓸 수 있도록 기존 포매팅 코드를 함수로 묶음
def make_row_data(row, xml_data, config, cls_map):
    f_map = config['fields']
    rcept_no = str(row.get('rcept_no', ''))
    corp_name = row.get('corp_name', '')
    
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
        config['type'],                             # 1. 구분 (CB, BW, EB)
        corp_name,                                  # 2. 회사명
        cls_map.get(row.get('corp_cls', ''), '기타'),# 3. 상장시장
        str(row.get('bddd', '-')),                  # 4. 최초 이사회결의일
        face_value_str,                             # 5. 권면총액(원)
        str(row.get('bd_intr_ex', '-')),            # 6. Coupon (표면이자율)
        str(row.get('bd_intr_sf', '-')),            # 7. YTM (만기이자율)
        str(row.get('bd_mtd', '-')),                # 8. 만기
        str(row.get(f_map['start'], '-')),          # 9. 전환청구 시작
        str(row.get(f_map['end'], '-')),            # 10. 전환청구 종료
        xml_data['put_option'],                     # 11. Put Option
        xml_data['call_option'],                    # 12. Call Option
        xml_data['call_ratio'],                     # 13. Call 비율
        xml_data['ytc'],                            # 14. YTC
        str(row.get('bdis_mthn', '-')),             # 15. 모집방식
        product_name,                               # 16. 발행상품
        price_str,                                  # 17. 행사(전환)가액(원)
        shares_str,                                 # 18. 전환주식수
        str(row.get(f_map['ratio'], '-')),          # 19. 주식총수대비 비율
        refix_str,                                  # 20. Refixing Floor
        str(row.get('pymd', '-')),                  # 21. 납입일
        purpose_str,                                # 22. 자금용도
        xml_data['investor'],                       # 23. 투자자
        link,                                       # 24. 링크
        rcept_no                                    # 25. 접수번호
    ]


def get_and_update_bonds():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=12)).strftime('%Y%m%d')

    print("최근 12일 주식연계채권(CB, BW, EB) 공시 탐색 중...")
    
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

    # 채권 종류별 설정값 (API 필드명이 다르므로 매핑)
    bond_configs = [
        {'type': 'CB', 'keyword': '전환사채권발행결정', 'endpoint': 'cvbdIsDecsn', 'fields': {'price': 'cv_prc', 'shares': 'cvisstk_cnt', 'ratio': 'cvisstk_tisstk_vs', 'start': 'cvrqpd_bgd', 'end': 'cvrqpd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'BW', 'keyword': '신주인수권부사채권발행결정', 'endpoint': 'bdwtIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'nstk_isstk_cnt', 'ratio': 'nstk_isstk_tisstk_vs', 'start': 'expd_bgd', 'end': 'expd_edd', 'refix': 'act_mktprcfl_cvprc_lwtrsprc'}},
        {'type': 'EB', 'keyword': '교환사채권발행결정', 'endpoint': 'exbdIsDecsn', 'fields': {'price': 'ex_prc', 'shares': 'extg_stkcnt', 'ratio': 'extg_tisstk_vs', 'start': 'exrqpd_bgd', 'end': 'exrqpd_edd', 'refix': ''}}
    ]

    worksheet = sh.worksheet('주식연계채권')
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}

    # 💡 [변경] 시트의 전체 데이터를 읽어와서 행 번호(Row Index)와 기존 값을 모두 매핑해둡니다. (Diff/Update 용도)
    all_sheet_data = worksheet.get_all_values()
    rcept_row_map = {row[24]: i + 1 for i, row in enumerate(all_sheet_data) if len(row) > 24}
    existing_rcept_nos = list(rcept_row_map.keys())

    for config in bond_configs:
        print(f"\n[{config['type']}] 데이터 확인 중...")
        df_filtered = all_filings[all_filings['report_nm'].str.contains(config['keyword'], na=False)]
        
        if df_filtered.empty:
            print(f"ℹ️ {config['type']} 공시가 없습니다.")
            continue
            
        corp_codes = df_filtered['corp_code'].unique()
        detail_dfs = []
        
        for code in corp_codes:
            detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
            df_detail = fetch_dart_json(f"https://opendart.fss.or.kr/api/{config['endpoint']}.json", detail_params)
            if not df_detail.empty:
                detail_dfs.append(df_detail)
                
        if not detail_dfs:
            continue
            
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        
        target_rcept_nos = df_filtered['rcept_no'].unique()
        df_merged = df_combined[df_combined['rcept_no'].isin(target_rcept_nos)]
        
        # ========================================================
        # 🟢 1. 신규 데이터 추가 로직 (기존 유지)
        # ========================================================
        new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        data_to_add = []
        for _, row in new_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            print(f" -> [신규] {row.get('corp_name', '')} 데이터 포매팅 중...")
            xml_data = extract_bond_xml_details(dart_key, rcept_no)
            
            # 함수로 분리한 포매팅 로직 호출
            new_row = make_row_data(row, xml_data, config, cls_map)
            data_to_add.append(new_row)
            
        if data_to_add:
            worksheet.append_rows(data_to_add)
            print(f"✅ {config['type']}: 신규 데이터 {len(data_to_add)}건 추가 완료!")

        # ========================================================
        # 🔄 2. [신규 추가] 기존 데이터 재검사 및 덮어쓰기 로직 (Recheck + Diff + Update)
        # ========================================================
        existing_data_df = df_merged[df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        update_count = 0
        
        for _, row in existing_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            row_idx = rcept_row_map.get(rcept_no)
            if not row_idx: continue

            # 1. 구글 시트에 현재 저장되어 있는 기존 값
            sheet_row = all_sheet_data[row_idx - 1]
            
            # 2. DART에서 가져온 최신 값으로 다시 25칸 구성
            xml_data = extract_bond_xml_details(dart_key, rcept_no)
            new_row = make_row_data(row, xml_data, config, cls_map)
            
            # 3. [Diff 검사] 빈 칸이 있을 수 있으니 길이 25로 맞추고 문자열로 변환하여 완전 동일한지 비교
            sheet_row_padded = sheet_row + [''] * (25 - len(sheet_row))
            new_row_str = [str(x) for x in new_row]

            # 두 데이터가 1개라도 다르면 (정정공시, 옵션 확정 등) 덮어쓰기!
            if sheet_row_padded != new_row_str:
                corp_name = row.get('corp_name', '')
                print(f" 🔄 [업데이트] {corp_name} 값이 변경/확정되었습니다. 시트를 덮어씁니다.")
                # 변경된 최신 값으로 해당 줄(예: A15) 전체 덮어쓰기
                worksheet.update(values=[new_row], range_name=f'A{row_idx}')
                update_count += 1
                time.sleep(1) # 구글 API 쓰기 할당량 초과 방지용 휴식

        if update_count > 0:
            print(f"✅ {config['type']}: 기존 데이터 {update_count}건 자동 업데이트 완료!")

if __name__ == "__main__":
    get_and_update_bonds()
