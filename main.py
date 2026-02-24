import os
import re
import io
import json
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "DART").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Seoul")

TARGET_REPORT_RE = re.compile(r"유상\s*증자\s*결정")

LIST_URL = "https://opendart.fss.or.kr/api/list.xml"
PIIC_URL = "https://opendart.fss.or.kr/api/piicDecsn.json"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"

def require_env(name: str, value: str):
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")

def clean_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def parse_int_maybe(s: str):
    s = clean_str(s)
    if not s:
        return None
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."):
        return None
    try:
        return int(float(t)) if "." in t else int(t)
    except:
        return None

def parse_float_maybe(s: str):
    s = clean_str(s)
    if not s:
        return None
    t = re.sub(r"[^\d\-\.]", "", s)
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except:
        return None

def corp_cls_to_market(corp_cls: str) -> str:
    m = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "ETC"}
    return m.get(clean_str(corp_cls), clean_str(corp_cls))

def safe_get(d: dict, key: str):
    return clean_str(d.get(key)) if isinstance(d, dict) else ""

def normalize_ws(s: str) -> str:
    s = clean_str(s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def amount_won_to_eok(won: int):
    if won is None:
        return ""
    return round(won / 100_000_000, 2)

def get_gsheet_client():
    require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_worksheet():
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    gc = get_gsheet_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)

def ensure_header(ws):
    header = [
        "rcept_no","회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수",
        "확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)","증자전 주식수","증자비율",
        "청약일","납입일","주관사","자금용도","투자자","증자금액",
    ]
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(header, value_input_option="USER_ENTERED")

def get_processed_rcept_set(ws):
    col = ws.col_values(1)
    if not col:
        return set()
    if col[0].strip().lower() == "rcept_no":
        col = col[1:]
    return set(x.strip() for x in col if x.strip())

def dart_list(bgn_de: str, end_de: str):
    require_env("DART_API_KEY", DART_API_KEY)
    results = []
    page_no = 1
    while page_no <= MAX_PAGES:
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "B",
            "pblntf_detail_ty": "B001",
            "sort": "date",
            "sort_mth": "desc",
            "page_no": str(page_no),
            "page_count": str(PAGE_COUNT),
        }
        r = requests.get(LIST_URL, params=params, timeout=30)
        r.raise_for_status()

        root = ET.fromstring(r.content)
        status = root.findtext("status")
        if status != "000":
            print(f"[list.xml] status={status}, message={root.findtext('message')}")
            break

        total_page = parse_int_maybe(root.findtext("total_page")) or 1
        for node in root.findall("list"):
            item = {child.tag: (child.text or "").strip() for child in list(node)}
            results.append(item)

        if page_no >= total_page:
            break
        page_no += 1
    return results

def dart_piic_for_rcept(corp_code: str, bgn_de: str, end_de: str, rcept_no: str):
    require_env("DART_API_KEY", DART_API_KEY)
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bgn_de": bgn_de, "end_de": end_de}
    r = requests.get(PIIC_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "000":
        print(f"[piicDecsn] status={data.get('status')}, message={data.get('message')}")
        return {}
    for row in data.get("list", []) or []:
        if str(row.get("rcept_no", "")).strip() == str(rcept_no).strip():
            return row
    return {}

def dart_document_zip(rcept_no: str) -> bytes:
    require_env("DART_API_KEY", DART_API_KEY)
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    r = requests.get(DOC_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.content

LABELS = {
    "board_date": ["이사회결의일","이사회 결의일","결정일"],
    "subscription_date": ["청약일","청약예정일"],
    "payment_date": ["납입일","납입예정일"],
    "underwriter": ["주관회사","대표주관회사","인수회사","주관증권사"],
    "investor": ["제3자배정","배정대상자","투자자"],
    "issue_price": ["확정발행가액","발행가액","1주당 발행가액","신주발행가액"],
    "base_price": ["기준주가","기준 주가"],
    "discount_rate": ["할인율","할증율","할인(할증)","할인(할증률)"],
    "issue_amount": ["확정발행금액","발행금액","총발행금액","조달금액","증자금액"],
}

def extract_from_tables(dfs):
    out = {k: "" for k in LABELS.keys()}
    for df in dfs:
        try:
            df = df.fillna("").astype(str)
        except:
            continue
        for _, row in df.iterrows():
            row_vals = [normalize_ws(v) for v in row.tolist()]
            for j, cell in enumerate(row_vals):
                if not cell:
                    continue
                for key, variants in LABELS.items():
                    if out.get(key):
                        continue
                    if any(v in cell for v in variants):
                        tail = [x for x in row_vals[j+1:] if x]
                        if tail:
                            out[key] = normalize_ws(" ".join(tail))
                        else:
                            parts = re.split(r"[:：]\s*", cell, maxsplit=1)
                            if len(parts) == 2:
                                out[key] = normalize_ws(parts[1])
                        break
    return out

def extract_from_text_fallback(html: str, current: dict):
    soup = BeautifulSoup(html, "lxml")
    lines = [normalize_ws(x) for x in soup.get_text("\n").splitlines()]
    lines = [x for x in lines if x]
    for key, variants in LABELS.items():
        if current.get(key):
            continue
        for i, line in enumerate(lines):
            if any(v in line for v in variants):
                if ":" in line or "：" in line:
                    parts = re.split(r"[:：]\s*", line, maxsplit=1)
                    if len(parts) == 2:
                        current[key] = normalize_ws(parts[1])
                        break
                if i + 1 < len(lines):
                    current[key] = normalize_ws(lines[i + 1])
                    break
    return current

def pick_main_html_from_zip(zbytes: bytes) -> str:
    try:
        zf = zipfile.ZipFile(io.BytesIO(zbytes))
    except zipfile.BadZipFile:
        return zbytes.decode("utf-8", errors="ignore")

    html_files = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
    if not html_files:
        all_files = zf.namelist()
        if not all_files:
            return ""
        largest = max(all_files, key=lambda n: zf.getinfo(n).file_size)
        raw = zf.read(largest)
        return raw.decode("utf-8", errors="ignore")

    largest_html = max(html_files, key=lambda n: zf.getinfo(n).file_size)
    raw = zf.read(largest_html)
    for enc in ("utf-8", "cp949"):
        try:
            return raw.decode(enc)
        except:
            continue
    return raw.decode("utf-8", errors="ignore")

def parse_document_fields(rcept_no: str):
    html = pick_main_html_from_zip(dart_document_zip(rcept_no))
    if not html:
        return {}
    try:
        dfs = pd.read_html(html)
        fields = extract_from_tables(dfs)
    except:
        fields = {k: "" for k in LABELS.keys()}
    return extract_from_text_fallback(html, fields)

def build_row(list_item: dict, piic: dict, doc: dict):
    corp_name = safe_get(list_item, "corp_name") or safe_get(piic, "corp_name")
    market = corp_cls_to_market(safe_get(list_item, "corp_cls") or safe_get(piic, "corp_cls"))

    new_common = parse_int_maybe(safe_get(piic, "nstk_ostk_cnt"))
    new_other = parse_int_maybe(safe_get(piic, "nstk_estk_cnt"))
    new_total = (new_common or 0) + (new_other or 0)
    new_total_str = str(new_total) if new_total else ""

    pre_common = parse_int_maybe(safe_get(piic, "bfic_tisstk_ostk"))
    pre_other = parse_int_maybe(safe_get(piic, "bfic_tisstk_estk"))
    pre_total = (pre_common or 0) + (pre_other or 0)
    pre_total_str = str(pre_total) if pre_total else ""

    if (new_common or 0) > 0 and (new_other or 0) > 0:
        product = "보통주+기타주"
    elif (new_common or 0) > 0:
        product = "보통주"
    elif (new_other or 0) > 0:
        product = "기타주"
    else:
        product = ""

    method = safe_get(piic, "ic_mthn")

    board_date = safe_get(doc, "board_date")
    subscription_date = safe_get(doc, "subscription_date")
    payment_date = safe_get(doc, "payment_date")
    underwriter = safe_get(doc, "underwriter")
    investor = safe_get(doc, "investor")

    issue_price_raw = safe_get(doc, "issue_price")
    base_price_raw = safe_get(doc, "base_price")
    discount_raw = safe_get(doc, "discount_rate")
    issue_amount_raw = safe_get(doc, "issue_amount")

    issue_price = parse_int_maybe(issue_price_raw)
    base_price = parse_int_maybe(base_price_raw)

    amount_won = None
    if issue_amount_raw:
        if "억" in issue_amount_raw:
            v = parse_float_maybe(issue_amount_raw)
            if v is not None:
                amount_won = int(v * 100_000_000)
        else:
            v = parse_int_maybe(issue_amount_raw)
            if v is not None:
                amount_won = v
    if amount_won is None and issue_price is not None and new_total:
        amount_won = int(issue_price * new_total)

    amount_eok = amount_won_to_eok(amount_won) if amount_won is not None else ""

    ratio = ""
    if pre_total and new_total:
        ratio = round((new_total / pre_total) * 100, 2)

    purpose_parts = []
    for k, label in [
        ("fdpp_fclt", "시설"),
        ("fdpp_op", "운영"),
        ("fdpp_bsninh", "영업양수"),
        ("fdpp_dtrp", "채무상환"),
        ("fdpp_ocsa", "타법인증권취득"),
        ("fdpp_etc", "기타"),
    ]:
        v = parse_int_maybe(safe_get(piic, k))
        if v and v > 0:
            purpose_parts.append(f"{label}:{amount_won_to_eok(v)}억")
    purpose = ", ".join(purpose_parts)

    return [
        safe_get(list_item, "rcept_no"),
        corp_name,
        market,
        board_date,
        method,
        product,
        new_total_str,
        str(issue_price) if issue_price is not None else issue_price_raw,
        str(base_price) if base_price is not None else base_price_raw,
        str(amount_eok) if amount_eok != "" else "",
        discount_raw,
        pre_total_str,
        str(ratio) if ratio != "" else "",
        subscription_date,
        payment_date,
        underwriter,
        purpose,
        investor,
        str(amount_won) if amount_won is not None else "",
    ]

def main():
    require_env("DART_API_KEY", DART_API_KEY)
    require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    bgn = today - timedelta(days=LOOKBACK_DAYS)
    bgn_de = bgn.strftime("%Y%m%d")
    end_de = today.strftime("%Y%m%d")

    ws = open_worksheet()
    ensure_header(ws)
    processed = get_processed_rcept_set(ws)

    items = dart_list(bgn_de=bgn_de, end_de=end_de)
    candidates = [it for it in items if TARGET_REPORT_RE.search(safe_get(it, "report_nm"))]
    new_items = [it for it in candidates if safe_get(it, "rcept_no") not in processed]

    rows_to_append = []
    for it in new_items:
        rcept_no = safe_get(it, "rcept_no")
        corp_code = safe_get(it, "corp_code")
        rcept_dt = safe_get(it, "rcept_dt")

        piic = {}
        if corp_code and rcept_dt:
            piic = dart_piic_for_rcept(corp_code, rcept_dt, rcept_dt, rcept_no)

        doc = {}
        try:
            doc = parse_document_fields(rcept_no)
        except Exception as e:
            print(f"[document parse error] {e}")
            doc = {}

        rows_to_append.append(build_row(it, piic, doc))

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended rows: {len(rows_to_append)}")
    else:
        print("✅ No new rows to append.")

if __name__ == "__main__":
    main()
