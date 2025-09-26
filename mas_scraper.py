import requests
from bs4 import BeautifulSoup
import json
import time
import sys
import os # Imported for file operations (replace, remove)
from datetime import date, datetime
from typing import Dict, Any

# ==============================================================================
# PART 1: SCRAPER CONFIGURATION AND FUNCTIONS
# (This section is unchanged)
# ==============================================================================

BASE_URL = "https://eservices.mas.gov.sg/Statistics/msb/ExchangeRates.aspx"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}

# ---------- CONFIGURE MODE ----------
DATE_MODE = "latest_month"   # options: "latest_month", "year_to_date", "last_n_months"
LAST_N_MONTHS = 3           # used only if DATE_MODE == "last_n_months"
# -----------------------------------

# static checkboxes from original scraper
STATIC_CHECKBOXES = {
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$0": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$1": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$2": "on",
    # per-100 units (example set)
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$0": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$1": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$2": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$3": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$4": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$5": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$6": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$7": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$8": "on",
}

def compute_start_end(mode: str = "latest_month", n_months: int = 1):
    """Return (start_year, start_month, end_year, end_month) as strings."""
    today = date.today()
    end_year = today.year
    end_month = today.month

    if mode == "latest_month":
        start_year, start_month = end_year, end_month

    elif mode == "year_to_date":
        start_year, start_month = end_year, 1

    elif mode == "last_n_months":
        if n_months < 1:
            n_months = 1
        total_months = end_year * 12 + (end_month - 1)
        start_total_months = total_months - (n_months - 1)
        start_year = start_total_months // 12
        start_month = (start_total_months % 12) + 1
    else:
        start_year, start_month = end_year, end_month

    return str(start_year), str(start_month), str(end_year), str(end_month)

def get_hidden_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    inputs = {}
    for tag in soup.find_all("input", {"type": "hidden"}):
        name = tag.get("name")
        if not name:
            continue
        inputs[name] = tag.get("value", "")
    return inputs

def parse_tables_to_json(soup: BeautifulSoup):
    tables_out = []
    tables = soup.find_all("table")
    for idx, table in enumerate(tables):
        caption = table.find("caption")
        caption_text = caption.get_text(strip=True) if caption else None

        headers = []
        header_row = table.find("tr")
        if header_row:
            ths = header_row.find_all("th")
            if ths:
                headers = [th.get_text(strip=True) for th in ths]
            else:
                tds = header_row.find_all("td")
                if tds:
                    headers = [f"col_{i+1}" for i in range(len(tds))]

        rows = []
        trs = table.find_all("tr")
        start_row = 1 if header_row and header_row.find_all("th") else 0
        for tr in trs[start_row:]:
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            row = [td.get_text(" ", strip=True) for td in tds]
            if headers and len(headers) == len(row):
                row_dict = dict(zip(headers, row))
            else:
                row_dict = {f"col_{i+1}": val for i, val in enumerate(row)}
            rows.append(row_dict)

        tables_out.append({
            "index": idx,
            "caption": caption_text,
            "headers": headers,
            "rows": rows
        })
    return tables_out

def build_post_payload(hidden_inputs: Dict[str, str], start_y: str, start_m: str, end_y: str, end_m: str) -> Dict[str, str]:
    payload = dict(hidden_inputs)
    payload.update({
        "ctl00$ContentPlaceHolder1$StartYearDropDownList": start_y,
        "ctl00$ContentPlaceHolder1$EndYearDropDownList": end_y,
        "ctl00$ContentPlaceHolder1$StartMonthDropDownList": start_m,
        "ctl00$ContentPlaceHolder1$EndMonthDropDownList": end_m,
        "ctl00$ContentPlaceHolder1$FrequencyDropDownList": "D",
        "ctl00$ContentPlaceHolder1$DisplayButton": "Display",
    })
    payload.update(STATIC_CHECKBOXES)
    return payload

def scrape_exchange_rates(session: requests.Session, date_mode: str = "latest_month", last_n_months: int = 1, debug: bool=False) -> Dict[str, Any]:
    resp_get = session.get(BASE_URL, headers=DEFAULT_HEADERS, timeout=30)
    resp_get.raise_for_status()
    soup_get = BeautifulSoup(resp_get.text, "html.parser")
    hidden = get_hidden_inputs(soup_get)
    if debug:
        print("Scraper: Hidden fields found:", list(hidden.keys()), file=sys.stderr)

    start_y, start_m, end_y, end_m = compute_start_end(mode=date_mode, n_months=last_n_months)
    if debug:
        print(f"Scraper: Using date range Start={start_y}-{start_m} End={end_y}-{end_m}", file=sys.stderr)

    payload = build_post_payload(hidden, start_y, start_m, end_y, end_m)

    headers_post = dict(DEFAULT_HEADERS)
    headers_post["Content-Type"] = "application/x-www-form-urlencoded"
    resp_post = session.post(BASE_URL, headers=headers_post, data=payload, timeout=60)
    resp_post.raise_for_status()
    soup_post = BeautifulSoup(resp_post.text, "html.parser")

    tables = parse_tables_to_json(soup_post)

    if not tables or not tables[0].get("rows"):
        raise ValueError("Scraping succeeded but no data tables were found in the response. Website structure may have changed.")


    result = {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "url": BASE_URL,
        "date_mode": date_mode,
        "start": {"year": start_y, "month": start_m},
        "end": {"year": end_y, "month": end_m},
        "tables": tables,
        "status_code": resp_post.status_code,
    }
    return result

# ==============================================================================
# PART 2: CONVERTER CONFIGURATION AND FUNCTIONS
# ==============================================================================

FINAL_FILENAME = "compact_rates.json"

NAME_TO_CODE = {
    'Euro':'EUR','Pound Sterling':'GBP','US Dollar':'USD','Australian Dollar':'AUD',
    'Canadian Dollar':'CAD','Chinese Renminbi':'CNY','Hong Kong Dollar':'HKD',
    'Indian Rupee':'INR','Indonesian Rupiah':'IDR','Japanese Yen':'JPY','Korean Won':'KRW',
    'Malaysian Ringgit':'MYR'
}

class NoScientificNotationEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        if isinstance(o, float):
            formatted_float = f"{o:.12f}".rstrip('0').rstrip('.')
            if not formatted_float or formatted_float == '-':
                return iter(["0"])
            return iter([formatted_float])
        return super(NoScientificNotationEncoder, self).iterencode(o, _one_shot)

def parse_currency_from_header(h):
    parts = h.split(' of')
    name = parts[-1].strip()
    for k in NAME_TO_CODE:
        if k.lower() in name.lower():
            return NAME_TO_CODE[k]
    return name.upper()

def convert_scraped_data(scraped_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes the dictionary from the scraper and RETURNS a final dictionary.
    """
    j = scraped_data
    t = j['tables'][0]
    headers = t['headers']
    rows = t['rows']
    latest = rows[-1]

    currency_headers = headers[1:]
    base_col_index = 4
    sgd_per = {}
    for i,h in enumerate(currency_headers):
        col_key = f"col_{base_col_index + i}"
        raw = latest.get(col_key)
        if raw is None or raw == '':
            continue
        val = float(raw)
        if '100' in h:
            val = val / 100.0
        iso = parse_currency_from_header(h)
        sgd_per[iso] = val

    if not sgd_per:
        raise ValueError("Could not parse any currency values from the data table.")

    out = {}
    for base, s_base in sgd_per.items():
        if s_base == 0:
            continue
        out[base] = {}
        for quote, s_quote in sgd_per.items():
            if s_quote == 0:
                continue
            out[base][quote] = s_base / s_quote
        out[base]['SGD'] = s_base

    fetched = j.get('fetched_at')
    if fetched:
        day = latest.get('col_3') or ''
        year = j.get('start', {}).get('year')
        month = j.get('start', {}).get('month')
        if year and month and day:
            try:
                dt_str = f"{year}-{int(month):02d}-{int(day):02d}T" + fetched.split('T')[1]
                dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
                out_dt = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                out_dt = fetched.replace('T', ' ').replace('Z', '')
        else:
            out_dt = fetched.replace('T', ' ').replace('Z', '')
    else:
        out_dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    out['datetime'] = out_dt

    return out

# ==============================================================================
# MAIN EXECUTION BLOCK WITH FALLBACK LOGIC
# ==============================================================================

def main():
    """Orchestrates the scraping and conversion process with a safe-write fallback."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept-Language": DEFAULT_HEADERS["Accept-Language"]
    })

    temp_filename = FINAL_FILENAME + ".tmp"

    try:
        print("Running scraper to fetch data from MAS website...")
        scraped_output = scrape_exchange_rates(
            s,
            date_mode=DATE_MODE,
            last_n_months=LAST_N_MONTHS,
            debug=True
        )
        print("Scraping complete.")

        print("\nRunning converter to process data...")
        final_data = convert_scraped_data(scraped_output)
        print("Conversion complete.")

        print(f"\nWriting new data to temporary file: {temp_filename}")
        with open(temp_filename, 'w', encoding='utf-8') as f:
            json.dump(
                final_data,
                f,
                indent=2,
                ensure_ascii=False,
                cls=NoScientificNotationEncoder
            )

        # --- FIX: Use os.replace() instead of os.rename() ---
        # os.replace() is atomic and will overwrite the destination file,
        # making it safe and compatible with Windows.
        os.replace(temp_filename, FINAL_FILENAME)
        print(f"Successfully updated '{FINAL_FILENAME}'")

    except Exception as e:
        print("\n--- SCRIPT FAILED ---", file=sys.stderr)
        print(f"An error occurred: {e}", file=sys.stderr)
        print(f"The existing '{FINAL_FILENAME}' was NOT modified.", file=sys.stderr)

        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            print(f"Removed temporary file '{temp_filename}'.", file=sys.stderr)
        
        sys.exit(1)

if __name__ == "__main__":
    main()