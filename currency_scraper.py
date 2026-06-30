import argparse
import calendar
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://eservices.mas.gov.sg/Statistics/msb/ExchangeRates.aspx"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}

STATIC_CHECKBOXES = {
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$0": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$1": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$2": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$0": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$1": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$2": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$3": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$4": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$5": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$6": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$7": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$8": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$9": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$11": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$14": "on",
    "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$15": "on",
}

NAME_TO_CODE = {
    'Euro': 'EUR', 'Pound Sterling': 'GBP', 'US Dollar': 'USD',
    'Australian Dollar': 'AUD', 'Canadian Dollar': 'CAD', 'Chinese Renminbi': 'CNY',
    'Hong Kong Dollar': 'HKD', 'Indian Rupee': 'INR', 'Indonesian Rupiah': 'IDR',
    'Japanese Yen': 'JPY', 'Korean Won': 'KRW', 'Malaysian Ringgit': 'MYR',
    'Thai Baht': 'THB', 'Philippine Peso': 'PHP', 'Swiss Franc': 'CHF',
    'New Taiwan Dollar': 'TWD',
}

QUARTER_END_MONTHS = {1: 3, 2: 6, 3: 9, 4: 12}


# ==============================================================================
# SHARED SCRAPING HELPERS
# ==============================================================================

class NoScientificNotationEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        if isinstance(o, float):
            formatted_float = f"{o:.12f}".rstrip('0').rstrip('.')
            if not formatted_float or formatted_float == '-':
                return iter(["0"])
            return iter([formatted_float])
        return super().iterencode(o, _one_shot)


def get_hidden_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    inputs = {}
    for tag in soup.find_all("input", {"type": "hidden"}):
        name = tag.get("name")
        if name:
            inputs[name] = tag.get("value", "")
    return inputs


def parse_tables_to_json(soup: BeautifulSoup):
    tables_out = []
    for idx, table in enumerate(soup.find_all("table")):
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
        start_row = 1 if header_row and header_row.find_all("th") else 0
        for tr in table.find_all("tr")[start_row:]:
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            row = [td.get_text(" ", strip=True) for td in tds]
            if headers and len(headers) == len(row):
                rows.append(dict(zip(headers, row)))
            else:
                rows.append({f"col_{i+1}": val for i, val in enumerate(row)})
        tables_out.append({"index": idx, "caption": caption_text, "headers": headers, "rows": rows})
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


def parse_currency_from_header(h: str) -> str:
    name = h.split(' of')[-1].strip()
    for k in NAME_TO_CODE:
        if k.lower() in name.lower():
            return NAME_TO_CODE[k]
    return name.upper()


def fetch_month_raw(session: requests.Session, year: int, month: int) -> Optional[Dict[str, Any]]:
    """Scrape MAS for a specific year/month and return raw result dict."""
    resp = session.get(BASE_URL, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()
    hidden = get_hidden_inputs(BeautifulSoup(resp.text, "html.parser"))

    start_y, start_m = str(year), str(month)
    payload = build_post_payload(hidden, start_y, start_m, start_y, start_m)
    headers_post = {**DEFAULT_HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
    resp_post = session.post(BASE_URL, headers=headers_post, data=payload, timeout=60)
    resp_post.raise_for_status()

    tables = parse_tables_to_json(BeautifulSoup(resp_post.text, "html.parser"))
    if not tables or not tables[0].get("rows"):
        return None

    return {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "url": BASE_URL,
        "start": {"year": start_y, "month": start_m},
        "end":   {"year": start_y, "month": start_m},
        "tables": tables,
        "status_code": resp_post.status_code,
    }


def parse_rates(scraped: Dict[str, Any]) -> Dict[str, float]:
    """Extract SGD-per-unit rates from raw scraped data."""
    t = scraped["tables"][0]
    headers = t["headers"]
    latest = t["rows"][-1]
    currency_headers = headers[1:]
    base_col_index = 4

    sgd_per = {}
    for i, h in enumerate(currency_headers):
        raw = latest.get(f"col_{base_col_index + i}")
        if not raw:
            continue
        try:
            val = float(raw)
        except ValueError:
            continue
        if "100" in h:
            val /= 100.0
        sgd_per[parse_currency_from_header(h)] = val
    return sgd_per


def build_cross_rates(sgd_per: Dict[str, float]) -> Dict[str, Any]:
    """Build cross-rate matrix from SGD-per-unit rates."""
    out: Dict[str, Any] = {}
    for base, s_base in sgd_per.items():
        if s_base == 0:
            continue
        out[base] = {q: s_base / s_q for q, s_q in sgd_per.items() if s_q != 0}
        out[base]["SGD"] = s_base
    return out


# ==============================================================================
# DAILY MODE  →  compact_rates.json
# ==============================================================================

DAILY_OUTPUT = "compact_rates.json"


def run_daily(session: requests.Session):
    yesterday = date.today() - timedelta(days=1)
    raw = fetch_month_raw(session, yesterday.year, yesterday.month)
    if not raw:
        raise ValueError("No data returned from MAS for latest month.")

    sgd_per = parse_rates(raw)
    if not sgd_per:
        raise ValueError("Could not parse any currency values.")

    out = build_cross_rates(sgd_per)

    # Resolve exact date from last row's day column
    t = raw["tables"][0]
    latest = t["rows"][-1]
    day_str = latest.get("col_3", "")
    fetched = raw.get("fetched_at", "")
    try:
        year = int(raw["start"]["year"])
        month = int(raw["start"]["month"])
        dt = datetime(year, month, int(day_str))
        out["datetime"] = dt.strftime("%Y-%m-%d") + "T" + fetched.split("T")[1].replace("Z", "")
    except Exception:
        out["datetime"] = fetched.replace("T", " ").replace("Z", "")

    tmp = DAILY_OUTPUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, cls=NoScientificNotationEncoder)
    os.replace(tmp, DAILY_OUTPUT)
    print(f"Daily rates saved to '{DAILY_OUTPUT}' (date: {out.get('datetime', '?')})")


# ==============================================================================
# QUARTERLY MODE  →  quarterly_rates.json
# ==============================================================================

QUARTERLY_OUTPUT = "quarterly_rates.json"


def quarter_end_date(year: int, q: int) -> date:
    end_month = QUARTER_END_MONTHS[q]
    last_day = calendar.monthrange(year, end_month)[1]
    return date(year, end_month, last_day)


def quarters_since_2021():
    today = date.today()
    year, q = 2021, 1
    while True:
        if quarter_end_date(year, q) >= today:
            break
        yield year, q, QUARTER_END_MONTHS[q]
        q += 1
        if q > 4:
            q, year = 1, year + 1


def run_quarterly(session: requests.Session):
    existing: Dict[str, Any] = {}
    if os.path.exists(QUARTERLY_OUTPUT):
        try:
            with open(QUARTERLY_OUTPUT, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    quarters_data: Dict[str, Any] = existing.get("quarters", {})
    errors = []

    for year, q, end_month in quarters_since_2021():
        label = f"{year}-Q{q}"
        print(f"Fetching {label} (end month: {year}-{end_month:02d})...", end=" ", flush=True)
        try:
            raw = fetch_month_raw(session, year, end_month)
            if raw is None:
                print("no data returned — skipping.")
                errors.append(label)
                continue

            sgd_per = parse_rates(raw)
            if not sgd_per:
                print("could not parse rates — skipping.")
                errors.append(label)
                continue

            out = build_cross_rates(sgd_per)
            # Resolve exact date
            latest = raw["tables"][0]["rows"][-1]
            day_str = latest.get("col_3", "")
            try:
                dt = datetime(year, end_month, int(day_str))
                out["date"] = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                out["date"] = f"{year}-{end_month:02d}"

            date_key = out.get("date", label)
            quarters_data[date_key] = out
            print(f"OK  (date: {date_key})")

        except Exception as e:
            print(f"FAILED: {e}")
            errors.append(label)

        time.sleep(1.5)

    output = {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": BASE_URL,
        "quarters": dict(sorted(quarters_data.items())),
    }

    tmp = QUARTERLY_OUTPUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NoScientificNotationEncoder)
    os.replace(tmp, QUARTERLY_OUTPUT)

    print(f"\nSaved {len(quarters_data)} quarters to '{QUARTERLY_OUTPUT}'.")
    if errors:
        print(f"Quarters with errors / no data: {errors}", file=sys.stderr)


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="MAS Exchange Rate Scraper")
    parser.add_argument(
        "--mode",
        choices=["daily", "quarterly"],
        default="daily",
        help="'daily' updates compact_rates.json; 'quarterly' updates quarterly_rates.json",
    )
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept-Language": DEFAULT_HEADERS["Accept-Language"],
    })

    try:
        if args.mode == "daily":
            run_daily(session)
        else:
            run_quarterly(session)
    except Exception as e:
        print(f"\n--- SCRIPT FAILED ---\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
