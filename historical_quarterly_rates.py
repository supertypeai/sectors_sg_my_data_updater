import requests
import json
import time
import os
import sys
from datetime import date, datetime
from typing import Dict, Any, Optional

from mas_scraper import (
    BASE_URL,
    DEFAULT_HEADERS,
    get_hidden_inputs,
    parse_tables_to_json,
    build_post_payload,
    parse_currency_from_header,
    NoScientificNotationEncoder,
)

OUTPUT_FILENAME = "quarterly_rates.json"

QUARTER_END_MONTHS = {1: 3, 2: 6, 3: 9, 4: 12}


def quarter_of(month: int) -> int:
    return (month - 1) // 3 + 1


def quarter_end_date(year: int, q: int) -> date:
    import calendar
    end_month = QUARTER_END_MONTHS[q]
    last_day = calendar.monthrange(year, end_month)[1]
    return date(year, end_month, last_day)


def quarters_since_2021():
    """Yield (year, quarter, end_month) for every fully completed quarter from Q1 2021."""
    today = date.today()

    year, q = 2021, 1
    while True:
        if quarter_end_date(year, q) >= today:
            break
        yield year, q, QUARTER_END_MONTHS[q]
        q += 1
        if q > 4:
            q = 1
            year += 1


def scrape_month(session: requests.Session, year: int, month: int) -> Optional[Dict[str, Any]]:
    """Scrape MAS exchange rates for a specific year/month and return the raw result."""
    resp_get = session.get(BASE_URL, headers=DEFAULT_HEADERS, timeout=30)
    resp_get.raise_for_status()
    soup_get = __import__("bs4").BeautifulSoup(resp_get.text, "html.parser")
    hidden = get_hidden_inputs(soup_get)

    start_y, start_m = str(year), str(month)
    end_y, end_m = str(year), str(month)

    payload = build_post_payload(hidden, start_y, start_m, end_y, end_m)
    headers_post = dict(DEFAULT_HEADERS)
    headers_post["Content-Type"] = "application/x-www-form-urlencoded"

    resp_post = session.post(BASE_URL, headers=headers_post, data=payload, timeout=60)
    resp_post.raise_for_status()
    soup_post = __import__("bs4").BeautifulSoup(resp_post.text, "html.parser")

    tables = parse_tables_to_json(soup_post)
    if not tables or not tables[0].get("rows"):
        return None

    return {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "url": BASE_URL,
        "start": {"year": start_y, "month": start_m},
        "end": {"year": end_y, "month": end_m},
        "tables": tables,
        "status_code": resp_post.status_code,
    }


def convert_to_quarter_rates(scraped_data: Dict[str, Any], year: int, month: int) -> Optional[Dict[str, Any]]:
    """Convert raw scraped data into a compact rates dict for one quarter-end date."""
    t = scraped_data["tables"][0]
    headers = t["headers"]
    rows = t["rows"]

    if not rows:
        return None

    latest = rows[-1]
    currency_headers = headers[1:]
    base_col_index = 4

    sgd_per = {}
    for i, h in enumerate(currency_headers):
        col_key = f"col_{base_col_index + i}"
        raw = latest.get(col_key)
        if raw is None or raw == "":
            continue
        try:
            val = float(raw)
        except ValueError:
            continue
        if "100" in h:
            val /= 100.0
        iso = parse_currency_from_header(h)
        sgd_per[iso] = val

    if not sgd_per:
        return None

    out = {}
    for base, s_base in sgd_per.items():
        if s_base == 0:
            continue
        out[base] = {}
        for quote, s_quote in sgd_per.items():
            if s_quote == 0:
                continue
            out[base][quote] = s_base / s_quote
        out[base]["SGD"] = s_base

    # Resolve the exact date from the last row's day column
    day_str = latest.get("col_3", "")
    try:
        dt = datetime(year, month, int(day_str))
        out["date"] = dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        out["date"] = f"{year}-{month:02d}"

    return out


def main():
    session = requests.Session()
    session.headers.update({
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept-Language": DEFAULT_HEADERS["Accept-Language"],
    })

    # Load existing data so we can resume / update in place
    existing: Dict[str, Any] = {}
    if os.path.exists(OUTPUT_FILENAME):
        try:
            with open(OUTPUT_FILENAME, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    quarters_data: Dict[str, Any] = existing.get("quarters", {})
    errors = []

    for year, q, end_month in quarters_since_2021():
        label = f"{year}-Q{q}"
        print(f"Fetching {label} (end month: {year}-{end_month:02d})...", end=" ", flush=True)

        try:
            raw = scrape_month(session, year, end_month)
            if raw is None:
                print(f"no data returned — skipping.")
                errors.append(label)
                continue

            rates = convert_to_quarter_rates(raw, year, end_month)
            if rates is None:
                print(f"could not parse rates — skipping.")
                errors.append(label)
                continue

            date_key = rates.get("date", label)
            quarters_data[date_key] = rates
            print(f"OK  (date: {date_key})")

        except Exception as e:
            print(f"FAILED: {e}")
            errors.append(label)

        # Be polite to the MAS server
        time.sleep(1.5)

    output = {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": BASE_URL,
        "quarters": dict(sorted(quarters_data.items())),
    }

    tmp = OUTPUT_FILENAME + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NoScientificNotationEncoder)
    os.replace(tmp, OUTPUT_FILENAME)

    print(f"\nSaved {len(quarters_data)} quarters to '{OUTPUT_FILENAME}'.")
    if errors:
        print(f"Quarters with errors / no data: {errors}", file=sys.stderr)


if __name__ == "__main__":
    main()
