"""
Weekly SGX periodic-financials update: scrape -> extract -> upsert to Supabase.

Designed to run unattended from GitHub Actions every Monday. It looks at the
last 7 days of SGX announcements, keeps the ones that carry half-yearly
financial statements for a top-N issuer by market cap, extracts the three
statement groups from the right PDF, converts to SGD, and upserts into
`sgx_periodic_financial` (primary key: symbol + date).

Re-running is safe and cheap. A filing is skipped before anything is downloaded
or sent to a model when the row it would produce is already in Supabase — the
announcement page states the period end, so the (symbol, date) key is known in
advance. A second check on `source_url` catches anything the first missed. The
7-day window therefore overlaps happily with last week's run.

Usage:
    python sgx_weekly_update.py                 # last 7 days, upsert
    python sgx_weekly_update.py --days 30       # wider window (backfill)
    python sgx_weekly_update.py --dry-run       # extract but do not write
    python sgx_weekly_update.py --plan-only     # list what it would process

Environment: SUPABASE_URL, SUPABASE_KEY, OPENROUTER_API_KEY.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

import sgx_scraper
from financial_statement_rag import build_client, extract_financials, parse_date
from sgx_financials_table import (
    HALF_BY_SUB_TITLE,
    build_table,
    financial_year,
    load_rates,
    safe_name,
    sgd_rate,
    to_sgd,
    top_symbols,
)
from sgx_pipeline import attachments_of, choose_pdf

TABLE = "sgx_periodic_financial"
# Composite primary key on the table; upserts resolve against it.
CONFLICT_KEY = "symbol,date"
DEFAULT_TOP = 200
DEFAULT_DAYS = 7
# SGX days run 16:00 -> 15:59:59 SGT.
DAY_START = "160000"
DAY_END = "155959"


def supabase_headers() -> dict:
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit("SUPABASE_URL / SUPABASE_KEY must be set")
    return {
        "url": url.rstrip("/"),
        "headers": {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
    }


def existing_rows() -> tuple[set, set]:
    """(symbol, date) keys and source URLs already in the table."""
    conn = supabase_headers()
    keys, urls = set(), set()
    step, offset = 1000, 0
    while True:
        resp = requests.get(
            f"{conn['url']}/rest/v1/{TABLE}",
            headers={**conn["headers"], "Range": f"{offset}-{offset + step - 1}"},
            params={"select": "symbol,date,source_url"},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json()
        for row in batch:
            keys.add((row["symbol"], row["date"]))
            if row.get("source_url"):
                urls.add(row["source_url"])
        if len(batch) < step:
            break
        offset += step
    return keys, urls


def upsert(rows: list[dict]) -> None:
    """Upserts on the table's (symbol, date) primary key."""
    if not rows:
        return
    conn = supabase_headers()
    resp = requests.post(
        f"{conn['url']}/rest/v1/{TABLE}",
        headers={**conn["headers"], "Prefer": "resolution=merge-duplicates"},
        params={"on_conflict": CONFLICT_KEY},
        data=json.dumps(rows),
        timeout=120,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Upsert failed [{resp.status_code}]: {resp.text[:500]}")


def window(days: int) -> tuple[str, str]:
    """SGX period bounds covering the last `days` days, inclusive of today."""
    today = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).date()
    start = today - timedelta(days=days)
    return f"{start:%Y%m%d}_{DAY_START}", f"{today:%Y%m%d}_{DAY_END}"


def fetch_recent_announcements(days: int) -> pd.DataFrame:
    """Scrapes the announcements API + detail pages for the window."""
    start, end = window(days)
    print(f"Announcement window: {start} -> {end}")

    sgx_scraper.PARAMS.update({"periodstart": start, "periodend": end})
    sgx_scraper.refresh_token()

    total = sgx_scraper.get("count")
    print(f"{total} financial-statement announcements in window")
    if not total:
        return pd.DataFrame()

    rows, page = [], 0
    while len(rows) < total:
        batch = sgx_scraper.get(pagestart=page)
        if not batch:
            break
        rows += batch
        page += 1

    df = pd.json_normalize(
        rows, "issuers", [c for c in rows[0] if c != "issuers"], meta_prefix="ann_"
    ).drop_duplicates(subset=["ann_ref_id", "stock_code"], ignore_index=True)

    # Detail pages carry sub_title and the attachment list.
    details, attachments = sgx_scraper.scrape_details(
        df["ann_url"].drop_duplicates().tolist()
    )
    if not details.empty:
        df = df.merge(details, on="ann_url", how="left")
    if not attachments.empty:
        grouped = attachments.groupby("ann_url").agg(
            attachment_count=("attachment_name", "size"),
            attachment_names=("attachment_name", sgx_scraper.ATTACHMENT_SEPARATOR.join),
            attachment_urls=("attachment_url", sgx_scraper.ATTACHMENT_SEPARATOR.join),
        ).reset_index()
        df = df.merge(grouped, on="ann_url", how="left")
        df["attachment_count"] = df["attachment_count"].fillna(0).astype(int)
    return df


def expected_date(row: pd.Series) -> str | None:
    """The `date` the row would get, read off the announcement's stated period end.

    Lets a filing be skipped before any download or model call. It matches what
    extraction reports because both describe the same period close: a full-year
    filing is stored on its second-half column, which ends on the year-end date.
    """
    parsed = parse_date(row.get("financial_period_ended"))
    if not parsed:
        return None
    return "{:04d}-{:02d}-{:02d}".format(*parsed)


def candidates(df: pd.DataFrame, symbols: set[str]) -> pd.DataFrame:
    """Top-N issuer, statements present, half-yearly period."""
    if df.empty:
        return df
    work = df[df["stock_code"].isin(symbols)].copy()
    work = work[work["sub_title"].isin(HALF_BY_SUB_TITLE)]
    if work.empty:
        return work
    work["period"] = work["sub_title"].map(HALF_BY_SUB_TITLE)
    work = work[work["attachment_count"].fillna(0) > 0]
    return work.reset_index(drop=True)


def process(row: pd.Series, rates: dict, client, seen_urls: set) -> dict | None:
    """Choose the statements PDF, extract it, convert to SGD."""
    pdf_path, ranking = choose_pdf(attachments_of(row))
    if pdf_path is None:
        print(f"  {row['stock_code']}: no statements PDF, skipping", file=sys.stderr)
        return None

    source_url = next(
        (a["url"] for a in attachments_of(row) if safe_name(a["name"]) == pdf_path.name),
        None,
    )
    if source_url in seen_urls:
        print(f"  {row['stock_code']}: already processed ({pdf_path.name}), skipping")
        return None

    result = extract_financials(pdf_path, client=client)
    period = result["period"]
    rate, quarter = sgd_rate(rates, period.get("currency"), period.get("period_end"))
    metrics = to_sgd(result["metrics"], rate) if rate else result["metrics"]
    if rate is None:
        print(f"  ! {row['stock_code']}: no SGD rate for {period.get('currency')}",
              file=sys.stderr)

    return {
        "ann_ref_id": row["ann_ref_id"],
        "stock_code": row["stock_code"],
        "symbol": row["stock_code"],
        "date": period.get("period_end"),
        "period": row["period"],
        "source_currency": period.get("currency"),
        "fx_rate_to_sgd": rate,
        "fx_quarter": quarter,
        "converted": rate is not None,
        "pdf": pdf_path.name,
        "source_url": source_url,
        "period_basis": period.get("period_basis", "as_reported"),
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--top", type=int, default=DEFAULT_TOP)
    parser.add_argument("--dry-run", action="store_true",
                        help="extract but don't write to Supabase")
    parser.add_argument("--plan-only", action="store_true",
                        help="list what would be processed, then stop")
    args = parser.parse_args()

    started = time.monotonic()
    print(f"=== SGX periodic financials weekly update "
          f"({datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}) ===")

    announcements = fetch_recent_announcements(args.days)
    if announcements.empty:
        print("Nothing announced in the window. Done.")
        return 0

    symbols = set(top_symbols(args.top)["symbol"])
    work = candidates(announcements, symbols)
    print(f"{len(work)} filings in scope (top {args.top} issuers, half-yearly statements)")
    if work.empty:
        return 0

    done_keys, done_urls = existing_rows()
    print(f"{len(done_keys)} rows already in {TABLE}")

    pending = []
    for _, row in work.iterrows():
        key = (row["stock_code"], expected_date(row))
        if key[1] and key in done_keys:
            print(f"  skip {row['stock_code']} {key[1]} {row['period']} — already stored")
            continue
        pending.append(row)

    print(f"{len(pending)} to process")
    if args.plan_only or not pending:
        for row in pending:
            print(f"  {row['stock_code']:>6} {expected_date(row)} {row['period']} "
                  f"— {row['sub_title']}")
        return 0

    rates, client, records = load_rates(), build_client(), []
    for i, row in enumerate(pending, 1):
        print(f"[{i}/{len(pending)}] {row['stock_code']} {row['period']}")
        try:
            record = process(row, rates, client, done_urls)
        except Exception as e:
            print(f"  ! {row['stock_code']} failed: {e}", file=sys.stderr)
            continue
        if record:
            records.append(record)
            done_urls.add(record["source_url"])

    if not records:
        print("Nothing new extracted. Done.")
        return 0

    table = build_table(records)
    payload = json.loads(table.to_json(orient="records"))
    # The three statement columns are jsonb in Postgres, not strings.
    for row in payload:
        for column in ("income_statement", "balance_sheet", "cash_flow"):
            row[column] = json.loads(row[column])

    if args.dry_run:
        print(f"[dry run] would upsert {len(payload)} rows:")
        print(table[["symbol", "date", "period", "financial_year"]].to_string(index=False))
    else:
        upsert(payload)
        print(f"Upserted {len(payload)} rows into {TABLE}")

    print(f"Done in {(time.monotonic() - started) / 60:.1f} min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
