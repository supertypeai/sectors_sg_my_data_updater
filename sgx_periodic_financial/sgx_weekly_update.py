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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    with_suffix,
    bare_symbol,
)
from sgx_pipeline import attachments_of, choose_pdf

TABLE = "sgx_periodic_financial"
# Composite primary key on the table; upserts resolve against it.
CONFLICT_KEY = "symbol,date"
DEFAULT_TOP = 200
DEFAULT_DAYS = 7
# Rows written per upsert during a long backfill.
UPSERT_CHUNK = 20
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


def explicit_window(start: str, end: str) -> tuple[str, str]:
    """SGX period bounds for an explicit YYYY-MM-DD range."""
    return (f"{start.replace('-', '')}_{DAY_START}",
            f"{end.replace('-', '')}_{DAY_END}")


def fetch_recent_announcements(days: int = None, bounds: tuple = None) -> pd.DataFrame:
    """Scrapes the announcements API for the window (detail pages come later)."""
    start, end = bounds if bounds else window(days)
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
    # The API title ends in the same sub-title the detail page reports
    # ("...::Full Yearly Results"), so scope can be decided before fetching a
    # single detail page — worth thousands of requests on a long backfill.
    df["api_sub_title"] = df["ann_title"].str.split("::").str[-1].str.strip()
    return df


def enrich_with_details(df: pd.DataFrame) -> pd.DataFrame:
    """Adds sub_title, financial_period_ended and attachments from detail pages."""
    if df.empty:
        return df
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


def prefilter(df: pd.DataFrame, symbols: set[str]) -> pd.DataFrame:
    """Scope from the API payload alone: top-N issuer, half-yearly statements."""
    if df.empty:
        return df
    bare = {bare_symbol(x) for x in symbols}
    work = df[df["stock_code"].isin(bare)]
    return work[work["api_sub_title"].isin(HALF_BY_SUB_TITLE)].reset_index(drop=True)


def in_range(date: str | None, start: str | None, stop: str | None) -> bool:
    """Whether a period end falls inside an inclusive YYYY-MM-DD range."""
    if not date:
        return False
    if start and date < start:
        return False
    if stop and date > stop:
        return False
    return True


def candidates(df: pd.DataFrame, symbols: set[str]) -> pd.DataFrame:
    """Top-N issuer, statements present, half-yearly period."""
    if df.empty:
        return df
    work = df[df["stock_code"].isin({bare_symbol(x) for x in symbols})].copy()
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


def to_payload(records: list[dict]) -> list[dict]:
    """Table rows ready for PostgREST (statement columns as jsonb, not strings)."""
    table = build_table(records)
    payload = json.loads(table.to_json(orient="records"))
    for row in payload:
        for column in ("income_statement", "balance_sheet", "cash_flow"):
            row[column] = json.loads(row[column])
    return payload


def choose_pdfs(rows: list, seen_urls: set) -> list:
    """Phase 1 — pick each filing's statements PDF, serially.

    Downloads stay single-threaded: links.sgx.com bans the IP for a burst.
    """
    chosen = []
    for i, row in enumerate(rows, 1):
        try:
            pdf_path, _ = choose_pdf(attachments_of(row))
        except Exception as e:
            print(f"  [{i}/{len(rows)}] {row['stock_code']}: download failed — {e}",
                  file=sys.stderr)
            continue
        if pdf_path is None:
            print(f"  [{i}/{len(rows)}] {row['stock_code']}: no statements PDF",
                  file=sys.stderr)
            continue
        source_url = next(
            (a["url"] for a in attachments_of(row)
             if safe_name(a["name"]) == pdf_path.name), None
        )
        if source_url in seen_urls:
            continue
        chosen.append((row, pdf_path, source_url))
        if i % 50 == 0:
            print(f"  [{i}/{len(rows)}] PDFs selected")
    return chosen


def extract_many(chosen: list, rates: dict, client, workers: int, flush) -> int:
    """Phase 2 — extract concurrently, flushing to Supabase as results arrive.

    A backfill runs for hours, so rows are written in chunks rather than in one
    upsert at the end; an interrupted run keeps everything it had finished, and
    re-running skips those filings.
    """
    done, buffer = 0, []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(extract_one_row, row, pdf, url, rates, client): row
            for row, pdf, url in chosen
        }
        for future in as_completed(futures):
            row = futures[future]
            done += 1
            try:
                record = future.result()
            except Exception as e:
                print(f"  ! {row['stock_code']} failed: {e}", file=sys.stderr)
                continue
            buffer.append(record)
            kept = sum(v is not None for g in record["metrics"].values() for v in g.values())
            print(f"  [{done}/{len(chosen)}] {record['symbol']} {record['date']} "
                  f"{record['period']}: {kept}/33 ({record['source_currency']})")
            if len(buffer) >= UPSERT_CHUNK:
                flush(buffer); buffer = []
    if buffer:
        flush(buffer)
    return done


def extract_one_row(row, pdf_path, source_url, rates: dict, client) -> dict:
    """Extraction half of `process`, for an already-chosen PDF."""
    result = extract_financials(pdf_path, client=client)
    period = result["period"]
    rate, quarter = sgd_rate(rates, period.get("currency"), period.get("period_end"))
    metrics = to_sgd(result["metrics"], rate) if rate else result["metrics"]
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
    parser.add_argument("--start", help="backfill: earliest announcement date, YYYY-MM-DD")
    parser.add_argument("--end", help="backfill: latest announcement date, YYYY-MM-DD")
    parser.add_argument("--period-from", help="keep only periods ending on/after this date")
    parser.add_argument("--period-to", help="keep only periods ending on/before this date")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP)
    parser.add_argument("--workers", type=int, default=1,
                        help="concurrent extractions (downloads stay serial)")
    parser.add_argument("--dry-run", action="store_true",
                        help="extract but don't write to Supabase")
    parser.add_argument("--plan-only", action="store_true",
                        help="list what would be processed, then stop")
    args = parser.parse_args()

    started = time.monotonic()
    print(f"=== SGX periodic financials weekly update "
          f"({datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}) ===")

    bounds = explicit_window(args.start, args.end) if (args.start and args.end) else None
    announcements = fetch_recent_announcements(args.days, bounds)
    if announcements.empty:
        print("Nothing announced in the window. Done.")
        return 0

    symbols = set(top_symbols(args.top)["symbol"])
    # Scope first from the API payload, so detail pages are only fetched for
    # filings that can actually produce a row.
    scoped = prefilter(announcements, symbols)
    print(f"{len(announcements)} announcements -> {len(scoped)} in scope "
          f"(top {args.top} issuers, half-yearly statements)")
    if scoped.empty:
        return 0

    work = candidates(enrich_with_details(scoped), symbols)

    if args.period_from or args.period_to:
        before = len(work)
        work = work[work.apply(
            lambda r: in_range(expected_date(r), args.period_from, args.period_to), axis=1
        )].reset_index(drop=True)
        print(f"{len(work)} of {before} fall in "
              f"{args.period_from or 'any'} .. {args.period_to or 'any'}")
    if work.empty:
        return 0

    done_keys, done_urls = existing_rows()
    print(f"{len(done_keys)} rows already in {TABLE}")

    pending = []
    for _, row in work.iterrows():
        # Stored symbols carry the exchange suffix; compare on the same form.
        key = (with_suffix(row["stock_code"]), expected_date(row))
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

    rates, client = load_rates(), build_client()

    print(f"\nPhase 1 — selecting statements PDFs for {len(pending)} filings (serial):")
    chosen = choose_pdfs(pending, done_urls)
    print(f"{len(chosen)} filings have a usable statements PDF")
    if not chosen:
        print("Nothing to extract. Done.")
        return 0

    written = 0

    def flush(batch):
        nonlocal written
        payload = to_payload(batch)
        if args.dry_run:
            print(f"  [dry run] would upsert {len(payload)} rows")
        else:
            upsert(payload)
            written += len(payload)
            print(f"  -> upserted {len(payload)} ({written} so far)")

    print(f"\nPhase 2 — extracting with {args.workers} worker(s):")
    extract_many(chosen, rates, client, args.workers, flush)
    print(f"\n{'Would have written' if args.dry_run else 'Wrote'} {written} rows to {TABLE}")

    print(f"Done in {(time.monotonic() - started) / 60:.1f} min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
