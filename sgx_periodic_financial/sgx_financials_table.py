"""
Batch pipeline: SGX announcements -> half-yearly financials table, in SGD.

Builds one row per (symbol, half) with the three statement groups as JSON, for
the top-N issuers by market cap:

    symbol,date,income_statement,balance_sheet,cash_flow,period

  * `date`   — last day of the half, taken from the report itself (period_end).
  * `period` — "H1" or "H2".
  * the three statement columns hold the extracted metrics as a JSON object.

Scope rules:
  * Only issuers in the top N by market_cap from Supabase `sgx_company_report`
    (null market caps excluded).
  * Only announcements that actually carry financial statements. "Notification
    of Results Release", "Profit Guidance", auditor comments and the like are
    skipped — they announce or discuss results without presenting them.
  * Only half-yearly periods. Full-year filings are read on their second-half
    column (see financial_statement_rag.as_second_half), half-year filings give
    H1. Q1/Q3 filings are skipped: a quarter is not a half.

Currency: every value is converted to SGD using the MAS quarterly rates in
../quarterly_rates.json, at the quarter-end on or before the period end. Reports
already in SGD pass through untouched.

Runs in two phases so the slow parts can be parallel where it is safe:
  1. download + score every candidate's PDFs — serial, SGX rate-limits hard.
  2. extract — concurrent, these are OpenRouter calls.
Both phases cache to disk, so re-running resumes rather than repeating work.

Usage:
    python sgx_financials_table.py --top 200 -o sgx_half_yearly_financials.csv
    python sgx_financials_table.py --top 200 --plan-only     # scope, no spend
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values

from financial_statement_rag import build_client, extract_financials, parse_date
from sgx_pipeline import attachments_of, choose_pdf, load_announcements

HERE = Path(__file__).parent
RATES_FILE = HERE.parent / "quarterly_rates.json"
ENV_FILE = HERE.parent / ".env"
RESULT_CACHE = HERE / "sgx_financials_cache.jsonl"
DEFAULT_OUT = HERE / "sgx_half_yearly_financials.csv"

# sub_title values that carry actual statements, mapped to the half they report.
# Full-year filings are extracted on their second-half column, so they are H2.
HALF_BY_SUB_TITLE = {
    "Full Yearly Results": "H2",
    "Half Yearly Results": "H1",
    "Second Quarter and/ or Half Yearly Results": "H1",
}

# Extraction is an API call, so several can be in flight; PDF downloads are not.
EXTRACT_WORKERS = 4

# Symbols are stored with the exchange suffix, matching how SGX tickers are
# written elsewhere (Yahoo-style "D05.SI").
SYMBOL_SUFFIX = ".SI"


def with_suffix(symbol: str) -> str:
    """Adds the exchange suffix, leaving an already-suffixed symbol alone."""
    symbol = str(symbol)
    return symbol if symbol.endswith(SYMBOL_SUFFIX) else symbol + SYMBOL_SUFFIX


def top_symbols(limit: int) -> pd.DataFrame:
    """Top `limit` issuers by market cap from Supabase, nulls excluded."""
    # Real environment first (that is all CI has), then the local .env.
    env = {**dotenv_values(ENV_FILE), **os.environ}
    url, key = env.get("SUPABASE_URL"), env.get("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit("SUPABASE_URL / SUPABASE_KEY not set (env or .env)")
    url = url.rstrip("/")

    resp = requests.get(
        f"{url}/rest/v1/sgx_company_report",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
        params={
            "select": "symbol,name,market_cap",
            "market_cap": "not.is.null",
            "order": "market_cap.desc",
            "limit": limit,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def candidates(df: pd.DataFrame, symbols: set[str]) -> pd.DataFrame:
    """Announcements in scope: top-N issuer, statements present, half-yearly."""
    work = df[df["stock_code"].isin(symbols)].copy()
    work = work[work["sub_title"].isin(HALF_BY_SUB_TITLE)]
    work["period"] = work["sub_title"].map(HALF_BY_SUB_TITLE)
    work = work[work["attachment_count"].fillna(0) > 0]
    # One row per announcement; the CSV repeats it per issuer of the filing.
    return work.drop_duplicates(subset=["ann_ref_id", "stock_code"], ignore_index=True)


def load_rates() -> dict:
    return json.loads(RATES_FILE.read_text())["quarters"]


def sgd_rate(rates: dict, currency: str | None, period_end: str | None) -> tuple[float | None, str | None]:
    """SGD per unit of `currency`, at the quarter-end on or before `period_end`."""
    if not currency:
        return None, None
    if currency == "SGD":
        return 1.0, "n/a"

    dates = sorted(rates)
    usable = [d for d in dates if period_end and d <= period_end] or dates
    quarter = usable[-1]
    rate = (rates[quarter].get(currency) or {}).get("SGD")
    return rate, quarter


def to_sgd(metrics: dict, rate: float) -> dict:
    """Scales every non-null metric by `rate` and rounds to a whole unit.

    Values are absolute currency units (the confidence gate already multiplied
    out the printed scale), so sub-unit precision is noise — a cent on a figure
    reported in thousands. Share counts are whole numbers too, and are not
    scaled by the FX rate.
    """
    share_fields = {"basic_shares_outstanding", "diluted_shares_outstanding"}
    out = {}
    for group, values in metrics.items():
        out[group] = {
            name: (
                None
                if value is None
                else int(round(value if name in share_fields else value * rate))
            )
            for name, value in values.items()
        }
    return out


def load_cache() -> dict:
    """Completed extractions, keyed by (ref_id, stock_code)."""
    done = {}
    if RESULT_CACHE.exists():
        for line in RESULT_CACHE.read_text().splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            done[(record["ann_ref_id"], record["stock_code"])] = record
    return done


_cache_lock = threading.Lock()


def append_cache(record: dict) -> None:
    with _cache_lock:
        with RESULT_CACHE.open("a") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()


def prepare_pdfs(rows: list[pd.Series]) -> dict:
    """Phase 1 — download and score attachments serially, returning ref -> pdf path."""
    chosen = {}
    for i, row in enumerate(rows, 1):
        key = (row["ann_ref_id"], row["stock_code"])
        try:
            pdf_path, ranking = choose_pdf(attachments_of(row))
        except Exception as e:
            print(f"  [{i}/{len(rows)}] {row['stock_code']}: download failed — {e}",
                  file=sys.stderr)
            continue
        if pdf_path is None:
            best = ranking[0]["score"] if ranking else 0
            print(f"  [{i}/{len(rows)}] {row['stock_code']}: no statements PDF "
                  f"(best score {best})", file=sys.stderr)
            continue
        chosen[key] = pdf_path
        print(f"  [{i}/{len(rows)}] {row['stock_code']} {row['period']}: {pdf_path.name}")
    return chosen


def extract_one(row: pd.Series, pdf_path: Path, rates: dict, client) -> dict | None:
    """Phase 2 — extract one report and convert it to SGD."""
    result = extract_financials(pdf_path, client=client)
    period = result["period"]
    currency = period.get("currency")
    rate, quarter = sgd_rate(rates, currency, period.get("period_end"))

    if rate is None:
        print(f"  ! {row['stock_code']}: no SGD rate for {currency}; storing unconverted",
              file=sys.stderr)

    metrics = to_sgd(result["metrics"], rate) if rate else result["metrics"]
    return {
        "ann_ref_id": row["ann_ref_id"],
        "stock_code": row["stock_code"],
        "symbol": row["stock_code"],
        "date": period.get("period_end"),
        "period": row["period"],
        "source_currency": currency,
        "fx_rate_to_sgd": rate,
        "fx_quarter": quarter,
        "converted": rate is not None,
        "pdf": pdf_path.name,
        "source_url": next(
            (a["url"] for a in attachments_of(row)
             if safe_name(a["name"]) == pdf_path.name),
            None,
        ),
        "period_basis": period.get("period_basis", "as_reported"),
        "metrics": metrics,
    }


def financial_year(date: str | None, period: str) -> int | None:
    """The financial year a half belongs to, keyed off the issuer's book close.

    The book-close (fiscal year-end) month decides the label: a year ending in
    the first half of a calendar year is mostly made up of the *previous*
    calendar year, so it is labelled year-1; one ending in the second half keeps
    its own year. Singtel closing 31 Mar 2026 is therefore FY2025, and OCBC
    closing 31 Dec 2025 is also FY2025.

    A "H1" row is six months before the close, so the year-end is derived
    first — which keeps both halves of one financial year under one label.
    """
    parsed = parse_date(date)
    if not parsed:
        return None
    year, month, _ = parsed

    if period == "H1":
        month += 6
        if month > 12:
            month -= 12
            year += 1

    return year - 1 if month <= 6 else year


def safe_name(name: str) -> str:
    """The on-disk form of an attachment name, as `sgx_pipeline.download` writes it."""
    return re.sub(r"[^\w\-. ]+", "_", name).strip()


_url_index: dict | None = None


def attachment_url_index() -> dict:
    """(ref_id, cached filename) -> source URL, for records that predate `source_url`."""
    global _url_index
    if _url_index is None:
        _url_index = {}
        for _, row in load_announcements().iterrows():
            for attachment in attachments_of(row):
                key = (row["ann_ref_id"], safe_name(attachment["name"]))
                _url_index[key] = attachment["url"]
    return _url_index


def source_url_for(record: dict) -> str | None:
    """The PDF the figures were read from — stored on new records, looked up on old.

    `pdf_url` is the name earlier records used for the same thing.
    """
    stored = record.get("source_url") or record.get("pdf_url")
    if stored:
        return stored
    return attachment_url_index().get((record["ann_ref_id"], record["pdf"]))


def filled_metrics(record: dict) -> int:
    return sum(v is not None for g in record["metrics"].values() for v in g.values())


def filing_date(record: dict) -> str:
    """Filing date out of the ref_id ("SG260414OTHRNZTP" -> "260414")."""
    match = re.match(r"SG(\d{6})", str(record.get("ann_ref_id", "")))
    return match.group(1) if match else ""


def deduplicate(records: list[dict]) -> list[dict]:
    """One record per (symbol, financial year, half).

    Issuers routinely file the same period more than once — a results
    announcement, a financial summary and a dividend notice all carry the
    "Full Yearly Results" sub-title. Each is extracted separately, so the
    fullest extraction wins, and the later filing breaks a tie (an amended
    filing supersedes the original).
    """
    best: dict[tuple, dict] = {}
    for record in records:
        key = (
            record["symbol"],
            financial_year(record["date"], record["period"]),
            record["period"],
        )
        rank = (filled_metrics(record), filing_date(record))
        if key not in best or rank > (filled_metrics(best[key]), filing_date(best[key])):
            best[key] = record
    return list(best.values())


def build_table(records: list[dict]) -> pd.DataFrame:
    """The requested six-column shape, statements serialised as JSON."""
    rows = []
    for record in deduplicate(records):
        metrics = record["metrics"]
        rows.append(
            {
                "symbol": with_suffix(record["symbol"]),
                "date": record["date"],
                "income_statement": json.dumps(metrics.get("income_statement", {})),
                "balance_sheet": json.dumps(metrics.get("balance_sheet", {})),
                "cash_flow": json.dumps(metrics.get("cash_flow", {})),
                "period": record["period"],
                "financial_year": financial_year(record["date"], record["period"]),
                "source_url": source_url_for(record),
            }
        )
    table = pd.DataFrame(rows)
    if not table.empty:
        table = table.sort_values(["symbol", "date"], ignore_index=True)
    return table


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--top", type=int, default=200, help="issuers by market cap")
    parser.add_argument("-o", "--out", default=str(DEFAULT_OUT))
    parser.add_argument("--limit", type=int, help="cap announcements (for testing)")
    parser.add_argument("--plan-only", action="store_true",
                        help="report what would be processed, then stop")
    parser.add_argument("--skip-download", action="store_true",
                        help="phase 2 only, using PDFs already in pdf_cache")
    args = parser.parse_args()

    started = time.monotonic()
    top = top_symbols(args.top)
    symbols = set(top["symbol"])
    print(f"Top {len(top)} issuers by market cap "
          f"(S${top.market_cap.min():,.0f} - S${top.market_cap.max():,.0f})")

    announcements = load_announcements()
    work = candidates(announcements, symbols)
    if args.limit:
        work = work.head(args.limit)

    done = load_cache()
    pending = [r for _, r in work.iterrows()
               if (r["ann_ref_id"], r["stock_code"]) not in done]

    print(f"{len(work)} in-scope filings across {work.stock_code.nunique()} issuers "
          f"({work.period.value_counts().to_dict()})")
    print(f"{len(done)} already extracted, {len(pending)} to do")

    if args.plan_only:
        print(work.groupby(["period"])["stock_code"].count().to_string())
        return 0

    if pending:
        print("\nPhase 1 — downloading and scoring attachments (serial):")
        chosen = prepare_pdfs(pending) if not args.skip_download else {}

        print(f"\nPhase 2 — extracting {len(chosen)} reports "
              f"({EXTRACT_WORKERS} workers):")
        rates = load_rates()
        client = build_client()
        completed = 0
        with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as pool:
            futures = {}
            for row in pending:
                key = (row["ann_ref_id"], row["stock_code"])
                if key in chosen:
                    futures[pool.submit(extract_one, row, chosen[key], rates, client)] = key
            for future in as_completed(futures):
                key = futures[future]
                completed += 1
                try:
                    record = future.result()
                except Exception as e:
                    print(f"  ! {key[1]} extraction failed: {e}", file=sys.stderr)
                    continue
                append_cache(record)
                done[key] = record
                kept = sum(v is not None for g in record["metrics"].values()
                           for v in g.values())
                print(f"  [{completed}/{len(futures)}] {record['symbol']} "
                      f"{record['date']} {record['period']}: {kept}/33 metrics "
                      f"({record['source_currency']})")

    table = build_table(list(done.values()))
    table.to_csv(args.out, index=False)
    print(f"\nWrote {len(table)} rows to {args.out} "
          f"in {(time.monotonic() - started) / 60:.1f} min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
