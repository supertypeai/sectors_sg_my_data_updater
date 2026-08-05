"""
End-to-end SGX pipeline: announcement -> right PDF -> extracted financials JSON.

Ties `sgx_scraper.py` (which lists announcements and their attachments) to
`financial_statement_rag.py` (which pulls metrics out of one report PDF). The
piece in between is *choosing* which attachment to read, because a results
announcement typically ships several: Singtel's FY26 filing has five, of which
only one is the actual financial statements.

Selection is content-based, not filename-based. Filenames are issuer-controlled
and inconsistent ("FY26-CCIFS.pdf", "KREIT 1H 2026_Unaudited Results.pdf"), so
every candidate is downloaded and scored on what it actually contains: how many
distinct financial-statement headings appear, how often, and how dense the page
text is in digits. A presentation deck loses because it has pictures of numbers,
not statements of them. Filename only contributes a small nudge.

Usage:
    python sgx_pipeline.py --row 264 -o singtel_full_25_test.json
    python sgx_pipeline.py --stock Z74 -o out.json
    python sgx_pipeline.py --ref SG260521OTHRZZGD --dry-run   # show scores only

Needs pdfplumber + openai (financial_statement_rag) and OPENROUTER_API_KEY.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import pdfplumber
import requests

from financial_statement_rag import build_client, extract_financials

ANNOUNCEMENTS_CSV = Path(__file__).parent / "sgx_announcements.csv"
PDF_DIR = Path(__file__).parent / "pdf_cache"
# links.sgx.com serves the PDFs without the Akamai gate that gates the HTML
# announcement pages, but stay polite anyway — a burst got this IP banned once.
DOWNLOAD_DELAY = 1.0
DOWNLOAD_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.sgx.com/"}

# Headings that only appear in a real financial-statements document. Counted per
# distinct phrase, so a report that says "statement of cash flows" seven times
# does not outrank one that presents all three statements once each.
STATEMENT_PHRASES = {
    "statement of financial position": 3.0,
    "balance sheet": 2.0,
    "statement of cash flows": 3.0,
    "cash flow statement": 2.0,
    "consolidated income statement": 3.0,
    "statement of profit or loss": 3.0,
    "statement of comprehensive income": 2.5,
    "statement of total return": 2.5,
    "notes to the financial statements": 2.0,
    "cash flows from operating activities": 2.0,
    "total assets": 1.5,
    "total equity": 1.5,
    "total liabilities": 1.5,
}
MAX_PHRASE_COUNT = 3  # diminishing returns past this
DISTINCT_PHRASE_WEIGHT = 4.0
DIGIT_RATIO_WEIGHT = 120.0

# Filename nudges only — never decisive on their own.
NAME_BONUS = {
    r"ccifs|financial\s*statement|unaudited|audited|\bfs\b": 6.0,
    r"result|mda|md&a|management\s*discussion": 3.0,
}
NAME_PENALTY = {
    r"slide|presentation|deck|infographic|factsheet": 10.0,
    r"news\s*release|media\s*release|press\s*release|\bnr\b": 6.0,
    r"circular|notice|proxy|minutes|sustainab|governance": 6.0,
}
# Below this nothing in the announcement looks like a financial report.
MIN_USABLE_SCORE = 10.0


def load_announcements(path: Path = ANNOUNCEMENTS_CSV) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(
            f"{path.name} not found. Run `python sgx_scraper.py` first to build it."
        )
    return pd.read_csv(path, dtype={"ann_submission_date": str})


def select_announcement(df: pd.DataFrame, row=None, ref=None, stock=None) -> pd.Series:
    """Picks one announcement by row index, reference, or (latest for) stock code."""
    if row is not None:
        return df.iloc[row]
    if ref:
        hits = df[df["ann_ref_id"] == ref]
        if hits.empty:
            raise SystemExit(f"No announcement with ref_id {ref}")
        return hits.iloc[0]
    if stock:
        hits = df[df["stock_code"].astype(str).str.upper() == stock.upper()]
        if hits.empty:
            raise SystemExit(f"No announcements for stock code {stock}")
        # Most recent filing for that issuer.
        return hits.sort_values("ann_submission_date").iloc[-1]
    raise SystemExit("Pass one of --row, --ref or --stock")


def attachments_of(row: pd.Series) -> list[dict]:
    """Splits the joined attachment cells back into name/url pairs."""
    if not row.get("attachment_count") or pd.isna(row.get("attachment_urls")):
        return []
    names = str(row["attachment_names"]).split(" | ")
    urls = str(row["attachment_urls"]).split(" | ")
    return [{"name": n, "url": u} for n, u in zip(names, urls)]


def download(attachment: dict, directory: Path = PDF_DIR) -> Path | None:
    """Downloads one attachment, reusing the cached copy when present.

    Cached under the announcement's own id, because attachment names are not
    unique across filings — "JMH.pdf", "Announcement.pdf" and "20251231
    Financial Report.pdf" are each used by several different announcements for
    entirely different documents. A flat cache served the first download to
    every later announcement that happened to share a filename.
    """
    # .../corporate-announcements/<announcement id>/<file>
    parts = [p for p in attachment["url"].split("/") if p]
    announcement_id = parts[-2] if len(parts) >= 2 else "misc"
    directory = directory / announcement_id
    directory.mkdir(parents=True, exist_ok=True)

    # Attachment names are issuer-supplied; keep them readable but path-safe.
    safe = re.sub(r"[^\w\-. ]+", "_", attachment["name"]).strip() or "attachment.pdf"
    path = directory / safe
    if path.exists() and path.stat().st_size > 0:
        return path

    resp = requests.get(attachment["url"], headers=DOWNLOAD_HEADERS, timeout=120)
    time.sleep(DOWNLOAD_DELAY)
    if resp.status_code != 200 or not resp.content:
        print(f"  ! download failed ({resp.status_code}): {attachment['name']}",
              file=sys.stderr)
        return None
    path.write_bytes(resp.content)
    return path


def score_pdf(path: Path) -> dict:
    """
    Scores how much a PDF looks like the financial statements themselves.

    Reads the text layer once and rewards distinct statement headings plus digit
    density. A slide deck scores near zero: its numbers live in images, so its
    text layer is sparse and headline-shaped.
    """
    try:
        with pdfplumber.open(path) as pdf:
            pages = len(pdf.pages)
            text = " ".join((p.extract_text() or "") for p in pdf.pages).lower()
    except Exception as e:
        return {"score": 0.0, "pages": 0, "error": str(e)}

    if not text.strip():
        # No text layer at all — scanned. Not disqualifying (the extractor can
        # OCR it), but it can't be scored on content, so it ranks last.
        return {"score": 0.0, "pages": pages, "chars": 0, "scanned": True}

    score, distinct = 0.0, 0
    for phrase, weight in STATEMENT_PHRASES.items():
        count = text.count(phrase)
        if count:
            distinct += 1
            score += weight * min(count, MAX_PHRASE_COUNT)
    score += DISTINCT_PHRASE_WEIGHT * distinct

    digit_ratio = len(re.findall(r"\d", text)) / max(len(text), 1)
    score += DIGIT_RATIO_WEIGHT * digit_ratio

    name = path.name.lower()
    for pattern, bonus in NAME_BONUS.items():
        if re.search(pattern, name):
            score += bonus
    for pattern, penalty in NAME_PENALTY.items():
        if re.search(pattern, name):
            score -= penalty

    return {
        "score": round(score, 2),
        "pages": pages,
        "chars": len(text),
        "distinct_phrases": distinct,
        "digit_ratio": round(digit_ratio, 4),
        "scanned": False,
    }


def choose_pdf(attachments: list[dict]) -> tuple[Path | None, list[dict]]:
    """Downloads every attachment and returns the best-scoring one plus the ranking."""
    candidates = []
    for attachment in attachments:
        if not attachment["name"].lower().endswith(".pdf"):
            continue
        path = download(attachment)
        if path is None:
            continue
        result = score_pdf(path)
        result.update(name=attachment["name"], path=str(path), url=attachment["url"])
        candidates.append(result)
        print(f"  {result['score']:7.2f}  {attachment['name']}")

    if not candidates:
        return None, []
    candidates.sort(key=lambda c: c["score"], reverse=True)
    best = candidates[0]
    if best["score"] < MIN_USABLE_SCORE:
        return None, candidates
    return Path(best["path"]), candidates


def run(row=None, ref=None, stock=None, out=None, dry_run=False, csv=ANNOUNCEMENTS_CSV,
        **extract_kwargs) -> dict:
    df = load_announcements(Path(csv))
    ann = select_announcement(df, row=row, ref=ref, stock=stock)

    print(f"{ann['stock_code']} {ann['security_name']} — {ann.get('sub_title')} "
          f"(period ended {ann.get('financial_period_ended')}, ref {ann['ann_ref_id']})")

    attachments = attachments_of(ann)
    if not attachments:
        raise SystemExit("That announcement has no attachments.")
    print(f"Scoring {len(attachments)} attachment(s):")

    pdf_path, ranking = choose_pdf(attachments)
    if pdf_path is None:
        raise SystemExit(
            "No attachment looks like a financial report "
            f"(best score {ranking[0]['score'] if ranking else 0} < {MIN_USABLE_SCORE})."
        )
    print(f"Selected: {pdf_path.name}")

    payload = {
        "announcement": {
            "ref_id": ann["ann_ref_id"],
            "stock_code": ann["stock_code"],
            "security_name": ann["security_name"],
            "sub_title": ann.get("sub_title"),
            "submission_date": ann.get("ann_submission_date"),
            "financial_period_ended": ann.get("financial_period_ended"),
            "announcement_url": ann["ann_url"],
        },
        "pdf_selection": {
            "selected": pdf_path.name,
            "selected_url": next(c["url"] for c in ranking if c["path"] == str(pdf_path)),
            "candidates": ranking,
        },
    }

    if dry_run:
        print("--dry-run: stopping before extraction.")
        return payload

    payload["extraction"] = extract_financials(
        pdf_path, client=build_client(), **extract_kwargs
    )

    if out:
        Path(out).write_text(json.dumps(payload, indent=2))
        metrics = payload["extraction"]["metrics"]
        kept = sum(v is not None for g in metrics.values() for v in g.values())
        total = sum(len(g) for g in metrics.values())
        used = payload["extraction"]["usage"]
        print(f"{kept}/{total} metrics extracted -> {out} "
              f"(cost ${used.get('cost_usd', 0.0):.5f})")
    else:
        print(json.dumps(payload, indent=2))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    picker = parser.add_mutually_exclusive_group(required=True)
    picker.add_argument("--row", type=int, help="0-based row index in the CSV")
    picker.add_argument("--ref", help="announcement ref_id, e.g. SG260521OTHRZZGD")
    picker.add_argument("--stock", help="stock code; uses that issuer's latest filing")
    parser.add_argument("-o", "--out", help="write the result JSON here")
    parser.add_argument("--csv", default=str(ANNOUNCEMENTS_CSV))
    parser.add_argument("--dry-run", action="store_true",
                        help="download and score the PDFs but skip extraction")
    parser.add_argument("--min-confidence", type=float, default=None)
    parser.add_argument("--context-chars", type=int, default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument(
        "--full-year-basis",
        choices=["second_half", "full_year"],
        default=None,
        help="what to read out of a full-year report (default: the second-half column)",
    )
    args = parser.parse_args()

    extract_kwargs = {}
    if args.full_year_basis:
        extract_kwargs["full_year_basis"] = args.full_year_basis
    if args.min_confidence is not None:
        extract_kwargs["min_confidence"] = args.min_confidence
    if args.context_chars is not None:
        extract_kwargs["context_chars"] = args.context_chars
    if args.model:
        extract_kwargs["model"] = args.model

    run(row=args.row, ref=args.ref, stock=args.stock, out=args.out,
        dry_run=args.dry_run, csv=args.csv, **extract_kwargs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
