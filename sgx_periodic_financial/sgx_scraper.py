"""
Scraper for SGX company announcements (https://api.sgx.com/announcements/v1.1/).

Two non-obvious requirements:
  1. curl_cffi impersonation — Akamai fingerprints the TLS handshake, so plain
     `requests`/`urllib` get a 403 even when the auth header is valid.
  2. An `authorizationToken` header — the SGX web app pulls it from its CMS
     (`we_chat_qr_validator`) and ROT13-decodes it before use; we do the same.
"""

import codecs
import json
import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests

API = "https://api.sgx.com/announcements/v1.1/"
APP_CONFIG_URL = "https://www.sgx.com/config/appconfig.json"
OUTPUT_CSV = "sgx_announcements.csv"
DETAIL_CACHE = "sgx_announcement_details.jsonl"
# Announcements can have several PDFs; they share one cell, joined by this.
ATTACHMENT_SEPARATOR = " | "
# Seconds between announcement-page requests. Do not lower this or parallelise
# it: links.sgx.com is behind Akamai and will ban the IP for a burst.
DETAIL_DELAY = 1.0


class BlockedError(Exception):
    """Raised when Akamai returns 403 Access Denied for the announcement pages."""

# Detail-page <dt> labels -> column names. Anything not listed is slugified.
DETAIL_FIELDS = {
    "Issuer/ Manager": "issuer_manager",
    "Securities": "securities",
    "Stapled Security": "stapled_security",
    "Announcement Title": "detail_title",
    "Date &Time of Broadcast": "broadcast_datetime",
    "Status": "status",
    "Announcement Sub Title": "sub_title",
    "Announcement Reference": "reference",
    "Submitted By (Co./ Ind. Name)": "submitted_by",
    "Designation": "designation",
    "For Financial Period Ended": "financial_period_ended",
    "Effective Date and Time of the event": "effective_datetime",
}

PARAMS = {
    # SGX days run 16:00 -> 15:59:59 SGT, so a range covering the whole of
    # 1 Sep 2025 - 30 Jun 2026 starts on the evening of 31 Aug.
    "periodstart": "20250831_160000",
    "periodend": "20260805_155959",
    "cat": "ANNC",
    "sub": "ANNC17",  # drop this key to fetch every sub-category
    # The API returns `data: null` for very large pages; 250 is the safe ceiling.
    "pagesize": 250,
}

session = requests.Session(impersonate="chrome")


def refresh_token():
    """Fetches the token the API gateway requires and caches it on the session."""
    cfg = session.get(APP_CONFIG_URL).json()
    token_url = f"{cfg['endpoints']['CMS_API_URL']}/?queryId={cfg['CMS_VERSION']}:we_chat_qr_validator"
    raw = session.get(token_url).json()["data"]["qrValidator"]
    session.headers["authorizationToken"] = codecs.encode(raw, "rot13")


def get(path="", **extra):
    """GETs an announcements endpoint, refreshing the token once if it has expired."""
    resp = session.get(API + path, params={**PARAMS, **extra})
    if resp.status_code == 401:
        refresh_token()
        resp = session.get(API + path, params={**PARAMS, **extra})
    resp.raise_for_status()
    return resp.json()["data"]


detail_session = requests.Session(impersonate="chrome")


def fetch_detail(url):
    """
    Scrapes one announcement page (links.sgx.com) for its detail fields.

    The page is a series of <dl><dt>label</dt><dd>value</dd></dl> groups plus a
    list of attachment links. Returns (fields dict, list of attachment dicts).
    """
    resp = detail_session.get(url, headers={"Referer": "https://www.sgx.com/"}, timeout=60)
    if resp.status_code == 403:
        raise BlockedError(f"403 Access Denied for {url}")
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    fields = {"ann_url": url}
    for dt in soup.select("dl dt"):
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        label = dt.get_text(strip=True)
        value = dd.get_text(" ", strip=True)
        if not value:
            continue
        # The description label embeds a long instruction sentence; trim it.
        if label.startswith("Description"):
            key = "description"
        else:
            key = DETAIL_FIELDS.get(label) or re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
        fields[key] = value

    attachments = [
        {
            "ann_url": url,
            "attachment_name": a.get_text(strip=True),
            "attachment_url": urljoin(url, a["href"]),
        }
        for a in soup.select("a.announcement-attachment")
    ]
    return fields, attachments


def scrape_details(urls):
    """
    Fetches announcement pages one at a time, caching each to DETAIL_CACHE.

    links.sgx.com is behind Akamai and bans the client IP for a sustained burst
    (a parallel run of ~1800 pages is enough), so this is deliberately serial
    with a delay between requests. Results are appended to a JSONL cache as they
    arrive, so a run interrupted by a ban resumes where it left off.
    """
    done = {}
    if os.path.exists(DETAIL_CACHE):
        with open(DETAIL_CACHE) as f:
            for line in f:
                record = json.loads(line)
                done[record["fields"]["ann_url"]] = record
        print(f"Reusing {len(done)} cached announcement pages from '{DETAIL_CACHE}'.")

    todo = [u for u in urls if u not in done]
    print(f"Fetching {len(todo)} announcement pages (~{len(todo) * DETAIL_DELAY / 60:.0f} min)...")

    with open(DETAIL_CACHE, "a") as cache:
        for i, url in enumerate(todo, 1):
            try:
                fields, attachments = fetch_detail(url)
            except BlockedError:
                print(f"  BLOCKED after {i - 1} pages — Akamai is refusing this IP.\n"
                      f"  {len(done)} pages are cached; wait a while and re-run to resume.")
                break
            except Exception as e:
                print(f"  WARN failed {url}: {e}")
                continue

            record = {"fields": fields, "attachments": attachments}
            done[url] = record
            cache.write(json.dumps(record) + "\n")
            cache.flush()

            if i % 50 == 0:
                print(f"  {i}/{len(todo)} pages")
            time.sleep(DETAIL_DELAY)

    details = [r["fields"] for r in done.values()]
    attachments = [a for r in done.values() for a in r["attachments"]]
    return pd.DataFrame(details), pd.DataFrame(attachments)


def main():
    refresh_token()

    # meta.totalItems on the paged response only reports the page size, so the
    # separate count endpoint is what tells us when to stop.
    total = get("count")
    print(f"{total} announcements for {PARAMS['periodstart']} -> {PARAMS['periodend']}")

    rows, page = [], 0
    while len(rows) < total:
        batch = get(pagestart=page)
        if not batch:
            break
        rows += batch
        page += 1
        print(f"  fetched {len(rows)}/{total}")

    # One row per (announcement, issuer), so it joins cleanly on stock_code.
    df = pd.json_normalize(rows, "issuers", [c for c in rows[0] if c != "issuers"], meta_prefix="ann_")
    df = df.drop_duplicates(subset=["ann_ref_id", "stock_code"], ignore_index=True)

    # Scrape each announcement page once, then merge onto every issuer row.
    details, attachments = scrape_details(df["ann_url"].drop_duplicates().tolist())
    if not details.empty:
        df = df.merge(details, on="ann_url", how="left")

    if not attachments.empty:
        # An announcement can carry several PDFs. Collapse them onto the
        # announcement so the row count stays one per (announcement, issuer);
        # multiple values are separated by ATTACHMENT_SEPARATOR.
        grouped = attachments.groupby("ann_url").agg(
            attachment_count=("attachment_name", "size"),
            attachment_names=("attachment_name", ATTACHMENT_SEPARATOR.join),
            attachment_urls=("attachment_url", ATTACHMENT_SEPARATOR.join),
        ).reset_index()
        df = df.merge(grouped, on="ann_url", how="left")
        df["attachment_count"] = df["attachment_count"].fillna(0).astype(int)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} rows x {df.shape[1]} columns to '{OUTPUT_CSV}'")


if __name__ == "__main__":
    main()
