"""
Scrape SGX corporate actions into the idx-parity tables:
sgx_dividend, sgx_ca_bonus, sgx_right_issue, sgx_stock_split.

Two sources, each doing what it alone can do:

  1. api.sgx.com/corporateactions/v1.0?ibmcode=<code>  (no auth, ~1992+, 13k rows)
     The only source with a full, dated cash-dividend history. Also carries
     BONUS / RIGHTS / ENTITLEMENT ratio rows, but its `exDate` is unreliable for
     non-cash actions (measured: exDate present but no price move), so it is not
     used for splits or for dated bonus/rights.

  2. api.sgx.com/announcements/v1.1 cat=CACT  (token + TLS impersonation)
     CACT06 cash dividend filings (gross AND net rate + Payment Type),
     CACT01 bonus, CACT18 rights, CACT21 split, CACT17 consolidation -
     all with the settled Ex Date / Record Date on the final filing.
     Verified against sgx_daily_data price jumps: CACT01 94% match.

Everything is dry-run by default. Nothing is written without --write --yes.

Traps encoded below (all measured, not guessed):
  * `ibmcode` must be lowercase-c; `ibmCode=` is silently ignored and returns
    all ~40k rows.
  * `code` is always null in corporateactions - join on ibmCode only.
  * The SGX `id` is only unique per ibmCode, and 8 ibmCodes carry two listings
    (NS8U/P7VU, AWX/XWA, ...). Uploading the raw id would violate the
    UNIQUE (id) constraints, so colliding ids are nulled and the row still
    upserts on its (symbol, date) primary key.
  * Same-day cash rows are summed into one row per (symbol, ex_date) so the
    grain matches idx_dividend and Yahoo. ENTITLEMENT "Rate: X Per Security"
    rows are cash components and belong in that sum: reconciled against Yahoo
    on 2023+ data, including them lifts exact agreement 78.6% -> 80.0%
    (C2PU 2026-08-11: 0.0632 + 0.0116 + 0.0129 = 0.0877 = Yahoo exactly).
  * ENTITLEMENT / BONUS ratio rows are NOT used for splits: most are bonus
    warrants, not share subdivisions, and the price does not move. Splits come
    from CACT21/CACT17 (explicit New:Old), which is what the price confirms.
  * "N Cash Options" rows and percentage-of-par rows carry no per-share amount
    and are skipped, not guessed at.
  * SGX never publishes cum_date; it is derived from sgx_daily_data.

Usage:
    python sgx_scrape/sgx_corporate_actions.py                  # dry run, 20 symbols
    python sgx_scrape/sgx_corporate_actions.py --all             # dry run, all active
    python sgx_scrape/sgx_corporate_actions.py --symbols D05 O39 --preview out.csv
    python sgx_scrape/sgx_corporate_actions.py --all --write --yes
"""
import argparse
import bisect
import codecs
import csv
import datetime
import json
import logging
import os
import re
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CORPORATE_ACTIONS_URL = "https://api.sgx.com/corporateactions/v1.0"
METADATA_URL = "https://api.sgx.com/marketmetadata/v2"
ANNOUNCEMENTS_URL = "https://api.sgx.com/announcements/v1.1/"
APP_CONFIG_URL = "https://www.sgx.com/config/appconfig.json"

# CACT category -> table. CACT06 (cash dividends) is deliberately absent: it is
# 11k filings and corporateactions already carries the same dividends with
# deeper history. Pass --with-cact06 to cross-check them instead.
CACT_SPECS = {
    "CACT21": "split",   # Stock Split / Subdivision
    "CACT17": "split",   # Share Consolidation
    "CACT01": "bonus",   # Bonus Issue / Capitalisation
    "CACT18": "rights",  # Rights
}
CACT06_SPEC = {"CACT06": "dividend"}
CACT_START = "20140101_000000"  # CACT21/CACT17 floor; older splits only exist as ratios

CURRENCY_RE = re.compile(r"(SGD|USD|HKD|MYR|THB|CNY|AUD|EUR|GBP|JPY|CHF|TWD|IDR|PHP)")
CENTS_RE = re.compile(r"CENTS?\b", re.I)
SKIP_RE = re.compile(r"cash options|please see details|NA\s+NA", re.I)
OFFER_RE = re.compile(r"OFFER OF\s*(\d+(?:\.\d+)?)\s*FOR\s*(\d+(?:\.\d+)?)", re.I)
RATIO_RE = re.compile(r"Ratio:\s*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", re.I)
CACT_RATIO_RE = re.compile(r"^\s*([\d.]+)\s*:\s*([\d.]+)\s*$")
NUMBER_RE = re.compile(r"(\d+(?:\.\d+)?)")

# Each table's conflict target: the composite PK the migration declares.
CONFLICT_KEYS = {
    "sgx_dividend": "symbol,date",
    "sgx_ca_bonus": "symbol,ex_date",
    "sgx_right_issue": "symbol,ex_date",
    "sgx_stock_split": "symbol,date",
    "sgx_upcoming_dividend": "symbol,ex_date",
}

# compact_rates.json lives at the repo root; same convention and file as
# sgx_screener_scraper_daily.py, so dividends and prices share one FX source.
# Daily-refreshed FX published by supertypeai/sectors_get_conversion_rate.
FX_URL = ("https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/"
          "master/conversion_rate.json")
# Local fallback: same shape, but only refreshed when currency_scraper.py runs and
# it carries currencies the remote file omits (CHF, JPY).
RATES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compact_rates.json")

BATCH_SIZE = 500
# corporateactions is a plain JSON API with no Akamai in front of it, unlike
# links.sgx.com (see CACT_DETAIL_WORKERS). 8 has been safe across 620 symbols.
SYMBOL_WORKERS = int(os.getenv("SGX_SYMBOL_WORKERS", "8"))
_log = logging.getLogger("sgx_corporate_actions")


def _flatten(table):
    return {cur: (row or {}).get("SGD")
            for cur, row in (table or {}).items()
            if cur != "datetime" and (row or {}).get("SGD")}


def load_sgd_rates(url=FX_URL, path=RATES_FILE):
    """{currency: SGD per unit}.

    Reads the daily-updated remote table and fills any gap from the local file.
    The remote scraper only requests a fixed currency list, so CHF and JPY are
    absent there even though SGX pays them - without the fallback those amounts
    would be left unconverted.
    """
    rates, source = {}, None
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        rates = _flatten(payload)
        source = f"remote ({payload.get('datetime', '?')})"
    except Exception as exc:
        _log.warning("FX fetch failed (%s); falling back to %s", exc, path)

    try:
        with open(path) as fh:
            local = _flatten(json.load(fh))
    except Exception as exc:
        local = {}
        if not rates:
            _log.warning("no FX table at %s (%s); amounts will NOT be converted", path, exc)

    missing = {c: v for c, v in local.items() if c not in rates}
    rates.update(missing)
    if source:
        _log.info("FX: %d currencies from %s%s", len(rates), source,
                  f", {len(missing)} filled from {os.path.basename(path)}" if missing else "")
    return rates


def to_sgd(value, currency, rates):
    """Convert a per-share amount to SGD.

    SGX declares dividends and rights prices in the counter's own currency
    (BBW pays HKD 2.8, not SGD 0.45). The table has no currency column, and its
    sibling idx_dividend is single-currency, so the stored figure must be SGD.
    An unknown currency is reported rather than silently treated as 1:1 - that is
    how a HKD amount ends up 6x too large.
    """
    if value is None:
        return None
    if not currency or str(currency).upper() == "SGD":
        return round(value, 8)
    rate = rates.get(str(currency).upper())
    if not rate:
        _log.warning("no SGD rate for %s; leaving the amount unconverted", currency)
        return round(value, 8)
    return round(value * rate, 8)
_token_lock = threading.Lock()


def get_with_retry(sess, url, params, timeout, attempts=3):
    """SGX occasionally stalls; a transient timeout must not silently become
    'this company has no corporate actions'."""
    last = None
    for attempt in range(attempts):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
        except Exception as exc:
            last = exc
        else:
            if resp.status_code >= 500 or resp.status_code in (408, 429):
                last = f"HTTP {resp.status_code}"      # transient: retry
            elif resp.status_code >= 400:
                # 400/403/404 never succeed on retry and each attempt sleeps.
                resp.raise_for_status()                # fail fast, no retry
            else:
                return resp
        if attempt + 1 < attempts:
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{url} failed after {attempts} attempts: {last}")


def session():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    return s


# ---------------------------------------------------------------- anticipation


def epoch_to_date(ms):
    """SGX sends epoch milliseconds, not ISO strings."""
    return datetime.datetime.utcfromtimestamp(ms / 1000).date() if ms else None


def corporate_actions_date(ms):
    """Date from a corporateactions row, shifted to the true calendar date.

    Every date in this feed is one day early - verified 75/75 against CACT06's
    announced dates (never 0 or 2), for Ex Date, Record Date AND Pay Date:
        D05 2026 dividend  corporateactions 12/15/23  ->  CACT06 13/16/24
    The price drop confirms CACT06: D05 falls 71.00 -> 69.90 on the 13th.
    Deriving cum_date from the shifted ex-date also reproduces the raw value,
    which is only coherent if this feed stores the cum date in `exDate`.
    """
    raw = epoch_to_date(ms)
    return raw + datetime.timedelta(days=1) if raw else None


def parse_payment_type(text):
    if re.search(r"ONE-?TIER", text, re.I):
        return "ONE-TIER TAX"
    if re.search(r"LESS TAX", text, re.I):
        return "LESS TAX"
    if re.search(r"TAX EXEMPT", text, re.I):
        return "TAX EXEMPT"
    if re.search(r"NET OF TAX", text, re.I):
        return "NET OF TAX"
    return None


def parse_cash(text):
    """
    Per-share cash amount from an SGX `particulars` string.

    Formats seen, all handled:
        Rate: SGD 0.81  Per Security   -> 0.81 SGD
        SGD 0.16 LESS TAX              -> 0.16 SGD
        010418 - 300918 SGD 0.0244     -> 0.0244 SGD (period prefix ignored)
        (SGD) 8 CENTS LESS TAX         -> 0.08 SGD
        1.5 CENTS                      -> 0.015 SGD (no currency token -> SGD)
        USD 0.0121 Per Security        -> 0.0121 USD
        15% LESS TAX                   -> None (percent of par, no amount)
        3 Cash Options                 -> None (no amount at all)

    Returns (value, currency, payment_type) or None.
    """
    if not text:
        return None
    t = " ".join(text.split())
    if SKIP_RE.search(t):
        return None

    payment_type = parse_payment_type(t)
    cents = bool(CENTS_RE.search(t))
    cur = CURRENCY_RE.search(t)

    if cur:
        currency = cur.group(1)
        after = t[cur.end():]
        m = NUMBER_RE.search(after)
        if not m:
            return None
        value = float(m.group(1))
    else:
        # A bare percentage is a rate on par value, never a per-share amount.
        if "%" in t:
            return None
        m = NUMBER_RE.search(t)
        if not m:
            return None
        if not cents:
            # "8" with no unit - the currency is unknowable, so refuse it.
            return None
        value = float(m.group(1))
        currency = "SGD"

    return (value / 100 if cents else value), currency, payment_type


def parse_offer(text):
    """'OFFER OF A FOR B' -> (a, b)."""
    m = OFFER_RE.search(text or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def parse_ratio(text):
    """'Ratio: X:Y' -> (X, Y)."""
    m = RATIO_RE.search(text or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def bonus_factor(text):
    """Share factor for a bonus issue, verified against 21 real price jumps
    (D05 2024 1-for-10 -> 1.1, 3-for-1 -> 4.0, Ratio 4:3 -> 1.75)."""
    pair = bonus_ratios(text)
    if not pair:
        return None
    old, new = pair
    return (old + new) / old if old else None


def rights_factor(old, new):
    """IDX parity: idx_right_issue.factor is (old + new) / old, ratio-only,
    independent of the issue price (PACK 5:102 -> 21.4, PEGE 3:1 -> 1.3333)."""
    return (old + new) / old if old else None


def rights_ratios(text):
    """(old, new) = (underlying shares held, rights securities issued).

    The same string means the opposite of BONUS, so the anncType must never be
    dropped. Verified against CACT18's explicit fields for the same symbol:
        'Ratio: 9:10'          -> 9 rights for 10 held -> old=10, new=9  (VI2)
        'Ratio: 1:5'           -> 1 right for 5 held   -> old=5,  new=1  (5HV/ADN)
        'OFFER OF A FOR B @ P' -> A rights for B held  -> old=B,  new=A
    """
    offer = parse_offer(text)
    if offer:
        new, old = offer
        return old, new
    ratio = parse_ratio(text)
    if ratio:
        new, old = ratio
        return old, new
    return None


# ------------------------------------------------------------------- sources


class AnnouncementClient:
    """CACT announcements. Needs a ROT13 token and a Chrome TLS fingerprint:
    Akamai 403s plain `requests` even with a valid auth header."""

    def __init__(self):
        self._session = cffi_requests.Session(impersonate="chrome")
        self._refresh_token()

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _refresh_token(self, attempts=4):
        """Fetch the ROT13 gateway token.

        Akamai answers a burst of requests with a 403 HTML "Access Denied" page
        rather than JSON, so this must detect that and back off instead of dying
        on a JSON decode error. Blocking is IP-wide and temporary.
        """
        with _token_lock:
            last = None
            for attempt in range(attempts):
                try:
                    resp = self._session.get(APP_CONFIG_URL, timeout=60)
                    if resp.status_code == 403 or not resp.text.strip().startswith("{"):
                        last = f"blocked (status {resp.status_code})"
                    else:
                        cfg = resp.json()
                        url = (f"{cfg['endpoints']['CMS_API_URL']}/?queryId="
                               f"{cfg['CMS_VERSION']}:we_chat_qr_validator")
                        tok = self._session.get(url, timeout=60)
                        if tok.status_code == 200 and tok.text.strip().startswith("{"):
                            raw = tok.json()["data"]["qrValidator"]
                            self._session.headers["authorizationToken"] = codecs.encode(raw, "rot13")
                            return
                        last = f"token fetch status {tok.status_code}"
                except Exception as exc:
                    last = repr(exc)
                time.sleep(20 * (attempt + 1))
            raise RuntimeError(
                "SGX is blocking this IP (Akamai 403). Wait and re-run, or pass "
                f"--skip-cact. Last error: {last}"
            )

    def list(self, sub, start=CACT_START, end=None, attempts=4):
        """All announcements for one CACT sub-category.

        Retried like detail(): a connection reset mid-listing would otherwise
        abort the run, and losing pages silently is worse - the caller cannot
        tell a truncated listing from a complete one.
        """
        end = end or datetime.datetime.utcnow().strftime("%Y%m%d_235959")
        out, page = [], 0
        while True:
            last = None
            for attempt in range(attempts):
                try:
                    resp = self._session.get(ANNOUNCEMENTS_URL, params={
                        "periodstart": start, "periodend": end, "cat": "CACT",
                        "sub": sub, "pagesize": 250, "pagestart": page}, timeout=120)
                    if resp.status_code == 401:
                        self._refresh_token()
                        continue
                    if resp.status_code == 403:
                        last = "HTTP 403 (banned)"
                        time.sleep(60 * (attempt + 1))
                        continue
                    resp.raise_for_status()
                    batch = (resp.json() or {}).get("data") or []
                    break
                except Exception as exc:
                    last = repr(exc)
                    time.sleep(5 * (attempt + 1))
            else:
                raise RuntimeError(f"listing {sub} page {page} failed after {attempts} "
                                   f"attempts: {last}")
            if not batch:
                return out
            out += batch
            page += 1

    def detail(self, url, attempts=3):
        """Announcement page <dl><dt>label</dt><dd>value</dd> pairs.

        links.sgx.com answers 502/503 under moderate concurrency. A failed detail
        page silently drops that dividend, so it is retried rather than skipped.
        """
        from bs4 import BeautifulSoup
        last = None
        for attempt in range(attempts):
            try:
                resp = self._session.get(url, headers={"Referer": "https://www.sgx.com/"},
                                         timeout=60)
                if resp.status_code == 403:
                    # Akamai has banned this IP; minutes, not seconds, to clear.
                    last = "HTTP 403 (banned)"
                    time.sleep(60 * (attempt + 1))
                    continue
                if resp.status_code >= 500 or resp.status_code == 429:
                    last = f"HTTP {resp.status_code}"
                    time.sleep(5 * (attempt + 1))
                    continue
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                out = {}
                for dt in soup.select("dl dt"):
                    dd = dt.find_next_sibling("dd")
                    if dd is not None:
                        out[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)
                return out
            except Exception as exc:
                last = repr(exc)
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"detail failed after {attempts} attempts ({last}): {url}")


def _parse_detail_date(text):
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%d-%b-%Y"):
        try:
            return datetime.datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_money(text):
    """'SGD 0.643' -> (0.643, 'SGD')"""
    m = CURRENCY_RE.search(text or "")
    if not m:
        return None, None
    n = NUMBER_RE.search(text[m.end():])
    return (float(n.group(1)) if n else None), m.group(1)


class CorporateActionsClient:
    """corporateactions/v1.0 - the dated cash-dividend history."""

    def __init__(self):
        self._session = session()

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def ibm_code(self, symbol):
        """marketmetadata only resolves currently-listed counters, which is
        exactly the active universe we scrape."""
        try:
            resp = get_with_retry(self._session, METADATA_URL, {"stock-code": symbol}, 30)
            data = resp.json().get("data")
        except Exception as exc:
            _log.error("[%s] marketmetadata failed after retries: %s", symbol, exc)
            return None
        return data[0].get("ibmCode") if data else None

    def rows(self, ibm_code):
        """All corporate actions for one ibmCode.

        `ibm_code` must be present and non-empty: a None/'' value makes the API
        ignore the filter and return all ~40k rows.

        The filter is a PREFIX match, not equality. Asking for 543 also returns
        543L (an Erajaya bond), 508 returns 5081, 507 returns 507Q/507X (SocGen
        warrants). Measured: 22 such rows across 4 symbols, all bonds or
        structured warrants whose coupons would otherwise be booked as
        dividends. So rows are filtered to an exact ibmCode here.
        """
        if not ibm_code:
            return []
        out, page = [], 0
        while True:
            # lowercase `ibmcode`: `ibmCode` is ignored and returns the whole table.
            resp = get_with_retry(self._session, CORPORATE_ACTIONS_URL,
                                  {"ibmcode": ibm_code, "pagesize": 250, "pagestart": page}, 60)
            payload = resp.json()
            batch = payload.get("data") or []
            if not batch:
                return out
            out += [r for r in batch if r.get("ibmCode") == ibm_code]
            page += 1
            if page >= payload.get("meta", {}).get("totalPages", 0):
                return out


# ------------------------------------------------------------------- builders


def build_dividends(symbol, rows, rates=None):
    """
    Sum same-day cash rows into one record per (symbol, ex_date).
    ENTITLEMENT rows carrying a per-share rate are cash components, not
    entitlements - confirmed against Yahoo.

    Each component is converted to SGD before the sum: a distribution can mix
    currencies across its components, and adding raw numbers would be nonsense.
    """
    buckets = defaultdict(lambda: {"amounts": [], "currency": None, "payment_type": None,
                                   "recording_date": None, "payment_date": None,
                                   "id": None})
    for row in rows:
        if row.get("anncType") not in ("DIVIDEND", "ENTITLEMENT", "ENTITL."):
            continue
        parsed = parse_cash(row.get("particulars"))
        if not parsed:
            continue
        value, currency, payment_type = parsed
        ex_date = corporate_actions_date(row.get("exDate"))
        if not ex_date:
            continue
        b = buckets[ex_date]
        b["amounts"].append(to_sgd(value, currency, rates or {}))
        b["currency"] = b["currency"] or currency
        b["payment_type"] = b["payment_type"] or payment_type
        b["recording_date"] = b["recording_date"] or corporate_actions_date(row.get("recDate"))
        b["payment_date"] = b["payment_date"] or corporate_actions_date(row.get("datePaid"))
        if b["id"] is None and row.get("particulars", "").upper().startswith("RATE:"):
            b["id"] = row.get("id")

    records = []
    for ex_date, b in buckets.items():
        amount = combine_same_day(b["amounts"])
        if amount <= 0:
            continue
        records.append({
            "symbol": symbol,
            "date": ex_date.isoformat(),
            "dividend": amount,
            "recording_date": b["recording_date"].isoformat() if b["recording_date"] else None,
            "payment_date": b["payment_date"].isoformat() if b["payment_date"] else None,
            "payment_type": b["payment_type"],
            "currency": "SGD",
            "id": b["id"],
            "source": "sgx_corporateactions",
            "_components": list(b["amounts"]),
        })
    return records


def bonus_ratios(text):
    """(old, new) in the SGX encoding, so (new+old)/old is the share factor.

    'OFFER OF A FOR B'  A new shares for B held -> old=B, new=A
    'Ratio: X:Y'        X old : Y new          -> old=X, new=Y
                        (matches CACT01's 'Additional: Old', e.g. D05 1:10)
    """
    offer = parse_offer(text)
    if offer:
        new, old = offer
        return old, new
    ratio = parse_ratio(text)
    if ratio:
        old, new = ratio
        return old, new
    return None


def build_bonus(symbol, rows):
    """BONUS rows with a real ratio."""
    out = []
    for row in rows:
        if row.get("anncType") != "BONUS":
            continue
        text = row.get("particulars") or ""
        # Bonus *warrants* are a separate security, not a share capitalisation.
        if re.search(r"WARRANT|WTS|\bWT\b|WRTS", text, re.I):
            continue
        pair = bonus_ratios(text)
        ex_date = corporate_actions_date(row.get("exDate"))
        if not pair or not ex_date:
            continue
        old, new = pair
        out.append({
            "symbol": symbol,
            "ex_date": ex_date.isoformat(),
            "recording_date": corporate_actions_date(row.get("recDate")).isoformat() if row.get("recDate") else None,
            "old_ratio": old,
            "new_ratio": new,
            "payment_date": corporate_actions_date(row.get("datePaid")).isoformat() if row.get("datePaid") else None,
            "id": row.get("id"),
            "source": "sgx_corporateactions",
        })
    return out


def build_rights(symbol, rows, rates=None):
    out = []
    for row in rows:
        if row.get("anncType") != "RIGHTS":
            continue
        text = row.get("particulars") or ""
        pair = rights_ratios(text)
        ex_date = corporate_actions_date(row.get("exDate"))
        if not pair or not ex_date:
            continue
        old, new = pair
        price = None
        m = re.search(r"@\s*(.*)$", text)
        if m:
            price, cur = _parse_money(m.group(1))
            price = to_sgd(price, cur, rates or {})
        out.append({
            "symbol": symbol,
            "ex_date": ex_date.isoformat(),
            "recording_date": corporate_actions_date(row.get("recDate")).isoformat() if row.get("recDate") else None,
            "old_ratio": old,
            "new_ratio": new,
            "price": price,
            "factor": rights_factor(old, new),
            "trading_period_start": None,
            "trading_period_end": None,
            "subscription_date": None,
            "id": row.get("id"),
            "source": "sgx_corporateactions",
        })
    return out


def cact_records(client, spec_map, wanted, counter_currency=None,
                 detail_workers=None, per_kind_wanted=None, rates=None,
                 announcements=None):
    """
    One record per (symbol, corporate-action reference), taking the newest
    filing because only the later Replacement carries the settled Ex Date.

    `wanted` is the set of bare codes to keep: the announcement listing is
    market-wide, so filtering here avoids fetching a detail page per filing
    (CACT06 alone is 11k of them). A mapping value may be a per-kind code set
    ({"dividend": {...}}) to narrow one category further.
    Returns {table: [records]}.
    """
    best = {}
    detail_workers = detail_workers or int(os.getenv("CACT_DETAIL_WORKERS", "3"))
    counter_currency = counter_currency or {}
    wanted = wanted or set()
    per_kind_wanted = per_kind_wanted or {}
    for sub, kind in spec_map.items():
        keep = per_kind_wanted.get(kind, wanted)
        # `announcements` lets the caller list a narrow window once and reuse it.
        listing = announcements.get(sub) if announcements else None
        if listing is None:
            listing = client.list(sub)
        _log.info("%s: %d announcements", sub, len(listing))

        # Only filings that touch a symbol we actually want. The listing is
        # market-wide and CACT06 alone is 11k filings, so this is the difference
        # between ~200 detail pages and ~11k.
        work = []
        for ann in listing:
            if ann.get("title", "").startswith("Withdrawal"):
                continue
            codes = [i.get("stock_code") for i in (ann.get("issuers") or [])
                     if i.get("stock_code") in keep]
            if codes:
                work.append((ann, codes))
        _log.info("%s: %d relevant filings (of %d listed)", sub, len(work), len(listing))

        # Announcement pages are static HTML and independent, so fetch them
        # concurrently. Measured: 297 ms/page serial, 120 ms at 3 workers,
        # 61 ms at 6. Eight workers earned a 403 on a sustained run, so the
        # default is 4 - well clear of the ban, still ~4x faster. Override with
        # CACT_DETAIL_WORKERS; lower it if you start seeing 403s.
        def fetch(item):
            ann, codes = item
            # A failed detail page drops that dividend component, which silently
            # changes the total (M44U 2026-11-04 became 0.00611 instead of
            # 0.00414 when one page 403'd). So this raises rather than skipping:
            # an incomplete table must never look like a complete one.
            return ann, codes, client.detail(ann["url"])

        with ThreadPoolExecutor(max_workers=detail_workers) as pool:
            for ann, codes, fields in pool.map(fetch, work):
                if not fields:
                    continue
                ref = fields.get("Corporate Action Reference")
                if not ref:
                    continue
                # One corporate-action ref can cover several filings: the same
                # dividend offered in two currencies, or a replacement. Key on
                # the announced dates as well so they do not collapse.
                for code in codes:
                    key = (kind, code, ref,
                           fields.get("Ex Date"), fields.get("Record Date"),
                           fields.get("Option Currency"),
                           fields.get("Gross Rate (Per Share)"))
                    previous = best.get(key)
                    if previous is None or ann["submission_date"] > previous["submitted"]:
                        best[key] = {"fields": fields, "submitted": ann["submission_date"],
                                     "code": code, "kind": kind, "sub": sub}

    out = defaultdict(list)
    for key, entry in best.items():
        kind, code = key[0], key[1]
        f = entry["fields"]
        symbol = code + ".SI"
        ex_date = _parse_detail_date(f.get("Ex Date"))
        rec_date = _parse_detail_date(f.get("Record Date"))

        if kind == "dividend":
            value, currency = _parse_money(
                f.get("Net Rate (Per Share)") or f.get("Gross Rate (Per Share)"))
            gross, _ = _parse_money(f.get("Gross Rate (Per Share)"))
            if not value or not ex_date:
                continue
            out["sgx_dividend"].append({
                "symbol": symbol, "date": ex_date.isoformat(),
                "dividend": to_sgd(value, currency, rates or {}),
                "dividend_original": to_sgd(gross, currency, rates or {}),
                "recording_date": rec_date.isoformat() if rec_date else None,
                "payment_date": _iso(_parse_detail_date(f.get("Pay Date"))),
                "payment_type": f.get("Payment Type"),
                "currency": "SGD", "id": None, "source": "sgx_cact",
                # The forward calendar is keyed on this. Take it from THIS
                # entry: `ref` in the enclosing scope is a leftover from the
                # fetch loop and is the same value for every row.
                "reference": f.get("Corporate Action Reference"),
                "_counter_currency": counter_currency.get(symbol),
            })

        elif kind == "bonus":
            pair = cact_ratio(f.get("Distribution Ratio (Additional: Old)"))
            if not pair or not ex_date:
                continue
            additional, old = pair
            out["sgx_ca_bonus"].append({
                "symbol": symbol, "ex_date": ex_date.isoformat(),
                "recording_date": rec_date.isoformat() if rec_date else None,
                "old_ratio": old, "new_ratio": additional,
                "payment_date": None,
                "id": None, "source": "sgx_cact",
            })

        elif kind == "rights":
            old = f.get("Rights Security Distribution Ratio- Underlying")
            new = f.get("Rights Security Distribution Ratio- Rights Security")
            if not old or not new or not ex_date:
                continue
            price, currency = _parse_money(f.get("Issue Price (Per Rights)"))
            price = to_sgd(price, currency, rates or {})
            uitoe = NUMBER_RE.search(str(old))
            nitoe = NUMBER_RE.search(str(new))
            if not uitoe or not nitoe:
                continue
            out["sgx_right_issue"].append({
                "symbol": symbol, "ex_date": ex_date.isoformat(),
                "recording_date": rec_date.isoformat() if rec_date else None,
                "old_ratio": float(uitoe.group(1)), "new_ratio": float(nitoe.group(1)),
                # 'Underlying' is old shares held, 'Rights Security' is new issued:
                # already the (old, new) order the schema wants.
                "price": price, "factor": rights_factor(float(uitoe.group(1)), float(nitoe.group(1))),
                "trading_period_start": _iso(_parse_detail_date(f.get("Trading Period From"))),
                "trading_period_end": _iso(_parse_detail_date(f.get("Trading Period To"))),
                # 'Exercise Period' is a range ('18/08/2026 TO 01/09/2026'); the
                # subscription deadline is its second date.
                "subscription_date": _iso(_period_end(f.get("Exercise Period"))),
                "id": None, "source": "sgx_cact",
            })

        elif kind == "split":
            pair = cact_ratio(f.get("Distribution Ratio (New: Old)"))
            if not pair or not ex_date:
                continue
            new, old = pair
            factor = new / old if old else None
            if not factor:
                continue
            out["sgx_stock_split"].append({
                "symbol": symbol, "date": ex_date.isoformat(),
                "split_ratio": factor, "ratio": f.get("Distribution Ratio (New: Old)"),
                "recording_date": rec_date.isoformat() if rec_date else None,
                "applied_on": None, "id": None, "source": "sgx_cact",
            })
    return out


def cact_ratio(text):
    m = CACT_RATIO_RE.match(str(text or ""))
    return (float(m.group(1)), float(m.group(2))) if m else None


def _iso(value):
    return value.isoformat() if value else None


def _period_end(text):
    """'18/08/2026 TO 01/09/2026' -> 01/09/2026. A single date passes through."""
    if not text:
        return None
    parts = re.split(r"\s+TO\s+", text, flags=re.I)
    return _parse_detail_date(parts[-1])


# ----------------------------------------------------------------------- data


def session_dates(symbols):
    """{symbol: {ex_date: last trading session before it}} from sgx_daily_data.
    SGX never publishes cum_date."""
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    sessions = defaultdict(list)
    for i in range(0, len(symbols), 100):
        batch = symbols[i:i + 100]
        params = {"select": "symbol,date", "order": "symbol,date",
                  "symbol": f"in.({','.join(batch)})"}
        offset = 0
        while True:
            resp = requests.get(f"{SUPABASE_URL.rstrip('/')}/rest/v1/sgx_daily_data",
                                headers={**headers, "Range": f"{offset}-{offset + 999}"},
                                params=params, timeout=120)
            resp.raise_for_status()
            rows = resp.json()
            for r in rows:
                sessions[r["symbol"]].append(r["date"])
            if len(rows) < 1000:
                break
            offset += 1000
    return sessions


def attach_cum_dates(records, sessions):
    """cum_date = the last trading session strictly before the ex-date."""
    for rows in records.values():
        for row in rows:
            key = row.get("date") or row.get("ex_date")
            series = sessions.get(row["symbol"]) or []
            i = bisect.bisect_left(series, key)
            row["cum_date"] = series[i - 1] if i else None
    return records


# ---------------------------------------------------------------------- dedupe


def combine_same_day(amounts):
    """One distribution can reach us either as components or as components PLUS
    their total, and the two must not be confused with each other.

    XZL 2025-03-06 arrives as 0.00248, 0.006, 0.00848 where the last is the sum
    of the first two - adding all three gives 0.01696, nearly double the real
    0.00879 Yahoo reports. Conversely M44U 2026-11-03 is 0.00414 + 0.00791 with
    no total row, and there the sum (0.01205) is correct.

    So: if one amount equals the sum of the others, that amount is the total;
    otherwise the components add up.
    """
    values = sorted({round(a, 10) for a in amounts}, reverse=True)
    if len(values) == 1:
        return values[0]
    largest, rest = values[0], values[1:]
    if rest and abs(largest - sum(rest)) <= max(1e-9, largest * 0.01):
        return largest
    return round(sum(values), 8)


def dedupe_ids(table, rows):
    """
    SGX `id` is unique per ibmCode, not globally: 8 dual-listed pairs share one.
    Null the colliding ids so UNIQUE (id) holds; the (symbol, date) PK still
    identifies the row.
    """
    counts = Counter(r["id"] for r in rows if r.get("id") is not None)
    colliding = {i for i, n in counts.items() if n > 1}
    dropped = 0
    for r in rows:
        if r.get("id") in colliding:
            r["id"] = None
            dropped += 1
    if dropped:
        _log.info("%s: nulled %d id(s) shared across dual-listed counters", table, dropped)
    return rows


def merge_dividend_sources(corp_rows, cact_rows, complete_cact_dates=False):
    """Union both sources' components for an ex-date, without double-counting.

    Neither source alone is complete. M44U 2026-11-04:
        corporateactions  0.00414 + 0.00791
        CACT06            0.00414 + 0.00611
    The 0.00414 is the SAME component reported twice; the full distribution is
    0.00414 + 0.00611 + 0.00791 = 0.01816, which is exactly what Yahoo reports.
    So take the distinct amounts across both sources, then apply the
    total-vs-components rule.
    """
    corp_by, cact_by = defaultdict(list), defaultdict(list)
    for r in corp_rows:
        corp_by[(r["symbol"], r["date"])].append(r)
    for r in cact_rows:
        cact_by[(r["symbol"], r["date"])].append(r)

    merged = []
    downgraded = 0
    for key in set(corp_by) | set(cact_by):
        # A windowed CACT read covers only recent filings, so a date with no CACT
        # row is NOT evidence that CACT has nothing - it may simply be outside the
        # window. Writing corporateactions alone then silently LOWERS a correct
        # stored value: N2IU 2025-10-29 is 0.0201 from the union but 0.0047 from
        # corporateactions alone (measured: 124 rows across 7 REITs). Skip those
        # dates unless the caller proved CACT was read in full.
        if not complete_cact_dates and key not in cact_by:
            downgraded += 1
            continue
        # Union the per-share COMPONENTS, not the per-source totals: M44U reports
        # 0.00414 in both sources, and summing the totals would count it twice.
        components = []
        for r in corp_by.get(key, []):
            components += r.get("_components") or [r["dividend"]]
        for r in cact_by.get(key, []):
            components.append(r["dividend"])

        # CACT rows are preferred for metadata (payment_type) and carry the
        # authoritative ex-date and the real amount where corporateactions only
        # has a scrip-option stub.
        base = dict((cact_by.get(key) or corp_by[key])[0])
        base.pop("_counter_currency", None)
        base.pop("_components", None)
        base["dividend"] = combine_same_day(components)
        merged.append(base)
    if downgraded:
        _log.warning("skipped %d date(s) where corporateactions has no CACT "
                     "counterpart (windowed CACT read); their stored values are "
                     "left untouched", downgraded)
    return merged


def merge_sources(primary, secondary, key):
    """
    `primary` wins on a key collision (it is the better-sourced row); the
    secondary only fills keys primary has nothing for.
    """
    seen = {key(r) for r in primary}
    return primary + [r for r in secondary if key(r) not in seen]


# ----------------------------------------------------------------------- write


def strip_internal(rows):
    """Remove bookkeeping keys before the rows reach Postgres or a CSV."""
    for r in rows:
        r.pop("_components", None)
        r.pop("_counter_currency", None)
    return rows


# The exact writable columns per table, from the migration. PostgREST accepts a
# bulk upsert only when every object in the batch carries the SAME keys
# (PGRST102 "All object keys must match"), and rejects unknown keys (PGRST204).
# Rows arrive with different provenances - a CACT dividend has
# `dividend_original`, a corporateactions one does not - so every row is
# normalised to this list, filling absent columns with NULL.
# `updated_on` is set explicitly in write_table(): a column DEFAULT only applies
# on INSERT, so an ON CONFLICT DO UPDATE leaves the old timestamp in place and
# the column would silently report a stale "last updated" (verified against the
# live table: a re-upsert of an identical row did not move it).
TABLE_COLUMNS = {
    "sgx_dividend": ["symbol", "date", "dividend", "yield", "dividend_original",
                     "recording_date", "cum_date", "payment_date", "payment_type",
                     "currency", "id", "source"],
    "sgx_ca_bonus": ["symbol", "ex_date", "recording_date", "old_ratio", "new_ratio",
                     "cum_date", "payment_date", "id", "source"],
    "sgx_right_issue": ["symbol", "ex_date", "recording_date", "old_ratio", "new_ratio",
                        "price", "factor", "cum_date", "trading_period_start",
                        "trading_period_end", "subscription_date", "id", "source"],
    "sgx_stock_split": ["symbol", "date", "split_ratio", "ratio", "cum_date",
                        "recording_date", "applied_on", "id", "source"],
    # Forward calendar, keyed (symbol, ex_date) like every other table in this
    # set. `reference` is retained as provenance only: one announcement can cover
    # many issuers, and one ex-date can carry several components.
    "sgx_upcoming_dividend": ["symbol", "ex_date", "recording_date",
                              "dividend_amount", "payment_date", "cum_date",
                              "reference", "source"],
}


def normalise(table, rows):
    """Project each row onto the table's column list, so a bulk upsert is
    homogeneous and no internal or stray key can reach Postgres."""
    cols = TABLE_COLUMNS[table]
    out = []
    for r in rows:
        dropped = set(r) - set(cols) - {"_components", "_counter_currency"}
        if dropped:
            # Expected: sources differ in provenance (a CACT row has no
            # `payment_type`, a corporateactions one has no `reference`).
            _log.debug("%s: not writable, dropping key(s) %s", table, sorted(dropped))
        out.append({c: r.get(c) for c in cols})
    return out


def write_table(table, rows):
    if not rows:
        _log.info("%s: nothing to write", table)
        return
    rows = normalise(table, rows)
    # ON CONFLICT DO UPDATE does not apply the column DEFAULT, so the timestamp
    # has to be carried in the payload or it never advances.
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for r in rows:
        r["updated_on"] = stamp
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates,return=minimal"}
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?on_conflict={CONFLICT_KEYS[table]}"
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        resp = requests.post(endpoint, headers=headers, json=batch, timeout=120)
        if resp.status_code >= 400:
            # PostgREST explains the rejection in the body; a bare "400" from
            # raise_for_status() hides which column or constraint is at fault.
            raise RuntimeError(f"{table} write rejected ({resp.status_code}): {resp.text[:800]}")
        resp.raise_for_status()
        _log.info("  %s: upserted %d/%d", table, min(i + BATCH_SIZE, len(rows)), len(rows))


def prune_calendar(before):
    """Drop forward-calendar rows whose ex-date has passed.

    ONLY sgx_upcoming_dividend. The four history tables are append/upsert-only:
    an expired calendar row is already in sgx_dividend by the time this runs, so
    deleting here loses nothing.
    """
    if not before or before > datetime.date.today().isoformat():
        raise RuntimeError(
            f"refusing to prune with bound {before!r}: it must be today or earlier")
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Prefer": "return=representation"}
    resp = requests.delete(
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/sgx_upcoming_dividend",
        headers=headers, params={"ex_date": f"lt.{before}"}, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"prune rejected ({resp.status_code}): {resp.text[:400]}")
    removed = len(resp.json() or [])
    _log.info("pruned %d expired calendar row(s) with ex_date < %s", removed, before)
    return removed


# ------------------------------------------------------------------------ main


def main():
    ap = argparse.ArgumentParser(description="Scrape SGX corporate actions (dry run by default)")
    ap.add_argument("--symbols", nargs="+", help="bare or .SI codes; default: first N active")
    ap.add_argument("--all", action="store_true", help="every active symbol in sgx_companies")
    ap.add_argument("--limit", type=int, default=20, help="active symbols when --all is absent")
    ap.add_argument("--preview", help="write the computed rows to this CSV")
    ap.add_argument("--skip-cact", action="store_true", help="corporateactions only (no announcement scraping)")
    ap.add_argument("--no-cact06", action="store_true",
                    help="skip CACT06 cash filings (the authoritative dividend source)")
    ap.add_argument("--notices-window", type=int, metavar="DAYS",
                    help="incremental mode: only read CACT filings announced in the "
                         "last DAYS days, and only refresh the symbols they touch "
                         "(one page per category instead of the full 2014+ history)")
    ap.add_argument("--split-calendar", action="store_true",
                    help="route dividends whose ex-date is in the future into "
                         "sgx_upcoming_dividend instead of sgx_dividend")
    ap.add_argument("--prune-calendar", action="store_true",
                    help="delete sgx_upcoming_dividend rows whose ex-date has passed "
                         "(they are already in sgx_dividend by then)")
    ap.add_argument("--write", action="store_true", help="upsert to Supabase")
    ap.add_argument("--yes", action="store_true", help="confirm the write")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The supabase client logs every HTTP call at INFO; keep our own output readable.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("SUPABASE_URL / SUPABASE_KEY must be set")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.symbols:
        symbols = [s if s.endswith(".SI") else s + ".SI" for s in args.symbols]
        currency_by_symbol = {
            r["symbol"]: r.get("currency")
            for r in sb.table("sgx_companies").select("symbol,currency")
            .in_("symbol", symbols).execute().data
        }
    else:
        rows = (sb.table("sgx_companies").select("symbol,currency")
                .eq("is_active", True).execute().data)
        currency_by_symbol = {r["symbol"]: r.get("currency") for r in rows}
        symbols = [r["symbol"] for r in rows]
        if not args.all:
            symbols = symbols[: args.limit]
    _log.info("symbols to scrape: %d", len(symbols))

    ca = CorporateActionsClient()
    # One announcement client for the whole run: it holds a TLS session and a
    # ROT13 token, and opening two means fetching the token twice.
    announce_client = None if args.skip_cact else AnnouncementClient()
    rates = load_sgd_rates()

    # Incremental mode: list the CACT window once, narrow the symbol set to the
    # counters it touches, then fetch corporateactions for only those. Listing 30
    # days is 1 page per category (measured: 76 filings) versus 11,328 for the
    # full history, and 54 symbols instead of 620.
    listing = None
    if args.notices_window and not args.skip_cact:
        client = announce_client
        window_start = (datetime.date.today()
                        - datetime.timedelta(days=args.notices_window)).strftime("%Y%m%d_000000")
        specs = dict(CACT_SPECS)
        if not args.no_cact06:
            specs.update(CACT06_SPEC)
        listing = {sub: client.list(sub, start=window_start) for sub in specs}
        touched = {i.get("stock_code") for sub in listing for ann in listing[sub]
                   for i in (ann.get("issuers") or []) if i.get("stock_code")}
        narrowed = [sym for sym in symbols if sym.replace(".SI", "") in touched]
        _log.info("window %d d: %d filings, %d symbols touched (of %d requested)",
                   args.notices_window, sum(len(v) for v in listing.values()),
                   len(narrowed), len(symbols))
        symbols = narrowed
        if not symbols:
            _log.info("nothing announced in the window; nothing to do")
            return

    sessions = session_dates(symbols)

    # corporateactions: one request per symbol, no auth, full dated history.
    ibm = {}
    with ThreadPoolExecutor(max_workers=SYMBOL_WORKERS) as pool:
        for symbol, code in zip(symbols, pool.map(lambda s: ca.ibm_code(s.replace(".SI", "")), symbols)):
            ibm[symbol] = code
    unresolved = [s for s, c in ibm.items() if not c]
    if unresolved:
        # Loud, because a missing code writes zero rows for a real company and
        # nothing downstream would notice the gap.
        _log.error("no ibmCode for %d symbol(s), they will yield NO rows: %s",
                   len(unresolved), unresolved)

    raw = {}
    failed = []

    def fetch_rows(symbol):
        try:
            return symbol, ca.rows(ibm.get(symbol)) or []
        except Exception as exc:
            _log.error("[%s] corporateactions fetch failed: %s", symbol, exc)
            return symbol, None

    with ThreadPoolExecutor(max_workers=SYMBOL_WORKERS) as pool:
        for symbol, rows in pool.map(fetch_rows, symbols):
            if rows is None:
                failed.append(symbol)
                continue
            raw[symbol] = rows
    total_rows = sum(len(v) for v in raw.values())
    _log.info("corporateactions rows: %d across %d symbols",
              total_rows, len({s for s, v in raw.items() if v}))
    if failed:
        _log.error("%d symbol(s) FAILED to fetch (not written): %s", len(failed), failed)

    records = defaultdict(list)
    incomplete = set()
    for symbol, rows in raw.items():
        usable = [r for r in rows if r.get("ibmCode") == ibm.get(symbol)] if ibm.get(symbol) else rows
        cash = [r for r in usable
                if r.get("anncType") in ("DIVIDEND", "ENTITLEMENT", "ENTITL.")]
        priced = [r for r in cash if parse_cash(r.get("particulars"))]
        # corporateactions is incomplete here: only a scrip-option stub, or no
        # priced dividend at all. Those are the only symbols CACT06 can add to.
        if not priced or any("Cash Options" in (r.get("particulars") or "") for r in cash):
            incomplete.add(symbol.replace(".SI", ""))

        records["sgx_dividend"] += build_dividends(symbol, rows, rates)
        records["sgx_ca_bonus"] += build_bonus(symbol, rows)
        records["sgx_right_issue"] += build_rights(symbol, rows, rates)

    if not args.skip_cact:
        client = announce_client
        specs = dict(CACT_SPECS)
        if not args.no_cact06:
            specs.update(CACT06_SPEC)
        wanted = {s.replace(".SI", "") for s in symbols}
        counter_currency = dict(currency_by_symbol)

        # CACT06 is only worth fetching for symbols where corporateactions is
        # demonstrably incomplete: it has a scrip-option stub ("N Cash Options")
        # or no priced dividend at all. Measured on the full universe: 199 of
        # 620 symbols qualify, and for the other 421 CACT06 contributed zero
        # new components (verified: 0 of 7 sampled rows were new). Split, bonus
        # and rights come from CACT21/17/01/18 which are small, so they still
        # run over every symbol.
        if listing is not None:
            # Windowed: only ~76 filings, so take them all. Filtering here would
            # drop filings for symbols whose corporateactions row happens to be
            # complete today but may be amended tomorrow.
            dividend_codes = wanted
            _log.info("windowed run: reading all %d CACT06 filings", len(listing.get("CACT06", [])))
        else:
            # Full history: 11k filings, so only spend detail fetches where
            # corporateactions is demonstrably incomplete.
            dividend_codes = incomplete
            _log.info("CACT06 needed for %d/%d symbols (corporateactions incomplete)",
                       len(incomplete), len(symbols))
        cact = cact_records(client, specs, wanted, counter_currency, rates=rates,
                            per_kind_wanted={"dividend": dividend_codes},
                            announcements=listing)
        # CACT06 wins for dividends: it is the only source with the true ex-date
        # (corporateactions is -1 day) and the only one with real amounts where
        # corporateactions says "N Cash Options". corporateactions still adds
        # every dividend CACT06 never announced.
        # A windowed CACT read (--notices-window) sees only recent filings, so
        # corporateactions-only dates must NOT be written: they would overwrite
        # correct values with the incomplete source.
        records["sgx_dividend"] = merge_dividend_sources(
            records["sgx_dividend"], cact.get("sgx_dividend", []),
            complete_cact_dates=(listing is None))
        records["sgx_ca_bonus"] = merge_sources(records["sgx_ca_bonus"], cact.get("sgx_ca_bonus", []),
                                                key=lambda r: (r["symbol"], r["ex_date"]))
        records["sgx_right_issue"] = merge_sources(records["sgx_right_issue"], cact.get("sgx_right_issue", []),
                                                   key=lambda r: (r["symbol"], r["ex_date"]))
        records["sgx_stock_split"] = cact.get("sgx_stock_split", [])

    records = attach_cum_dates(records, sessions)

    # History vs forward calendar. A dividend announced today can have an
    # ex-date 114 days out, so the split is on ex-date, not announcement date:
    # one pipeline then feeds both the permanent history and the calendar.
    if args.split_calendar:
        today = datetime.date.today().isoformat()
        history, buckets = [], {}
        for r in records["sgx_dividend"]:
            if r["date"] > today:
                # The calendar is keyed (symbol, ex_date) like sgx_dividend, so
                # same-day components are summed. W05 2026-10-29 is 0.01 + 0.03
                # and its history row reads 0.04 - the calendar must mirror that.
                key = (r["symbol"], r["date"])
                b = buckets.get(key)
                if b is None:
                    buckets[key] = {
                        "symbol": r["symbol"],
                        "ex_date": r["date"],
                        "recording_date": r.get("recording_date"),
                        "dividend_amount": r.get("dividend") or 0.0,
                        "payment_date": r.get("payment_date"),
                        "cum_date": r.get("cum_date"),
                        "reference": r.get("reference"),
                        "source": r.get("source"),
                    }
                else:
                    b["dividend_amount"] += r.get("dividend") or 0.0
                    b["cum_date"] = b["cum_date"] or r.get("cum_date")
                    b["payment_date"] = b["payment_date"] or r.get("payment_date")
            else:
                history.append(r)
        upcoming = list(buckets.values())
        records["sgx_dividend"] = history
        records["sgx_upcoming_dividend"] = upcoming
        _log.info("split: %d history, %d forward (ex_date after %s)",
                   len(history), len(upcoming), today)

    for table, rows in records.items():
        strip_internal(rows)
        dedupe_ids(table, rows)        # The PK is (symbol, date-or-ex_date), so a second event on the same day
        # cannot be stored. Log instead of dropping it silently: SIA's 2020
        # rights issue is two events (100:295 and 2:3) on one ex-date.
        uniq = {}
        collisions = []
        for r in rows:
            k = (r["symbol"], r.get("date") or r.get("ex_date"))
            if k in uniq:
                collisions.append((k, uniq[k], r))
            uniq[k] = r
        for k, kept, dropped in collisions:
            _log.warning("%s: %s has two events on one date; keeping id=%s "
                         "(old=%s new=%s), dropping id=%s (old=%s new=%s)",
                         table, k[0], kept.get("id"), kept.get("old_ratio"),
                         kept.get("new_ratio"), dropped.get("id"),
                         dropped.get("old_ratio"), dropped.get("new_ratio"))
        records[table] = list(uniq.values())
        _log.info("%s: %d rows", table, len(records[table]))

    if args.preview:
        """One CSV per table, since each has its own columns."""
        base = os.path.splitext(args.preview)[0]
        for table, rows in records.items():
            if not rows:
                continue
            path = f"{base}_{table}.csv"
            # Union of keys: CACT rows carry columns corporateactions rows do not.
            fieldnames = sorted({k for r in rows for k in r})
            with open(path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            _log.info("preview written: %s (%d rows)", path, len(rows))

    try:
        if args.write:
            if not args.yes:
                raise SystemExit("Refusing to write without --yes")
            for table, rows in records.items():
                write_table(table, rows)
            # Order matters: write history first, so a row is never deleted from
            # the calendar before it exists in sgx_dividend.
            if args.prune_calendar:
                prune_calendar(datetime.date.today().isoformat())
        else:
            _log.info("DRY RUN: nothing written. Pass --write --yes to upsert.")
    finally:
        ca.close()
        if announce_client is not None:
            announce_client.close()


if __name__ == "__main__":
    main()
