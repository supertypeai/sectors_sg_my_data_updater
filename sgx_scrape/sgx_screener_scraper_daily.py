"""
SGX Daily Combined Scraper

Runs two pipelines in parallel after a single shared symbol fetch:
  1. Metrics — PE, PB, PCF, EPS, Beta, etc.  → sgx_metrics_daily
  2. Price   — Close, Volume, Market Cap      → sgx_daily_data

Usage:
    python sgx_screener_scraper_daily.py --mode daily        # daily update
    python sgx_screener_scraper_daily.py --mode full         # 1-year history
    python sgx_screener_scraper_daily.py --mode daily --csv  # preview CSVs only
"""

import os
import sys
import time
import json
import logging
import argparse
import numpy as np
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client, Client
from symbol_utils import with_suffix

# ==========================================
# ENV & LOGGING
# ==========================================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("sgx_daily_combined.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ==========================================
# CONFIG
# ==========================================
METRICS_TABLE = "sgx_metrics_daily"
DAILY_TABLE   = "sgx_daily_data"
BATCH_SIZE    = 500
EMPTY_RETRIES = 1
SGX_HEADERS   = {"User-Agent": "Mozilla/5.0"}
# compact_rates.json lives at the repo root, one level above sgx_scrape/.
RATES_FILE    = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compact_rates.json")

# A market cap is trusted over SGX's only when it disagrees with
# units_in_issue x close by more than these factors (a per-leg double count);
# smaller drift is logged and left alone.
MARKET_CAP_FACTOR_HIGH = 1.5
MARKET_CAP_FACTOR_LOW  = 0.67
MARKET_CAP_DRIFT       = 0.15


def _load_rates() -> dict:
    """FX table (currency -> {currency: rate}) used to express prices and market
    caps in SGD. Missing/unreadable file degrades to no conversion (rate 1.0)."""
    try:
        with open(RATES_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load {RATES_FILE}: {e}")
        return {}


def _rate(rates: dict, currency) -> float:
    """SGD value of one unit of `currency`; 1.0 for SGD/unknown so callers can
    multiply blindly."""
    if not currency or str(currency).upper() == "SGD":
        return 1.0
    return (rates.get(currency) or {}).get("SGD") or 1.0


def _db_currency(base_df: pd.DataFrame) -> pd.Series:
    """The counter's trading currency per row (SGD when the column is absent)."""
    if "currency" in base_df.columns:
        return base_df["currency"].fillna("SGD")
    return pd.Series("SGD", index=base_df.index)


def _fx_by_symbol(base_df: pd.DataFrame, currency_by_code: dict | None = None) -> dict:
    """SGD rate per stored symbol. `currency_by_code` (api code -> currency) wins
    where present, else the DB trading currency. One lookup behind the two
    currency sources the pipelines need (reporting for metrics, trading for
    prices), so neither builds its own rate map."""
    rates = _load_rates()
    override = currency_by_code or {}
    return {
        sym: _rate(rates, override.get(code) or cur)
        for sym, code, cur in zip(base_df["symbol"], base_df["api_symbol"], _db_currency(base_df))
    }

# ==========================================
# API URLs
# ==========================================
SCREENER_URL = (
    "https://api.sgx.com/stockscreener/v2.0/all"
    "?params=stockCode"
    "%2CsalesTTM%2CsalesPercentageChange"
    "%2CpriceToEarningsRatio%2CpriceToBookRatio%2CpriceToCashFlowPerShareRatio"
    "%2CnetProfitMargin%2CtotalDebtToTotalEquityRatio"
)

MCAP_URL = "https://api.sgx.com/stockscreener/v2.0/all?params=stockCode%2CmarketCapitalization"

RATIOS_URL = (
    "https://api.sgx.com/ratiosreports/v2.0/countryCode/SGP/stockCode/{symbol}"
    "?params=beta%2CnormalizedDilutedEPS%2CpriceSales%2CoperatingMargin"
    "%2CquickRatio%2CcurrentRatio%2ClongTermDebtEquity"
    "%2Ceps5YearGrowth%2CrevenueShare5YearGrowth%2CassetTurnover"
)

SNAPSHOT_URL = (
    "https://api.sgx.com/snapshotreports/v2.0/countryCode/SGP/stockCode/{symbol}"
    "?params=stockCode%2CcurrencyIdForMarketCap%2CtradedCurrency"
)

# params=trading_time,vl,lt,o,h,l -> daily OHLCV: o=open, h=high, l=low, lt=last/close, vl=volume
HISTORIC_URL = (
    "https://api.sgx.com/securities/v1.1//charts/historic/{kind}/code/{symbol}/{period}"
    "?params=trading_time,vl,lt,o,h,l"
)

# ==========================================
# FIELD MAPS
# ==========================================
SCREENER_FIELD_MAP = {
    "stockCode":                    "symbol",
    "salesTTM":                     "revenue_ttm",
    "salesPercentageChange":        "one_year_sales_growth",
    "priceToEarningsRatio":         "pe",
    "priceToBookRatio":             "pb",
    "priceToCashFlowPerShareRatio": "pcf",
    "netProfitMargin":              "net_profit_margin",
    "totalDebtToTotalEquityRatio":  "debt_to_equity",
}

RATIOS_FIELD_MAP = {
    "beta":                    "beta",
    "normalizedDilutedEPS":    "eps",
    "priceSales":              "ps_ttm",
    "operatingMargin":         "operating_margin",
    "quickRatio":              "quick_ratio",
    "currentRatio":            "current_ratio",
    "longTermDebtEquity":      "debt_to_equity",
    "eps5YearGrowth":          "five_year_eps_growth",
    "revenueShare5YearGrowth": "five_year_sales_growth",
    "assetTurnover":           "asset_turnover",
}

# Columns sgx_metrics_daily accepts. Anything else in the payload fails the
# upsert with PGRST204, so the frame is narrowed to these before writing.
METRICS_COLUMNS = ["symbol"]

NUMERIC_METRICS = [
    "revenue_ttm", "one_year_sales_growth",
    "pe", "pb", "pcf", "ps_ttm",
    "net_profit_margin", "operating_margin", "debt_to_equity",
    "quick_ratio", "current_ratio", "beta", "eps", "asset_turnover",
    "five_year_eps_growth", "five_year_sales_growth",
]

# SGX reports these in the counter's traded currency; every other metric is a
# ratio and is currency-independent. They are scaled to SGD (like prices and
# market caps above) so the table is comparable across counters.
CURRENCY_METRICS = ["revenue_ttm", "eps"]

# ==========================================
# SHARED
# ==========================================
def create_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_symbols() -> pd.DataFrame:
    """Fetch active symbols from sgx_companies. Returns df with symbol + api_symbol
    (no .SI) + is_reit + name/currency (currency = traded currency of the counter)."""
    client = create_supabase()
    rows = client.table("sgx_companies").select("symbol,name,sector,currency").eq("is_active", True).execute().data
    df = pd.DataFrame(rows)
    df["api_symbol"] = df["symbol"].str.replace(r"\.SI$", "", regex=True)
    df["is_reit"] = df["sector"].str.upper() == "REIT"
    logger.info(f"Loaded {len(df)} active symbols from sgx_companies ({df['is_reit'].sum()} REITs).")
    return df


# ==========================================
# PIPELINE 1: METRICS → sgx_metrics_daily
# ==========================================
def _fetch_screener() -> pd.DataFrame:
    resp = requests.get(SCREENER_URL, headers=SGX_HEADERS, timeout=30)
    resp.raise_for_status()
    records = resp.json().get("data", [])
    logger.info(f"Screener: {len(records)} records received.")
    df = pd.DataFrame(records).rename(columns=SCREENER_FIELD_MAP)
    return df[df["symbol"].notna() & (df["symbol"] != "")]


def _fetch_ratios_single(symbol: str) -> dict:
    try:
        resp = requests.get(RATIOS_URL.format(symbol=symbol), headers=SGX_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            # No row: transient/unsupported. Flag so the stored row is kept,
            # not NULL-wiped (see upsert_metrics).
            return {"symbol": symbol, "_failed": True}
        row = {RATIOS_FIELD_MAP[k]: v for k, v in data[0].items() if k in RATIOS_FIELD_MAP}
        row["symbol"] = symbol
        return row
    except Exception as e:
        logger.warning(f"[{symbol}] ratios fetch failed: {e}")
        return {"symbol": symbol, "_failed": True}


def build_metrics_df(base_df: pd.DataFrame, currency_maps: dict | None = None) -> pd.DataFrame:
    api_symbols = base_df["api_symbol"].tolist()
    if currency_maps is None:
        currency_maps = _fetch_currency_maps(api_symbols)

    screener_df = _fetch_screener()
    logger.info(f"Fetching ratios for {len(api_symbols)} symbols...")
    results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(_fetch_ratios_single, s): s for s in api_symbols}
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if i % 200 == 0:
                logger.info(f"  Ratios: {i}/{len(api_symbols)} done...")
    ratios_df = pd.DataFrame(results)
    # Ratios are keyed by the bare API code; df["symbol"] is the stored suffixed
    # form. Normalise before the merge or every ratios column lands NULL.
    ratios_df["symbol"] = ratios_df["symbol"].map(with_suffix)
    logger.info("Ratios: completed.")

    df = base_df.merge(screener_df, left_on="api_symbol", right_on="symbol", how="left")
    df["symbol"] = df["symbol_x"]
    df.drop(columns=["symbol_x", "symbol_y", "api_symbol", "sector", "currency", "is_reit"], inplace=True, errors="ignore")

    df = df.merge(ratios_df, on="symbol", how="left", suffixes=("", "_r"))

    if "debt_to_equity_r" in df.columns:
        df["debt_to_equity"] = df["debt_to_equity_r"].combine_first(df["debt_to_equity"])
        df.drop(columns=["debt_to_equity_r"], inplace=True)

    for col in NUMERIC_METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # SGX quotes revenue/EPS in the counter's reporting currency, which the
    # snapshot API exposes as tradedCurrency (sgx_companies.currency is the
    # trading currency — often SGD for a counter that reports in USD/CNY/etc., so
    # it cannot be used here). Convert both to SGD; the other metrics are ratios.
    reported = {code: traded for code, (_mcap, traded) in currency_maps.items() if traded}
    fx_by_symbol = _fx_by_symbol(base_df, reported)
    for col in CURRENCY_METRICS:
        if col in df.columns:
            df[col] = df[col] * df["symbol"].map(fx_by_symbol).fillna(1.0)

    # Normalize to the stored (suffixed) form before write — bare source can never write bare.
    df["symbol"] = df["symbol"].map(with_suffix)

    keep = METRICS_COLUMNS + NUMERIC_METRICS + ["_failed"]
    df = df[[c for c in keep if c in df.columns]]

    logger.info(f"Metrics dataset: {len(df)} records, {len(df.columns)} columns.")
    return df


def upsert_metrics(df: pd.DataFrame, client: Client):
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.drop_duplicates(subset=["symbol"], keep="last")
    # ponytail: a failed ratios fetch drops the whole row so its stored values
    # survive; it refreshes on the next successful run instead of NULL-wiping.
    if "_failed" in df.columns:
        failed = df["_failed"].fillna(False).astype(bool)
        if failed.any():
            logger.warning(f"Keeping stored metrics for {int(failed.sum())} symbol(s) with failed ratios fetch.")
        df = df[~failed].drop(columns=["_failed"])
    records = [
        {k: (None if isinstance(v, float) and (np.isnan(v) or np.isinf(v)) else v)
         for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]
    total = len(records)
    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        client.table(METRICS_TABLE).upsert(batch, on_conflict="symbol").execute()
        logger.info(f"  Metrics upserted: {min(i + BATCH_SIZE, total)}/{total}")
    logger.info(f"Metrics done. {total} records → '{METRICS_TABLE}'.")


# ==========================================
# PIPELINE 2: PRICE + MCAP → sgx_daily_data
# ==========================================
def _fetch_currency_single(symbol: str) -> tuple[str, str | None, str | None]:
    try:
        resp = requests.get(SNAPSHOT_URL.format(symbol=symbol), headers=SGX_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if data:
            return symbol, data[0].get("currencyIdForMarketCap"), data[0].get("tradedCurrency")
    except Exception as e:
        logger.warning(f"[{symbol}] snapshot currency fetch failed: {e}")
    return symbol, None, None


def _fetch_currency_maps(api_symbols: list[str]) -> dict:
    """api code -> (currencyIdForMarketCap, tradedCurrency) from the SGX snapshot
    API. Fetched once per run and shared by both pipelines: the metrics pipeline
    uses tradedCurrency (the counter's reporting currency) and the price pipeline
    uses currencyIdForMarketCap (the currency market cap is quoted in)."""
    maps = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        for sym, mcap_cur, traded_cur in executor.map(_fetch_currency_single, api_symbols):
            maps[sym] = (mcap_cur, traded_cur)
    return maps


def _historic_kinds(is_reit: bool) -> list[str]:
    """SGX serves history per security type and returns an empty list for the
    wrong one. Business and stapled trusts live under 'businesstrusts' whatever
    their sector says, and depositary receipts under 'adrs'. Sector only hints
    at the likely type, so every type is tried before giving up."""
    if is_reit:
        return ["reits", "businesstrusts", "stocks", "adrs"]
    return ["stocks", "businesstrusts", "reits", "adrs"]


def _fetch_historic_single(symbol: str, period: str, is_reit: bool = False, fx: float = 1.0) -> pd.DataFrame:
    try:
        records = []
        errors = 0
        # A type that errored may have been the one holding this symbol's data,
        # so the ladder is retried; four clean empties mean the symbol has none.
        for attempt in range(EMPTY_RETRIES + 1):
            if attempt:
                time.sleep(2 * attempt)
            errors = 0
            for kind in _historic_kinds(is_reit):
                try:
                    resp = requests.get(
                        HISTORIC_URL.format(kind=kind, symbol=symbol, period=period),
                        headers=SGX_HEADERS,
                        timeout=15,
                    )
                    resp.raise_for_status()
                    records = resp.json().get("data", {}).get("historic", [])
                except Exception as e:
                    errors += 1
                    logger.warning(f"[{symbol}] {kind} fetch failed: {e}")
                    continue
                if records:
                    break
            if records or not errors:
                break
        if not records:
            logger.warning(
                f"[{symbol}] no history on any security type"
                + (f" ({errors} of 4 errored)" if errors else "")
            )
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["trading_time"].str[:8], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "lt": "close", "vl": "volume"})
        # The SGX historic price is in the counter's traded currency; scale it to
        # SGD so close lines up with the (SGD) market_cap. fx=1.0 for SGD counters.
        for col in ("open", "high", "low", "close"):
            df[col] = (pd.to_numeric(df[col], errors="coerce") * fx).round(6)
        df["volume"] = (pd.to_numeric(df["volume"], errors="coerce").fillna(0) * 1000).astype("int64")
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]
    except Exception as e:
        logger.warning(f"[{symbol}] historic fetch failed: {e}")
        return pd.DataFrame()


def _to_stored_symbols(base_df: pd.DataFrame, *frames: pd.DataFrame) -> tuple:
    """Everything above joins on the bare API code, but the table stores the same
    suffixed form as sgx_companies — map back before the rows are written."""
    stored = dict(zip(base_df["api_symbol"], base_df["symbol"]))
    for frame in frames:
        # Fall back to suffixing directly, so a code missing from base_df can
        # never be written back in the bare form.
        frame["symbol"] = frame["symbol"].map(
            lambda code: stored.get(code) or with_suffix(code)
        )
    return frames


def _reit_units_by_symbol() -> dict:
    """Latest units_in_issue per stored symbol from sgx_reit_performance (annual)."""
    rows = create_supabase().table("sgx_reit_performance").select(
        "symbol,financial_year,units_in_issue").execute().data
    latest = {}
    for r in rows:
        if not r.get("units_in_issue"):
            continue
        fy = r.get("financial_year") or 0
        cur = latest.get(r["symbol"])
        if cur is None or fy > cur[1]:
            latest[r["symbol"]] = (r["units_in_issue"], fy)
    return {s: u for s, (u, _) in latest.items()}


def _apply_market_cap_guard(latest_df: pd.DataFrame, base_df: pd.DataFrame) -> pd.DataFrame:
    """SGX's market cap can be a multiple of units_in_issue x close — a per-leg
    double count for stapled/dual-leg trusts (SERT is ~2x) or a stale value. When
    it disagrees by a factor, trust the definitional units x close. Units are
    annual, so only factor-level drift is overridden; smaller drift is logged."""
    if latest_df.empty or "market_cap" not in latest_df.columns:
        return latest_df
    try:
        units_by_symbol = _reit_units_by_symbol()
    except Exception as e:
        logger.warning(f"Market cap guard skipped (units fetch failed): {e}")
        return latest_df

    # Sibling counters of the same trust (SEB/SET, 8U7U/UD1U) share units but only
    # one carries the sgx_reit_performance row - match the rest by company name.
    name_by_symbol = dict(zip(base_df["symbol"], base_df.get("name", base_df["symbol"])))
    units_by_name = {}
    for sym, units in units_by_symbol.items():
        nm = name_by_symbol.get(sym)
        if nm:
            units_by_name.setdefault(nm, units)

    corrected = 0
    for i, row in latest_df.iterrows():
        close, mcap = row.get("close"), row.get("market_cap")
        if pd.isna(close) or pd.isna(mcap) or close <= 0:
            continue
        units = units_by_symbol.get(row["symbol"]) or units_by_name.get(name_by_symbol.get(row["symbol"]))
        if not units:
            continue
        basis = units * close
        ratio = mcap / basis
        if ratio > MARKET_CAP_FACTOR_HIGH or ratio < MARKET_CAP_FACTOR_LOW:
            latest_df.at[i, "market_cap"] = int(round(basis))
            corrected += 1
            logger.warning(f"[{row['symbol']}] market cap overridden: SGX {mcap:,.0f} -> units basis {basis:,.0f} (x{ratio:.2f})")
        elif abs(ratio - 1) > MARKET_CAP_DRIFT:
            logger.warning(f"[{row['symbol']}] market cap {mcap:,.0f} off units basis by x{ratio:.2f} (kept)")
    if corrected:
        logger.info(f"Market cap guard corrected {corrected} symbol(s).")
    return latest_df


def build_daily_df(base_df: pd.DataFrame, mode: str, currency_maps: dict | None = None) -> tuple:
    """Returns (all_dates_df, latest_date_df_with_market_cap)."""
    api_symbols = base_df["api_symbol"].tolist()
    if currency_maps is None:
        currency_maps = _fetch_currency_maps(api_symbols)
    period = "1y" if mode == "full" else "1m"

    rates = _load_rates()
    fx_by_symbol = _fx_by_symbol(base_df)  # prices are quoted in the trading currency
    fx_by_code = {code: fx_by_symbol[sym] for sym, code in zip(base_df["symbol"], base_df["api_symbol"])}

    reit_set = set(base_df.loc[base_df["is_reit"], "api_symbol"].tolist())
    logger.info(f"Fetching historic prices ({period}) for {len(api_symbols)} symbols ({len(reit_set)} REITs)...")
    frames = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_historic_single, s, period, s in reit_set, fx_by_code.get(s, 1.0)): s for s in api_symbols}
        for i, future in enumerate(as_completed(futures), 1):
            df = future.result()
            if not df.empty:
                frames.append(df)
            if i % 100 == 0:
                logger.info(f"  Prices: {i}/{len(api_symbols)} done...")

    if not frames:
        logger.warning("No price data returned.")
        return pd.DataFrame(), pd.DataFrame()

    price_df = pd.concat(frames, ignore_index=True)
    price_df.dropna(subset=["close"], inplace=True)
    logger.info(f"Prices: {len(price_df)} records for {price_df['symbol'].nunique()} symbols.")

    # Market cap joined on latest date only
    logger.info("Fetching market cap...")
    resp = requests.get(MCAP_URL, headers=SGX_HEADERS, timeout=30)
    resp.raise_for_status()
    mcap_data = resp.json().get("data", [])
    if not mcap_data:
        logger.warning("Market cap API returned no data.")
        return _to_stored_symbols(
            base_df,
            price_df,
            price_df[price_df["date"] == price_df["date"].max()].copy(),
        )
    mcap_df = pd.DataFrame(mcap_data)[["stockCode", "marketCapitalization"]]
    mcap_df.columns = ["symbol", "market_cap"]
    mcap_df["market_cap"] = pd.to_numeric(mcap_df["market_cap"], errors="coerce")

    # Snapshot currency is fetched once per run (see main); map it onto the frame.
    mcap_currency_map = {sym: mcap for sym, (mcap, _t) in currency_maps.items() if mcap}
    logger.info(f"Currency fetched for {len(mcap_currency_map)} symbols.")
    mcap_df["currency_for_market_cap"] = mcap_df["symbol"].map(mcap_currency_map)

    # Convert non-SGD market caps to SGD (same rates used to normalize prices above).
    non_sgd = mcap_df["currency_for_market_cap"].notna() & (mcap_df["currency_for_market_cap"] != "SGD")
    if non_sgd.any():
        mcap_df.loc[non_sgd, "market_cap"] = mcap_df[non_sgd].apply(
            lambda row: row["market_cap"] * _rate(rates, row["currency_for_market_cap"]), axis=1
        )
        logger.info(f"Converted {non_sgd.sum()} non-SGD market caps to SGD.")

    mcap_df["market_cap"] = mcap_df["market_cap"].round(0).astype("Int64")
    mcap_df = mcap_df.drop(columns=["currency_for_market_cap"])
    logger.info(f"Market cap fetched for {len(mcap_df)} symbols.")

    latest_date = price_df["date"].max()
    latest_df = price_df[price_df["date"] == latest_date].copy()
    latest_df = latest_df.merge(mcap_df, on="symbol", how="left")
    logger.info(f"Market cap joined for {latest_df['market_cap'].notna().sum()} symbols on {latest_date}.")

    price_df, latest_df = _to_stored_symbols(base_df, price_df, latest_df)
    latest_df = _apply_market_cap_guard(latest_df, base_df)
    return price_df, latest_df


def upsert_daily(price_df: pd.DataFrame, latest_df: pd.DataFrame, client: Client):
    price_df = price_df.drop_duplicates(subset=["symbol", "date"], keep="last")
    latest_df = latest_df.drop_duplicates(subset=["symbol", "date"], keep="last")
    records = price_df.to_dict(orient="records")
    total = len(records)
    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        client.table(DAILY_TABLE).upsert(batch).execute()
        logger.info(f"  Daily upserted: {min(i + BATCH_SIZE, total)}/{total}")

    # Rows without a market cap carry nothing the price batch above hasn't
    # written, and upserting them NULLs the stored market_cap. The column is
    # missing entirely when the market cap API returned nothing.
    if "market_cap" in latest_df.columns:
        dropped = int(latest_df["market_cap"].isna().sum())
        if dropped:
            logger.warning(f"Keeping stored market cap for {dropped} symbol(s) with none today.")
        latest_df = latest_df[latest_df["market_cap"].notna()]
    else:
        latest_df = latest_df.iloc[0:0]

    latest_records = latest_df.where(pd.notna(latest_df), None).to_dict(orient="records")
    if latest_records:
        client.table(DAILY_TABLE).upsert(latest_records).execute()
    logger.info(f"Daily done. {total} records + {len(latest_records)} market caps → '{DAILY_TABLE}'.")


# ==========================================
# MAIN
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="SGX Daily Combined Scraper")
    parser.add_argument("--mode", choices=["daily", "full"], default="daily",
                        help="'daily' = last 1 month, 'full' = 1 year of history")
    parser.add_argument("--csv", action="store_true", help="Save CSVs instead of upserting")
    args = parser.parse_args()

    logger.info(f"=== SGX Daily Combined Scraper started (mode: {args.mode}) ===")

    base_df = fetch_symbols()
    if base_df.empty:
        logger.error("No symbols found. Exiting.")
        sys.exit(1)

    # Snapshot currency is fetched once and shared: metrics needs the reporting
    # currency of revenue/EPS, price needs the market cap currency.
    currency_maps = _fetch_currency_maps(base_df["api_symbol"].tolist())

    # Run both pipelines concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        metrics_future = executor.submit(build_metrics_df, base_df.copy(), currency_maps)
        daily_future   = executor.submit(build_daily_df,   base_df.copy(), args.mode, currency_maps)

    # One pipeline failing must not discard the other's data: that is the silent
    # stall this scraper already shipped once.
    failed = []
    try:
        metrics_df = metrics_future.result()
    except Exception as e:
        logger.error(f"Metrics pipeline failed: {e}")
        metrics_df, failed = pd.DataFrame(), failed + ["metrics"]
    try:
        price_df, latest_df = daily_future.result()
    except Exception as e:
        logger.error(f"Price pipeline failed: {e}")
        price_df, latest_df, failed = pd.DataFrame(), pd.DataFrame(), failed + ["price"]

    if args.csv:
        metrics_df.to_csv("sgx_metrics_preview.csv", index=False)
        latest_df.to_csv("sgx_daily_data_preview.csv", index=False)
        logger.info("Saved: sgx_metrics_preview.csv, sgx_daily_data_preview.csv")
    else:
        client = create_supabase()
        if not metrics_df.empty:
            upsert_metrics(metrics_df, client)
        if not price_df.empty:
            upsert_daily(price_df, latest_df, client)

    logger.info("=== SGX Daily Combined Scraper finished ===")
    if failed:
        sys.exit(f"Pipeline(s) failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
