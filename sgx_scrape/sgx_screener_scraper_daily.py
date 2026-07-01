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
import logging
import argparse
import numpy as np
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client, Client

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
SGX_HEADERS   = {"User-Agent": "Mozilla/5.0"}

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

MCAP_URL = "https://api.sgx.com/stockscreener/v2.0/all?params=stockCode,marketCapitalization"

RATIOS_URL = (
    "https://api.sgx.com/ratiosreports/v2.0/countryCode/SGP/stockCode/{symbol}"
    "?params=beta%2CnormalizedDilutedEPS%2CpriceSales%2CoperatingMargin"
    "%2CquickRatio%2CcurrentRatio%2ClongTermDebtEquity"
    "%2Ceps5YearGrowth%2CrevenueShare5YearGrowth%2CassetTurnover"
)

HISTORIC_URL = (
    "https://api.sgx.com/securities/v1.1//charts/historic/stocks/code/{symbol}/{period}"
    "?params=trading_time,vl,lt"
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

NUMERIC_METRICS = [
    "revenue_ttm", "one_year_sales_growth",
    "pe", "pb", "pcf", "ps_ttm",
    "net_profit_margin", "operating_margin", "debt_to_equity",
    "quick_ratio", "current_ratio", "beta", "eps", "asset_turnover",
    "five_year_eps_growth", "five_year_sales_growth",
]

# ==========================================
# SHARED
# ==========================================
def create_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_symbols() -> pd.DataFrame:
    """Fetch active symbols from sgx_companies. Returns df with symbol + api_symbol (no .SI)."""
    client = create_supabase()
    rows = client.table("sgx_companies").select("symbol").eq("is_active", True).execute().data
    df = pd.DataFrame(rows)
    df["api_symbol"] = df["symbol"].str.replace(r"\.SI$", "", regex=True)
    logger.info(f"Loaded {len(df)} active symbols from sgx_companies.")
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
            return {"symbol": symbol}
        row = {RATIOS_FIELD_MAP[k]: v for k, v in data[0].items() if k in RATIOS_FIELD_MAP}
        row["symbol"] = symbol
        return row
    except Exception as e:
        logger.warning(f"[{symbol}] ratios fetch failed: {e}")
        return {"symbol": symbol}


def build_metrics_df(base_df: pd.DataFrame) -> pd.DataFrame:
    api_symbols = base_df["api_symbol"].tolist()

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
    logger.info("Ratios: completed.")

    df = base_df.merge(screener_df, left_on="api_symbol", right_on="symbol", how="left")
    df["symbol"] = df["symbol_x"]
    df.drop(columns=["symbol_x", "symbol_y", "api_symbol"], inplace=True, errors="ignore")

    df = df.merge(ratios_df, on="symbol", how="left", suffixes=("", "_r"))

    if "debt_to_equity_r" in df.columns:
        df["debt_to_equity"] = df["debt_to_equity_r"].combine_first(df["debt_to_equity"])
        df.drop(columns=["debt_to_equity_r"], inplace=True)

    for col in NUMERIC_METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"Metrics dataset: {len(df)} records, {len(df.columns)} columns.")
    return df


def upsert_metrics(df: pd.DataFrame, client: Client):
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.drop_duplicates(subset=["symbol"], keep="last")
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
def _fetch_historic_single(symbol: str, period: str) -> pd.DataFrame:
    try:
        resp = requests.get(HISTORIC_URL.format(symbol=symbol, period=period), headers=SGX_HEADERS, timeout=15)
        resp.raise_for_status()
        records = resp.json().get("data", {}).get("historic", [])
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["trading_time"].str[:8], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.rename(columns={"lt": "close", "vl": "volume"})
        df["close"] = pd.to_numeric(df["close"], errors="coerce").round(6)
        df["volume"] = (pd.to_numeric(df["volume"], errors="coerce").fillna(0) * 1000).astype("int64")
        return df[["symbol", "date", "close", "volume"]]
    except Exception as e:
        logger.warning(f"[{symbol}] historic fetch failed: {e}")
        return pd.DataFrame()


def build_daily_df(base_df: pd.DataFrame, mode: str) -> tuple:
    """Returns (all_dates_df, latest_date_df_with_market_cap)."""
    api_symbols = base_df["api_symbol"].tolist()
    period = "1y" if mode == "full" else "1m"

    logger.info(f"Fetching historic prices ({period}) for {len(api_symbols)} symbols...")
    frames = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_historic_single, s, period): s for s in api_symbols}
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
    mcap_df = pd.DataFrame(mcap_data)[["stockCode", "marketCapitalization"]]
    mcap_df.columns = ["symbol", "market_cap"]
    mcap_df["market_cap"] = pd.to_numeric(mcap_df["market_cap"], errors="coerce").round(0).astype("Int64")
    logger.info(f"Market cap fetched for {len(mcap_df)} symbols.")

    latest_date = price_df["date"].max()
    latest_df = price_df[price_df["date"] == latest_date].copy()
    latest_df = latest_df.merge(mcap_df, on="symbol", how="left")
    logger.info(f"Market cap joined for {latest_df['market_cap'].notna().sum()} symbols on {latest_date}.")

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

    latest_records = latest_df.where(pd.notna(latest_df), None).to_dict(orient="records")
    client.table(DAILY_TABLE).upsert(latest_records).execute()
    logger.info(f"Daily done. {total} records + market cap → '{DAILY_TABLE}'.")


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

    # Run both pipelines concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        metrics_future = executor.submit(build_metrics_df, base_df.copy())
        daily_future   = executor.submit(build_daily_df,   base_df.copy(), args.mode)

    metrics_df          = metrics_future.result()
    price_df, latest_df = daily_future.result()

    if args.csv:
        metrics_df.to_csv("sgx_metrics_preview.csv", index=False)
        latest_df.to_csv("sgx_daily_data_preview.csv", index=False)
        logger.info("Saved: sgx_metrics_preview.csv, sgx_daily_data_preview.csv")
    else:
        client = create_supabase()
        upsert_metrics(metrics_df, client)
        if not price_df.empty:
            upsert_daily(price_df, latest_df, client)

    logger.info("=== SGX Daily Combined Scraper finished ===")


if __name__ == "__main__":
    main()
