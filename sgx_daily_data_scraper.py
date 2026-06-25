import os
import logging
import argparse
from math import ceil
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. Configuration Class ---
class Config:
    """Groups all configuration variables for easy management."""
    COMPANY_SOURCE_TABLE = "sgx_companies"
    DAILY_DATA_TABLE = "sgx_daily_data"
    YF_PERIOD_DAILY = "1mo"
    YF_PERIOD_FULL_PRIMARY = "2y"
    YF_PERIOD_FULL_FALLBACK = "max"
    UPLOAD_BATCH_SIZE = 1000

# --- 2. Setup Logging and Environment ---
LOG_FILENAME = 'sgx_daily_data_importer.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME),
        logging.StreamHandler()
    ]
)
load_dotenv()

# --- 3. Core Functions ---

def get_supabase_client() -> Client | None:
    """Initializes and returns a Supabase client."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        logging.error("Supabase URL or Key not found in environment variables.")
        return None
    try:
        client = create_client(url, key)
        logging.info("Successfully connected to Supabase.")
        return client
    except Exception as e:
        logging.error(f"Failed to create Supabase client: {e}")
        return None

def get_active_symbols(client: Client) -> list[str]:
    """
    Fetches a list of active stock symbols from the 'sgx_companies' table,
    ordered by market cap descending.
    """
    logging.info(f"Fetching active symbols from table '{Config.COMPANY_SOURCE_TABLE}'...")
    try:
        # REMOVE .limit(10) FOR PRODUCTION RUNS.
        response = client.table(Config.COMPANY_SOURCE_TABLE).select("symbol, market_cap").eq("is_active", True).execute()
        # response = client.table(Config.COMPANY_SOURCE_TABLE).select("symbol, market_cap").eq("is_active", True).order("market_cap", desc=True).limit(100).execute()

        if not response.data:
            logging.warning(f"No active symbols found in '{Config.COMPANY_SOURCE_TABLE}'.")
            return []
            
        symbols = [item['symbol'] for item in response.data if item.get('symbol')]
        logging.info(f"Successfully fetched {len(symbols)} active symbols.")
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbols from Supabase: {e}")
        return []

SGX_HISTORIC_URL = "https://api.sgx.com/securities/v1.1//charts/historic/stocks/code/{symbol}/{period}?params=trading_time,vl,lt"
SGX_HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_sgx_historic_single(symbol: str, period: str) -> pd.DataFrame:
    """Fetch historic price/volume data for one symbol from the SGX API."""
    url = SGX_HISTORIC_URL.format(symbol=symbol, period=period)
    try:
        resp = requests.get(url, headers=SGX_HEADERS, timeout=15)
        resp.raise_for_status()
        records = resp.json().get("data", {}).get("historic", [])
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["trading_time"].str[:8], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.rename(columns={"lt": "close", "vl": "volume"})
        df["close"] = pd.to_numeric(df["close"], errors="coerce").round(6)
        # SGX reports volume in thousands — multiply to get actual share count
        df["volume"] = (pd.to_numeric(df["volume"], errors="coerce").fillna(0) * 1000).astype("int64")
        return df[["symbol", "date", "close", "volume"]]
    except Exception as e:
        logging.warning(f"[{symbol}] Failed to fetch SGX historic data: {e}")
        return pd.DataFrame()

def fetch_and_prepare_daily_data(symbols: list[str], mode: str, country: str = 'sg') -> pd.DataFrame:
    """Fetches daily close/volume data from the SGX historic API concurrently."""
    if not symbols:
        logging.warning("Symbol list is empty. Skipping data fetch.")
        return pd.DataFrame()

    period = "1y" if mode == "full" else "1m"
    logging.info(f"Fetching SGX historic data in '{mode}' mode ({period}) for {len(symbols)} symbols...")

    frames = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_sgx_historic_single, sym, period): sym for sym in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            df = future.result()
            if not df.empty:
                frames.append(df)
            if i % 100 == 0:
                logging.info(f"  Fetched {i}/{len(symbols)} symbols...")

    if not frames:
        logging.warning("No data returned from SGX API.")
        return pd.DataFrame()

    df_long = pd.concat(frames, ignore_index=True)
    df_long.dropna(subset=["close"], inplace=True)
    logging.info(f"Successfully fetched {df_long.shape[0]} records for {df_long['symbol'].nunique()} symbols.")
    return df_long

def upsert_in_batches(client: Client, table_name: str, df: pd.DataFrame):
    """Upserts a DataFrame to Supabase in manageable batches."""
    if df.empty:
        logging.info("No data to upload. Skipping upsert.")
        return

    records = df.to_dict(orient='records')
    total_records = len(records)
    num_batches = ceil(total_records / Config.UPLOAD_BATCH_SIZE)

    logging.info(f"Preparing to upsert {total_records} records to '{table_name}' in {num_batches} batches...")
    for i in range(num_batches):
        start_idx = i * Config.UPLOAD_BATCH_SIZE
        end_idx = start_idx + Config.UPLOAD_BATCH_SIZE
        batch = records[start_idx:end_idx]
        
        logging.info(f"Upserting batch {i + 1}/{num_batches} ({len(batch)} records)...")
        try:
            client.table(table_name).upsert(batch).execute()
        except Exception as e:
            logging.error(f"Failed to upsert batch {i + 1}: {e}")
            if batch:
                logging.error(f"First record in failed batch: {batch[0]}")
            return

    logging.info(f"Successfully upserted all {total_records} records.")

def fetch_market_cap() -> pd.DataFrame:
    """Fetches stockCode and marketCapitalization from the SGX screener API."""
    url = "https://api.sgx.com/stockscreener/v2.0/all"
    params = {"params": "stockCode,marketCapitalization"}
    logging.info("Fetching market cap data from SGX API...")
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        df = pd.DataFrame(data)[["stockCode", "marketCapitalization"]]
        df.columns = ["symbol", "market_cap"]
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce").round(0).astype("Int64")
        logging.info(f"Fetched market cap for {len(df)} symbols.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch market cap data: {e}")
        return pd.DataFrame()


# --- 4. Main Execution Block ---

def main():
    """Main execution function to run the data import process."""
    parser = argparse.ArgumentParser(
        description="Fetch and update daily SGX stock data in Supabase.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Since this script is specifically for SGX, we can simplify the arguments
    # and hardcode the country.
    parser.add_argument(
        '--mode',
        choices=['daily', 'full'],
        required=True,
        help=(
            "Specify the scraping mode:\n"
            "'daily': Fetches the last 30 days of data. Ideal for daily cron jobs.\n"
            "'full':  Fetches 1 year of data, with a 'max' history fallback for any failures."
        )
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Perform a dry run: fetch data but don't upload to Supabase."
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help="Save output to CSV instead of upserting to Supabase."
    )
    args = parser.parse_args()
    
    country = 'sg' # This script is specifically for the Singapore Exchange
    
    logging.info(f"--- SGX Daily Data Importer Started (Mode: {args.mode.upper()}) ---")
    
    supabase_client = get_supabase_client()
    if not supabase_client:
        logging.critical("Could not establish a connection to Supabase. Exiting.")
        return

    active_symbols = get_active_symbols(supabase_client)
    if not active_symbols:
        logging.warning("No active symbols found. Script finished.")
        return

    daily_data_df = fetch_and_prepare_daily_data(active_symbols, args.mode, country)
    if daily_data_df.empty:
        logging.warning("No new data was fetched or prepared. Script finished.")
        return

    # Fetch market cap and join onto the latest date only
    mcap_df = fetch_market_cap()
    if not mcap_df.empty:
        latest_date = daily_data_df["date"].max()
        latest_df = daily_data_df[daily_data_df["date"] == latest_date].copy()
        latest_df = latest_df.merge(mcap_df, on="symbol", how="left")
        logging.info(f"Joined market cap for {latest_df['market_cap'].notna().sum()} symbols on latest date {latest_date}.")
    else:
        latest_date = daily_data_df["date"].max()
        latest_df = daily_data_df[daily_data_df["date"] == latest_date].copy()
        latest_df["market_cap"] = None

    if args.csv:
        csv_path = "sgx_daily_data_preview.csv"
        latest_df.to_csv(csv_path, index=False)
        logging.info(f"[CSV] Saved {len(latest_df)} records ({latest_date}) to '{csv_path}'.")
    elif args.dry_run:
        logging.info(f"[DRY RUN] Would have upserted {len(daily_data_df)} records to '{Config.DAILY_DATA_TABLE}'.")
        logging.info(f"[DRY RUN] Sample record: {daily_data_df.iloc[0].to_dict()}")
    else:
        # Upsert all daily data, then upsert latest_df to update market_cap on today's rows
        upsert_in_batches(supabase_client, Config.DAILY_DATA_TABLE, daily_data_df)
        upsert_in_batches(supabase_client, Config.DAILY_DATA_TABLE, latest_df)
    
    logging.info(f"--- SGX Daily Data Importer Finished Successfully (Mode: {args.mode.upper()}) ---")

if __name__ == "__main__":
    main()