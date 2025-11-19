import os
import logging
import argparse
from math import ceil
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. Configuration Class ---
class Config:
    """Groups all configuration variables for easy management."""
    COMPANY_SOURCE_TABLE = "sgx_companies"
    DAILY_DATA_TABLE = "sgx_daily_data"
    YF_PERIOD_DAILY = "1mo"
    YF_PERIOD_FULL_PRIMARY = "1y"
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
        logging.info(f"Successfully fetched {len(symbols)} active symbols (Top 10 by Market Cap).")
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbols from Supabase: {e}")
        return []

def fetch_and_prepare_daily_data(symbols: list[str], mode: str, country: str) -> pd.DataFrame:
    """
    Fetches daily close data, applying the correct ticker suffix for Yahoo Finance.
    """
    if not symbols:
        logging.warning("Symbol list is empty. Skipping data fetch.")
        return pd.DataFrame()

    # --- CRITICAL FIX: Add the correct exchange suffix for yfinance ---
    ticker_extension = ".SI" if country == 'sg' else ".KL"
    full_tickers = [s + ticker_extension for s in symbols]
    
    final_data = pd.DataFrame()

    if mode == 'daily':
        period = Config.YF_PERIOD_DAILY
        logging.info(f"Fetching data in '{mode}' mode for {len(full_tickers)} symbols (e.g., {full_tickers[0] if full_tickers else ''})...")
        final_data = yf.download(tickers=full_tickers, period=period, progress=False, auto_adjust=False, actions=False)
    
    elif mode == 'full':
        logging.info(f"Starting 'full' mode fetch for {len(full_tickers)} symbols.")
        logging.info(f"--> Step 1: Attempting batch download with period='{Config.YF_PERIOD_FULL_PRIMARY}'...")
        data_1y = yf.download(tickers=full_tickers, period=Config.YF_PERIOD_FULL_PRIMARY, progress=False, auto_adjust=False, actions=False)
        
        successful_tickers = data_1y['Close'].columns.dropna().tolist()
        failed_tickers = [s for s in full_tickers if s not in successful_tickers]
        
        final_data = data_1y
        
        if failed_tickers:
            logging.warning(f"--> Step 2: {len(failed_tickers)} symbols failed the 1-year fetch. Retrying with fallback.")
            logging.info(f"Failed tickers: {failed_tickers}")
            logging.info(f"--> Step 3: Attempting fallback batch download with period='{Config.YF_PERIOD_FULL_FALLBACK}'...")
            data_max = yf.download(tickers=failed_tickers, period=Config.YF_PERIOD_FULL_FALLBACK, progress=False, auto_adjust=False, actions=False)
            
            if not data_max.empty:
                logging.info("Combining results from primary and fallback downloads.")
                final_data = pd.concat([data_1y, data_max], axis=1)
        else:
            logging.info("--> All symbols successfully fetched with 1-year period. No fallback needed.")

    if final_data.empty:
        logging.warning("yfinance download returned an empty DataFrame after all attempts.")
        return pd.DataFrame()
        
    logging.info("Processing and transforming downloaded data...")
    close_prices = final_data['Close']
    df_long = close_prices.stack().reset_index()
    df_long.columns = ['date', 'symbol', 'close']
    
    # --- CRITICAL FIX: Strip the suffix to match the base symbol in the database ---
    df_long['symbol'] = df_long['symbol'].str.replace(ticker_extension, '', regex=False)
    
    df_long.dropna(subset=['close'], inplace=True)
    df_long['close'] = df_long['close'].astype(float).round(6)
    df_long['date'] = df_long['date'].dt.strftime('%Y-%m-%d')
    
    logging.info(f"Successfully transformed data into {df_long.shape[0]} daily records.")
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

    upsert_in_batches(supabase_client, Config.DAILY_DATA_TABLE, daily_data_df)
    
    logging.info(f"--- SGX Daily Data Importer Finished Successfully (Mode: {args.mode.upper()}) ---")

if __name__ == "__main__":
    main()