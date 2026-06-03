import os
import sys
import json
import time
import random
import logging
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env')
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure simple console logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL_TEMPLATE = (
    "https://api.sgx.com/companygeneralinformation/v2.0/countryCode/SGP/stockCode/{symbol}"
    "?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
)

def fetch_symbols(limit: int = 10):
    """Fetch up to `limit` symbols from sgx_companies table."""
    try:
        response = supabase.table('sgx_companies')\
            .select('symbol')\
            .eq('is_active', True)\
            .is_('public_date', 'null')\
            .limit(limit)\
            .execute()
        data = response.data or []
        symbols = [row['symbol'] for row in data if 'symbol' in row]
        logger.info(f"Fetched {len(symbols)} symbols from Supabase")
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        return []

def fetch_public_date(symbol: str) -> str | None:
    """Call SGX API for a symbol and return the `publicDate` string if successful."""
    url = API_URL_TEMPLATE.format(symbol=symbol)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        # Expected structure: {"data":[{..."publicDate":"2026-05-22"...}]}
        data = payload.get('data', [])
        if not data:
            logger.warning(f"No data returned for symbol {symbol}")
            return None
        public_date = data[0].get('publicDate')
        if not public_date:
            logger.warning(f"publicDate missing for symbol {symbol}")
        return public_date
    except Exception as e:
        logger.error(f"API error for symbol {symbol}: {e}")
        return None

def main():
    symbols = fetch_symbols(limit=750)  # Changed back to 5 based on your logs
    successful_updates = []
    
    for sym in symbols:
        public_date = fetch_public_date(sym)
        if public_date:
            try:
                # Explicitly update only the target row and field
                supabase.table('sgx_companies')\
                    .update({"public_date": public_date})\
                    .eq('symbol', sym)\
                    .execute()
                
                # logger.info(f"Updated symbol {sym} -> public_date: {public_date}")
                successful_updates.append(sym)  # Keep track of the success
            except Exception as e:
                logger.error(f"Failed to update symbol {sym}: {e}")
                
        # Random delay to avoid rate limiting
        time.sleep(random.uniform(1, 2))

    logger.info(f"Finished. Successfully updated {len(successful_updates)} records.")

if __name__ == '__main__':
    main()
