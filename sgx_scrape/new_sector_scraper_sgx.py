# NOTE: This is a copy of the original script for reference. No functional changes needed.
import os
import sys
import json
import requests
import concurrent.futures
import traceback
from dotenv import load_dotenv
from supabase import create_client, Client

# ==========================================
# CONFIGURATION
# ==========================================
EXEMPT_SYMBOLS = {'TCPD', 'C70', 'TPED', 'TATD', 'CWCU', 'PJX'}

# SGX APIs
STOCKS_API_URL = "https://api.sgx.com/securities/v1.1"
SCREENER_API_URL = "https://api.sgx.com/stockscreener/v2.0/all?params=companyName,stockCode,sector,priceCurrCode"
SNAPSHOT_API_URL = "https://api.sgx.com/snapshotreports/v2.0/countryCode/SGP/stockCode/{code}?params=companyName,sectorName,industryName,reportingCurrency,tradedCurrency"

MAX_WORKERS = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.sgx.com",
    "Referer": "https://www.sgx.com/"
}

SECTOR_MAPPING = {
    "Industrial & Commercial Services": "Industrials",
    "Industrial Goods": "Industrials",
    "Industrial Services": "Industrials",
    "Transportation": "Industrials",
    "Retailers": "Consumer Cyclical",
    "Cyclical Consumer Services": "Consumer Cyclical",
    "Retail Trade": "Consumer Cyclical",
    "Personal & Household Products & Services": "Consumer Defensive",
    "Holding Companies": "Financial Services",
    "Energy Minerals": "Energy"
}

# Statistics & Error Collection
stats = {
    'db_total_records': 0,
    'db_active_records': 0,
    'sgx_active_records': 0,
    'to_insert': 0,
    'to_deactivate': 0,
    'to_reactivate': 0,
    'inserted': 0,
    'deactivated': 0,
    'reactivated': 0,
    'skipped_exempt_deactivate': 0,
    'skipped_exempt_inactive': 0
}

global_errors = []

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def normalize_sub_sector(sub):
    if not sub:
        return "Unknown"
    if sub.endswith(' - Regional'):
        return sub.replace(' - Regional', '')
    return sub

def chunked_list(lst, n):
    """Yield successive n-sized chunks from lst to prevent API request too large."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def load_env_vars():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env file.")
        sys.exit(1)
    return url, key

def init_supabase():
    url, key = load_env_vars()
    return create_client(url, key)

# Core logic functions remain unchanged – see original script for full implementation.

if __name__ == "__main__":
    # This file is now located in sgx_scrape folder. No execution code needed here.
    pass
