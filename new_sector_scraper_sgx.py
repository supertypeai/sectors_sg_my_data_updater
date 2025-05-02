import os
import json
import time
import requests
import concurrent.futures
import traceback

# Output settings
OUTPUT_DIR = os.path.join(os.getcwd(), 'sector_data')
OUTPUT_JSON = os.path.join(OUTPUT_DIR, 'data_sgx.json')
MAX_WORKERS = 4  # adjust based on performance

# URL for SGX stockscreener API
SCREENER_API_URL = (
    "https://api.sgx.com/stockscreener/v2.0/all?"
    "params=companyName,stockCode,"
    "sector,priceCurrCode,sector"
)

# URL template for detailed snapshot data
SNAPSHOT_API_URL = (
    "https://api.sgx.com/snapshotreports/v2.0/countryCode/SGP/stockCode/{code}"
    "?params=companyName,stockCode,sectorName,industryName,reportingCurrency,tradedCurrency"
)


def fetch_screener_symbols():
    """
    Fetch symbol list from SGX screener API and return list of dicts with 'Code' & 'Trading Name'.
    """
    try:
        print("[STEP] Fetching data from SGX screener API...")
        resp = requests.get(SCREENER_API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json().get('data', [])
        symbols = []
        for item in data:
            code = item.get('stockCode')
            name = item.get('companyName')
            if not code or not name:
                continue
            symbols.append({'Code': code, 'Trading Name': name})
        print(f"[STEP] Retrieved {len(symbols)} symbols from API.")
        return symbols
    except Exception as e:
        print(f"[ERROR] Failed to fetch screener symbols: {e}")
        traceback.print_exc()
        raise


def fetch_snapshot(item: dict) -> dict:
    code_full = item['Code']
    stock_code = code_full.split('.')[0]
    url = SNAPSHOT_API_URL.format(code=stock_code)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        records = resp.json().get('data', [])
        if not records:
            raise ValueError("Empty data returned from snapshot API")
        entry = records[0]
        return {
            'symbol': code_full,
            'name': entry.get('companyName'),
            'sector': entry.get('sectorName'),
            'sub_sector': entry.get('industryName'),
            'currency': entry.get('reportingCurrency')
        }
    except Exception as e:
        print(f"[ERROR] Snapshot fetch failed for {stock_code}: {e}")
        return {
            'symbol': code_full,
            'name': None,
            'sector': None,
            'sub_sector': None,
            'currency': None
        }


def scrape_all(symbols: list) -> list:
    print(f"[STEP] Starting parallel snapshot fetch for {len(symbols)} symbols...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_snapshot, sym) for sym in symbols]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    print("[STEP] Completed all snapshot fetches.")
    # Filter out entries with any null values
    filtered = [r for r in results if all(r.get(k) is not None for k in ('name', 'sector', 'sub_sector', 'currency'))]
    excluded = len(results) - len(filtered)
    print(f"[STEP] Filtered results: kept {len(filtered)}, excluded {excluded} entries with null fields.")
    return filtered


if __name__ == '__main__':
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        symbols = fetch_screener_symbols()
        enriched = scrape_all(symbols)

        print(f"[STEP] Writing output JSON to {OUTPUT_JSON}")
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(enriched, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] Script failed: {e}")
