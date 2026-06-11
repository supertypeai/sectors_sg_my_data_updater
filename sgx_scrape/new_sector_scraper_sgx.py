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

global_errors =[]

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

# ==========================================
# CORE LOGIC
# ==========================================
def fetch_db_state(supabase: Client) -> tuple:
    print("[STEP] Fetch DB State: Grab all symbols and their is_active status from Supabase.")
    all_symbols = set()
    active_symbols = set()
    page_size = 1000
    offset = 0
    
    try:
        while True:
            response = supabase.table("sgx_companies").select("symbol, is_active").range(offset, offset + page_size - 1).execute()
            data = response.data
            if not data:
                break
                
            for r in data:
                all_symbols.add(r['symbol'])
                if r['is_active']:
                    active_symbols.add(r['symbol'])
                    
            if len(data) < page_size:
                break
            offset += page_size
            
        stats['db_total_records'] = len(all_symbols)
        stats['db_active_records'] = len(active_symbols)
        print(f"       -> Retrieved {stats['db_total_records']} total DB symbols ({stats['db_active_records']} active).")
    except Exception as e:
        global_errors.append(f"DB Fetch Error: {str(e)}\n{traceback.format_exc()}")
        
    return all_symbols, active_symbols

def fetch_sgx_state() -> set:
    print("[STEP] Fetch SGX State (API 1): Grab all currently active/suspended stock codes from SGX.")
    active_sgx_symbols = set()
    try:
        resp = requests.get(STOCKS_API_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json().get('data', {}).get('prices',[])
        
        valid_types = {"stocks", "reits", "businesstrusts"}
        
        for item in data:
            symbol = item.get('nc')
            sec_type = item.get('type', 'unknown')
            
            # Filter logic: We accept all statuses (including 'SUSP'), but strict on sec_type
            if sec_type in valid_types:
                if symbol:
                    active_sgx_symbols.add(symbol)
                    
        stats['sgx_active_records'] = len(active_sgx_symbols)
        print(f"       -> SGX API reports {stats['sgx_active_records']} targeted symbols (including SUSP).")
    except Exception as e:
        global_errors.append(f"SGX API 1 Error: {str(e)}\n{traceback.format_exc()}")
        
    return active_sgx_symbols

def fetch_enrichment_data(symbols_to_insert: set) -> list:
    if not symbols_to_insert:
        return []

    print(f"[STEP] Fetch Details (API 2 & 3): Fetching Screener and Snapshot data for {len(symbols_to_insert)} new companies with coalesce logic.")
    insert_payloads = {}
    
    # 1. Fetch from API 2 (Screener)
    try:
        resp = requests.get(SCREENER_API_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        screener_data = resp.json().get('data',[])
        
        for item in screener_data:
            raw_code = item.get('stockCode', '')
            base_code = raw_code.split('.')[0] if raw_code else ""
            
            if base_code in symbols_to_insert:
                insert_payloads[base_code] = {
                    "symbol": base_code,
                    "name": item.get('companyName'), # Might be None
                    "sector": item.get('sector'),    # Might be None
                    "currency": item.get('priceCurrCode'), # Might be None
                    "is_active": True,
                    "investing_symbol": base_code
                }
    except Exception as e:
        global_errors.append(f"API 2 (Screener) Error: {str(e)}")

    # Ensure all symbols are in the payload dict in case API 2 completely missed them
    for sym in symbols_to_insert:
        if sym not in insert_payloads:
            insert_payloads[sym] = {
                "symbol": sym, "name": None, "sector": None, 
                "currency": None, "is_active": True, "investing_symbol": sym
            }

    # 2. Fetch from API 3 (Snapshot) concurrently
    def fetch_snapshot(base_code):
        url = SNAPSHOT_API_URL.format(code=base_code)
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            records = r.json().get('data',[])
            if records:
                return base_code, records[0]
        except Exception:
            pass # Fail silently per item, coalesce logic handles fallbacks
        return base_code, {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_snapshot, code): code for code in symbols_to_insert}
        for future in concurrent.futures.as_completed(future_to_code):
            base_code, snap_data = future.result()
            payload = insert_payloads[base_code]
            
            # --- COALESCE LOGIC ---
            # Priority: API 2 -> API 3 -> Default Fallback
            
            # Name
            final_name = payload.get('name') or snap_data.get('companyName') or base_code
            
            # Sector
            raw_sector = payload.get('sector') or snap_data.get('sectorName')
            final_sector = SECTOR_MAPPING.get(raw_sector, raw_sector) if raw_sector else "Unknown"
            
            # Sub-Sector (Only available in API 3)
            raw_sub = snap_data.get('industryName')
            final_sub_sector = normalize_sub_sector(raw_sub) if raw_sub else "Unknown"
            
            # Currency
            final_currency = payload.get('currency') or snap_data.get('tradedCurrency') or snap_data.get('reportingCurrency') or "SGD"
            
            # Assign coalesced values back
            payload['name'] = final_name
            payload['sector'] = final_sector
            payload['sub_sector'] = final_sub_sector
            payload['currency'] = final_currency

    return list(insert_payloads.values())

def main():
    dry_run = "--dryrun" in sys.argv
    if dry_run:
        print("\n" + "="*50)
        print("!! RUNNING IN DRY-RUN MODE !!")
        print("No changes will be executed on the DB.")
        print("="*50 + "\n")

    try:
        supabase = init_supabase()
        
        # 1 & 2. Get State
        db_all, db_active = fetch_db_state(supabase)
        sgx_active = fetch_sgx_state()
        
        # Stop immediately if critical API errors happen to prevent wiping DB
        if global_errors:
            raise Exception("Critical errors occurred while fetching base states. Execution halted.")

        # 3. The Diff Logic
        raw_to_insert = sgx_active - db_all
        raw_to_deactivate = db_active - sgx_active
        raw_to_reactivate = sgx_active.intersection(db_all - db_active)

        to_insert = raw_to_insert
        to_deactivate = set()
        to_reactivate = set()

        # Apply Exception rules
        for sym in raw_to_deactivate:
            if sym in EXEMPT_SYMBOLS:
                stats['skipped_exempt_deactivate'] += 1
            else:
                to_deactivate.add(sym)

        for sym in raw_to_reactivate:
            if sym in EXEMPT_SYMBOLS:
                stats['skipped_exempt_inactive'] += 1
            else:
                to_reactivate.add(sym)

        stats['to_insert'] = len(to_insert)
        stats['to_deactivate'] = len(to_deactivate)
        stats['to_reactivate'] = len(to_reactivate)

        print("\n[STEP] Compare (The Diff):")
        print(f"  To Insert (IPO): {len(to_insert)} symbols in SGX, but NOT in DB.")
        print(f"  To Deactivate (Delisted): {len(to_deactivate)} symbols active in DB, but NOT in SGX.")
        print(f"  To Reactivate: {len(to_reactivate)} symbols marked inactive in DB, but showing as active in SGX.\n")

        # 4. Fetch details for NEW companies only
        new_payloads =[]
        if to_insert:
            new_payloads = fetch_enrichment_data(to_insert)

        # =========================================================================
        # PREVIEW / SNIPPET OF UPCOMING CHANGES
        # =========================================================================
        print("\n" + "="*50)
        print("[PREVIEW] SNIPPET OF DATA TO BE INSERTED OR UPDATED:")
        print("="*50)
        
        if new_payloads:
            print(f"\n---> TO INSERT ({len(new_payloads)} payloads):")
            print(json.dumps(new_payloads, indent=2))
        else:
            print("\n---> TO INSERT: None")

        if to_deactivate:
            print(f"\n---> TO DEACTIVATE ({len(to_deactivate)} symbols):")
            print(f"     {list(to_deactivate)}")
        else:
            print("\n---> TO DEACTIVATE: None")
            
        if to_reactivate:
            print(f"\n---> TO REACTIVATE ({len(to_reactivate)} symbols):")
            print(f"     {list(to_reactivate)}")
        else:
            print("\n---> TO REACTIVATE: None")
            
        print("="*50 + "\n")

        # 5. Execute
        print("[STEP] Batch Execution: Push batched updates and inserts to Supabase.")
        if dry_run:
            print("  [DRY RUN] Execution skipped.")
            stats['deactivated'] = len(to_deactivate)
            stats['reactivated'] = len(to_reactivate)
            stats['inserted'] = len(new_payloads)
        else:
            # Deactivate
            if to_deactivate:
                for chunk in chunked_list(list(to_deactivate), 100):
                    try:
                        supabase.table("sgx_companies").update({"is_active": False}).in_("symbol", chunk).execute()
                        stats['deactivated'] += len(chunk)
                    except Exception as e:
                        global_errors.append(f"Deactivation DB Error: {str(e)}")

            # Reactivate
            if to_reactivate:
                for chunk in chunked_list(list(to_reactivate), 100):
                    try:
                        supabase.table("sgx_companies").update({"is_active": True}).in_("symbol", chunk).execute()
                        stats['reactivated'] += len(chunk)
                    except Exception as e:
                        global_errors.append(f"Reactivation DB Error: {str(e)}")

            # Insert
            if new_payloads:
                for chunk in chunked_list(new_payloads, 100):
                    try:
                        supabase.table("sgx_companies").insert(chunk).execute()
                        stats['inserted'] += len(chunk)
                    except Exception as e:
                        global_errors.append(f"Insert DB Error: {str(e)}")

    except Exception as e:
        global_errors.append(f"Main Process Crash: {str(e)}\n{traceback.format_exc()}")

    # Finally - Output Summary & Handle Errors
    print("\n-- Upsert Summary --")
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")

    if global_errors:
        print("\n" + "!" * 50)
        print("THE FOLLOWING ERRORS OCCURRED DURING EXECUTION:")
        print("!" * 50)
        for err in global_errors:
            print(f"- {err}\n")
        sys.exit("Script completed, but with errors (see above).")

if __name__ == "__main__":
    main()