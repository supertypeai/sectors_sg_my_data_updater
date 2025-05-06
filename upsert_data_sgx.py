import os
import json
from dotenv import load_dotenv
from supabase import create_client, Client

# Symbols to never deactivate (always keep active)
EXEMPT_SYMBOLS = {'TCPD', 'C70', 'TPED', 'TATD'}

# Statistics collector
stats = {
    'total_entries': 0,
    'usable_entries': 0,
    'inserted': 0,
    'updated': 0,
    'skipped_no_change': 0,
    'upsert_errors': 0,
    'deactivated': 0,
    'activated_exempt': 0,
    'skipped_exempt_deactivate': 0
}

# Sub-sector overrides to clean up labels
def normalize_sub_sector(sub):
    if sub.endswith(' - Regional'):
        return sub.replace(' - Regional', '')
    return sub

# Load environment variables
def load_env_vars():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env file.")
        exit(1)
    return url, key

# Initialize Supabase client
def init_supabase():
    url, key = load_env_vars()
    print(f"Connecting to Supabase at {url}")
    return create_client(url, key)

# Load and filter JSON data
def load_json_data(filepath: str) -> list:
    print(f"Loading JSON data from {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    usable = []
    for entry in data:
        stats['total_entries'] += 1
        symbol = entry.get('symbol')
        if not symbol or symbol in ('', '-', None):
            continue
        name = entry.get('name')
        sector = entry.get('sector')
        sub_sector = entry.get('sub_sector')
        currency = entry.get('currency')
        if not all([name, sector, sub_sector, currency]):
            continue
        entry['sub_sector'] = normalize_sub_sector(sub_sector)
        usable.append(entry)
        stats['usable_entries'] += 1
    print(f"Filtered down to {stats['usable_entries']} usable entries from {stats['total_entries']} total.")
    return usable

# Fetch existing rows with only necessary columns
def fetch_existing_rows(supabase: Client, table: str) -> dict:
    response = supabase.table(table).select("symbol, name, sector, sub_sector, currency, is_active").execute()
    rows = {row['symbol']: row for row in response.data}
    print(f"Fetched {len(rows)} existing rows in '{table}'.")
    return rows

# Upsert JSON entries (insert full payload or update only if changed)
def upsert_entries(supabase: Client, table: str, entries: list, existing_rows: dict):
    for entry in entries:
        symbol = entry['symbol']
        existing = existing_rows.get(symbol)
        # Always ensure is_active=True on insert or update
        if existing:
            # If nothing changed including activation, skip
            if (
                existing.get('name') == entry['name'] and
                existing.get('sector') == entry['sector'] and
                existing.get('sub_sector') == entry['sub_sector'] and
                existing.get('currency') == entry['currency'] and
                existing.get('is_active') is True
            ):
                stats['skipped_no_change'] += 1
                continue
            # Update only changed fields and reactivate if was inactive
            payload = {
                'symbol': symbol,
                'name': entry['name'],
                'sector': entry['sector'],
                'sub_sector': entry['sub_sector'],
                'currency': entry['currency'],
                'is_active': True
            }
            is_insert = False
        else:
            # New insert with full payload
            payload = {**entry, 'is_active': True}
            is_insert = True
        try:
            supabase.table(table).upsert(payload, on_conflict='symbol').execute()
            if is_insert:
                stats['inserted'] += 1
            else:
                stats['updated'] += 1
        except Exception:
            stats['upsert_errors'] += 1

# Deactivate old symbols, but always keep exempt active

def deactivate_old(supabase: Client, table: str, existing_symbols: set, new_symbols: set):
    to_check = existing_symbols - new_symbols
    for symbol in to_check:
        if symbol in EXEMPT_SYMBOLS:
            try:
                supabase.table(table).update({'is_active': True}).eq('symbol', symbol).execute()
                stats['activated_exempt'] += 1
            except Exception:
                pass
            stats['skipped_exempt_deactivate'] += 1
            continue
        try:
            supabase.table(table).update({'is_active': False}).eq('symbol', symbol).execute()
            stats['deactivated'] += 1
        except Exception:
            pass

# Main workflow
def main():
    supabase = init_supabase()
    table_name = "sgx_companies"
    filepath = "sector_data/data_sgx.json"

    entries = load_json_data(filepath)
    new_symbols = {e['symbol'] for e in entries}
    existing_rows = fetch_existing_rows(supabase, table_name)

    upsert_entries(supabase, table_name, entries, existing_rows)
    deactivate_old(supabase, table_name, set(existing_rows.keys()), new_symbols)

    # Summary
    print("-- Upsert Summary --")
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")

if __name__ == "__main__":
    main()
