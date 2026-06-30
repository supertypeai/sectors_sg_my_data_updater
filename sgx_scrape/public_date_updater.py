import os
import sys
import time
import random
import logging
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env')
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SGX_HEADERS = {"User-Agent": "Mozilla/5.0"}
METADATA_URL   = "https://api.sgx.com/marketmetadata/v2?stock-code={symbol}"
CORPORATE_URL  = "https://api.sgx.com/corporateinformation/v1.0?ibmcode={ibmcode}"


def fetch_ibm_code(symbol: str) -> str | None:
    """Get ibmCode for a stock symbol from the marketmetadata API."""
    try:
        resp = requests.get(METADATA_URL.format(symbol=symbol), headers=SGX_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json().get('data', [])
        if not data:
            return None
        return data[0].get('ibmCode')
    except Exception as e:
        logger.error(f"[{symbol}] marketmetadata fetch failed: {e}")
        return None


def fetch_corporate_info(ibmcode: str) -> dict | None:
    """Fetch full corporate info using ibmCode."""
    try:
        resp = requests.get(CORPORATE_URL.format(ibmcode=ibmcode), headers=SGX_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json().get('data', [])
        if not data:
            return None
        return data[0]
    except Exception as e:
        logger.error(f"[ibmcode={ibmcode}] corporateinformation fetch failed: {e}")
        return None


def parse_corporate_info(info: dict) -> dict:
    """Map API fields to DB column names."""
    # Combine address lines, skip blank ones
    address_parts = [
        info.get('registeredOffice1', '') or '',
        info.get('registeredOffice2', '') or '',
        info.get('registeredOffice3', '') or '',
        info.get('registeredOffice4', '') or '',
    ]
    address = ', '.join(p.strip() for p in address_parts if p.strip())

    return {
        'website':     info.get('webAddress') or None,
        'description': info.get('background') or None,
        'address':     address or None,
        'market':      info.get('market') or None,
    }


def fetch_symbols():
    """Fetch all active symbols from sgx_companies."""
    response = supabase.table('sgx_companies').select('symbol').eq('is_active', True).execute()
    return [row['symbol'] for row in (response.data or []) if row.get('symbol')]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--preview', nargs='+', help='Preview data for specific symbols without upserting')
    args = parser.parse_args()

    if args.preview:
        for sym in args.preview:
            api_sym = sym.replace('.SI', '')
            ibmcode = fetch_ibm_code(api_sym)
            if not ibmcode:
                print(f"[{sym}] Could not get ibmCode.")
                continue
            info = fetch_corporate_info(ibmcode)
            if not info:
                print(f"[{sym}] No corporate info returned.")
                continue
            parsed = parse_corporate_info(info)
            print(f"\n=== {sym} (ibmCode: {ibmcode}) ===")
            for k, v in parsed.items():
                print(f"  {k}: {v}")
        return

    symbols = fetch_symbols()
    logger.info(f"Processing {len(symbols)} symbols...")
    updated, failed = 0, 0

    for sym in symbols:
        api_sym = sym.replace('.SI', '')
        ibmcode = fetch_ibm_code(api_sym)
        if not ibmcode:
            failed += 1
            continue

        info = fetch_corporate_info(ibmcode)
        if not info:
            failed += 1
            continue

        parsed = parse_corporate_info(info)
        parsed = {k: v for k, v in parsed.items() if v is not None}

        if parsed:
            try:
                supabase.table('sgx_companies').update(parsed).eq('symbol', sym).execute()
                updated += 1
            except Exception as e:
                logger.error(f"[{sym}] Supabase update failed: {e}")
                failed += 1

        time.sleep(random.uniform(0.3, 0.6))

    logger.info(f"Done. {updated} updated, {failed} failed.")


if __name__ == '__main__':
    main()
