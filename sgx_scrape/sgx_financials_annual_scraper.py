"""
SGX Financials Scraper (Multi-threaded & Optimized)

Usage Guide:
------------
1. Full Update
   Fetches and updates up to 750 active symbols for ALL available years.
   Command: python sgx_financials_scraper.py --fullUpdate

2. Incremental Update (Monthly/Daily runs)
   Fetches and updates all active symbols for the LATEST available year only (1 record per company).
   Command: python sgx_financials_scraper.py --incremental

3. Specific Companies Update
   Fetches and updates specific companies for ALL available years.
   You can provide the raw symbol (e.g., D05) or the DB symbol (e.g., D05.SI).
   Command: python sgx_financials_scraper.py --specific D05 U11 C07
"""

import os
import sys
import argparse
import time
import random
import logging
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from supabase import create_client, Client

# ==========================================
# ENV & SUPABASE SETUP
# ==========================================
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env')
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure simple console logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SGX API endpoints
API_BASE_URL = "https://api.sgx.com/financialstatementreports/v2.0/{report_type}/countryCode/SGP/stockCode/{stock_code}?params=all"

# ==========================================
# STRICT WHITELISTS BASED ON YOUR SCHEMA
# ==========================================
INCOME_STMT_TARGETS = {
    "minorities", "net_income", "income_taxes", "pretax_income", "total_revenue", 
    "interest_income", "interest_expense", "operating_income", "operating_expense", 
    "net_trading_income", "net_interest_income", "int_income_breakdown", 
    "other_non_interest_income", "diluted_shares_outstanding", "operating_expense_breakdown", 
    "non_operating_income_or_loss", "net_fee_and_commission_income", 
    "amortization_of_intangible_assets", "allowances_for_credit_and_other_losses"
}

BALANCE_SHEET_TARGETS = {
    "net_loan", "credit_rwa", "gross_loan", "market_rwa", "total_asset", 
    "time_deposit", "total_equity", "earning_asset", "total_capital", "total_deposit", 
    "non_loan_asset", "current_account", "operational_rwa", "savings_account", 
    "total_liabilities", "core_capital_tier1", "allowance_for_loans", 
    "total_risk_weighted_asset", "supplementary_capital_tier2", 
    "non_interest_bearing_liabilities", "other_interest_bearing_liabilities"
}

CASH_FLOW_TARGETS = {
    "net_cash_flow", "free_cash_flow", "capital_expenditure", 
    "financing_cash_flow", "investing_cash_flow", "operating_cash_flow"
}

def transform_metrics(raw_data: dict, report_type: str) -> dict | None:
    """ Maps SGX keys, filters by whitelist, and calculates FCF/CapEx. """
    if not raw_data:
        return None
        
    cleaned = {}
    
    if report_type == 'income':
        target_list = INCOME_STMT_TARGETS
        key_map = {
            'totalRevenue': 'total_revenue', 'netIncome': 'net_income',
            'provisionForIncomeTaxes': 'income_taxes', 'netIncomeBeforeTaxes': 'pretax_income',
            'totalOperatingExpenses': 'operating_expense', 'dilutedAvgShares': 'diluted_shares_outstanding',
            'operatingIncome': 'operating_income', 'totalInterest': 'interest_expense',
            'depreciation': 'amortization_of_intangible_assets'
        }
    elif report_type == 'bs':
        target_list = BALANCE_SHEET_TARGETS
        key_map = {
            'totalAssets': 'total_asset', 'totalEquity': 'total_equity',
            'totalLiabilities': 'total_liabilities'
        }
    elif report_type == 'cf':
        target_list = CASH_FLOW_TARGETS
        key_map = {
            'netChangeInCash': 'net_cash_flow', 'cashFromOperatingAct': 'operating_cash_flow',
            'cashFromInvestingAct': 'investing_cash_flow', 'cashFromFinancingAct': 'financing_cash_flow'
        }
    else:
        return None

    for k, v in raw_data.items():
        if v is None: continue
        target_key = key_map.get(k)
        if target_key and target_key in target_list:
            try:
                num_val = float(v)
                cleaned[target_key] = int(num_val) if num_val.is_integer() else num_val
            except (ValueError, TypeError):
                cleaned[target_key] = v

    # Cash Flow explicit calculations
    if report_type == 'cf':
        ocf_raw = raw_data.get('cashFromOperatingAct')
        capex_raw = raw_data.get('capitalExpenditure')
        
        if capex_raw is not None:
            try:
                c_val = float(capex_raw)
                abs_capex = abs(c_val)
                cleaned['capital_expenditure'] = int(abs_capex) if abs_capex.is_integer() else abs_capex
            except ValueError: pass
                
        if ocf_raw is not None and capex_raw is not None:
            try:
                fcf = float(ocf_raw) + float(capex_raw)
                cleaned['free_cash_flow'] = int(fcf) if fcf.is_integer() else fcf
            except ValueError: pass

    return cleaned if cleaned else None

def fetch_symbols(limit: int = 750, specific_symbols: list = None) -> list:
    """Fetch symbols from database."""
    try:
        query = supabase.table('sgx_companies').select('symbol')
        if specific_symbols:
            formatted_symbols = [s if '.' in s else f"{s}.SI" for s in specific_symbols]
            query = query.in_('symbol', formatted_symbols)
        else:
            query = query.eq('is_active', True).limit(limit)
            
        response = query.execute()
        return [row['symbol'] for row in response.data] if response.data else []
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        return []

def fetch_with_retry(session: requests.Session, report_type: str, stock_code: str, max_retries=3) -> list:
    """ Fetches SGX API with a retry mechanism and exponential backoff delay. """
    url = API_BASE_URL.format(report_type=report_type, stock_code=stock_code)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get('data', [])
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"[API ERROR] Failed to fetch {report_type} for {stock_code} after {max_retries} attempts. Reason: {e}")
                return []
            
            # Exponential backoff: 2s, 4s, etc., plus a little random jitter
            delay = (2 ** attempt) + random.uniform(0.5, 1.5)
            logger.warning(f"[API RETRY] {stock_code} ({report_type}) - Attempt {attempt + 1} failed. Retrying in {delay:.2f}s... Reason: {e}")
            time.sleep(delay)

def process_symbol(symbol: str, is_incremental: bool, updated_on: str):
    """ Thread worker function to process a single company """
    stock_code = symbol.split('.')[0]
    
    # Instantiate a local session per thread to ensure thread-safety
    with requests.Session() as session:
        # Fetch data with delays to prevent immediate rate limit triggers across threads
        income_data = fetch_with_retry(session, 'incomeStatement', stock_code)
        time.sleep(random.uniform(1.0, 2.0)) 
        
        bs_data = fetch_with_retry(session, 'balanceSheet', stock_code)
        time.sleep(random.uniform(1.0, 2.0))
        
        cf_data = fetch_with_retry(session, 'cashFlow', stock_code)

    # Group data by periodEndDate
    combined_financials = {}
    for item in income_data:
        date = item.get('periodEndDate')
        if date: combined_financials.setdefault(date, {})['income'] = item
    for item in bs_data:
        date = item.get('periodEndDate')
        if date: combined_financials.setdefault(date, {})['bs'] = item
    for item in cf_data:
        date = item.get('periodEndDate')
        if date: combined_financials.setdefault(date, {})['cf'] = item

    # --- Incremental Logic Filter ---
    if is_incremental and combined_financials:
        latest_date = max(combined_financials.keys())
        combined_financials = {latest_date: combined_financials[latest_date]}

    # Construct payload for upsert
    upsert_payloads = []
    for date, metrics in combined_financials.items():
        try:
            financial_year = int(date.split('-')[0])
        except (IndexError, ValueError):
            continue
            
        upsert_payloads.append({
            "symbol": symbol,
            "financial_year": financial_year,
            "income_stmt_metrics": transform_metrics(metrics.get('income', {}), 'income'),
            "balance_sheet_metrics": transform_metrics(metrics.get('bs', {}), 'bs'),
            "cash_flow_metrics": transform_metrics(metrics.get('cf', {}), 'cf'),
            "updated_on": updated_on,
            "date": date
        })

    if not upsert_payloads:
        logger.info(f"[SKIP] No valid financial data found for {symbol}.")
        return

    # Upsert with Retry Mechanism
    max_upsert_retries = 3
    for attempt in range(max_upsert_retries):
        try:
            supabase.table('sgx_financials_annual').upsert(upsert_payloads).execute()
            logger.info(f"[SUCCESS] Upserted {len(upsert_payloads)} records for {symbol}.")
            break
        except Exception as e:
            if attempt == max_upsert_retries - 1:
                logger.error(f"[DB ERROR] Failed to upsert data for {symbol} after {max_upsert_retries} attempts. Reason: {e}")
            else:
                delay = random.uniform(2.0, 4.0)
                logger.warning(f"[DB RETRY] Upsert failed for {symbol} (Attempt {attempt + 1}). Retrying in {delay:.2f}s... Reason: {e}")
                time.sleep(delay)

def main():
    parser = argparse.ArgumentParser(description="SGX Financials Scraper (Optimized)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--fullUpdate', action='store_true', help='Update all symbols for ALL years.')
    group.add_argument('--incremental', action='store_true', help='Update all symbols for LATEST year only.')
    group.add_argument('--specific', nargs='+', help='Update specific symbols (e.g., D05 U11).')
    args = parser.parse_args()

    logger.info("Initializing SGX Scraper...")
    symbols = fetch_symbols(limit=750, specific_symbols=args.specific)
    if not symbols:
        logger.warning("No symbols found to process. Exiting.")
        return

    total_symbols = len(symbols)
    mode = "Incremental Update (1 record/latest year per company)" if args.incremental else "Full Update (All historical years)"
    logger.info(f"Starting execution for {total_symbols} symbols. MODE: {mode}")
        
    updated_on = datetime.now(timezone.utc).isoformat()
    
    # Using ThreadPoolExecutor for concurrency (5 threads max)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_symbol, sym, args.incremental, updated_on): sym 
            for sym in symbols
        }
        
        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            sym = futures[future]
            try:
                future.result() 
                logger.info(f"[{completed_count}/{total_symbols}] Finished processing for {sym}.")
            except Exception as e:
                logger.error(f"[{completed_count}/{total_symbols}] [CRITICAL THREAD ERROR] Unhandled exception for symbol {sym}: {e}")

    logger.info("Scraping Completed Successfully.")

if __name__ == '__main__':
    main()