"""
SGX Financials Scraper

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
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables securely
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env')
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure console logging
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
    """
    1. Maps SGX keys to Target Schema keys.
    2. Drops keys that DO NOT EXIST in the Strict Whitelist.
    3. Handles Free Cash Flow & absolute CapEx math.
    """
    if not raw_data:
        return None
        
    cleaned = {}
    
    # 1. Establish mappings and target lists per report
    if report_type == 'income':
        target_list = INCOME_STMT_TARGETS
        key_map = {
            'totalRevenue': 'total_revenue',
            'netIncome': 'net_income',
            'provisionForIncomeTaxes': 'income_taxes',
            'netIncomeBeforeTaxes': 'pretax_income',
            'totalOperatingExpenses': 'operating_expense',
            'dilutedAvgShares': 'diluted_shares_outstanding',
            'operatingIncome': 'operating_income',
            'totalInterest': 'interest_expense',
            'depreciation': 'amortization_of_intangible_assets'
        }
    elif report_type == 'bs':
        target_list = BALANCE_SHEET_TARGETS
        key_map = {
            'totalAssets': 'total_asset',
            'totalEquity': 'total_equity',
            'totalLiabilities': 'total_liabilities'
        }
    elif report_type == 'cf':
        target_list = CASH_FLOW_TARGETS
        key_map = {
            'netChangeInCash': 'net_cash_flow',
            'cashFromOperatingAct': 'operating_cash_flow',
            'cashFromInvestingAct': 'investing_cash_flow',
            'cashFromFinancingAct': 'financing_cash_flow'
            # capitalExpenditure handled manually below
        }
    else:
        return None

    # 2. Iterate, Map, Convert Types, and STRICTLY Filter
    for k, v in raw_data.items():
        if v is None:
            continue
            
        target_key = key_map.get(k)
        if target_key and target_key in target_list:
            try:
                num_val = float(v)
                cleaned[target_key] = int(num_val) if num_val.is_integer() else num_val
            except (ValueError, TypeError):
                cleaned[target_key] = v

    # 3. Explicit Calculations for Cash Flow
    if report_type == 'cf':
        ocf_raw = raw_data.get('cashFromOperatingAct')
        capex_raw = raw_data.get('capitalExpenditure')
        
        if capex_raw is not None:
            try:
                c_val = float(capex_raw)
                abs_capex = abs(c_val)
                # CapEx as positive
                cleaned['capital_expenditure'] = int(abs_capex) if abs_capex.is_integer() else abs_capex
            except ValueError:
                pass
                
        if ocf_raw is not None and capex_raw is not None:
            try:
                # FCF = OCF + CapEx (SGX CapEx is negative, so addition subtracts it correctly)
                fcf = float(ocf_raw) + float(capex_raw)
                cleaned['free_cash_flow'] = int(fcf) if fcf.is_integer() else fcf
            except ValueError:
                pass

    return cleaned if cleaned else None

def fetch_symbols(limit: int = 750, specific_symbols: list = None) -> list:
    """Fetch symbols from sgx_companies table based on execution mode."""
    try:
        query = supabase.table('sgx_companies').select('symbol')
        
        if specific_symbols:
            query = query.in_('symbol', specific_symbols)
        else:
            # Default to active symbols with limit
            query = query.eq('is_active', True).limit(limit)
            
        response = query.execute()
        symbols = [row['symbol'] for row in response.data] if response.data else []
        logger.info(f"Fetched {len(symbols)} symbols from Supabase")
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        return []

def fetch_sgx_financials(session: requests.Session, report_type: str, stock_code: str) -> list:
    """Fetch specific financial report from SGX API with connection pooling."""
    url = API_BASE_URL.format(report_type=report_type, stock_code=stock_code)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json().get('data', [])
    except Exception as e:
        logger.error(f"API error for {stock_code} ({report_type}): {e}")
        return []

def main():
    # Setup argparse for CLI flags
    parser = argparse.ArgumentParser(description="SGX Financials Scraper")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--fullUpdate', action='store_true', help='Update all active symbols (limit 750) for ALL years.')
    group.add_argument('--incremental', action='store_true', help='Update all active symbols for the LATEST available year only (1 record per company).')
    group.add_argument('--specific', nargs='+', help='Update specific symbols (e.g., D05 U11) for ALL years.')
    args = parser.parse_args()

    # Determine execution logic based on flags
    specific_symbols = args.specific if args.specific else None
    symbols = fetch_symbols(limit=750, specific_symbols=specific_symbols)
    
    if not symbols:
        logger.info("No symbols found to process. Exiting.")
        return

    updated_on = datetime.now(timezone.utc).isoformat()
    
    if args.incremental:
        logger.info("MODE: Incremental Update (Only the LATEST year per company will be updated).")
    else:
        logger.info("MODE: Full Update (All historical years will be processed).")

    # Use a single session to persist connections across all symbols
    with requests.Session() as session:
        for symbol in symbols:
            stock_code = symbol.split('.')[0]  # e.g., 'D05.SI' -> 'D05'
            logger.info(f"Processing financials for symbol: {symbol} (API stock_code: {stock_code})")

            # 1. Fetch reports with random delays
            income_data = fetch_sgx_financials(session, 'incomeStatement', stock_code)
            time.sleep(random.uniform(1.5, 3.0)) 
            
            bs_data = fetch_sgx_financials(session, 'balanceSheet', stock_code)
            time.sleep(random.uniform(1.5, 3.0))
            
            cf_data = fetch_sgx_financials(session, 'cashFlow', stock_code)

            # 2. Group data by 'periodEndDate'
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

            # --- ADDED: Incremental Logic Filter ---
            # If incremental mode is ON, find the absolute latest date and discard the rest.
            if args.incremental and combined_financials:
                latest_date = max(combined_financials.keys())
                combined_financials = {latest_date: combined_financials[latest_date]}
            # ---------------------------------------

            # 3. Construct payload for upsert
            upsert_payloads = []
            
            for date, metrics in combined_financials.items():
                try:
                    financial_year = int(date.split('-')[0])
                except (IndexError, ValueError):
                    continue
                    
                row = {
                    "symbol": symbol,
                    "financial_year": financial_year,
                    "income_stmt_metrics": transform_metrics(metrics.get('income', {}), 'income'),
                    "balance_sheet_metrics": transform_metrics(metrics.get('bs', {}), 'bs'),
                    "cash_flow_metrics": transform_metrics(metrics.get('cf', {}), 'cf'),
                    "updated_on": updated_on,
                    "date": date
                }
                upsert_payloads.append(row)

            if not upsert_payloads:
                logger.warning(f"No valid financial data parsed for {symbol}")
                time.sleep(random.uniform(1.5, 3.0))
                continue

            # 4. Perform the Upsert
            try:
                supabase.table('sgx_financials_annual').upsert(upsert_payloads).execute()
                logger.info(f"Successfully upserted {len(upsert_payloads)} records for {symbol}.")
            except Exception as e:
                logger.error(f"Failed to upsert data for {symbol}: {e}")
                
            # Random delay before processing the next company
            time.sleep(random.uniform(1.5, 3.0))

    logger.info("Finished processing all symbols.")

if __name__ == '__main__':
    main()