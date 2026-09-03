import pandas as pd
from supabase import create_client
import os
import requests
import numpy as np
from datetime import datetime,timedelta
import json
import sys
import pytz
import argparse
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')
import time
import concurrent.futures
from symbol_utils import bare_symbol, db_symbol

# Parallel Yahoo fetches (shared 4-thread pool, same as sg_my_scraper).
_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Transient-failure retries (429 / network drops).
_MAX_RETRIES = 3

# curl_cffi (impersonate=chrome) session so the pool shares one connection pool.
import yf_custom  # noqa: E402

# Alias: all `yf.Ticker(...)` below use the session-wired Ticker.
yf = yf_custom

# Symbols are stored with their exchange suffix ("D05.SI"); Yahoo tickers are
# built from the bare code, so strip before re-appending one.



def fetch_existing_symbol(country,supabase):
    if country == "SG":
        data = supabase.table("sgx_companies").select("symbol").eq("is_active", True).execute()
    elif country == "MY":
        data = supabase.table("klse_companies").select("symbol").execute()

    data = pd.DataFrame(data.data)

    return data

def upsert_db(update_data, supabase, country):
    if country == "SG":
        table = "sgx_companies"
    elif country == "MY":
        table = "klse_companies"

    # Normalize symbols before writing. SGX stores suffixed form ("D05.SI");
    # KLSE stores the bare Bursa code ("1155", no suffix).
    update_data = update_data.copy()
    update_data["symbol"] = update_data["symbol"].map(lambda s: db_symbol(s, country))
    # Dedupe: bare + suffixed rows for the same company would collide on the conflict key.
    update_data = update_data.drop_duplicates(subset=["symbol"], keep="last")

    # Only upsert columns the table actually has (avoids 400s, e.g. SG monthly
    # highlight cols absent on sgx_companies).
    try:
        existing = list(supabase.table(table).select("*").limit(1).execute().data[0].keys())
    except Exception:
        existing = None  # fall back to attempting all

    # BATCH: one upsert per column (was N*M per-ticker round-trips).
    for i in update_data.columns.drop('symbol'):
        if existing is not None and i not in existing:
            print(f"Skipping column {i}: not present on {table}")
            continue
        df = update_data[["symbol", i]]

        # Upsert only real values: NaN rows (failed fetch) leave the stored DB
        # value untouched instead of NULLing it.
        df_not_na = df[~df[i].isna()].copy()

        if i in ['historical_earnings', 'historical_revenue']:
            df_not_na[i] = df_not_na[i].apply(json.loads)

        if df_not_na.empty:
            print(f"Finish updating data for column {i} (no values)")
            continue
        records = df_not_na.to_dict("records")
        # ON CONFLICT DO UPDATE preserves columns absent from the payload.
        try:
            supabase.table(table).upsert(records, on_conflict="symbol").execute()
        except Exception as e:
            print(f"Failed to update {i}: {e}")

        print(f"Finish updating data for column {i}")

def fetch_div_ttm(stock, currency, symbol, curr,resp):
    try:
        ticker = yf.Ticker(f"{bare_symbol(stock)}.{symbol}")

        div = pd.DataFrame(ticker.dividends).reset_index()
        div_rate = 0
        if not div.empty:
            div.columns = div.columns.str.lower()
            div['date'] = pd.to_datetime(div['date'], utc=True)

            one_year_ago = datetime.now(pytz.timezone('Asia/Singapore')) - timedelta(days=365)
            # Convert the cutoff to tz-aware UTC so the comparison is safe whether
            # yfinance returns tz-naive or tz-aware dates.
            one_year_ago_utc = one_year_ago.astimezone(pytz.utc)
            recent_dividends = div[div.date >= one_year_ago_utc]
            div_rate = recent_dividends['dividends'].sum()

            data_currency = ticker.info.get('currency', None)

            if data_currency != curr:
                if data_currency in resp and currency in resp[data_currency]:
                    curr_value = resp[data_currency][currency]
                    div_rate = div_rate * curr_value

    except Exception as e:
        # Re-raise: caller's retry/backoff handles transient failures. Empty
        # dividends df (no divs -> div_rate=0 -> None below) is NOT an exception.
        raise

    if div_rate == 0:
        div_rate = None
    div_ttm = pd.DataFrame(data={'symbol': stock, 'dividend_ttm': div_rate}, index=[0])

    return div_ttm

def update_div_ttm(country, country_data, supabase, resp):
    div_ttm = pd.DataFrame()
    base_delay = 2

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"

    stocks = country_data.symbol.unique()

    def _div_one(stock):
        retry_count = 0
        max_retries = 3
        while True:
            try:
                data = fetch_div_ttm(stock, curr, symbol, curr, resp)
                return data
            except Exception as e:
                retry_count += 1
                error_message = str(e).lower()
                # Retry transient errors (429 / HTTPError / connection / timeout /
                # JSONDecode — yfinance's typical 429 symptom: HTML/empty body).
                # String matching covers requests/urllib3 (their ConnectionError
                # is not builtin); JSONDecodeError never appears in the message.
                is_transient = ("rate limit" in error_message or "429" in error_message
                                or "too many requests" in error_message
                                or "connection" in error_message
                                or "timeout" in error_message
                                or "http error" in error_message
                                or "server error" in error_message
                                or isinstance(e, json.JSONDecodeError))
                if retry_count < max_retries and is_transient:
                    time.sleep(base_delay * (2 ** (retry_count - 1)))
                    continue
                if is_transient:
                    print(f"Rate-limited/transient on {stock} after {max_retries} retries")
                else:
                    print(f"Error fetching {stock}: {e}")
                return pd.DataFrame(data={'symbol': [stock], 'dividend_ttm': [None]})

    results = list(_POOL.map(_div_one, stocks))
    div_ttm = pd.concat(results, ignore_index=True) if results else pd.DataFrame(columns=["symbol", "dividend_ttm"])

    upsert_db(div_ttm, supabase, country)

def earnings_fetcher(ticker, currency, stock, country,resp):

    CURRENCY_OVERRIDES = {
        'TCPD': 'THB', 'TATD': 'THB', 'TPED': 'THB',
    }

    def get_data_currency(ticker, stock, country):
        if stock in CURRENCY_OVERRIDES:
            return CURRENCY_OVERRIDES[stock]
        try:
            data_currency = ticker.info["financialCurrency"]
        except (KeyError, TypeError, ValueError) as e:
            print(f"Error fetching financialCurrency: {e}. Trying secondary method.")
            try:
                data_currency = ticker.info["currency"]
            except (KeyError, TypeError, ValueError) as e2:
                print(f"Error fetching currency: {e2}. Using default currency based on country.")
                data_currency = "SGD" if country == "SG" else "MYR"
                print(f"Defaulting to {data_currency} for country {country}.")
        return data_currency

    def extract_financials(ticker):
        # Network errors (429/HTTPError) from ticker.financials MUST propagate
        # so _hist_one's retry re-fetches; only missing-data falls back to NaN.
        try:
            yearly_financials = ticker.financials.loc[["Total Revenue", "Net Income"]]
            yearly_financials = yearly_financials.T
            yearly_financials.index = pd.to_datetime(yearly_financials.index).year
            yearly_financials = yearly_financials.reset_index()
            yearly_financials.columns = ['period', 'revenue', 'earnings']
            yearly_financials['period'] = yearly_financials['period'].astype(int)

            last_financial = yearly_financials[yearly_financials.period >= datetime.now().year-2].iloc[0:1, :]

            try:
                quarterly_rev = ticker.quarterly_financials.loc["Total Revenue"][0:4]
                quarterly_net = ticker.quarterly_financials.loc["Net Income"][0:4]

                ttm_net_income = pd.DataFrame(data={
                    'period': 'TTM',
                    'revenue': quarterly_rev.sum(),
                    'earnings': quarterly_net.sum()
                }, index=[0])
            except (KeyError, TypeError, ValueError) as e:
                print(f"Error computing TTM financials from quarterly data for stock {stock}: {e}. Using latest yearly data as TTM.")
                ttm_net_income = last_financial.copy()
                ttm_net_income['period'] = 'TTM'

            financial_all = pd.concat([ttm_net_income, yearly_financials], ignore_index=True)

            return financial_all, last_financial
        except (KeyError, TypeError, ValueError) as e:
            print(f"No Net Income and Revenue data for ticker {ticker}: {e}")
            return np.nan, np.nan

    data_currency = get_data_currency(ticker, stock, country)

    conversion_rate = 1.0
    if data_currency != currency:
        try:
            conversion_rate = resp[data_currency][currency]
        except (KeyError, TypeError, ValueError) as e:
            print(f"Error finding conversion rate: {e}. Using default conversion rate of 1.0")

    financial_all, last_financial = extract_financials(ticker)
    if isinstance(financial_all, pd.DataFrame):
        if conversion_rate != 1.0:
            financial_all['earnings'] = financial_all['earnings'] * conversion_rate
            financial_all['revenue'] = financial_all['revenue'] * conversion_rate
            last_financial["earnings"] = last_financial["earnings"] * conversion_rate
            last_financial["revenue"] = last_financial["revenue"] * conversion_rate

        net_income_json = financial_all[["period", 'earnings']].to_json(orient='records')
        revenue_json = financial_all[["period", 'revenue']].to_json(orient='records')
    else:
        net_income_json = revenue_json = np.nan

    return net_income_json, revenue_json, last_financial

def update_historical_data(country, country_data, supabase, resp):
    df_earnings = pd.DataFrame()

    def _hist_one(stock):
        try:
            # Retry fetch (HTTP) with exponential backoff.
            net_income = revenue = last_data = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    ticker = yf.Ticker(f"{bare_symbol(stock)}.SI") if country == "SG" else yf.Ticker(f"{bare_symbol(stock)}.KL")
                    currency = "SGD" if country == "SG" else "MYR"
                    net_income, revenue, last_data = earnings_fetcher(ticker, currency, stock, country, resp)
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    time.sleep(2 ** (attempt - 1))

            data = pd.DataFrame(data={'symbol': stock, 'historical_earnings': net_income, 'historical_revenue': revenue}, index=[0])
            # Attach last-year row directly: merge on symbol is fragile (dtype
            # mismatch / NaN key silently drops the row when financials fail).
            if isinstance(last_data, pd.DataFrame) and last_data.shape[0] > 0:
                last = last_data.iloc[0]
                data["revenue"] = last.get("revenue")
                data["earnings"] = last.get("earnings")
            else:
                data["revenue"] = np.nan
                data["earnings"] = np.nan
            return data
        except Exception as e:
            print(f"Error fetching historical data for {stock}: {e}")
            # NaN row -> upsert_db skips it (DB keeps its stored value).
            return pd.DataFrame(data={'symbol': stock, 'historical_earnings': np.nan,
                                      'historical_revenue': np.nan, 'revenue': np.nan,
                                      'earnings': np.nan}, index=[0])

    results = list(_POOL.map(_hist_one, country_data.symbol.unique()))
    df_earnings = pd.concat(results, ignore_index=True) if results else df_earnings

    upsert_db(df_earnings, supabase, country)


def fetch_highlight_data(stock, currency, country_code, resp):
    row_list = [stock]

    ticker = yf.Ticker(f"{bare_symbol(stock)}.{country_code}")

    # Network errors (429/HTTPError) from ticker.info MUST propagate so the
    # caller's retry re-fetches; only missing keys are caught and defaulted.
    data_currency = ticker.info.get('currency', currency)

    try:
        dividend = ticker.info['dividendRate']

        if data_currency != currency:
            curr_value = resp[data_currency][currency]
            dividend = dividend * curr_value
    except (KeyError, TypeError, ValueError):
        try:
            last_dividend_date = datetime.utcfromtimestamp(ticker.info['lastDividendDate']).year
        except (KeyError, TypeError, ValueError):
            last_dividend_date = np.nan

        if np.isnan(last_dividend_date):
            dividend = np.nan
        elif last_dividend_date < datetime.now().year:
            dividend = 0
        else:
            dividend = np.nan

    row_list.append(dividend)

    try:
        dividend_yield = ticker.info['dividendYield'] / 100
    except (KeyError, TypeError, ValueError):
        if np.isnan(dividend):
            dividend_yield = np.nan
        elif dividend == 0:
            dividend_yield = 0
        else:
            dividend_yield = np.nan

    row_list.append(dividend_yield)

    for metrics in ['profitMargins', "operatingMargins", "grossMargins", "quickRatio", "currentRatio", "debtToEquity", "payoutRatio", "trailingEps"]:
        try:
            metrics_value = ticker.info[metrics]
        except (KeyError, TypeError, ValueError):
            metrics_value = np.nan

        if metrics == "debtToEquity":
            metrics_value = metrics_value / 100

        row_list.append(metrics_value)

    data = pd.DataFrame([row_list])
    data.columns = ['symbol', 'forward_dividend', 'forward_dividend_yield', 'net_profit_margin',
                    "operating_margin", "gross_margin", "quick_ratio", "current_ratio",
                    "debt_to_equity", "payout_ratio", "eps"]

    return data

def update_financial_data(country, country_data, supabase,resp):
    highlight_data = pd.DataFrame()

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"
    else:
        return

    stocks = country_data.symbol.unique()

    def _fin_one(stock):
        # Retry fetch (HTTP) with exponential backoff; log final failure.
        last_err = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return fetch_highlight_data(stock, curr, symbol, resp)
            except Exception as e:
                last_err = e
                if attempt < _MAX_RETRIES:
                    time.sleep(2 ** (attempt - 1))
        print(f"Failed to fetch highlight for {stock} after {_MAX_RETRIES} retries: {last_err}")
        return None

    results = [r for r in _POOL.map(_fin_one, stocks) if r is not None]
    highlight_data = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    upsert_db(highlight_data, supabase, country)


def main():
    parser = argparse.ArgumentParser(description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument('country', type=str, help='Specify the Country Type the Pipeline will be ran')
    parser.add_argument("fetch_type", type=str, help='Specify Pipeline Period (Weekly/monthly/historical)')
    parser.add_argument('--specific', nargs='+', help='Only process specific symbols (e.g. TCPD TATD)')

    args = parser.parse_args()

    if args.country not in ['SG','MY']:
        raise ValueError("Please Specify Country Code Between SG and MY")

    if args.fetch_type not in ['weekly','monthly', 'historical']:
        raise ValueError("Please Specify Fetch Type Between weekly, monthly, and historical")

    print("Attempting to load conversion rates from quarterly_rates.json...")
    try:
        with open('quarterly_rates.json', 'r') as f:
            quarterly = json.load(f)
        latest_quarter = max(quarterly['quarters'].keys())
        resp = quarterly['quarters'][latest_quarter]
        print(f"...Success! Using rates from latest quarter: {latest_quarter}")
    except Exception as e:
        print(f"...Warning: Could not load quarterly_rates.json ({e}). Falling back to live URL.")
        resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json').json()
        print("...Successfully fetched current rates from URL as a fallback.")

    load_dotenv()
    url_supabase = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)

    country_data = fetch_existing_symbol(args.country,supabase)

    if args.specific:
        country_data = country_data[country_data['symbol'].isin(args.specific)]
        print(f"Filtering to specific symbols: {args.specific}")

    if args.fetch_type == "weekly":
        update_div_ttm(args.country,country_data,supabase,resp)
    elif args.fetch_type == "monthly":
        update_financial_data(args.country,country_data,supabase,resp)
    elif args.fetch_type == "historical":
        update_historical_data(args.country, country_data, supabase,resp)

if __name__ == "__main__":
    main()
