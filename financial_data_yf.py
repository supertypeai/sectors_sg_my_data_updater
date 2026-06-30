import pandas as pd
import yfinance as yf
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
from random import uniform

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

    for i in update_data.columns.drop('symbol'):
        df = update_data[["symbol",i]]

        df_na = df[df[i].isna()]
        df_na[i] = None

        df_not_na = df[~df[i].isna()]

        if i in ['historical_earnings','historical_revenue']:
            df_not_na[i] = df_not_na[i].apply(json.loads)

        for df in [df_not_na,df_na]:
            for ticker in df.symbol.unique():
                try:
                    supabase.table(table).update(
                        {i: df[df.symbol == ticker].iloc[0][i]}
                    ).eq("symbol", ticker).execute()
                except:
                    print(f"Failed to update {i} for {ticker}.")

        print(f"Finish updating data for column {i}")

def fetch_div_ttm(stock, currency, symbol, curr,resp):
    try:
        ticker = yf.Ticker(f"{stock}.{symbol}")

        div = pd.DataFrame(ticker.dividends).reset_index()
        div.columns = div.columns.str.lower()
        div['date'] = pd.to_datetime(div['date'])

        one_year_ago = datetime.now(pytz.timezone('Asia/Singapore')) - timedelta(days=365)
        recent_dividends = div[div.date >= one_year_ago]
        div_rate = recent_dividends['dividends'].sum()

        data_currency = ticker.info.get('currency', None)

        if data_currency != curr:
            if data_currency in resp and currency in resp[data_currency]:
                curr_value = resp[data_currency][currency]
                div_rate = div_rate * curr_value

    except Exception as e:
        div_rate = 0

    if div_rate == 0:
        div_rate = None
    div_ttm = pd.DataFrame(data={'symbol': stock, 'dividend_ttm': div_rate}, index=[0])

    return div_ttm

def update_div_ttm(country, country_data, supabase, resp):
    div_ttm = pd.DataFrame()
    base_delay = 2
    max_delay = 60
    current_delay = base_delay

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"

    stocks = country_data.symbol.unique()

    for stock in stocks:
        retry_count = 0
        max_retries = 3
        success = False

        while not success and retry_count < max_retries:
            try:
                data = fetch_div_ttm(stock, curr, symbol, curr,resp)
                div_ttm = pd.concat([div_ttm, data], ignore_index=True)
                success = True
                current_delay = base_delay

                sleep_time = uniform(1, 3)
                time.sleep(sleep_time)

            except Exception as e:
                retry_count += 1
                error_message = str(e).lower()

                if "rate limit" in error_message:
                    if retry_count < max_retries:
                        time.sleep(current_delay)
                        current_delay = min(current_delay * 2, max_delay)
                    continue
                else:
                    print(f"Error fetching {stock}: {e}")
                    break

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
        except Exception as e:
            print(f"Error fetching financialCurrency: {e}. Trying secondary method.")
            try:
                data_currency = ticker.info["currency"]
            except Exception as e2:
                print(f"Error fetching currency: {e2}. Using default currency based on country.")
                data_currency = "SGD" if country == "SG" else "MYR"
                print(f"Defaulting to {data_currency} for country {country}.")
        return data_currency

    def extract_financials(ticker):
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
            except Exception as e:
                print(f"Error computing TTM financials from quarterly data for stock {stock}: {e}. Using latest yearly data as TTM.")
                ttm_net_income = last_financial.copy()
                ttm_net_income['period'] = 'TTM'

            financial_all = pd.concat([ttm_net_income, yearly_financials], ignore_index=True)

            return financial_all, last_financial
        except Exception as e:
            print(f"No Net Income and Revenue data for ticker {ticker}: {e}")
            return np.nan, np.nan

    data_currency = get_data_currency(ticker, stock, country)

    conversion_rate = 1.0
    if data_currency != currency:
        try:
            conversion_rate = resp[data_currency][currency]
        except Exception as e:
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

def update_historical_data(country, country_data, supabase,resp):
    df_earnings = pd.DataFrame()

    for stock in country_data.symbol.unique():

        ticker = yf.Ticker(f"{stock}.SI") if country == "SG" else yf.Ticker(f"{stock}.KL")

        currency = "SGD" if country == "SG" else "MYR"

        net_income,revenue, last_data = earnings_fetcher(ticker,currency, stock, country,resp)

        if type(last_data) == float:
            last_data = pd.DataFrame(data={'symbol':np.nan, "period": np.nan,'earnings':np.nan,'revenue':np.nan}, index=[0])
        elif last_data.shape[0] == 0:
            nan_row = pd.DataFrame([[np.nan]*last_data.shape[1]], columns=last_data.columns)
            last_data = pd.concat([last_data, nan_row], ignore_index=True)

        last_data["symbol"] = stock

        data = pd.DataFrame(data={'symbol':stock, 'historical_earnings':net_income,'historical_revenue':revenue}, index=[0])

        data = data.merge(last_data,on="symbol").drop('period',axis=1)

        df_earnings = pd.concat([df_earnings,data])

    upsert_db(df_earnings,supabase,country)


def fetch_highlight_data(stock, currency, country_code,resp):
    row_list = [stock]

    ticker = yf.Ticker(f"{stock}.{country_code}")

    try:
        data_currency = ticker.info['currency']

        try:
            dividend = ticker.info['dividendRate']

            if data_currency != currency:
                curr_value = resp[data_currency][currency]
                dividend = dividend * curr_value
        except Exception as e:
            try:
                last_dividend_date = datetime.utcfromtimestamp(ticker.info['lastDividendDate']).year
            except Exception as le:
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
        except Exception as de:
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
            except Exception as me:
                metrics_value = np.nan

            if metrics == "debtToEquity":
                metrics_value = metrics_value / 100

            row_list.append(metrics_value)

        data = pd.DataFrame([row_list])
        data.columns = ['symbol', 'forward_dividend', 'forward_dividend_yield', 'net_profit_margin',
                        "operating_margin", "gross_margin", "quick_ratio", "current_ratio",
                        "debt_to_equity", "payout_ratio", "eps"]
    except Exception as main_e:
        data = pd.DataFrame([[stock, None, None, None, None, None, None, None, None, None, None]])
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

    for stock in stocks:
        try:
            data = fetch_highlight_data(stock, curr, symbol,resp)
        except Exception as e:
            continue

        highlight_data = pd.concat([highlight_data, data], ignore_index=True)

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
