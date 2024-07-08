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

def fetch_existing_symbol(country,supabase):
    if country == "SG":
        data = supabase.table("sgx_companies").select("symbol").execute()
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

        for df in [df_not_na,df_na]: 
            for ticker in df.symbol.unique():
                try:
                    supabase.table(table).update(
                        {i: df[df.symbol == ticker].iloc[0][i]}
                    ).eq("symbol", ticker).execute()
                except:
                    print(f"Failed to update {i} for {ticker}.")
        
        print(f"Finish updating data for column {i}")

# Dividend TTM Function
def fetch_div_ttm(stock, currency, symbol,curr):
    try:
        ticker = yf.Ticker(f"{stock}.{symbol}")

        div = pd.DataFrame(ticker.dividends).reset_index()
        div.columns = div.columns.str.lower()

        div['date'] = pd.to_datetime(div['date'])

        div_rate = div[div.date >= datetime.now(pytz.timezone('Asia/Singapore')) - timedelta(days = 365)].dividends.sum()

        data_currency = ticker.info['currency']

        if data_currency != curr:
            resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json')
            resp = resp.json()
            curr_value = resp[data_currency][currency]

            div_rate = div_rate * curr_value
    except:
        div_rate = 0

    div_ttm = pd.DataFrame(data={'symbol':stock, 'dividend_ttm':div_rate}, index=[0])

    return div_ttm

def update_div_ttm(country,country_data,supabase):
    div_ttm = pd.DataFrame()

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"

    for stock in country_data.symbol.unique():
        
        print(f"start fetching data {stock}")
        data = fetch_div_ttm(stock,curr,symbol,curr)

        div_ttm = pd.concat([div_ttm,data])

        print(f"Succes get data for {stock}")

        print(f"---------------------------------------------------------")

    upsert_db(div_ttm,supabase,country)

# Monthly Financial Data Function
def fetch_highlight_data(stock, currency, country_code):
    row_list = [stock]

    ticker = yf.Ticker(f"{stock}.{country_code}")

    try:
        data_currency = ticker.info['currency']

        try:
            dividend = ticker.info['dividendRate']    

            if data_currency != currency:
                resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json')
                resp = resp.json()
                curr_value = resp[data_currency][currency]

                dividend = dividend * curr_value
        except:

            try:
                last_dividend_date = datetime.utcfromtimestamp(ticker.info['lastDividendDate']).year
            except:
                last_dividend_date = np.nan

            if np.isnan(last_dividend_date):
                dividend = np.nan
            elif last_dividend_date < datetime.now().year:  
                dividend = 0
            else:
                dividend = np.nan
                
            print(f"{stock} doesn't have any data for forward dividend")
        
        row_list.append(dividend)

        try:
            dividend_yield = ticker.info['dividendYield']
        except:
            if np.isnan(dividend):
                dividend_yield = np.nan
            elif dividend == 0:    
                dividend_yield = 0
            
            print(f"{stock} doesn't have any data for forward dividend_yield")
        
        row_list.append(dividend_yield)
        
        for metrics in ['profitMargins',"operatingMargins","grossMargins","quickRatio","currentRatio","debtToEquity","payoutRatio","trailingEps"]:
            try:
                metrics_value = ticker.info[metrics]
            except:
                metrics_value = np.nan
                print(f"{stock} doesn't have any data for {metrics}")
            
            if metrics == "debtToEquity":
                metrics_value = metrics_value/100
                
            row_list.append(metrics_value)

        data = pd.DataFrame(row_list).T
        data.columns = ['symbol','forward_dividend','forward_dividend_yield','net_profit_margin',"operating_margin","gross_margin","quick_ratio","current_ratio","debt_to_equity","payout_ratio","eps"]
    except:
        data = pd.DataFrame([stock,None,None,None,None,None,None,None,None,None,None]).T
        data.columns = ['symbol','forward_dividend','forward_dividend_yield','net_profit_margin',"operating_margin","gross_margin","quick_ratio","current_ratio","debt_to_equity","payout_ratio","eps"]

    return data

def update_financial_data(country,country_data,supabase):
    highlight_data = pd.DataFrame()

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"

    for stock in country_data.symbol.unique():
        
        print(f"start fetching data {stock}")
        data = fetch_highlight_data(stock,curr,symbol)

        highlight_data = pd.concat([highlight_data,data])

        print(f"Succes get data for {stock}")

        print(f"---------------------------------------------------------")
    
    upsert_db(highlight_data,supabase,country)
    

def main():
    parser = argparse.ArgumentParser(description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument('country', type=str, help='Specify the Country Type the Pipeline will be ran')
    parser.add_argument("fetch_type", type=str, help='Specify Pipeline Period (Weekly/Monthly)')

    args = parser.parse_args()

    if args.country not in ['SG','MY']:
        raise ValueError("Please Specify Country Code Between SG and MY")
    
    if args.fetch_type not in ['weekly','monthly']:
        raise ValueError("Please Specify Fetch Type Between weekly and monthly")
    
    # Specify DB Credentials
    load_dotenv()
    url_supabase = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)

    # Take Existing symbol From DB
    country_data = fetch_existing_symbol(args.country,supabase)
    country_data = country_data.iloc[0:5,:]

    if args.fetch_type == "weekly":
        update_div_ttm(args.country,country_data,supabase)
    elif args.fetch_type == "monthly":
        update_financial_data(args.country,country_data,supabase)

if __name__ == "__main__":
    main()


