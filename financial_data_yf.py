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

resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json')
resp = resp.json()
print("resp: ", resp)

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
            curr_value = resp[data_currency][currency]

            div_rate = div_rate * curr_value
    except Exception as e:
        print(f"{stock} failed to retrieve and will be filled by 0 the error message:", e)
        div_rate = 0

    div_ttm = pd.DataFrame(data={'symbol':stock, 'dividend_ttm':div_rate}, index=[0])

    return div_ttm

def update_div_ttm(country,country_data,supabase):
    div_ttm = pd.DataFrame()
    base_delay = 2  # Base delay in seconds
    max_delay = 60  # Maximum delay in seconds
    current_delay = base_delay

    if country == "SG":
        curr = "SGD"
        symbol = "SI"
    elif country == "MY":
        curr = "MYR"
        symbol = "KL"

    for stock in country_data.symbol.unique():
        retry_count = 0
        max_retries = 3
        success = False
        
        while not success and retry_count < max_retries:
            try:
                print(f"start fetching data {stock}")
                data = fetch_div_ttm(stock,curr,symbol,curr)
                div_ttm = pd.concat([div_ttm,data])
                print(f"Success get data for {stock}")
                success = True
                current_delay = base_delay  # Reset delay on success
                
                # Add a small random delay between successful requests
                time.sleep(uniform(1, 3))
                
            except Exception as e:
                retry_count += 1
                if "rate limit" in str(e).lower():
                    print(f"Rate limit hit for {stock}, attempt {retry_count} of {max_retries}")
                    if retry_count < max_retries:
                        print(f"Waiting {current_delay} seconds before retrying...")
                        time.sleep(current_delay)
                        current_delay = min(current_delay * 2, max_delay)  # Exponential backoff
                    continue
                else:
                    print(f"Error fetching {stock}: {e}")
                    break
                    
        print(f"---------------------------------------------------------")

    upsert_db(div_ttm,supabase,country)

def earnings_fetcher(ticker,currency,stock, country):
   try:
      data_currency = ticker.info["financialCurrency"]
   except:
      if stock in ['TCPD','TPED','TATD']:
         data_currency = "THB"
      else:
         try:
            data_currency = ticker.info["currency"]
         except:
            print(stock, " don't have any currency and will use SGD for sgx and MYR for KLSE as the default")
            data_currency = "SGD" if country == "SG" else "MYR"
      
   if data_currency == currency:
      # Take the yearly net income value
      try:
         yearly_financials = ticker.financials.loc[["Total Revenue","Net Income"]]
         yearly_financials = yearly_financials.T
         yearly_financials.index = pd.to_datetime(yearly_financials.index).year
         yearly_financials = pd.DataFrame(yearly_financials).reset_index()
         yearly_financials.columns = ['period', 'revenue','earnings']

         yearly_financials['period'] = yearly_financials['period'].astype('int')

         last_financial = yearly_financials[yearly_financials.period >= datetime.now().year-2].iloc[0:1,:]

         # TTM Net Income
         try:
            ttm_net_income = pd.DataFrame(data={'period':'TTM', 'revenue':ticker.quarterly_financials.loc["Total Revenue"][0:4].sum(),'earnings':ticker.quarterly_financials.loc["Net Income"][0:4].sum()}, index=[0])
         except:
            ttm_net_income = pd.DataFrame(data={'period':'TTM', 'revenue':np.nan, 'earnings':np.nan}, index=[0])
         
         financial_all = pd.concat([ttm_net_income,yearly_financials])
         
         # Convert to JSON
         net_income = financial_all[["period",'earnings']].to_json(orient='records')
         revenue = financial_all[["period",'revenue']].to_json(orient='records')

      except:
         net_income = np.nan
         revenue = np.nan
         last_financial = np.nan
         print(f"No Net Income and Revenue data for ticker {ticker}.SI")

   else:
    #   resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json')
    #   resp = resp.json()
      curr_value = resp[data_currency][currency]

      try:
         yearly_financials = ticker.financials.loc[["Total Revenue","Net Income"]]
         yearly_financials = yearly_financials.T
         yearly_financials.index = pd.to_datetime(yearly_financials.index).year
         yearly_financials = pd.DataFrame(yearly_financials).reset_index()
         yearly_financials.columns = ['period', 'revenue','earnings']

         last_financial = yearly_financials[yearly_financials.period >= datetime.now().year-2].iloc[0:1,:]
         last_financial["earnings"] = last_financial["earnings"] * curr_value
         last_financial["revenue"] = last_financial["revenue"] * curr_value

         # TTM Net Income
         try:
            ttm_net_income = pd.DataFrame(data={'period':'TTM', 'revenue':ticker.quarterly_financials.loc["Total Revenue"][0:4].sum(),'earnings':ticker.quarterly_financials.loc["Net Income"][0:4].sum()}, index=[0])
         except:
            ttm_net_income = pd.DataFrame(data={'period':'TTM', 'revenue':np.nan, 'earnings':np.nan}, index=[0])
         
         financial_all = pd.concat([ttm_net_income,yearly_financials])

         financial_all['earnings'] = financial_all['earnings'] * curr_value
         financial_all['revenue'] = financial_all['revenue'] * curr_value
         
         # Convert to JSON
         net_income = financial_all[["period",'earnings']].to_json(orient='records')
         revenue = financial_all[["period",'revenue']].to_json(orient='records')
         
      except:
         net_income = np.nan
         revenue = np.nan
         last_financial = np.nan
         print(f"No Net Income and Revenue data for ticker {ticker}.SI")

   
   return net_income, revenue, last_financial

def update_historical_data(country, country_data, supabase):
    df_earnings = pd.DataFrame()

    for stock in country_data.symbol.unique():
        
        ticker = yf.Ticker(f"{stock}.SI") if country == "SG" else yf.Ticker(f"{stock}.KL")

        currency = "SGD" if country == "SG" else "MYR"

        net_income,revenue, last_data = earnings_fetcher(ticker,currency, stock, country)

        if type(last_data) == float:
            last_data = pd.DataFrame(data={'symbol':np.nan, "period": np.nan,'earnings':np.nan,'revenue':np.nan}, index=[0])
        elif last_data.shape[0] == 0:
            nan_row = pd.DataFrame([[np.nan]*last_data.shape[1]], columns=last_data.columns)
            last_data = pd.concat([last_data, nan_row], ignore_index=True)

        last_data["symbol"] = stock

        data = pd.DataFrame(data={'symbol':stock, 'historical_earnings':net_income,'historical_revenue':revenue}, index=[0])

        data = data.merge(last_data,on="symbol").drop('period',axis=1)

        df_earnings = pd.concat([df_earnings,data])

        print(f"success get {ticker} earnings")

    upsert_db(df_earnings,supabase,country)


# Monthly Financial Data Function
def fetch_highlight_data(stock, currency, country_code):
    row_list = [stock]

    ticker = yf.Ticker(f"{stock}.{country_code}")

    try:
        data_currency = ticker.info['currency']

        try:
            dividend = ticker.info['dividendRate']    

            if data_currency != currency:
                # resp = requests.get('https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json')
                # resp = resp.json()
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
    parser.add_argument("fetch_type", type=str, help='Specify Pipeline Period (Weekly/monthly/historical)')

    args = parser.parse_args()

    if args.country not in ['SG','MY']:
        raise ValueError("Please Specify Country Code Between SG and MY")
    
    if args.fetch_type not in ['weekly','monthly', 'historical']:
        raise ValueError("Please Specify Fetch Type Between weekly, monthly, and historical")
    
    # Specify DB Credentials
    load_dotenv()
    url_supabase = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)

    # Take Existing symbol From DB
    country_data = fetch_existing_symbol(args.country,supabase)

    if args.fetch_type == "weekly":
        update_div_ttm(args.country,country_data,supabase)
    elif args.fetch_type == "monthly":
        update_financial_data(args.country,country_data,supabase)
    elif args.fetch_type == "historical":
        update_historical_data(args.country, country_data, supabase)

if __name__ == "__main__":
    main()
