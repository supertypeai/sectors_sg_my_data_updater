import argparse
import datetime
import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from supabase import create_client

import yf_custom as yf


def recursively_clean_nans(obj):
    """
    Recursively traverses a nested object (dict, list) and replaces any
    NaN-like values (e.g., np.nan, pd.NA) with None.
    """
    if isinstance(obj, dict):
        return {k: recursively_clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursively_clean_nans(e) for e in obj]
    # Use pd.isna for robust NaN checking (handles np.nan, None, pd.NaT, etc.)
    if pd.isna(obj):
        return None
    return obj

def safe_relative_diff(num1: float, num2: float):
    # if both are 0, 0 difference
    if num1 == 0:
        return 0
    # if the divisor is 0, return the number itself
    if num2 == 0:
        return num1
    # else return the relative difference
    return (num1 / num2) - 1


def GetGeneralData(country):
    if country == "sg":
        # url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
        url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    elif country == "my":
        url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=42&filter-domain=&page=0&page-size=2000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    data_from_api = None
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        html = response.read()

    data_from_api = json.loads(html)
    data_from_api = pd.DataFrame(data_from_api["data"])

    # for i in range(10):
    #     response = requests.get(url, headers=headers)

    #     if response.status_code == 200:
    #         json_data = response.json()
    #         data_from_api = pd.DataFrame(json_data["data"])
    #         break
    return data_from_api


def yf_data_updater(data_prep: pd.DataFrame, country):
    """
    Updates financial fundamentals (market cap, volume, PE, etc.) for each symbol.
    Includes debug prints to trace values and identify NoneType errors.
    """

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        # print(f"\n--- Processing symbol: {symbol} (row {index}) ---")
        try:
            # Determine ticker extension and fetch info
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(symbol + ticker_extension)
            info = ticker.info

            currency_info = info.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"
            currency = currency_info or row.get("currency")
            # print(f"Currency from info: {currency_info} -> using currency: {currency}")

            # Fetch conversion rate if needed
            if currency and currency != country_currency:
                rate = data.get(currency, {}).get(country_currency)
                # print(f"Lookup rate {currency}->{country_currency}: {rate}")
                if rate is None:
                    # print(f"Warning: no conversion rate for {currency}->{country_currency}. Skipping currency adjustment.")
                    continue
                else:
                    rate = float(rate)

            # Data we will update
            desired_values = {
                "marketCap": "market_cap",
                "volume": "volume",
                "trailingPE": "pe",
                "priceToSalesTrailing12Months": "ps_ttm",
                "priceToBook": "pb",
                "beta": "beta",
                "operatingCashflow": "ocf",
                "fiveYearAvgDividendYield": "dividend_yield_5y_avg"
            }
            if country == "sg":
                desired_values["shortName"] = "short_name"

            # Iterate desired keys
            for key_dv, col in desired_values.items():
                try:
                    raw_val = info.get(key_dv, np.nan)

                    if col == "market_cap":
                        # print(f"Raw marketCap: {raw_val}")
                        if raw_val is not None and raw_val is not np.nan:
                            if currency and currency != country_currency and rate is not None:
                                adjusted = raw_val * rate
                                # print(f"Adjusted market_cap = {raw_val} * {rate} = {adjusted}")
                                data_prep.at[index, col] = adjusted
                            else:
                                data_prep.at[index, col] = raw_val
                        else:
                            # print(f"marketCap missing for {symbol}")
                            data_prep.at[index, col] = np.nan

                    elif col == "ocf":
                        ocf_val = raw_val
                        # print(f"Raw operatingCashflow: {ocf_val}")
                        if ocf_val not in [None, 0, np.nan]:
                            mcap = info.get("marketCap")
                            # print(f"Using marketCap {mcap} / ocf {ocf_val}")
                            data_prep.at[index, "pcf"] = mcap / ocf_val
                        else:
                            data_prep.at[index, "pcf"] = np.nan

                    elif col == "pe":
                        yf_pe = raw_val
                        # print(f"Raw trailingPE: {yf_pe} (type {type(yf_pe)})")
                        # Convert string anomalies
                        if isinstance(yf_pe, str):
                            pe_str = yf_pe.strip().lower()
                            if pe_str in ["none", "nan"]:
                                yf_pe = np.nan
                            elif pe_str in ["inf", "infinity"]:
                                yf_pe = float('inf')
                            else:
                                try:
                                    yf_pe = float(yf_pe)
                                except Exception:
                                    yf_pe = np.nan
                            # print(f"Converted trailingPE to float: {yf_pe}")

                        if not pd.isna(yf_pe) and np.isfinite(yf_pe):
                            # print(f"Using valid trailingPE: {yf_pe}")
                            data_prep.at[index, col] = yf_pe
                        else:
                            # print(f"Invalid trailingPE, falling back to close/EPS for {symbol}")
                            close_list = row.get("close", [])
                            last_close = None
                            if isinstance(close_list, list) and close_list:
                                last_close = close_list[-1].get("close")
                            eps = row.get("eps")
                            # print(f"last_close: {last_close}, eps: {eps}")
                            if last_close is not None and eps:
                                fallback = last_close / eps
                                # print(f"Fallback PE = {last_close} / {eps} = {fallback}")
                                data_prep.at[index, col] = fallback
                            else:
                                # print(f"Cannot compute fallback PE for {symbol}")
                                data_prep.at[index, col] = np.nan

                    else:
                        # print(f"Setting {col} = {raw_val}")
                        data_prep.at[index, col] = raw_val

                except KeyError:
                    # print(f"KeyError for {key_dv} on symbol {symbol}")
                    if col == "pe":
                        # Fallback as above
                        close_list = row.get("close", [])
                        last_close = close_list[-1].get("close") if isinstance(close_list, list) and close_list else None
                        eps = row.get("eps")
                        # print(f"Fallback (KeyError) last_close: {last_close}, eps: {eps}")
                        data_prep.at[index, col] = (last_close / eps) if (last_close and eps) else np.nan
                    else:
                        data_prep.at[index, col] = np.nan

        except Exception as e:
            print(f"Error updating symbol {symbol}: {e}")

    # Drop temporary ocf if exists
    if "ocf" in data_prep.columns:
        data_prep = data_prep.drop(columns=["ocf"])

    return data_prep

def update_dividend_growth_rate(data_prep: pd.DataFrame, country):
    """
    Updates dividend growth rate for each symbol and assigns it to the DataFrame.
    """

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(symbol + ticker_extension)
            
            current_year = datetime.now().year
            dividend_last_1_year = ticker.history(
                start=f"{current_year - 1}-01-01", 
                end=f"{current_year - 1}-12-31"
            )["Dividends"].sum()
            dividend_current = ticker.history(
                start=f"{current_year}-01-01", 
                end=f"{current_year}-12-31"
            )["Dividends"].sum()
            dividend_growth_rate = safe_relative_diff(dividend_current, dividend_last_1_year)
            data_prep.loc[index, "dividend_growth_rate"] = dividend_growth_rate

        except Exception as e:
            print(f"error updating dividend growth rate for symbol {symbol} : ", e)
    
    return data_prep

def update_close_history_data(data_prep: pd.DataFrame, country):
    """
    This function updates the history close data for near-future values and assigns 
    it to the 'close' column of the data_prep DataFrame.
    """
    date_format = "%Y-%m-%d"
    # Set the start date as 31 days ago and then format it
    last_date = (datetime.now() - timedelta(days=31)).strftime(date_format)

    # Generate a list of dates for the next 31 days
    list_dates = [
        (datetime.strptime(last_date, date_format) + timedelta(days=i)).strftime(date_format)
        for i in range(1, 32)
    ]
    
    new_close = []
    
    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            # Get ticker with proper extension
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)
            currency_info = ticker.info.get("currency", None)
            currency = currency_info or row.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"

            # Retrieve ticker history
            try:
                yf_data = ticker.history(period="1mo").reset_index()
            except Exception as e:
                yf_data = ticker.history(period="max").reset_index()
            
            close_data = []
            for i in range(len(yf_data)):
                curr = yf_data.iloc[i]
                curr_date = curr["Date"].strftime(date_format)
                if curr_date in list_dates:
                    curr_close = float(curr["Close"])
                    # Convert currency if necessary
                    if currency != country_currency:
                        rate = float(data[currency][country_currency])
                        curr_close = curr_close * rate
                    close_data.append({
                        "date": curr_date,
                        "close": curr_close if np.isfinite(curr_close) else None
                    })
                    
            # Filter close data to only dates later than last_date
            close_data = [close for close in close_data if close["date"] > last_date]
            new_close.append(close_data if close_data else row["close"])
        except Exception as e:
            print(f"error in symbol {symbol} : ", e)
            new_close.append(row["close"])
    
    # Update the DataFrame with the newly fetched close data
    try:
        data_prep = data_prep.assign(close=new_close)
        if "ocf" in data_prep.columns:
            data_prep = data_prep.drop("ocf", axis="columns")
    except Exception as e:
        print(f"[DEBUG] Error assigning close data: {e}")
        data_prep = data_prep.assign(close=new_close)
        print(f"[DEBUG] data_prep after assigning close (unable to drop 'ocf'):\n{data_prep}")
    
    return data_prep

def update_historical_dividends(data_prep: pd.DataFrame, country):
    """
    Update the DataFrame by adding/updating the 'historical_dividends' column.
    The function iterates over each row, retrieves the ticker's full historical data
    to determine the latest close price, and then computes dividend breakdowns by year.
    
    Parameters:
      data_prep (pd.DataFrame): DataFrame containing at least a 'symbol' column.
      country (str): Country code (e.g. "my" or "sg") to determine the ticker extension.
      
    Returns:
      pd.DataFrame: The updated DataFrame with a new column 'historical_dividends'.
    """
    date_format = "%Y-%m-%d"
    # Ensure the 'historical_dividends' column exists and is of type object
    data_prep["historical_dividends"] = None

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            # Get ticker with proper extension
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)
            
            # Fetch full historical data to determine the latest close price
            full_history = ticker.history(period="max").reset_index()
            if full_history.empty:
                raise ValueError("No historical data available")
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)
            latest_close = full_history.iloc[-1]["Close"]
            # print(f"[DEBUG] Latest close for {symbol}: {latest_close}")
            
            # --- Calculate historical_dividends ---
            # print(f"[DEBUG] Processing historical_dividends for symbol: {symbol}")
            dividends_series = ticker.dividends
            if not dividends_series.empty:
                dividends_df = dividends_series.reset_index()
                dividends_df.columns = ["Date", "Dividend"]
                dividends_df["year"] = dividends_df["Date"].dt.year
                # Calculate yield per dividend event using latest_close as the reference price
                dividends_df["yield"] = dividends_df["Dividend"] / latest_close if latest_close else np.nan
                # print(f"[DEBUG] Dividend DataFrame for {symbol}:\n", dividends_df)
                historical_dividends = []
                for year, group in dividends_df.groupby("year"):
                    breakdown = []
                    total_dividend = group["Dividend"].sum()
                    total_yield = total_dividend / latest_close if latest_close else np.nan
                    for _, row_div in group.iterrows():
                        breakdown.append({
                            "date": row_div["Date"].strftime(date_format),
                            "total": row_div["Dividend"],
                            "yield": row_div["yield"]
                        })
                    # print(f"[DEBUG] Year {year}: total_dividend={total_dividend}, total_yield={total_yield}, breakdown={breakdown}")
                    historical_dividends.append({
                        "year": int(year),
                        "breakdown": breakdown,
                        "total_yield": total_yield,
                        "total_dividend": total_dividend
                    })
                # print(f"[DEBUG] Final historical_dividends for {symbol}: {historical_dividends}")
                data_prep.at[index, "historical_dividends"] = historical_dividends
            else:
                # print(f"[DEBUG] No dividend data available for {symbol}")
                # data_prep.at[index, "historical_dividends"] = None
                continue
                
        except Exception as e:
            print(f"[DEBUG] Error processing historical_dividends for {symbol}: {e}")
            # data_prep.at[index, "historical_dividends"] = None
            continue
            
    return data_prep

def update_all_time_price(data_prep: pd.DataFrame, country: str):
    """
    Update the DataFrame by adding/updating the 'all_time_price' column.
    For each ticker, the function fetches historical close prices and computes:
      - YTD low/high: extremes within the current year.
      - 52-week low/high: extremes within the last 365 days.
      - 90-day low/high: extremes within the last 90 days.
      - All-time low/high: extremes over the full available history.
    
    Debug print statements are included to show computed values.
    
    Parameters:
      data_prep (pd.DataFrame): DataFrame containing at least a 'symbol' column.
      country (str): Country code (e.g., "my" or "sg") to determine ticker extension.
    
    Returns:
      pd.DataFrame: The updated DataFrame with a new column 'all_time_price'.
    """
    date_format = "%Y-%m-%d"
    
    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        # Determine ticker extension based on country
        ticker_extension = ".KL" if country.lower() == "my" else ".SI"
        ticker_full = symbol + ticker_extension
        # print(f"[DEBUG] Processing symbol: {ticker_full}")
        
        try:
            # Create ticker object
            ticker = yf.Ticker(ticker_full)
            
            # === Fetch full historical data ===
            full_history = ticker.history(period="max").reset_index()
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)
            
            if full_history.empty:
                raise ValueError("No historical data available")
            
            latest_date = full_history.iloc[-1]["Date"]
            latest_close = full_history.iloc[-1]["Close"]
            # print(f"[DEBUG] Latest date for {ticker_full}: {latest_date.strftime(date_format)}, Latest close: {latest_close}")
            
            # === Compute All-time extremes using .loc with idxmin/idxmax ===
            all_time_low_index = full_history["Close"].idxmin()
            all_time_high_index = full_history["Close"].idxmax()
            all_time_low_row = full_history.loc[all_time_low_index]
            all_time_high_row = full_history.loc[all_time_high_index]
            all_time_low = {"date": all_time_low_row["Date"].strftime(date_format), "price": all_time_low_row["Close"]}
            all_time_high = {"date": all_time_high_row["Date"].strftime(date_format), "price": all_time_high_row["Close"]}
            # print(f"[DEBUG] All-time low for {ticker_full}: {all_time_low}")
            # print(f"[DEBUG] All-time high for {ticker_full}: {all_time_high}")
            
            # === Compute YTD extremes ===
            current_year = datetime.now().year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            if not current_year_data.empty:
                ytd_low_index = current_year_data["Close"].idxmin()
                ytd_high_index = current_year_data["Close"].idxmax()
                ytd_low_row = full_history.loc[ytd_low_index]
                ytd_high_row = full_history.loc[ytd_high_index]
                ytd_low = {"date": ytd_low_row["Date"].strftime(date_format), "price": ytd_low_row["Close"]}
                ytd_high = {"date": ytd_high_row["Date"].strftime(date_format), "price": ytd_high_row["Close"]}
                # print(f"[DEBUG] YTD low for {ticker_full} ({current_year}): {ytd_low}")
                # print(f"[DEBUG] YTD high for {ticker_full} ({current_year}): {ytd_high}")
            else:
                ytd_low = ytd_high = None
                # print(f"[DEBUG] No YTD data available for {ticker_full} in {current_year}")
            
            # === Compute 52-week extremes ===
            start_52w = latest_date - timedelta(days=365)
            data_52w = full_history[full_history["Date"] >= start_52w]
            if not data_52w.empty:
                w52_low_index = data_52w["Close"].idxmin()
                w52_high_index = data_52w["Close"].idxmax()
                w52_low_row = full_history.loc[w52_low_index]
                w52_high_row = full_history.loc[w52_high_index]
                w52_low = {"date": w52_low_row["Date"].strftime(date_format), "price": w52_low_row["Close"]}
                w52_high = {"date": w52_high_row["Date"].strftime(date_format), "price": w52_high_row["Close"]}
                # print(f"[DEBUG] 52-week low for {ticker_full}: {w52_low}")
                # print(f"[DEBUG] 52-week high for {ticker_full}: {w52_high}")
            else:
                w52_low = w52_high = None
                # print(f"[DEBUG] No 52-week data available for {ticker_full}")
            
            # === Compute 90-day extremes ===
            start_90d = latest_date - timedelta(days=90)
            data_90d = full_history[full_history["Date"] >= start_90d]
            if not data_90d.empty:
                d90_low_index = data_90d["Close"].idxmin()
                d90_high_index = data_90d["Close"].idxmax()
                d90_low_row = full_history.loc[d90_low_index]
                d90_high_row = full_history.loc[d90_high_index]
                d90_low = {"date": d90_low_row["Date"].strftime(date_format), "price": d90_low_row["Close"]}
                d90_high = {"date": d90_high_row["Date"].strftime(date_format), "price": d90_high_row["Close"]}
                # print(f"[DEBUG] 90-day low for {ticker_full}: {d90_low}")
                # print(f"[DEBUG] 90-day high for {ticker_full}: {d90_high}")
            else:
                d90_low = d90_high = None
                # print(f"[DEBUG] No 90-day data available for {ticker_full}")
            
            # === Assemble the price extremes dictionary ===
            price_extremes = {
                "ytd_low": ytd_low,
                "ytd_high": ytd_high,
                "52_w_low": w52_low,
                "52_w_high": w52_high,
                "90_d_low": d90_low,
                "90_d_high": d90_high,
                "all_time_low": all_time_low,
                "all_time_high": all_time_high
            }
            
            # json_str = json.dumps(price_extremes)
            # print(f"[DEBUG] Final all_time_price for {ticker_full}: {json_str}")
            data_prep.at[index, "all_time_price"] = price_extremes
            
        except Exception as e:
            print(f"[DEBUG] Error processing all_time_price for {ticker_full}: {e}")
            # data_prep.at[index, "all_time_price"] = None
            continue
            
    return data_prep

def update_change_data(data_prep: pd.DataFrame, country):
    """
    Update the DataFrame by computing change metrics for each symbol/ticker.
    This function retrieves full historical price data and calculates several changes:
      - change_ytd: Change from the first close of the current year.
      - change_1y: Change from the close ~1 year ago.
      - change_3y: Change from the close ~3 years ago.
    
    Parameters:
      data_prep (pd.DataFrame): DataFrame containing at least a 'symbol' column.
      country (str): Country code (e.g., "my" or "sg") to determine the ticker extension.
      
    Returns:
      pd.DataFrame: The updated DataFrame with new columns for each change metric.
    """
    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            # Get ticker with proper extension.
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)
            
            # Fetch full historical data.
            full_history = ticker.history(period="max").reset_index()
            if full_history.empty:
                raise ValueError("No historical data available")
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)
            
            latest_date = full_history.iloc[-1]["Date"]
            latest_close = full_history.iloc[-1]["Close"]
            
            def get_close_on_or_before(target_dt):
                subset = full_history[full_history["Date"] <= target_dt]
                return subset.iloc[-1]["Close"] if not subset.empty else None
            
            # Calculate target dates.
            # YTD: first available close in current year.
            current_year = latest_date.year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            ytd_close = current_year_data.iloc[0]["Close"] if not current_year_data.empty else None
            one_year_target = latest_date - timedelta(days=365)
            three_year_target = latest_date - timedelta(days=3 * 365)
            
            close_1y = get_close_on_or_before(one_year_target)
            close_3y = get_close_on_or_before(three_year_target)

            # Helper for computing percentage change safely.
            def compute_change(latest, past):
                if past is None or past == 0:
                    return np.nan
                return (latest - past) / past
            
            data_prep.loc[index, "change_ytd"]   = compute_change(latest_close, ytd_close)
            data_prep.loc[index, "change_1y"]    = compute_change(latest_close, close_1y)
            data_prep.loc[index, "change_3y"]    = compute_change(latest_close, close_3y)
            
        except Exception as e:
            print(f"[DEBUG] Error calculating change metrics for {symbol}: {e}")
            for metric in ["change_ytd", "change_1y", "change_3y"]:
                data_prep.loc[index, metric] = np.nan
                
    return data_prep

def employee_updater(data_final, country):
    # getting the employee_num from sgx web
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    iv_data_dict = {
        "investing_symbol": [],
        "status": [],
        "employee_num_sgx": []
    }
    yf_data_dict = {
        "investing_symbol": [],
        "employee_num": []
    }
    special_case = {
        'SRTA.SI': 'STA.BK',
        'CERG.SI': '1130.HK',
        'CTDM_p.SI': 'CTDM.SI',
        'TIAN.SI': '600329.SS',
        'UOAL.SI': 'UOS.AX',
        'WLAR.SI': '0854.HK',
        'IHHH.SI': 'IHHH.KL',
        'TOPG.SI': 'TPGC.KL',
        'AVJH.SI': 'AVJ.AX',
        'MYSC.SI': 'MSCB.KL',
        'SHNG.SI': '0069.HK',
        'PRTL.SI': 'PRU.L',
        'AMTI.SI': 'AMTD.K',
        'STELy.SI': 'STEL.SI',
        'COUA.SI': '1145.HK',
        'SRIT.SI': 'STGT.BK',
        'NIOI.SI': 'NIO',
        'EMPE.SI': 'EMI.PS',
        'YUNN.SI': '1298.HK',
        'CKFC.SI': '0834.HK',
        'COMB.SI': '2342.HK'
    }
    for iv_symbol in data_final["investing_symbol"].tolist():
        iv_data_dict["investing_symbol"].append(iv_symbol)
        ticker_extension = ".KL" if country == "my" else ".SI"
        iv_symbol = iv_symbol + ticker_extension
        if iv_symbol in special_case.keys():
            for key, value in zip(special_case.keys(), special_case.values()):
                if iv_symbol == key:
                    url = f"https://api.sgx.com/companygeneralinformation/v1.0/countryCode/SG/ricCode/{value}?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
        else:
            url = f"https://api.sgx.com/companygeneralinformation/v1.0/countryCode/SG/ricCode/{iv_symbol}?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
        response = requests.get(url)
        if response.status_code == 200:
            iv_data_dict["status"].append(response.status_code)
            try:
                employee_num_sgx = response.json()["data"][0]["noOfEmployees"]
            except:
                employee_num_sgx = np.nan
            iv_data_dict["employee_num_sgx"].append(employee_num_sgx)
        else:
            iv_data_dict["status"].append(response.status_code)
            iv_data_dict["employee_num_sgx"].append(np.nan)
    for iv_symbol, yf_symbol in zip(data_final["investing_symbol"].tolist(), data_final["symbol"].tolist()):
        yf_data_dict["investing_symbol"].append(iv_symbol)
        try:
            temp = yf.Ticker(yf_symbol + ".SI")
            employee = temp.info["fullTimeEmployees"]
            yf_data_dict["employee_num"].append(employee)
        except:
            yf_data_dict["employee_num"].append(np.nan)
    employee_sgx = pd.DataFrame(iv_data_dict).drop("status", axis=1)
    employee_yf = pd.DataFrame(yf_data_dict)
    new_en = []
    for en_yf, en_sgx in zip(employee_yf["employee_num"].tolist(), employee_sgx["employee_num_sgx"].tolist()):
        if en_sgx > 0:
            new_en.append(en_sgx)
        else:
            if en_yf > 0:
                new_en.append(en_yf)
            else:
                new_en.append(np.nan)
    data_final = data_final.assign(employee_num=new_en)
    return data_final


def convert_to_number(x):
    if isinstance(x, str):
        if 'T' in x:
            return float(x.replace('T', '')) * 1e12
        elif 'B' in x:
            return float(x.replace('B', '')) * 1e9
        elif 'M' in x:
            return float(x.replace('M', '')) * 1e6
        elif 'K' in x:
            return float(x.replace('K', '')) * 1e3
        else:
            try:
                return float(x.replace(',', ''))
            except ValueError:
                return np.nan
    elif isinstance(x, (int, float)):
        return x
    else:
        return np.nan


def rename_and_convert(data, period):
    if period == "monthly":
        rename_cols = {
            'Name': 'name',
            'Symbol': 'investing_symbol',
            'currency': 'currency',
            'Sector': 'sector',
            'Industry': 'industry',
            'Employees': 'employee_num',
            'Last': 'close',
            'ChgPct': 'percentage_change',
            'FundamentalMarketCap': 'market_cap',
            'Volume_x': 'volume',
            'FundamentalRatio': 'pe',
            'FundamentalRevenue': 'revenue',
            'EPS': 'eps',
            'FundamentalBeta': 'beta',
            'dividend': 'dividend',
            'dividend_yield': 'dividend_yield',
            'TechnicalDay': 'daily_signal',
            'TechnicalWeek': 'weekly_signal',
            'TechnicalMonth': 'monthly_signal',
            'PerformanceYtd': 'ytd_percentage_change',
            'PerformanceYear': 'one_year_percentage_change',
            'Performance3Year': 'three_year_percentage_change',
            'P/E Ratio TTM': 'pe_ttm',
            'Price to Sales TTM': 'ps_ttm',
            'Price to Cash Flow MRQ': 'pcf',
            'Price to Free Cash Flow TTM': 'pcf_ttm',
            'Price to Book MRQ': 'pb',
            '5 Year EPS Growth 5YA': 'five_year_eps_growth',
            '5 Year Sales Growth 5YA': 'five_year_sales_growth',
            '5 Year Capital Spending Growth 5YA': 'five_year_capital_spending_growth',
            'Asset Turnover TTM': 'asset_turnover',
            'Inventory Turnover TTM': 'inventory_turnover_ttm',
            'Receivable Turnover TTM': 'receivable_turnover',
            'Gross margin TTM': 'gross_margin',
            'Operating margin TTM': 'operating_margin',
            'Net Profit margin TTM': 'net_profit_margin',
            'Quick Ratio MRQ': 'quick_ratio',
            'Current Ratio MRQ': 'current_ratio',
            'Total Debt to Equity MRQ': 'debt_to_equity',
            'Dividend Yield 5 Year Avg. 5YA': 'five_year_dividend_average',
            'Dividend Growth Rate ANN': 'dividend_growth_rate',
            'Payout Ratio TTM': 'payout_ratio'
        }
        cleaned_data = data[rename_cols.keys()].rename(rename_cols, axis=1)

        cleaned_data.replace(['-', 'N/A'], np.nan, inplace=True)
        cleaned_data['revenue'] = cleaned_data['revenue'].apply(convert_to_number)
        cleaned_data['market_cap'] = cleaned_data['market_cap'].apply(convert_to_number)
        return cleaned_data

    elif period == "daily":
        rename_cols = {
            'Symbol': 'investing_symbol',
            'Last': 'close',
            'ChgPct': 'percentage_change',
            'FundamentalMarketCap': 'market_cap',
            'Volume': 'volume',
            'FundamentalRatio': 'pe',
            'FundamentalRevenue': 'revenue',
            'FundamentalBeta': 'beta',
            'TechnicalDay': 'daily_signal',
            'TechnicalWeek': 'weekly_signal',
            'TechnicalMonth': 'monthly_signal'
        }
        cleaned_data = data[rename_cols.keys()].rename(rename_cols, axis=1)

        cleaned_data.replace(['-', 'N/A'], np.nan, inplace=True)
        cleaned_data['revenue'] = cleaned_data['revenue'].apply(convert_to_number)
        cleaned_data['market_cap'] = cleaned_data['market_cap'].apply(convert_to_number)
        return cleaned_data


def clean_daily_foreign_data(foreign_daily_data):
    """
    SGX/KLSE Daily Data Fetching Cleansing.

    Parameters:
    - foreign_daily_data: dataframe, KLSE/SGX daily data from investing.com api hit

    Returns:
    - foreign_daily_data: dataframe, Cleaned KLSE/SGX daily data
    """

    # Replace '-' data with ''
    foreign_daily_data = foreign_daily_data.replace('-', np.nan)

    # Remove percentage and change data to decimal
    # for i in ['ytd', 'one_year', 'three_year']:
    #     foreign_daily_data[f"{i}_percentage_change"] = foreign_daily_data[f"{i}_percentage_change"] / 100

    #     # Rename columns
    # foreign_daily_data.rename(columns={"ytd_percentage_change": "change_ytd",
    #                                    "one_year_percentage_change": "change_1y",
    #                                    "three_year_percentage_change": "change_3y"}, inplace=True)

    # Delete redundant percentage change columns
    foreign_daily_data.drop(["percentage_change", "close"], axis=1, inplace=True)

    # Change data type to float
    float_columns = ['market_cap', 'volume', 'pe', 'revenue', 'beta']

    foreign_daily_data[float_columns] = foreign_daily_data[float_columns].applymap(
        lambda x: float(str(x).replace(',', '')))

    return foreign_daily_data


def clean_periodic_foreign_data(foreign_periodic_data, foreign_sectors):
    """
    SGX/KLSE Periodic Data Fetching Cleansing.

    Parameters:
    - foreign_periodic_data: dataframe, periodic data from investing.com data scraping using request
    - foreign_sectors: dataframe, KLSE/SGX sectors mapping to IDX sectors

    Returns:
    - foreign_periodic_data: dataframe, Cleaned KLSE/SGX periodic data
    """

    # Replace '-' data with ''
    foreign_periodic_data = foreign_periodic_data.replace('-', np.nan)

    foreign_periodic_data['dividend_yield'] = foreign_periodic_data['dividend_yield'].apply(
        lambda x: float(x.strip('%')) / 100 if pd.notnull(x) else np.nan)

    for i in ["gross_margin", "operating_margin", 'net_profit_margin', "debt_to_equity", "five_year_dividend_average",
              'dividend_growth_rate', "payout_ratio", "five_year_eps_growth", "five_year_sales_growth",
              "five_year_capital_spending_growth"]:
        foreign_periodic_data[i] = foreign_periodic_data[i].apply(
            lambda x: float(x.replace('%', '').replace(',', '')) / 100 if pd.notnull(x) else np.nan)

    foreign_periodic_data.rename(columns={"five_year_dividend_average": "dividend_yield_5y_avg"}, inplace=True)

    float_columns = ['eps', 'dividend', 'dividend_yield', 'pe_ttm', 'ps_ttm', 'pcf', 'pcf_ttm', 'pb',
                     'five_year_eps_growth',
                     'five_year_sales_growth', 'five_year_capital_spending_growth',
                     'asset_turnover', 'inventory_turnover_ttm', 'receivable_turnover',
                     'gross_margin', 'operating_margin', 'net_profit_margin', 'quick_ratio',
                     'current_ratio', 'debt_to_equity', 'dividend_yield_5y_avg',
                     'dividend_growth_rate', 'payout_ratio']

    foreign_periodic_data[float_columns] = foreign_periodic_data[float_columns].applymap(
        lambda x: float(str(x).replace(',', '')))

    foreign_periodic_data = foreign_periodic_data.merge(foreign_sectors, on=["sector", 'industry']).drop(
        ["sector", 'industry'], axis=1).rename(columns={"sectors_id": "sector", "sub_sector_id": "sub_sector"})

    return foreign_periodic_data

def update_estimate_growth_data(data_prep: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Update the DataFrame by fetching next‑year EPS and sales growth estimates for each symbol.
    
    New columns added:
      - one_year_eps_growth   : Next‑year EPS growth (decimal, e.g. 0.0225 for +2.25%)
      - one_year_sales_growth : Next‑year sales growth (decimal, e.g. 0.0285 for +2.85%)
    
    Parameters:
      data_prep (pd.DataFrame): Must contain at least a 'symbol' column.
      country (str): Country code ("my" → ".KL", otherwise ".SI").
      
    Returns:
      pd.DataFrame: The input DataFrame with two new columns.
    """
    for idx, row in data_prep.iterrows():
        symbol = row["symbol"]
        ext = ".KL" if country.lower() == "my" else ".SI"
        try:
            ticker = yf.Ticker(symbol + ext)
            
            # Fetch the two tables
            ge = ticker.growth_estimates      # DataFrame indexed by period: ['0q','+1q','0y','+1y',...]
            re = ticker.revenue_estimate      # DataFrame indexed by period with a 'growth' column
            
            # Extract the +1y entries (EPS = stockTrend, sales = growth)
            eps_1y = ge.at["+1y", "stockTrend"] if "+1y" in ge.index else np.nan
            sales_1y = re.at["+1y", "growth"] if "+1y" in re.index else np.nan
            
            data_prep.loc[idx, "one_year_eps_growth"] = eps_1y
            data_prep.loc[idx, "one_year_sales_growth"] = sales_1y
            
        except Exception as e:
            # on any failure, set NaN
            print(f"[DEBUG] Failed to fetch estimates for {symbol}: {e}")
            # data_prep.loc[idx, ["one_year_eps_growth", "one_year_sales_growth"]] = np.nan

    return data_prep

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument("-sg", "--singapore", action="store_true", default=False, help="Update singapore data")
    parser.add_argument("-my", "--malaysia", action="store_true", default=False, help="Update malaysia data")
    parser.add_argument("-d", "--daily", action="store_true", default=False, help="Update daily data")
    parser.add_argument("-m", "--monthly", action="store_true", default=False, help="Update monthly data")
    parser.add_argument("-w", "--weekly", action="store_true", default=False, help="Update weekly data")

    args = parser.parse_args()
    if args.singapore and args.malaysia:
        print("Error: Please specify either -sg or -my, not both.")
        raise SystemExit(1)
    if args.daily and args.monthly:
        print("Error: Please specify either -d or -m, not both.")
        raise SystemExit(1)

    url_currency = 'https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json'
    response = requests.get(url_currency)
    data = response.json()

    url_supabase = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)
    logging.basicConfig(filename="logs.log", level=logging.INFO)
    country = "my" if args.malaysia else "sg"
    sg_sectors = pd.read_csv("sectors_mapping/sectors_sg.csv", sep=";")
    my_sectors = pd.read_csv("sectors_mapping/sectors_my.csv", sep=";")
    foreign_sectors = my_sectors if args.malaysia else sg_sectors

    if args.monthly:
        db = "klse_companies" if args.malaysia else "sgx_companies"
        data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        data_final = employee_updater(data_db, country)
    elif args.weekly:
        data_general = GetGeneralData(country)
        data_general = rename_and_convert(data_general, "daily")
        data_general = clean_daily_foreign_data(data_general)
        # print("\nStep 3: After clean_daily_foreign_data:")
        # print(data_general.head())
        # print("Columns in data_general after clean_daily_foreign_data:", data_general.columns.tolist())
        db = "klse_companies" if args.malaysia else "sgx_companies"
        data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe',
                     'revenue', 'beta', 'weekly_signal',
                     'monthly_signal', 'change_ytd', 'change_1y', 'change_3y']
        data_db.drop(drop_cols, axis=1, inplace=True)
        data_final = pd.merge(data_general, data_db, on="investing_symbol", how="inner")
        data_final = data_final.drop(
            ["revenue", 'dividend_ttm', 'forward_dividend', 'forward_dividend_yield', 'net_profit_margin',
             "operating_margin", "gross_margin", "quick_ratio", "current_ratio", "debt_to_equity", "payout_ratio",
             "eps"], axis=1)
        # print("\nStep 10: data_final after dropping additional columns:")
        # print(data_final.head())
        # print("Columns in data_final after final drop:", data_final.columns.tolist())
    elif args.daily:
        db = "klse_companies" if args.malaysia else "sgx_companies"
        if args.singapore:
            data_db = supabase.table(db).select("*").eq("is_active", True).execute()
            # data_db = supabase.table(db).select("*").eq("is_active", True).limit(50).execute()
        else:
            data_db = supabase.table(db).select("*").execute()
        # data_db = supabase.table(db).select("*").eq("is_active", True).limit(50).execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe',
                     'revenue', 'beta', 'weekly_signal',
                     'monthly_signal', 'earnings']
        data_db.drop(drop_cols, axis=1, inplace=True, errors='ignore')
        data_final = yf_data_updater(data_db, country)
        data_final = update_change_data(data_final, country)
        data_final = update_close_history_data(data_final, country)
        data_final = update_dividend_growth_rate(data_final, country)
        
        if args.singapore:
            data_final = update_historical_dividends(data_final, country)
            data_final = update_all_time_price(data_final, country) 
            data_final = update_estimate_growth_data(data_final, country)
            # print("")  

    invalid_yf_symbol = ['KIPR', 'PREI', 'YTLR', 'IGRE', 'ALQA', 'TWRE', 'AMFL', 'UOAR', 'AMRY', 'HEKR', 'SENT', 'AXSR',
                         'CAMA', 'SUNW', 'ATRL', 'PROL', 'KLCC', '5270']
    data_final = data_final[~data_final["symbol"].isin(invalid_yf_symbol)]
    data_final.to_csv("data_my.csv", index=False) if args.malaysia else data_final.to_csv("data_sg.csv", index=False)
    
    # --- BEFORE CLEANING ---
    print("\n--- [DEBUG STEP 1: BEFORE CLEANING] Checking for records with NaN values ---")
    records_before_cleaning = data_final.to_dict("records")
    problematic_records_before = []
    for record in records_before_cleaning:
        try:
            # This will fail if NaN is present. We use `allow_nan=True` just to print it later.
            json.dumps(record, allow_nan=False, default=str)
        except ValueError:
            problematic_records_before.append(record)

    if problematic_records_before:
        print(f"  - FOUND {len(problematic_records_before)} records with NaN values before cleaning. Example:")
        # Print the first problematic record found
        bad_record = problematic_records_before[0]
        print(f"\n--- Problematic Record (Symbol: {bad_record.get('symbol', 'N/A')}) BEFORE CLEANING ---")
        # Use json.dumps with `allow_nan=True` to be able to print the structure containing 'NaN'
        print(json.dumps(bad_record, indent=2, allow_nan=True, default=str))
    else:
        print("  - OK. No records with NaN values found before cleaning.")

    # --- APPLY CLEANING LOGIC ---
    print("\n--- [STEP 2: APPLYING CLEANING LOGIC] ---")
    # First, handle top-level NaNs
    data_final.replace({np.nan: None}, inplace=True)
    print("  - Applied top-level NaN replacement.")

    # These columns can contain nested structures (lists of dicts) with NaNs.
    json_like_cols = ['close', 'historical_dividends', 'all_time_price']
    for col in json_like_cols:
        if col in data_final.columns:
            print(f"  - Applying recursive NaN cleaning to nested data in column: '{col}'")
            # The .apply() method with the recursive function cleans NaNs inside these nested structures.
            data_final[col] = data_final[col].apply(recursively_clean_nans)
            
    # --- AFTER CLEANING ---
    print("\n--- [STEP 3: PREPARING FOR UPSERT] Converting DataFrame to records ---")
    records = data_final.to_dict("records")
    print(f"  - Converted {len(records)} rows to a list of dictionaries.")

    print("\n--- [DEBUG STEP 4: AFTER CLEANING] Verifying all records for any remaining NaN values ---")
    found_error_after_cleaning = False
    for record in records:
        try:
            json.dumps(record, allow_nan=False, default=str)
        except ValueError:
            print(f"  - ERROR: Found a record with NaN even after cleaning! Symbol: {record.get('symbol', 'N/A')}")
            found_error_after_cleaning = True
    if not found_error_after_cleaning:
        print("  - OK. Verification complete. No NaN values remain.")
        
    print(f"\n--- [STEP 5: UPSERT] Sending {len(records)} records to Supabase table '{db}' ---")
    supabase.table(db).upsert(records, returning='minimal').execute()
    print("Upsert operation successful.")
