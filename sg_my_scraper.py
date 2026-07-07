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
import re

def recursively_clean_nans(obj):
    if isinstance(obj, dict):
        return {k: recursively_clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursively_clean_nans(e) for e in obj]
    if pd.isna(obj):
        return None
    return obj

def safe_relative_diff(num1: float, num2: float):
    if num1 == 0:
        return 0
    if num2 == 0:
        return num1
    return (num1 / num2) - 1


def yf_data_updater(data_prep: pd.DataFrame, country):

    def clean_short_name(name: str) -> str | None:
        if not isinstance(name, str) or pd.isna(name):
            return None

        cleaned_name = name.strip()

        if cleaned_name.lower() == 'null':
            return None
        if '.si,' in cleaned_name.lower() and ',' in cleaned_name.lower():
            return None

        cleaned_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_name)

        suffix_pattern = re.compile(
            r'\s*('
            r'(- watch list|USD OV|TH SDR 1to1|A\$|HK\$|GROUP|LIMITED|LTD|PLC|PCL|DRC'
            r'|REIT|TRUST|TR|T|COM|NCCPS|SGD|USD|EUR|CNY|GBP|OV)$'
            r')',
            re.IGNORECASE
        )
        cleaned_name = suffix_pattern.sub('', cleaned_name)

        leading_pattern = re.compile(r'^\$\s*|^[acht]\s+', re.IGNORECASE)
        cleaned_name = leading_pattern.sub('', cleaned_name)

        words = cleaned_name.split()
        processed_words = []
        for word in words:
            if word.isupper():
                processed_words.append(word)
            else:
                processed_words.append(word.title())

        cleaned_name = " ".join(processed_words)
        cleaned_name = cleaned_name.strip()
        return cleaned_name if cleaned_name else None

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(symbol + ticker_extension)
            info = ticker.info

            currency_info = info.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"
            currency = currency_info or row.get("currency")

            if currency and currency != country_currency:
                rate = data.get(currency, {}).get(country_currency)
                if rate is None:
                    continue
                else:
                    rate = float(rate)

            desired_values = {
                "fiveYearAvgDividendYield": "dividend_yield_5y_avg"
            }
            if country == "sg":
                desired_values["shortName"] = "short_name"
            elif country == "my":
                desired_values.update({
                    "marketCap": "market_cap",
                    "volume": "volume",
                    "trailingPE": "pe",
                    "priceToSalesTrailing12Months": "ps_ttm",
                    "priceToBook": "pb",
                    "beta": "beta",
                    "operatingCashflow": "ocf",
                })

            for key_dv, col in desired_values.items():
                try:
                    raw_val = info.get(key_dv, np.nan)

                    if col == "market_cap":
                        if raw_val is not None and raw_val is not np.nan:
                            if currency and currency != country_currency and rate is not None:
                                data_prep.at[index, col] = raw_val * rate
                            else:
                                data_prep.at[index, col] = raw_val
                        else:
                            data_prep.at[index, col] = np.nan

                    elif col == "short_name":
                        data_prep.at[index, col] = clean_short_name(raw_val)

                    elif col == "ocf":
                        ocf_val = raw_val
                        if ocf_val not in [None, 0, np.nan]:
                            mcap = info.get("marketCap")
                            data_prep.at[index, "pcf"] = mcap / ocf_val
                        else:
                            data_prep.at[index, "pcf"] = np.nan

                    elif col == "dividend_yield_5y_avg":
                        if raw_val is not None and not pd.isna(raw_val):
                            data_prep.at[index, col] = raw_val / 100

                    elif col == "pe":
                        yf_pe = raw_val
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

                        if not pd.isna(yf_pe) and np.isfinite(yf_pe):
                            data_prep.at[index, col] = yf_pe
                        else:
                            close_list = row.get("close", [])
                            last_close = None
                            if isinstance(close_list, list) and close_list:
                                last_close = close_list[-1].get("close")
                            eps = row.get("eps")
                            if last_close is not None and eps:
                                data_prep.at[index, col] = last_close / eps
                            else:
                                data_prep.at[index, col] = np.nan

                    else:
                        data_prep.at[index, col] = raw_val

                except KeyError:
                    if col == "pe":
                        close_list = row.get("close", [])
                        last_close = close_list[-1].get("close") if isinstance(close_list, list) and close_list else None
                        eps = row.get("eps")
                        data_prep.at[index, col] = (last_close / eps) if (last_close and eps) else np.nan
                    else:
                        continue

        except Exception as e:
            print(f"Error updating symbol {symbol}: {e}")

    if "ocf" in data_prep.columns:
        data_prep = data_prep.drop(columns=["ocf"])

    return data_prep

def update_dividend_growth_rate(data_prep: pd.DataFrame, country):
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
            continue

    return data_prep

def update_close_history_data(data_prep: pd.DataFrame, country):
    date_format = "%Y-%m-%d"
    last_date = (datetime.now() - timedelta(days=31)).strftime(date_format)

    list_dates = [
        (datetime.strptime(last_date, date_format) + timedelta(days=i)).strftime(date_format)
        for i in range(1, 32)
    ]

    new_close = []

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)
            currency_info = ticker.info.get("currency", None)
            currency = currency_info or row.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"

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
                    if currency != country_currency:
                        rate = float(data[currency][country_currency])
                        curr_close = curr_close * rate
                    close_data.append({
                        "date": curr_date,
                        "close": curr_close if np.isfinite(curr_close) else None
                    })

            close_data = [close for close in close_data if close["date"] > last_date]
            new_close.append(close_data if close_data else row["close"])
        except Exception as e:
            print(f"error in symbol {symbol} : ", e)
            new_close.append(row["close"])

    try:
        data_prep = data_prep.assign(close=new_close)
        if "ocf" in data_prep.columns:
            data_prep = data_prep.drop("ocf", axis="columns")
    except Exception as e:
        print(f"[DEBUG] Error assigning close data: {e}")
        data_prep = data_prep.assign(close=new_close)

    return data_prep

def update_historical_dividends(data_prep: pd.DataFrame, country):
    date_format = "%Y-%m-%d"
    if "historical_dividends" not in data_prep.columns:
        data_prep["historical_dividends"] = None

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)

            full_history = ticker.history(period="max").reset_index()
            if full_history.empty:
                raise ValueError("No historical data available")
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)
            latest_close = full_history.iloc[-1]["Close"]

            dividends_series = ticker.dividends
            if not dividends_series.empty:
                dividends_df = dividends_series.reset_index()
                dividends_df.columns = ["Date", "Dividend"]
                dividends_df["year"] = dividends_df["Date"].dt.year
                dividends_df["yield"] = dividends_df["Dividend"] / latest_close if latest_close else np.nan
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
                    historical_dividends.append({
                        "year": int(year),
                        "breakdown": breakdown,
                        "total_yield": total_yield,
                        "total_dividend": total_dividend
                    })
                data_prep.at[index, "historical_dividends"] = historical_dividends
            else:
                continue

        except Exception as e:
            print(f"[DEBUG] Error processing historical_dividends for {symbol}: {e}")
            continue

    return data_prep

def update_all_time_price(data_prep: pd.DataFrame, country: str):
    date_format = "%Y-%m-%d"

    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        ticker_extension = ".KL" if country.lower() == "my" else ".SI"
        ticker_full = symbol + ticker_extension

        try:
            ticker = yf.Ticker(ticker_full)

            full_history = ticker.history(period="max").reset_index()
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)

            if full_history.empty:
                raise ValueError("No historical data available")

            latest_date = full_history.iloc[-1]["Date"]
            latest_close = full_history.iloc[-1]["Close"]

            all_time_low_row = full_history.loc[full_history["Close"].idxmin()]
            all_time_high_row = full_history.loc[full_history["Close"].idxmax()]
            all_time_low = {"date": all_time_low_row["Date"].strftime(date_format), "price": all_time_low_row["Close"]}
            all_time_high = {"date": all_time_high_row["Date"].strftime(date_format), "price": all_time_high_row["Close"]}

            current_year = datetime.now().year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            if not current_year_data.empty:
                ytd_low_row = full_history.loc[current_year_data["Close"].idxmin()]
                ytd_high_row = full_history.loc[current_year_data["Close"].idxmax()]
                ytd_low = {"date": ytd_low_row["Date"].strftime(date_format), "price": ytd_low_row["Close"]}
                ytd_high = {"date": ytd_high_row["Date"].strftime(date_format), "price": ytd_high_row["Close"]}
            else:
                ytd_low = ytd_high = None

            start_52w = latest_date - timedelta(days=365)
            data_52w = full_history[full_history["Date"] >= start_52w]
            if not data_52w.empty:
                w52_low_row = full_history.loc[data_52w["Close"].idxmin()]
                w52_high_row = full_history.loc[data_52w["Close"].idxmax()]
                w52_low = {"date": w52_low_row["Date"].strftime(date_format), "price": w52_low_row["Close"]}
                w52_high = {"date": w52_high_row["Date"].strftime(date_format), "price": w52_high_row["Close"]}
            else:
                w52_low = w52_high = None

            start_90d = latest_date - timedelta(days=90)
            data_90d = full_history[full_history["Date"] >= start_90d]
            if not data_90d.empty:
                d90_low_row = full_history.loc[data_90d["Close"].idxmin()]
                d90_high_row = full_history.loc[data_90d["Close"].idxmax()]
                d90_low = {"date": d90_low_row["Date"].strftime(date_format), "price": d90_low_row["Close"]}
                d90_high = {"date": d90_high_row["Date"].strftime(date_format), "price": d90_high_row["Close"]}
            else:
                d90_low = d90_high = None

            data_prep.at[index, "all_time_price"] = {
                "ytd_low": ytd_low,
                "ytd_high": ytd_high,
                "52_w_low": w52_low,
                "52_w_high": w52_high,
                "90_d_low": d90_low,
                "90_d_high": d90_high,
                "all_time_low": all_time_low,
                "all_time_high": all_time_high
            }

        except Exception as e:
            print(f"[DEBUG] Error processing all_time_price for {ticker_full}: {e}")
            continue

    return data_prep

def update_change_data(data_prep: pd.DataFrame, country):
    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(row["symbol"] + ticker_extension)

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

            current_year = latest_date.year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            ytd_close = current_year_data.iloc[0]["Close"] if not current_year_data.empty else None
            close_1y = get_close_on_or_before(latest_date - timedelta(days=365))
            close_3y = get_close_on_or_before(latest_date - timedelta(days=3 * 365))

            def compute_change(latest, past):
                if past is None or past == 0:
                    return np.nan
                return (latest - past) / past

            if country != "sg":
                data_prep.loc[index, "change_ytd"] = compute_change(latest_close, ytd_close)
                data_prep.loc[index, "change_1y"]  = compute_change(latest_close, close_1y)
            data_prep.loc[index, "change_3y"]  = compute_change(latest_close, close_3y)

        except Exception as e:
            print(f"[DEBUG] Error calculating change metrics for {symbol}: {e}")
            continue

    return data_prep

def employee_updater(data_final, country):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    iv_data_dict = {
        "symbol": [],
        "status": [],
        "employee_num_sgx": []
    }
    yf_data_dict = {
        "symbol": [],
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
    for sym in data_final["symbol"].tolist():
        iv_data_dict["symbol"].append(sym)
        ticker_extension = ".KL" if country == "my" else ".SI"
        sym_with_ext = sym + ticker_extension
        if sym_with_ext in special_case.keys():
            for key, value in zip(special_case.keys(), special_case.values()):
                if sym_with_ext == key:
                    url = f"https://api.sgx.com/companygeneralinformation/v1.0/countryCode/SG/ricCode/{value}?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
        else:
            url = f"https://api.sgx.com/companygeneralinformation/v1.0/countryCode/SG/ricCode/{sym_with_ext}?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
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
    for sym in data_final["symbol"].tolist():
        yf_data_dict["symbol"].append(sym)
        try:
            temp = yf.Ticker(sym + ".SI")
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


def update_estimate_growth_data(data_prep: pd.DataFrame, country: str) -> pd.DataFrame:
    for idx, row in data_prep.iterrows():
        symbol = row["symbol"]
        ext = ".KL" if country.lower() == "my" else ".SI"
        try:
            ticker = yf.Ticker(symbol + ext)

            ge = ticker.growth_estimates

            eps_1y = ge.at["+1y", "stockTrend"] if "+1y" in ge.index else np.nan

            data_prep.loc[idx, "one_year_eps_growth"] = eps_1y

        except Exception as e:
            print(f"[DEBUG] Failed to fetch estimates for {symbol}: {e}")
            continue

    return data_prep

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument("-sg", "--singapore", action="store_true", default=False, help="Update singapore data")
    parser.add_argument("-my", "--malaysia", action="store_true", default=False, help="Update malaysia data")
    parser.add_argument("-d", "--daily", action="store_true", default=False, help="Update daily data")
    parser.add_argument("-m", "--monthly", action="store_true", default=False, help="Update monthly data")

    args = parser.parse_args()
    if args.singapore and args.malaysia:
        print("Error: Please specify either -sg or -my, not both.")
        raise SystemExit(1)
    if args.daily and args.monthly:
        print("Error: Please specify either -d or -m, not both.")
        raise SystemExit(1)

    country = "my" if args.malaysia else "sg"

    if country == "sg":
        print("Attempting to use local compact_rates.json for SGX...")
        try:
            with open('compact_rates.json', 'r') as f:
                data = json.load(f)
            print("...Success! Loaded conversion rates from local compact_rates.json file.")
        except Exception as e:
            print(f"...Warning: Could not load local file due to an error ({e}).")
            print("...Falling back to fetching current rates from the live URL.")
            url_currency = 'https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json'
            response = requests.get(url_currency)
            data = response.json()
            print("...Successfully fetched current rates from URL as a fallback.")
    else:
        print("Fetching current conversion rates from URL for KLSE data.")
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
    elif args.daily:
        db = "klse_companies" if args.malaysia else "sgx_companies"
        if args.singapore:
            data_db = supabase.table(db).select("*").eq("is_active", True).execute()
        else:
            data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe', 'revenue', 'beta', 'weekly_signal', 'monthly_signal', 'earnings']
        data_db.drop(drop_cols, axis=1, inplace=True, errors='ignore')
        data_final = yf_data_updater(data_db, country)
        data_final = update_change_data(data_final, country)
        data_final = update_dividend_growth_rate(data_final, country)
        if not args.singapore:
            data_final = update_close_history_data(data_final, country)

        if args.singapore:
            data_final = update_historical_dividends(data_final, country)
            data_final = update_all_time_price(data_final, country)
            data_final = update_estimate_growth_data(data_final, country)

    invalid_yf_symbol = ['KIPR', 'PREI', 'YTLR', 'IGRE', 'ALQA', 'TWRE', 'AMFL', 'UOAR', 'AMRY', 'HEKR', 'SENT', 'AXSR',
                         'CAMA', 'SUNW', 'ATRL', 'PROL', 'KLCC', '5270']
    data_final = data_final[~data_final["symbol"].isin(invalid_yf_symbol)]
    data_final.to_csv("data_my.csv", index=False) if args.malaysia else data_final.to_csv("data_sg.csv", index=False)

    records_before_cleaning = data_final.to_dict("records")
    problematic_records_before = []
    for record in records_before_cleaning:
        try:
            json.dumps(record, allow_nan=False, default=str)
        except ValueError:
            problematic_records_before.append(record)

    if problematic_records_before:
        bad_record = problematic_records_before[0]

    data_final.replace({np.nan: None}, inplace=True)

    json_like_cols = ['close', 'historical_dividends', 'all_time_price']
    for col in json_like_cols:
        if col in data_final.columns:
            data_final[col] = data_final[col].apply(recursively_clean_nans)

    records = data_final.to_dict("records")

    found_error_after_cleaning = False
    for record in records:
        try:
            json.dumps(record, allow_nan=False, default=str)
        except ValueError:
            found_error_after_cleaning = True

    supabase.table(db).upsert(records, returning='minimal').execute()
    print("Upsert operation successful.")
