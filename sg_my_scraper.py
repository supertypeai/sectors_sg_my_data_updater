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
        url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
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
    date_format = "%Y-%m-%d"
    last_date = (datetime.now() - timedelta(days=31)).strftime(date_format)

    # get the list of dates within 1 latest month
    list_dates = []
    for i in range(1, 32):
        temp_date = (datetime.strptime(last_date, date_format) + timedelta(days=i)).strftime(date_format)
        list_dates.append(temp_date)

    new_close = []
    for index, row in data_prep.iterrows():
        symbol = row["symbol"]
        try:
            try:
                ticker_extension = ".KL" if country == "my" else ".SI"
                ticker = yf.Ticker(row["symbol"] + ticker_extension)
                currency = ticker.info["currency"]
                country_currency = "MYR" if country == "my" else "SGD"
            except Exception as e:
                raise AttributeError(f"no data available for {symbol}, possibly delisted: {e}")
            # update dividend_growth_rate
            current_year = datetime.now().year
            dividend_last_1_year = ticker.history(start=f"{current_year - 1}-01-01", end=f"{current_year - 1}-12-31")[
                "Dividends"].sum()
            dividend_current = ticker.history(start=f"{current_year}-01-01", end=f"{current_year}-12-31")[
                "Dividends"].sum()
            dividend_growth_rate = safe_relative_diff(dividend_current, dividend_last_1_year)
            data_prep.loc[index, "dividend_growth_rate"] = dividend_growth_rate

            # update data from info yfinance
            data_json = ticker.info
            desired_values = {
                "shortName": "short_name",
                "marketCap": "market_cap",
                "volume": "volume",
                "trailingPE": "pe",
                "priceToSalesTrailing12Months": "ps_ttm",
                "priceToBook": "pb",
                "beta": "beta",
                "operatingCashflow": "ocf",
                "totalRevenue": "revenue"
            }
            for key_dv, val_dv in zip(desired_values.keys(), desired_values.values()):
                try:
                    if val_dv == "market_cap":
                        if currency != country_currency:
                            rate = float(data[currency][country_currency])
                            temp_val = data_json[key_dv] * rate
                        else:
                            temp_val = data_json[key_dv]
                        data_prep.loc[index, val_dv] = temp_val
                    elif val_dv == "revenue":
                        financial_currency = ticker.info["financialCurrency"]
                        if financial_currency != country_currency:
                            rate = float(data[financial_currency][country_currency])
                            temp_val = data_json[key_dv] * rate
                        else:
                            temp_val = data_json[key_dv]
                        data_prep.loc[index, val_dv] = temp_val
                    elif val_dv == "ocf":
                        temp_val = data_json["marketCap"] / data_json[key_dv]
                        data_prep.loc[index, "pcf"] = temp_val
                    else:
                        data_prep.loc[index, val_dv] = data_json[key_dv]
                except KeyError as e:
                    """
                    if this appear, that means yf don't have the data of the metrics
                    so it will be filled by NaN, or we can just still used investing.com values
                    for "pe" it will be calculated first with this formula, pe = close/eps_ttm
                    """
                    # print(f"data unavailable from YF for {symbol}, trying to use data from other source: {e}")
                    if val_dv == "pe":
                        temp_pe = (row["close"][-1]["close"] / row["eps"]) if row["eps"] != 0 else np.nan
                        data_prep.loc[index, "pe"] = temp_pe
                    else:
                        data_prep.loc[index, val_dv] = np.nan

            # update data from history yfinance
            try:
                yf_data = ticker.history(period="1mo").reset_index()
            except:
                """
                New stock that doesn't have 30 days history data, will go to this section instead
                and retrieved all the data
                """
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
                    temp = {
                        "date": curr_date,
                        "close": curr_close if np.isfinite(curr_close) else None
                    }
                    close_data.append(temp)
            close_data = [close for close in close_data if close["date"] > last_date]
            new_close.append(close_data)
        except Exception as e:
            print(f"error in symbol {symbol} : ", e)
            new_close.append(None)
    try:
        data_prep = data_prep.assign(close=new_close).drop("ocf", axis="columns")
    except:
        data_prep = data_prep.assign(close=new_close)
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
            'TechnicalMonth': 'monthly_signal',
            'PerformanceYtd': 'ytd_percentage_change',
            'PerformanceYear': 'one_year_percentage_change',
            'Performance3Year': 'three_year_percentage_change',
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
    for i in ["daily", 'weekly', 'monthly', 'ytd', 'one_year', 'three_year']:
        foreign_daily_data[f"{i}_percentage_change"] = foreign_daily_data[f"{i}_percentage_change"] / 100

        # Rename columns
    foreign_daily_data.rename(columns={"ytd_percentage_change": "change_ytd",
                                       "one_year_percentage_change": "change_1y",
                                       "three_year_percentage_change": "change_3y"}, inplace=True)

    # Delete redundant percentage change columns
    foreign_daily_data.drop(["percentage_change", "close"], axis=1, inplace=True)

    # Change data type to float
    float_columns = ['market_cap', 'volume', 'pe', 'revenue', 'beta', 'change_ytd', 'change_1y', 'change_3y', ]

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
        db = "klse_companies" if args.malaysia else "sgx_companies"
        data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe',
                     'revenue', 'beta', 'daily_signal', 'weekly_signal',
                     'monthly_signal', 'change_1d', 'change_7d', 'change_1m',
                     'change_ytd', 'change_1y', 'change_3y']
        data_db.drop(drop_cols, axis=1, inplace=True)
        data_final = pd.merge(data_general, data_db, on="investing_symbol", how="inner")
        data_final = data_final.drop(
            ["revenue", 'dividend_ttm', 'forward_dividend', 'forward_dividend_yield', 'net_profit_margin',
             "operating_margin", "gross_margin", "quick_ratio", "current_ratio", "debt_to_equity", "payout_ratio",
             "eps"], axis=1)
    elif args.daily:
        db = "klse_companies" if args.malaysia else "sgx_companies"
        data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe',
                     'revenue', 'beta', 'daily_signal', 'weekly_signal',
                     'monthly_signal',
                     'change_ytd', 'change_1y', 'change_3y']
        if args.malaysia:
            drop_cols += ["short_name"]
        data_db.drop(drop_cols, axis=1, inplace=True, errors='ignore')
        data_final = yf_data_updater(data_db, country)
    invalid_yf_symbol = ['KIPR', 'PREI', 'YTLR', 'IGRE', 'ALQA', 'TWRE', 'AMFL', 'UOAR', 'AMRY', 'HEKR', 'SENT', 'AXSR',
                         'CAMA', 'SUNW', 'ATRL', 'PROL', 'KLCC', '5270']
    data_final = data_final[~data_final["symbol"].isin(invalid_yf_symbol)]
    data_final.to_csv("data_my.csv", index=False) if args.malaysia else data_final.to_csv("data_sg.csv", index=False)
    records = data_final.replace({np.nan: None}).to_dict("records")

    supabase.table(db).upsert(records, returning='minimal').execute()
    print("Upsert operation successful.")
