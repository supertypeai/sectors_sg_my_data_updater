import requests
import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import pandas as pd
import datetime
from datetime import datetime
import logging
import argparse
import numpy as np
from bs4 import BeautifulSoup
import json
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request
proxy = os.environ.get("PROXY")

proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

def GetGeneralData(country):
    if country == "sg":
        url ="https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    elif country == "my":
        url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=42&filter-domain=&page=0&page-size=2000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    data_from_api = None
    with urllib.request.urlopen(url) as response:
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

def GetAdditionalData(links):
    data_list = []
    failed_links = {
        "links" : [],
        "page" : []
    }
    for link in links[:5]:
        try:
            data_dict = {
                "Url" : link
            }
            # Page Overview
            url = f"https://www.investing.com{link}"
            response = requests.get(url)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, "html.parser")
                close = soup.find(class_ = "text-5xl/9 font-bold text-[#232526] md:text-[42px] md:leading-[60px]").get_text()
                change_percent = soup.find('span', {'data-test': 'instrument-price-change-percent'}).get_text().replace("(", "").replace(")", "")
                currency = soup.find(class_ = "ml-1.5 font-bold").get_text()
                values = soup.find_all(class_ = "flex flex-wrap items-center justify-between border-t border-t-[#e6e9eb] pt-2.5 sm:pb-2.5 pb-2.5")
                expected_values = ["Volume", "Market Cap", "Revenue", "P/E Ratio", "EPS",  "Dividend (Yield)"]
                data_dict["close"] = close
                data_dict["change_percent"] = change_percent
                data_dict["currency"] = currency
                for value in values:
                    value = value.get_text()
                    for expected_value in expected_values:
                        if expected_value in value:
                            if expected_value == "Dividend (Yield)":
                                value = value.replace(expected_value, "")
                                try:
                                    dividend, yields = value.split("(")
                                    data_dict["dividend"] = dividend
                                    data_dict["dividend_yield"] = yields.replace(")", "")
                                except:
                                    data_dict["dividend"] = "-"
                                    data_dict["dividend_yield"] = "-"
                            else:
                                data_dict[expected_value] = value.replace(expected_value, "")
                company_profile = soup.find(class_ = "mt-6 font-semibold md:mt-0")
                desired_infos = ["Industry", "Sector"]
                for info in company_profile:
                    info = info.get_text()
                    for desired_info in desired_infos:
                        if desired_info in info:
                            data_dict[desired_info] = info.replace(desired_info, "")
            else:
                failed_links["links"].append(link)
                failed_links["page"].append("overview")
                logging.error(f"error at overview page with link: {link}")
                print(f"error at overview page with link: {link}")
                
            # Page Ratios
            url = f"https://www.investing.com{link}-ratios"
            response = requests.get(url)
            if response.status_code == 200:
                html_content= response.text
                soup = BeautifulSoup(html_content, "html.parser")
                values = soup.find_all("tr")
                expected_values = [
                    "P/E Ratio TTM", "Price to Sales TTM", "Price to Cash Flow MRQ", "Price to Free Cash Flow TTM", "Price to Book MRQ",
                    "5 Year EPS Growth 5YA", "5 Year Sales Growth 5YA", "5 Year Capital Spending Growth 5YA", "Asset Turnover TTM",
                    "Inventory Turnover TTM", "Receivable Turnover TTM", "Gross margin TTM", "Operating margin TTM", "Net Profit margin TTM",
                    "Quick Ratio MRQ", "Current Ratio MRQ", "Total Debt to Equity MRQ", "Dividend Yield 5 Year Avg. 5YA", "Dividend Growth Rate ANN",
                    "Payout Ratio TTM"
                ]
                for value in values:
                    temp = value.get_text()
                    for expected_value in expected_values:
                        if expected_value in temp:
                            metric = value.find_all("td")[1].get_text()
                            data_dict[expected_value] = metric
            else:
                failed_links["links"].append(link)
                failed_links["page"].append("ratios")
                logging.error(f"error at ratios page with link: {link}")
                print(f"error at ratios page with link: {link}")
            data_list.append(data_dict)
        except Exception as e:
            logging.error(f"error in {link}: ", e)
            failed_links["links"].append(link)
            failed_links["page"].append("all")
    extension = pd.DataFrame(data_list)
    return extension, failed_links

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
            'Name' : 'name', 
            'Symbol' : 'symbol',
            'currency' : 'currency',
            'Sector' : 'sector', 
            'Industry' : 'industry', 
            'Last' : 'close', 
            'ChgPct' : 'percentage_change',
            'FundamentalMarketCap' : 'market_cap', 
            'Volume_x' : 'volume', 
            'FundamentalRatio' : 'pe', 
            'FundamentalRevenue' : 'revenue',
            'EPS' : 'eps',
            'FundamentalBeta' : 'beta', 
            'dividend' : 'dividend',
            'dividend_yield' : 'dividend_yield',
            'TechnicalDay' : 'daily_signal',
            'TechnicalWeek' : 'weekly_signal', 
            'TechnicalMonth' : 'monthly_signal',
            'PerformanceDay' : 'daily_percentage_change', 
            'PerformanceWeek' : 'weekly_percentage_change',
            'PerformanceMonth' : 'monthly_percentage_change', 
            'PerformanceYtd' : 'ytd_percentage_change',
            'PerformanceYear' : 'one_year_percentage_change', 
            'Performance3Year' : 'three_year_percentage_change', 
            'P/E Ratio TTM' : 'pe_ttm',
            'Price to Sales TTM' : 'ps_ttm', 
            'Price to Cash Flow MRQ' : 'pcf', 
            'Price to Free Cash Flow TTM' : 'pcf_ttm', 
            'Price to Book MRQ' : 'pb', 
            '5 Year EPS Growth 5YA' : 'five_year_eps_growth',
            '5 Year Sales Growth 5YA' : 'five_year_sales_growth', 
            '5 Year Capital Spending Growth 5YA' : 'five_year_capital_spending_growth',
            'Asset Turnover TTM' : 'asset_turnover', 
            'Inventory Turnover TTM' : 'inventory turnover (ttm)', 
            'Receivable Turnover TTM' : 'receivable_turnover',
            'Gross margin TTM' : 'gross_margin', 
            'Operating margin TTM' : 'operating_margin', 
            'Net Profit margin TTM' : 'net_profit_margin', 
            'Quick Ratio MRQ' : 'quick_ratio',
            'Current Ratio MRQ' : 'current_ratio', 
            'Total Debt to Equity MRQ' : 'debt_to_equity', 
            'Dividend Yield 5 Year Avg. 5YA' : 'five_year_dividend_average',
            'Dividend Growth Rate ANN' : 'dividend_growth_rate', 
            'Payout Ratio TTM' : 'payout_ratio'
        }
        cleaned_data = data[rename_cols.keys()].rename(rename_cols, axis = 1)

        cleaned_data.replace(['-', 'N/A'], np.nan, inplace=True)
        cleaned_data['revenue'] = cleaned_data['revenue'].apply(convert_to_number)
        cleaned_data['market_cap'] = cleaned_data['market_cap'].apply(convert_to_number)
        return cleaned_data
    
    elif period == "daily":
        rename_cols = {
            'Symbol' : 'symbol',
            'Last' : 'close', 
            'ChgPct' : 'percentage_change',
            'FundamentalMarketCap' : 'market_cap', 
            'Volume' : 'volume', 
            'FundamentalRatio' : 'pe', 
            'FundamentalRevenue' : 'revenue',
            'FundamentalBeta' : 'beta', 
            'TechnicalDay' : 'daily_signal',
            'TechnicalWeek' : 'weekly_signal', 
            'TechnicalMonth' : 'monthly_signal',
            'PerformanceDay' : 'daily_percentage_change', 
            'PerformanceWeek' : 'weekly_percentage_change',
            'PerformanceMonth' : 'monthly_percentage_change', 
            'PerformanceYtd' : 'ytd_percentage_change',
            'PerformanceYear' : 'one_year_percentage_change', 
            'Performance3Year' : 'three_year_percentage_change',
        }
        cleaned_data = data[rename_cols.keys()].rename(rename_cols, axis = 1)

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
    foreign_daily_data = foreign_daily_data.replace('-',np.nan)

    # Remove percentage and change data to decimal
    for i in ["daily",'weekly','monthly','ytd','one_year','three_year']:
        foreign_daily_data[f"{i}_percentage_change"] = foreign_daily_data[f"{i}_percentage_change"]/100 

    # Rename columns
    foreign_daily_data.rename(columns={"daily_percentage_change":"change_1d", "weekly_percentage_change":'change_7d', 
                        "monthly_percentage_change":"change_1m", 
                        "ytd_percentage_change":"change_ytd",
                        "one_year_percentage_change":"change_1y",
                        "three_year_percentage_change":"change_3y"}, inplace=True)

    # Delete redundant percentage change columns
    foreign_daily_data.drop("percentage_change",axis=1, inplace = True)

    # Change data type to float
    float_columns = ['close', 'market_cap', 'volume','pe', 'revenue', 'beta','change_1d',
       'change_7d', 'change_1m', 'change_ytd', 'change_1y', 'change_3y',]

    foreign_daily_data[float_columns] = foreign_daily_data[float_columns].applymap(lambda x:float(str(x).replace(',', '')))

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
    foreign_periodic_data = foreign_periodic_data.replace('-',np.nan)

    foreign_periodic_data['dividend_yield'] = foreign_periodic_data['dividend_yield'].apply(lambda x: float(x.strip('%')) / 100 if pd.notnull(x) else np.nan)

    for i in ["gross_margin","operating_margin",'net_profit_margin',"debt_to_equity","five_year_dividend_average",'dividend_growth_rate',"payout_ratio","five_year_eps_growth","five_year_sales_growth","five_year_capital_spending_growth"]:
        foreign_periodic_data[i] = foreign_periodic_data[i].apply(lambda x: float(x.replace('%', '').replace(',', '')) / 100 if pd.notnull(x) else np.nan)

    foreign_periodic_data.rename(columns={"five_year_dividend_average":"dividend_yield_5y_avg"}, inplace=True) 
    
    float_columns = ['eps', 'dividend', 'dividend_yield', 'pe_ttm', 'ps_ttm', 'pcf', 'pcf_ttm', 'pb', 'five_year_eps_growth',
       'five_year_sales_growth', 'five_year_capital_spending_growth',
       'asset_turnover', 'inventory turnover (ttm)', 'receivable_turnover',
       'gross_margin', 'operating_margin', 'net_profit_margin', 'quick_ratio',
       'current_ratio', 'debt_to_equity', 'dividend_yield_5y_avg',
       'dividend_growth_rate', 'payout_ratio']

    foreign_periodic_data[float_columns] = foreign_periodic_data[float_columns].applymap(lambda x:float(str(x).replace(',', '')))

    foreign_periodic_data = foreign_periodic_data.merge(foreign_sectors, on = ["sector",'industry']).drop(["sector",'industry'], axis=1).rename(columns={"sectors_id":"sector","sub_sector_id":"sub_sector"})

    return foreign_periodic_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update sg or my data. If no argument is specified, the sg data will be updated.")
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
    
    url_supabase = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)
    logging.basicConfig(filename="logs.log", level=logging.INFO)
    country = "my" if args.malaysia else "sg"
    sg_sectors = pd.read_csv("sectors_mapping/sectors_sg.csv", sep = ";")
    my_sectors = pd.read_csv("sectors_mapping/sectors_my.csv", sep = ";")
    foreign_sectors = my_sectors if args.malaysia else sg_sectors

    if args.monthly:
        data_general = GetGeneralData(country)
        links = data_general["Url"].tolist()
        extension, failed_links = GetAdditionalData(links)
        data_full = pd.merge(data_general, extension, on = "Url", how = "inner")
        # Retry the failed links
        n_try = 0
        if len(failed_links["links"]) != 0:
            failed_links["links"] = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]]
            while len(failed_links["links"]) != 0 or n_try < 10:
                if len(failed_links["links"]) == 0:
                    break
                new_extension, failed_links = GetAdditionalData(failed_links["links"])
                n_try += 1
            remaining = data_general[data_general["Url"].isin(failed_links["links"])]
            remaining = remaining.assign(Url = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]])
            updated_extension = pd.merge(remaining, new_extension, on = "Url", how = "inner")
            data_final = pd.concat([data_full[~data_full["Url"].isin(failed_links["links"])], updated_extension])
        else:
            data_final = data_full.copy()
        data_final = rename_and_convert(data_final, "monthly")
        data_final = clean_daily_foreign_data(data_final)
        data_final = clean_periodic_foreign_data(data_final, foreign_sectors)
    elif args.daily:
        data_general = GetGeneralData(country)
        data_general = rename_and_convert(data_general, "daily")
        data_general = clean_daily_foreign_data(data_general)
        db = "sgx_companies" if args.malaysia else "klse_companies"
        data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['close', 'market_cap', 'volume', 'pe',
        'revenue', 'beta', 'daily_signal', 'weekly_signal',
        'monthly_signal', 'change_1d', 'change_7d', 'change_1m',
        'change_ytd', 'change_1y', 'change_3y']
        data_db.drop(drop_cols, axis = 1, inplace = True)
        data_final = pd.merge(data_general, data_db, on = "symbol", how = "inner")
    data_final.to_csv("data_my.csv", index = False) if args.malaysia else data_final.to_csv("data_sg.csv", index = False)
