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

def GetGeneralData(country):
    if country == "sg":
        url ="https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    elif country == "my":
        url = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=42&filter-domain=&page=0&page-size=2000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for i in range(10):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            data = pd.DataFrame(json_data["data"])
            break
        else:
            continue
    return data

def GetAdditionalData(links):
    data_list = []
    failed_links = {
        "links" : [],
        "page" : []
    }
    for link in links:
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

def rename_and_convert(data):
    rename_cols = {
        'Name' : 'name', 
        'Symbol' : 'symbol',
        'currency' : 'currency',
        'Sector' : 'sector', 
        'Industry' : 'industry', 
        'close' : 'close', 
        'change_percent' : 'percentage_change',
        'Market Cap' : 'market_cap', 
        'Volume_y' : 'volume', 
        'P/E Ratio' : 'pe', 
        'Revenue' : 'revenue',
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

logging.basicConfig(filename="logs.log", level=logging.INFO)
data = GetGeneralData("sg")
links = data["Url"].tolist()
extension, failed_links = GetAdditionalData(links)
data_full = pd.merge(data, extension, on = "Url", how = "inner")
# Retry the failed links
n_try = 0
failed_links["links"] = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]]
while len(failed_links["links"]) != 0 or n_try < 10:
    print(f"iterasi ke-{n_try+1}")
    if len(failed_links["links"]) == 0:
        break
    new_extension, failed_links = GetAdditionalData(failed_links["links"])
    n_try += 1
remaining = data[data["Url"].isin(failed_links["links"])]
remaining = remaining.assign(Url = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]])
updated_extension = pd.merge(remaining, new_extension, on = "Url", how = "inner")
data_final = pd.concat([data_full[~data_full["Url"].isin(failed_links["links"])], updated_extension])
data_final = rename_and_convert(data_final)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument("-sg", "--singapore", action="store_true", default=False, help="Update singapore data")
    parser.add_argument("-my", "--malaysia", action="store_true", default=False, help="Update malaysia data")

    args = parser.parse_args()
    if args.singapore and args.malaysia:
        print("Error: Please specify either -a or -q, not both.")
        raise SystemExit(1)
    
    logging.basicConfig(filename="logs.log", level=logging.INFO)
    country = "my" if args.malaysia else "sg" 
    data = GetGeneralData(country)
    links = data["Url"].tolist()
    extension, failed_links = GetAdditionalData(links)
    data_full = pd.merge(data, extension, on = "Url", how = "inner")
    # Retry the failed links
    n_try = 0
    failed_links["links"] = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]]
    while len(failed_links["links"]) != 0 or n_try < 10:
        print(f"iterasi ke-{n_try+1}")
        if len(failed_links["links"]) == 0:
            break
        new_extension, failed_links = GetAdditionalData(failed_links["links"])
        n_try += 1
    remaining = data[data["Url"].isin(failed_links["links"])]
    remaining = remaining.assign(Url = [link.split("?")[0] if "?" in link else link for link in failed_links["links"]])
    updated_extension = pd.merge(remaining, new_extension, on = "Url", how = "inner")
    data_final = pd.concat([data_full[~data_full["Url"].isin(failed_links["links"])], updated_extension])
    data_final = rename_and_convert(data_final)
    data.to_csv("data_my.csv", index = False) if args.malaysia else data.to_csv("data_sg.csv", index = False)