import json
import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import os
import logging
from requests_html import HTMLSession
import urllib.request
import time
from urllib.request import urlopen, Request
import ssl


# Set the logging level for the 'websockets' logger
logging.getLogger('websockets').setLevel(logging.WARNING)

# If you need to configure logging for requests-html as well
logging.getLogger('requests_html').setLevel(logging.WARNING)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'

HEADERS = {
        "User-Agent": USER_AGENT,
        "Referer": "https://www.investing.com/",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

# INVESTING_API_SG = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=36&filter-domain=&page=0&page-size=1000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"
INVESTING_API_MY = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default?fields-list=id%2Cname%2Csymbol%2CisCFD%2Chigh%2Clow%2Clast%2ClastPairDecimal%2Cchange%2CchangePercent%2Cvolume%2Ctime%2CisOpen%2Curl%2Cflag%2CcountryNameTranslated%2CexchangeId%2CperformanceDay%2CperformanceWeek%2CperformanceMonth%2CperformanceYtd%2CperformanceYear%2Cperformance3Year%2CtechnicalHour%2CtechnicalDay%2CtechnicalWeek%2CtechnicalMonth%2CavgVolume%2CfundamentalMarketCap%2CfundamentalRevenue%2CfundamentalRatio%2CfundamentalBeta%2CpairType&country-id=42&filter-domain=&page=0&page-size=2000&limit=0&include-additional-indices=false&include-major-indices=false&include-other-indices=false&include-primary-sectors=false&include-market-overview=false"

# SYMBOL_MAP = {
#   "HEKR" : "/equities/hektar-reit", # REITs
#   "KIPR" : "/equities/kip-real-estate-investment-trust", # REITs
#   "TWRE" : "/equities/tower-real-estate", # REITs
#   "PREI" : "/equities/pavilion-real-estate-inv-trust", # REITs
#   "YTLR" : "/equities/ytl-hospitality-reit", # REITs
#   "KLCC" : "/equities/klcc-prop-reits---stapled-sec", # REITs
#   "CAMA" : "/equities/capitamalls-malaysia-trust", # REITs
#   "ATRL" : "/equities/atrium", # REITs
#   "UOAR" : "/equities/uoa-reit", # REITs
#   "AMRY" : "/equities/amanahraya-real-estate", # REITs
#   "ALQA" : "/equities/al-aqar-healthcare-reit", # REITs
#   "SUNW" : "/equities/sunway-real-estate-invest-trust", # REITs
#   "AMFL" : "/equities/amfirst", # REITs
#   "SENT" : "/equities/mrcb-quill-unit", # REITs
#   "AXSR" : "/equities/axis-real-estate-investment-trust", # REITs
#   "IGRE" : "/equities/igb-real-estate-investment-trust", # REITs
#   "PROL" : "/equities/prolintas-infra-business-trust-unt", # Business Trusts
#   "PELK" : "/equities/pelikan-international-corp-company-profile"
# }

def complete_url(url: str) -> str:
  return f"https://www.investing.com{url}-company-profile"

def get_investing_api():
    try:
        url = INVESTING_API_MY
        # Set up SSL context to ignore SSL certificate errors
        ssl._create_default_https_context = ssl._create_unverified_context
        
        # Set up proxy support
        proxy = os.environ.get("PROXY")
        if proxy:
            proxy_support = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        
        # Fetch the URL content
        request = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            print(f"Response status code: {status_code} for {url}")
            
            if status_code == 200:
                html = response.read()
                data_from_api = json.loads(html)
                return data_from_api['data']
  
            else:
                print(f"Failed to open {url}: Status code {status_code}")
                return None
    except Exception as e:
        print(f"Failed to open {url}: {e}")
        return None

# data_list = get_investing_api()
def get_url_from_api(data_list: list, investing_symbol: str) -> str | None:
  for data in data_list:
    if (data['Symbol'] == investing_symbol):
       return data['Url']
  
  return None
# print(get_url_from_api(data_list, "PELK"))

def read_page(url: str) -> BeautifulSoup | None:
    try:
        # Set up SSL context to ignore SSL certificate errors
        ssl._create_default_https_context = ssl._create_unverified_context
        
        # Set up proxy support
        proxy = os.environ.get("PROXY")
        if proxy:
            proxy_support = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        
        # Fetch the URL content
        request = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            print(f"Response status code: {status_code} for {url}")
            
            if status_code == 200:
                html = response.read()
                if html:
                    soup = BeautifulSoup(html, "html.parser")
                    return soup
                else:
                    print(f"Failed to read content from {url}")
                    return None
            else:
                print(f"Failed to open {url}: Status code {status_code}")
                return None
    except Exception as e:
        print(f"Failed to open {url}: {e}")
        return None

def scrap_null_data():
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data_klse.json') for i in range(1,5)]

  # Get data from api
  data_list_api = get_investing_api()

  # Iterate each file
  file_idx = 0
  for file_path in data_file_path:
    file_idx += 1

    f = open(file_path)
    all_data_list = json.load(f)
    null_list = []


    for i in range(len(all_data_list)):
      data = all_data_list[i]
      if (data['sector'] is None or data['sub_sector'] is None):
        null_list.append({"idx" : i, "data" : data})

    for null_data in null_list:
      symbol = null_data['data']['investing_symbol']
      suffix_url = get_url_from_api(data_list_api, symbol)

      # Get URL to investing.com
      if suffix_url is not None:
        url = complete_url(suffix_url)
        soup = None

        data_dict = {
          "investing_symbol" : symbol,
          "sector" : None,
          "sub_sector" : None
        }

        attempt = 1
        while (data_dict['sector'] is None and data_dict['sub_sector'] is None and attempt <= 3):
          soup = read_page(url)
          if (soup is not None):
            try:
              company_header = soup.find("div", {"class" : "companyProfileHeader"})
              needed_data = company_header.findAll("a")
              sector = None
              sub_sector = None
              if (len(needed_data) == 2):
                sector = needed_data[1].get_text().replace(u'\xa0', u' ')
                sub_sector = needed_data[0].get_text().replace(u'\xa0', u' ')
              else:
                print(f"There is no 2 needed data on {url}")
              
              data_dict['sector'] = sector
              data_dict['sub_sector'] = sub_sector

              null_data['data'] = data_dict
            except:
              print(f"Failed to get data from {url}")
          else:
            print(f"Detected None type for Beautifulsoup for {url}")

          if (data_dict['sector'] is None and data_dict['sub_sector'] is None):
            print(f"Failed to get data from {url} on attempt {attempt}. Retrying...")
          attempt +=1

      else:
        print(f"This data for {symbol} is not provided in the API")

    # After done with each file
    for null_data in null_list:
      all_data_list[null_data['idx']] = null_data['data']
    
    # Save last
    filename = f"P{file_idx}_data_klse.json"
    print(f"==> Finished data is exported in {filename}")
    file_path = os.path.join(cwd, "data", filename)

    # Save to JSON
    with open(file_path, "w") as output_file:
      json.dump(all_data_list, output_file, indent=2)


