from bs4 import BeautifulSoup
import json
import logging
import os
import time
import urllib.request
from requests_html import HTMLSession

# Set the logging level for the 'websockets' logger
logging.getLogger('websockets').setLevel(logging.WARNING)

# If you need to configure logging for requests-html as well
logging.getLogger('requests_html').setLevel(logging.WARNING)



# Setup Constant 
BASE_URL = "https://my.bursamalaysia.com/stock-details?stockcode="
USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'

SYMBOL_MAP = {
  # "HEKR" : "", # REITs
  # "KIPR" : "", # REITs
  # "TWRE" : "", # REITs
  "LBSBq" : "LBSB",
  # "PREI" : "", # REITs
  # "YTLR" : "", # REITs
  # "KLCC" : "", # REITs
  # "CAMA" : "", # REITs
  "SETIq" : "SETI",
  "ANNJq" : "ANNJ",
  # "ATRL" : "", # REITs
  "YONGq" : "YONG",
  # "UOAR" : "", # REITs
  # "AMRY" : "", # REITs
  # "ALQA" : "", # REITs
  # OMHO -> Special Case, ada Sector tapi ga ada SubSector
  # MDCH -> Special Case, ada Sector tapi ga ada SubSector
  # "SUNW" : "", # REITs
  # "AMFL" : "", # REITs
  # "SENT" : "", # REITs
  # "AXSR" : "", # REITs
  "SWAYq" : "SWAY",
  # "IGRE" : "", # REITs
  # "PROL" : "", # Business Trusts
  "PELK" : "PBSB"
}

def get_url(base_url: str, symbol: str) -> str:
  return f"{base_url}{symbol}.KL"

def read_page(url: str):
  try:
    headers = {'User-Agent': USER_AGENT}
    request = urllib.request.Request(url,None,headers)
    time.sleep(2)
    response = urllib.request.urlopen(request).read()
    soup = BeautifulSoup(response, 'html.parser')
    return soup
  
  except Exception as e:
    print(f"Failed to open {url}: {e}")
    return None

def read_page_session(url: str):
  try:
    session = HTMLSession()
    response = session.get(url)
    response.html.render(sleep=5, timeout=10)

    soup = BeautifulSoup(response.html.html, "html.parser")
    return soup
  except Exception as e:
    print(f"Failed to open {url}: {e}")
    return None
  finally:
    session.close()
    print(f"Session in {url} is closed")

def scrap_stock_page(base_url: str, symbol: str, new_symbol: str) -> dict | None:
  url = get_url(base_url, new_symbol)
  soup = read_page(url)
  
  sector = None
  sub_sector = None

  if (soup is not None):

    print(f"Scraping from {url}")
    # Get Sector
    try:
      sector_elm = soup.findAll("a", {"class": "stock-links"})
      sector = sector_elm[0].get_text()
      if (len(sector_elm) > 1):
        sub_sector = sector_elm[1].get_text()
      else:
        sub_sector = sector
    except:
      print(f"Failed to get Sector and Subsector data from {url}")
      sector = None
      sub_sector = None

    if (sector is not None and sub_sector is not None):
      print(f"Successfully scrap from {symbol} stock page")
    else:
      if (sector is None):
        print(f"Detected None type for Sector variable from {symbol} stock page")
      if (sub_sector is None):
        print(f"Detected None type for Sector variable from {symbol} stock page")
    
    stock_data = dict()
    stock_data['investing_symbol'] = symbol
    stock_data['sector'] = sector
    stock_data['sub_sector'] = sub_sector

    return stock_data
  else:
    print(f"None type of BeautifulSoup")
    return None

def scrap_function_my(symbol_list: list, process_idx: int):
  print(f"==> Start scraping from process P{process_idx}")
  all_data = []
  cwd = os.getcwd()
  start_idx = 0
  count = 0

  # Iterate in symbol list
  for i in range(start_idx, len(symbol_list)):
    attempt_count = 1
    symbol = symbol_list[i]

    if (symbol is not None):
      scrapped_data = {
        "investing_symbol" : symbol,
        "sector" : None,
        "sub_sector" : None
      }

      # Check if symbol is in symbol_list
      if (symbol in SYMBOL_MAP):
        new_symbol = SYMBOL_MAP[symbol]
      else:
        new_symbol = symbol

      # Handling for page that returns None although it should not
      while ((scrapped_data is None or (scrapped_data['sector'] is None and scrapped_data['sub_sector'] is None)) and attempt_count <= 3):
        scrapped_data = scrap_stock_page(BASE_URL, symbol, new_symbol)

        if (scrapped_data is None or (scrapped_data['sector'] is None and scrapped_data['sub_sector'] is None)):
          print(f"Data not found! Retrying.. Attempt: {attempt_count}")
        attempt_count += 1

      all_data.append(scrapped_data)

    if (i % 10 == 0 and count != 0):
      print(f"CHECKPOINT || P{process_idx} {i} Data")
    
    count += 1
  
  # Save last
  filename = f"P{process_idx}_data_sgx.json"
  print(f"==> Finished data is exported in {filename}")
  file_path = os.path.join(cwd, "data", filename)

  # Save to JSON
  with open(file_path, "w") as output_file:
    json.dump(all_data, output_file, indent=2)

  return all_data




ADDITIONAL_BASE_URL = "https://www.tradingview.com/symbols/MYX-"

ADDITIONAL_SYMBOL_MAP = {
  "HEKR" : "HEKTAR", # REITs
  "KIPR" : "KIPREIT", # REITs
  "TWRE" : "TWRREIT", # REITs
  "PREI" : "PAVREIT", # REITs
  "YTLR" : "YTLREIT", # REITs
  # "KLCC" : "KLCC", # REITs
  "CAMA" : "CLMT", # REITs
  "ATRL" : "ATRIUM", # REITs
  "UOAR" : "UOAREIT", # REITs
  "AMRY" : "ARREIT", # REITs
  "ALQA" : "ALAQAR", # REITs
  "SUNW" : "SUNREIT", # REITs
  "AMFL" : "AMFIRST", # REITs
  "SENT" : "SENTRAL", # REITs
  "AXSR" : "AXREIT", # REITs
  "IGRE" : "IGBREIT", # REITs
  "PROL" : "PLINTAS", # Business Trusts
}

def scrap_stock_page_additional( symbol : str) -> dict :
  if (symbol in SYMBOL_MAP):
    new_symbol = SYMBOL_MAP[symbol]
  else:
    new_symbol = symbol
  url = get_url(ADDITIONAL_BASE_URL, new_symbol)
  soup = read_page(url)

  data_dict = {
    "symbol" : symbol,
    "sector" : None,
    "sub_sector" : None
  }

  if (soup is not None):
    try:
      container = soup.find("div", {"data-container-name" : "company-info-id"})
      needed_data = container.findAll("a")
      sector = None
      sub_sector = None
      if (len(needed_data) > 1):
        sector = needed_data[0].get_text().replace(u'\xa0', u' ')
        sub_sector = needed_data[1].get_text().replace(u'\xa0', u' ')
      else:
        print(f"There is at least 2 data needed on {url}")
      
      data_dict['sector'] = sector
      data_dict['sub_sector'] = sub_sector

      return data_dict
    except:
      print(f"Failed to get data from {url}")
      return data_dict
  else:
    print(f"Detected None type for Beautifulsoup for {url}")

def scrap_null_data_my():
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data_klse.json') for i in range(1,5)]


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

      attempt = 1
      while ( attempt <= 3):
        data_dict = scrap_stock_page_additional(symbol)
        null_data['data'] = data_dict

        if (data_dict['sector'] is not None and data_dict['sub_sector'] is not None):
          print(f"Successfully get data for stock {symbol}")
          break
        else:
          print(f"Failed to get data from {symbol} on attempt {attempt}. Retrying...")
        attempt +=1

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
