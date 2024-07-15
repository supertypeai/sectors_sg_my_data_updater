from requests_html import HTMLSession
from bs4 import BeautifulSoup
import os
import json
import logging

# Set the logging level for the 'websockets' logger
logging.getLogger('websockets').setLevel(logging.WARNING)

# If you need to configure logging for requests-html as well
logging.getLogger('requests_html').setLevel(logging.WARNING)


BASE_URL = "https://www.tradingview.com/symbols/MYX-"

SYMBOL_MAP = {
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

def get_url(symbol: str) -> str:
  return f"{BASE_URL}{symbol}"

def read_page(url: str) -> BeautifulSoup | None:
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

def scrap_stock_page( symbol : str) -> dict :
  if (symbol in SYMBOL_MAP):
    new_symbol = SYMBOL_MAP[symbol]
  else:
    new_symbol = symbol
  url = get_url(new_symbol)
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
  


def scrap_null_data():
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
        data_dict = scrap_stock_page(symbol)
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
