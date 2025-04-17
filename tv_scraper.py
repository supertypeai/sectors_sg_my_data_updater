from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import time
import re

from datetime import datetime
import pandas as pd

from supabase import create_client
from dotenv import load_dotenv

def clean_value(value):
    cleaned = re.sub(r'[\u202a\u202c\u202f]', '', value)
    return cleaned.replace('−', '-')

def convert_to_numeric(value):
    mult = {'T':1e12, 'B':1e9, 'M':1e6, 'K':1e3}
    if value and value[-1] in mult:
        return int(float(value[:-1]) * mult[value[-1]])
    return float(value)

def get_eps_growth(driver):
    html = driver.find_element(
        By.XPATH,
        '//*[@id="js-category-content"]/div[2]/div/div[1]/div[2]/div/div[3]/div[2]/div/div[1]'
    ).get_attribute("innerHTML")
    soup = BeautifulSoup(html, 'html.parser')
    
    years = [d.get_text(strip=True) for d in soup.select('.values-OWKkVLyj .value-OxVAcLqi')]
    estimated = [clean_value(d.get_text(strip=True)) for d in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]
    
    locked_years_count = len(soup.select('[data-name="Estimate"] .alignLeft-OxVAcLqi'))
    estimated_years = years[locked_years_count:]
    
    pairs = [(y, e) for y, e in zip(estimated_years, estimated) if re.match(r'^\d{4}$', y)]
    if not pairs:
        return pd.DataFrame(columns=['year', 'eps_estimate'])
    
    years_list, estimated_list = zip(*pairs)
    df = pd.DataFrame({'year': years_list, 'eps_estimate': estimated_list})
    df['eps_estimate'] = df['eps_estimate'].apply(convert_to_numeric)
    df['year'] = df['year'].astype(int)
    return df[df.year == datetime.now().year + 1]

def get_revenue(driver):
    html = driver.find_element(
        By.XPATH,
        '//*[@id="js-category-content"]/div[2]/div/div[1]/div[2]/div/div[7]/div[2]/div/div[1]'
    ).get_attribute("innerHTML")
    soup = BeautifulSoup(html, 'html.parser')
    
    years = [d.get_text(strip=True) for d in soup.select('.values-OWKkVLyj .value-OxVAcLqi')]
    estimated = [clean_value(d.get_text(strip=True)) for d in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]
    
    locked_years_count = len(soup.select('[data-name="Estimate"] .alignLeft-OxVAcLqi'))
    estimated_years = years[locked_years_count:]
    
    pairs = [(y, e) for y, e in zip(estimated_years, estimated) if re.match(r'^\d{4}$', y)]
    if not pairs:
        return pd.DataFrame(columns=['year', 'revenue_estimate'])
    
    years_list, estimated_list = zip(*pairs)
    df = pd.DataFrame({'year': years_list, 'revenue_estimate': estimated_list})
    df['revenue_estimate'] = df['revenue_estimate'].apply(convert_to_numeric)
    df['year'] = df['year'].astype(int)
    return df[df.year == datetime.now().year + 1]

# ————— Setup Supabase —————
load_dotenv()
db  = "sgx_companies"
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

active_stock = supabase.table(db).select("symbol").execute()
symbols = pd.DataFrame(active_stock.data).symbol.tolist()
print(f"[DEBUG] Retrieved symbols: {symbols}")

# ————— Selenium Setup —————
chrome_options = webdriver.ChromeOptions()
for opt in ["--window-size=1200,1200", "--ignore-certificate-errors"]:
    chrome_options.add_argument(opt)

driver = webdriver.Chrome(service=Service(), options=chrome_options)
wait   = WebDriverWait(driver, 10)

# ————— Scrape Loop —————
eps_records = []
rev_records = []

for symbol in symbols:
    print(f"\n[DEBUG] Processing {symbol}")
    driver.get(f"https://www.tradingview.com/symbols/SGX-{symbol}/forecast/")
    time.sleep(3)

    # click *all* "Annual" (FY) tabs on the page
    try:
        fy_buttons = wait.until(EC.presence_of_all_elements_located((By.ID, "FY")))
        for btn in fy_buttons:
            try:
                btn.click()
            except:
                driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
        print(f"[DEBUG] Clicked {len(fy_buttons)} Annual toggles")
    except Exception as e:
        print(f"[DEBUG] Could not click Annual toggles")

    # scrape EPS
    try:
        df_e = get_eps_growth(driver)
        print(f"[DEBUG] EPS rows: {len(df_e)}")
        for _, r in df_e.iterrows():
            eps_records.append((symbol, r.year, r.eps_estimate))
    except Exception as e:
        # print(f"[DEBUG] EPS error: {e}")
        continue

    # scrape Revenue
    try:
        df_r = get_revenue(driver)
        print(f"[DEBUG] Revenue rows: {len(df_r)}")
        for _, r in df_r.iterrows():
            rev_records.append((symbol, r.year, r.revenue_estimate))
    except Exception as e:
        # print(f"[DEBUG] Revenue error: {e}")
        continue

driver.quit()

# ————— Build & Summarize —————
df_eps = pd.DataFrame(eps_records, columns=["symbol","year","eps_estimate"])
df_rev = pd.DataFrame(rev_records, columns=["symbol","year","revenue_estimate"])
df_all = df_eps.merge(df_rev, on=["symbol","year"], how="outer")

eps_syms = set(df_eps.symbol)
rev_syms = set(df_rev.symbol)
both, eps_only, rev_only, none = (
    sorted(eps_syms & rev_syms),
    sorted(eps_syms - rev_syms),
    sorted(rev_syms - eps_syms),
    sorted(set(symbols) - (eps_syms|rev_syms))
)

print("\n=== Coverage ===")
print(f"Both:       {both}")
print(f"EPS only:   {eps_only}")
print(f"Rev only:   {rev_only}")
print(f"No data:    {none}")

print("\n=== Shapes ===")
print(f"df_eps: {df_eps.shape}")
print(f"df_rev: {df_rev.shape}")
print(f"df_all: {df_all.shape}")

print("\n=== Sample Combined ===")
print(df_all.sort_values(["symbol","year"]))
