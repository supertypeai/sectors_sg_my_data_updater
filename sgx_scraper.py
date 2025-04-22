import os
import time
import glob
import traceback
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc
from supabase import create_client  # ensure supabase-py is installed
from dotenv import load_dotenv

# ---------------------------
# Configuration
# ---------------------------
load_dotenv()

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
if not supabase_url or not supabase_key:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")

download_dir = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_dir, exist_ok=True)

print("[DEBUG] Initializing Supabase client...")
supabase = create_client(supabase_url, supabase_key)
print("[DEBUG] Supabase client initialized.")

print("[DEBUG] Setting up undetected-chromedriver...")
options = uc.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-blink-features=AutomationControlled")

prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)
ua = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
)
options.add_argument(f"--user-agent={ua}")

driver = None
try:
    driver = uc.Chrome(options=options)
    print("[DEBUG] Chrome launched.")

    driver.get("https://investors.sgx.com/stock-screener")
    print("[DEBUG] Loaded stock screener page.")
    time.sleep(5)

    # Remove blocking overlay if present
    try:
        driver.execute_script(
            "const overlay = document.querySelector('div.sgx-header-overlay');"
            "if (overlay) overlay.remove();"
        )
        print("[DEBUG] Removed header overlay.")
    except Exception:
        pass

    # Click More Actions
    more_btn = driver.find_element(By.CSS_SELECTOR, ".widget-stock-screener-toolbar-button--more-actions")
    try:
        more_btn.click()
    except:
        driver.execute_script("arguments[0].click();", more_btn)
    time.sleep(1)

    # Click Download
    download_link = driver.find_element(By.CSS_SELECTOR, "a[data-id='download']")
    try:
        download_link.click()
    except:
        driver.execute_script("arguments[0].click();", download_link)

    # Wait for CSV to finish downloading
    timeout, poll, elapsed = 30, 0.5, 0
    csv_path = None
    while elapsed < timeout:
        files = glob.glob(os.path.join(download_dir, "*.csv"))
        if files:
            candidate = max(files, key=os.path.getctime)
            if not candidate.endswith(".crdownload"):
                csv_path = candidate
                break
        time.sleep(poll)
        elapsed += poll
    if not csv_path:
        raise FileNotFoundError("CSV download did not complete within timeout.")
    print(f"[DEBUG] Downloaded CSV at: {csv_path}")

    # Read CSV and build lookup
    df = pd.read_csv(csv_path)
    required = ['Code']
    if 'Code' not in df.columns:
        raise KeyError("CSV missing required 'Code' column.")

    # Clean symbols
    df['Code'] = df['Code'].astype(str).str.strip()
    csv_codes = set(df['Code'])
    print(f"[DEBUG] Found {len(csv_codes)} unique codes in CSV.")

    # Fetch existing symbols
    resp = supabase.table("sgx_companies_test").select("symbol").execute()
    existing = {r['symbol'] for r in getattr(resp, 'data', resp)}
    print(f"[DEBUG] Retrieved {len(existing)} existing symbols from DB.")

    # Determine status updates
    to_activate = list(csv_codes & existing)
    to_deactivate = list(existing - csv_codes)
    print(f"[DEBUG] Will activate {len(to_activate)} and deactivate {len(to_deactivate)} records.")

    # Activate existing
    if to_activate:
        supabase.table("sgx_companies_test").update({"is_active": True}) \
                 .in_("symbol", to_activate).execute()

    # Deactivate missing
    if to_deactivate:
        supabase.table("sgx_companies_test").update({"is_active": False}) \
                 .in_("symbol", to_deactivate).execute()

    # NOTE: Upsert logic removed per requirements

    # Cleanup downloaded file
    os.remove(csv_path)
    print(f"[DEBUG] Deleted downloaded CSV: {csv_path}")

except Exception as e:
    print(f"[ERROR] {e}")
    traceback.print_exc()
finally:
    if driver:
        driver.quit()
        print("[DEBUG] WebDriver closed.")
