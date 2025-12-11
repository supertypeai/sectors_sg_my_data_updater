import requests
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

# Normalize company names function
def normalize_company_names(df: pd.DataFrame, columns_to_clean: list) -> pd.DataFrame:
    """
    Standardizes company suffixes, converts text to lowercase, and removes all periods (.).
    """
    
    # Define the mapping of common suffixes to their standardized form
    # The regex (\.|) handles both 'word' and 'word.' variations.
    replacements = {
        r'\bbhd\b(\.|)': 'berhad',
        r'\bcorp\b(\.|)': 'corporation',
        r'\bltd\b(\.|)': 'limited'
    }

    # Iterate over each column name provided
    for col in columns_to_clean:
        if col in df.columns:
            # 1. Convert the entire column to lowercase
            df[col] = df[col].astype(str).str.lower()
            
            # 2. Apply all defined suffix replacements (handles bhd. -> berhad)
            for pattern, replacement in replacements.items():
                df[col] = df[col].str.replace(
                    pat=pattern, 
                    repl=replacement, 
                    regex=True
                )
            
            # 3. GLOBAL CLEANUP: Remove ALL periods (e.g., in acronyms like y.s.g.)
            # We use regex=False for simple literal string replacement of the dot
            df[col] = df[col].str.replace('.', '', regex=False)
            
        else:
            print(f"Warning: Column '{col}' not found in the DataFrame. Skipping.")
            
    return df

proxies = {
    'http': os.getenv("proxy_url"),
    'https': os.getenv("proxy_url")
}

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.bursamalaysia.com/', 
}

payload_data = {
    "pageSize": 1088,
    "pageNo": 1, 
    "shariaMode": False,
    "filters": [],
    "selection": {
        "sku": "overview",
        "sortBy": "percentage_change",
        "sortOrder": "desc",
        "searchBy": ""
    },
    "searchBy": "",
    "sku": "overview",
    "sortBy": "percentage_change",
    "sortOrder": "desc"
}

url = "https://my.bursamalaysia.com/api/v1/market/stock-screener"
response = requests.post(url, proxies=proxies, verify=False, headers=headers, json=payload_data)

# Get data and convert to DataFrame
data = response.json()
df = pd.DataFrame(data['returnData'])

# Get existing companies from Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

curr_df = supabase.table("klse_companies").select("symbol",'name').execute()
curr_df = curr_df.data
curr_df = pd.DataFrame(curr_df)

curr_df = normalize_company_names(curr_df, ['name'])
df = normalize_company_names(df, ['name'])

comb_df = curr_df.merge(df[['stock_code','name','sector_name',"sub_sector","id"]], left_on='name', right_on='name', how='inner')

comb_df.rename(columns={"sector_name":"sector"}, inplace=True)

klse_df = pd.read_csv("sectors_mapping/top 50 klse companies sectors.csv", delimiter=";")

final_df = pd.concat([comb_df[~comb_df.symbol.isin(klse_df['symbol'].tolist())][["symbol","name","sector","sub_sector"]],klse_df[["symbol","name","sector","sub_sector"]]])

for i in range(0,final_df.shape[0]):
    supabase.table("klse_companies").update(
        {"sector": final_df.iloc[i].sector,
        "sub_sector": final_df.iloc[i].sub_sector}
    ).eq("symbol", final_df.iloc[i].symbol).execute()