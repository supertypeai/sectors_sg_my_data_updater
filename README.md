# Financial Data and Sector Scraper Project

This Python project consolidates data fetching, cleaning, and scraping functionalities for financial datasets. It integrates market data from multiple sources including Yahoo Finance, Investing.com, and various stock exchange web pages, and updates a Supabase database with the scraped results. The project targets companies in the Singapore (SGX) and Malaysian (KLSE) markets, with additional functionalities for financial data normalization and historical analysis.

---

# Technical Features

This section details the technical implementation of the project's core features by referencing the key functions and modules from each Python file.

---

## Data Cleaning and Normalization

- **cleansing.py**
  - **`clean_daily_foreign_data`**:  
    Processes daily data retrieved from external APIs by:
    - Replacing placeholder '-' entries with `NaN`.
    - Converting percentage change columns (daily, weekly, monthly, YTD, one-year, three-year) into decimals (dividing by 100).
    - Renaming columns (e.g., renaming "daily_percentage_change" to "change_1d") and dropping redundant columns.
    - Ensuring numerical integrity by converting designated columns (e.g., `close`, `market_cap`, `volume`, etc.) from string representations (with potential commas) into floats.

  - **`clean_periodic_foreign_data`**:  
    Applies cleaning procedures on periodic financial data by:
    - Replacing placeholder '-' values with `NaN`.
    - Converting percentage strings (e.g., dividend yield, margins, growth rates) into float values by stripping symbols and dividing by 100.
    - Renaming columns (e.g., "five_year_dividend_average" to "dividend_yield_5y_avg").
    - Merging the cleaned data with an external sector mapping DataFrame to standardize sector and sub-sector identifiers.

---

## Multi-Source Financial Data Retrieval

- **financial_data_yf.py**
  - **`fetch_existing_symbol`**:  
    Retrieves a list of existing company symbols from a Supabase table (handling both SGX and KLSE depending on the provided country parameter).

  - **`upsert_db`**:  
    Iterates through DataFrame columns (excluding 'symbol') to update the corresponding entries in the Supabase database. It includes:
    - Handling missing values and converting earnings/revenue data from JSON strings if necessary.
    - Logging and exception handling for each field update.
  
  - **`fetch_div_ttm` & `update_div_ttm`**:  
    - **`fetch_div_ttm`**: Fetches dividend data for a given ticker using `yfinance.Ticker`, aggregating dividend amounts over the last 365 days and converting them according to a retrieved currency conversion rate.
    - **`update_div_ttm`**: Applies an exponential backoff strategy with retry logic to ensure robust dividend data fetching per symbol before performing an upsert to the database.
  
  - **`earnings_fetcher` & `update_historical_data`**:  
    - **`earnings_fetcher`**: Extracts yearly net income and revenue from ticker financial statements, including calculation for trailing twelve month (TTM) figures and JSON conversion of the results.
    - **`update_historical_data`**: Iterates over a list of symbols to compile historical financial information, incorporating the output of `earnings_fetcher` into a final DataFrame that is subsequently upserted into the database.

---

## Web Scraping for Sector Information

- **sector_scraper_klse.py**
  - **`scrap_stock_page`**:  
    Leverages `urllib.request` with custom headers and `BeautifulSoup` to parse HTML pages from a targeted base URL. It extracts:
    - Sector and sub-sector information from specific HTML elements.
    - Implements basic retry logic and error notifications in cases where data retrieval is incomplete.
  
  - **`scrap_function_my`**:  
    Wraps around the scraping function to iterate through a list of symbols for KLSE, controlling:
    - Retrying of requests (up to three attempts per symbol) if sector data is missing.
    - Logging checkpoints and saving intermediate JSON outputs per process.

- **sector_scraper_sgx.py**
  - **`scrap_stock_page`**:  
    Similar to the KLSE scraper but focuses on SGX where it:
    - Retrieves industry and sub-industry data from designated HTML classes.
    - Uses multiple base URLs and symbol mappings to improve data coverage.
  
  - **`scrap_function_sg`**:  
    Processes a list of SGX symbols with looped retries across multiple endpoints and incorporates delays between requests to prevent rate-limiting issues.

---

## Database Integration

- **sector_scraper_main.py**
  - **`combine_data`**:  
    Merges multiple JSON outputs produced by the scraping functions into a single Pandas DataFrame by:
    - Sorting and aligning data on a specified symbol column.
    - Performing DataFrame updates and merging with pre-existing database data.
    - Generating final JSON (and optionally CSV) files to be upserted into the Supabase database.

- **financial_data_yf.py** (within the upsert functions) and **sg_my_scraper.py**:  
  Both integrate data processing routines with database upsert and update operations via the Supabase client, ensuring that the gathered financial information is centrally stored and updated.

---

## Multiprocessing and Retry Strategies

- **sector_scraper_main.py**
  - Implements Python’s `multiprocessing.Process` to parallelize the scraping across multiple processes.
  - Splits the symbols list into chunks to be handled concurrently by the respective scraping functions (`scrap_function_my` and `scrap_function_sg`).
  - Integrates retry mechanisms at multiple levels (within `scrap_function_my`/`scrap_function_sg` and their underlying page-scraping functions) to handle transient failures.

- **financial_data_yf.py**
  - Utilizes an exponential backoff approach (with incremental delays) in `update_div_ttm` to handle rate limits and temporary network issues during dividend data fetching.

---

## Custom Caching and Rate Limiting

- **yf_custom.py**
  - **`YFSession`** and Custom **`Ticker`** Class:
    - Extends `yfinance.Ticker` to employ a customized session (`YFSession`) that integrates caching (via `SQLiteCache` from `requests_cache`) to store and reuse API responses, significantly reducing redundant network calls.
    - Implements a rate limiting mechanism using `pyrate_limiter` and `requests_ratelimiter` to restrict API request frequency (max 2 requests per 2 seconds).
    - This design ensures that data retrieval from Yahoo Finance is both efficient and adheres to API usage policies.

---

## Additional Data Processing (sg_my_scraper.py)

- **`GetGeneralData`**:  
  Fetches general market data from an Investing.com API endpoint and converts the JSON response into a Pandas DataFrame.
  
- **`yf_data_updater`**:  
  Updates key financial metrics (market cap, volume, PE ratio, etc.) using historical data fetched via `yfinance.history`, and applies currency conversions where necessary.
  
- **`update_historical_dividends`**:  
  Computes dividend breakdowns by year, calculating yields based on the latest close prices from historical data.
  
- **`update_all_time_price` & `update_change_data`**:  
  - **`update_all_time_price`**: Calculates various price extremes (YTD, 52-week, 90-day, and all-time lows/highs) by processing the full historical price data.
  - **`update_change_data`**: Computes percentage changes over different periods (YTD, one-year, three-year) by comparing the latest close price with historical values.
  
- **`employee_updater`**:  
  Retrieves and merges employee numbers from both an SGX API and Yahoo Finance, using fallback logic if one source is unavailable.

- **`rename_and_convert`**:  
  Standardizes column names and converts financial figures (e.g., handling shorthand notations like "B" for billion) to numerical values.

---


## Installation

1. **Clone the Repository:**

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Install Requirements:**

   Make sure you have Python 3.7+ and install dependencies with:

   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables:**

   Create a `.env` file in the project root with the following variables:

   ```env
   SUPABASE_URL=<your_supabase_url>
   SUPABASE_KEY=<your_supabase_key>
   ```

---

## Usage

- **Data Cleaning:**  
  Run cleansing functions within your Python scripts to preprocess raw financial data.

- **Financial Data Updating:**  
  Execute `financial_data_yf.py` to fetch current market data, update dividend information, and refresh earnings details in your Supabase database.

- **Sector Scraping:**  
  Run `sector_scraper_main.py` with proper command-line arguments (`SG` or `MY`) to start the multiprocessing scraping process:
  
  ```bash
  python sector_scraper_main.py SG
  ```
  
  or
  ```bash
  python sector_scraper_main.py MY

- **Additional Data Retrieval:**  
  Use `sg_my_scraper.py` to update a range of financial metrics including historical prices, dividend breakdowns, and relative change calculations.

- **Custom Yahoo Finance Ticker:**  
  The `yf_custom.py` module is automatically used by other components to fetch financial data with enhanced caching and rate limiting.