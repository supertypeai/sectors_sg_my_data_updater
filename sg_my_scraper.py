import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta

# Parallel Yahoo fetches (8 workers, shared pool). Keeps under Yahoo's per-IP
# rate limit while cutting wall time ~3x vs sequential (measured PoC).
_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=8)

# Transient-failure retries (429 / network drops).
_MAX_RETRIES = 3


def _retry_backoff(attempt):
    """Sleep 2s, 4s, 8s... after attempts 1, 2, 3..."""
    time.sleep(2 ** (attempt - 1))

# No +1y analyst estimate: strip one_year_eps_growth from the payload
# (DB keeps its existing value).
_NO_ESTIMATE_SYMBOLS = set()

# Yahoo fetch failed (429/network): strip recomputed columns from the payload
# so NaN -> NULL never wipes the existing good DB value.
_FAILED_SYMBOLS = set()

import numpy as np
import pandas as pd
import requests
from supabase import create_client

import yf_custom as yf
import re
from symbol_utils import bare_symbol, with_suffix

# Symbols are stored with their exchange suffix ("D05.SI", "1155.KL"), while
# Yahoo tickers and the SGX APIs are built from the bare code. Strip before
# re-appending an extension so a stored symbol never becomes "D05.SI.SI".


def recursively_clean_nans(obj):
    if isinstance(obj, dict):
        return {k: recursively_clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursively_clean_nans(e) for e in obj]
    if pd.isna(obj):
        return None
    return obj

def safe_relative_diff(num1: float, num2: float):
    if num1 == 0:
        return 0
    if num2 == 0:
        return num1
    return (num1 / num2) - 1


def yf_data_updater(data_prep: pd.DataFrame, country, rates=None):
    """Fetch per-symbol Yahoo data in parallel (shared 8-thread pool).
    `rates` is the currency-conversion dict; workers read it via closure so
    no module-global race exists.
    """
    rates = rates if rates is not None else {}  # callers pass explicitly

    # PRE-CREATE written columns: concurrent .at/.loc must not race on column
    # creation (GIL does NOT make new-key insertion atomic). short_name needs
    # OBJECT dtype — writing str into float64 triggers a non-atomic dtype-upgrade
    # (pandas 2.2+) that can lose concurrent writes.
    write_cols = ["dividend_yield_5y_avg"]
    if country == "sg" and "short_name" not in data_prep.columns:
        data_prep["short_name"] = None  # object dtype
    if country == "my":
        # ocf is also written via .at; pre-create it too (dropped after pool).
        write_cols += ["market_cap", "volume", "pe", "ps_ttm", "pb", "beta", "pcf", "ocf"]
    for col in write_cols:
        if col not in data_prep.columns:
            data_prep[col] = np.nan

    def clean_short_name(name: str) -> str | None:
        if not isinstance(name, str) or pd.isna(name):
            return None

        cleaned_name = name.strip()

        if cleaned_name.lower() == 'null':
            return None
        if '.si,' in cleaned_name.lower() and ',' in cleaned_name.lower():
            return None

        cleaned_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_name)

        suffix_pattern = re.compile(
            r'\s*('
            r'(- watch list|USD OV|TH SDR 1to1|A\$|HK\$|GROUP|LIMITED|LTD|PLC|PCL|DRC'
            r'|REIT|TRUST|TR|T|COM|NCCPS|SGD|USD|EUR|CNY|GBP|OV)$'
            r')',
            re.IGNORECASE
        )
        cleaned_name = suffix_pattern.sub('', cleaned_name)

        leading_pattern = re.compile(r'^\$\s*|^[acht]\s+', re.IGNORECASE)
        cleaned_name = leading_pattern.sub('', cleaned_name)

        words = cleaned_name.split()
        processed_words = []
        for word in words:
            if word.isupper():
                processed_words.append(word)
            else:
                processed_words.append(word.title())

        cleaned_name = " ".join(processed_words)
        cleaned_name = cleaned_name.strip()
        return cleaned_name if cleaned_name else None

    def _update_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            # Retry the fetch (ticker.info is an HTTP call) with backoff.
            info = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    info = yf.Ticker(bare_symbol(symbol) + ticker_extension).info
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)

            currency_info = info.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"
            currency = currency_info or row.get("currency")

            if currency and currency != country_currency:
                rate = rates.get(currency, {}).get(country_currency)
                if rate is None:
                    # No conversion rate: mark failed so main() strips cols
                    # instead of NULL-wiping stored values.
                    _FAILED_SYMBOLS.add(bare_symbol(symbol))
                    return
                else:
                    rate = float(rate)

            desired_values = {
                "fiveYearAvgDividendYield": "dividend_yield_5y_avg"
            }
            if country == "sg":
                desired_values["shortName"] = "short_name"
            elif country == "my":
                desired_values.update({
                    "marketCap": "market_cap",
                    "volume": "volume",
                    "trailingPE": "pe",
                    "priceToSalesTrailing12Months": "ps_ttm",
                    "priceToBook": "pb",
                    "beta": "beta",
                    "operatingCashflow": "ocf",
                })

            for key_dv, col in desired_values.items():
                try:
                    raw_val = info.get(key_dv, np.nan)

                    if col == "market_cap":
                        if raw_val is not None and raw_val is not np.nan:
                            if currency and currency != country_currency and rate is not None:
                                data_prep.at[index, col] = raw_val * rate
                            else:
                                data_prep.at[index, col] = raw_val
                        else:
                            data_prep.at[index, col] = np.nan

                    elif col == "short_name":
                        data_prep.at[index, col] = clean_short_name(raw_val)

                    elif col == "ocf":
                        ocf_val = raw_val
                        if ocf_val not in [None, 0, np.nan]:
                            mcap = info.get("marketCap")
                            data_prep.at[index, "pcf"] = mcap / ocf_val
                        else:
                            data_prep.at[index, "pcf"] = np.nan

                    elif col == "dividend_yield_5y_avg":
                        if raw_val is not None and not pd.isna(raw_val):
                            data_prep.at[index, col] = raw_val / 100

                    elif col == "pe":
                        yf_pe = raw_val
                        if isinstance(yf_pe, str):
                            pe_str = yf_pe.strip().lower()
                            if pe_str in ["none", "nan"]:
                                yf_pe = np.nan
                            elif pe_str in ["inf", "infinity"]:
                                yf_pe = float('inf')
                            else:
                                try:
                                    yf_pe = float(yf_pe)
                                except Exception:
                                    yf_pe = np.nan

                        if not pd.isna(yf_pe) and np.isfinite(yf_pe):
                            data_prep.at[index, col] = yf_pe
                        else:
                            close_list = row.get("close", [])
                            last_close = None
                            if isinstance(close_list, list) and close_list:
                                last_close = close_list[-1].get("close")
                            eps = row.get("eps")
                            if last_close is not None and eps:
                                data_prep.at[index, col] = last_close / eps
                            else:
                                data_prep.at[index, col] = np.nan

                    else:
                        data_prep.at[index, col] = raw_val

                except KeyError:
                    if col == "pe":
                        close_list = row.get("close", [])
                        last_close = close_list[-1].get("close") if isinstance(close_list, list) and close_list else None
                        eps = row.get("eps")
                        data_prep.at[index, col] = (last_close / eps) if (last_close and eps) else np.nan
                    else:
                        continue

        except Exception as e:
            print(f"Error updating symbol {symbol}: {e}")
            # Transient fetch failure (429/network): mark failed so main()
            # strips recomputed cols (no NaN -> NULL wipe).
            _FAILED_SYMBOLS.add(bare_symbol(symbol))

    list(_POOL.map(_update_one, data_prep.iterrows()))

    if "ocf" in data_prep.columns:
        data_prep = data_prep.drop(columns=["ocf"])

    return data_prep

def update_dividend_growth_rate(data_prep: pd.DataFrame, country):
    # PRE-CREATE the column: concurrent writes must not race on column creation.
    if "dividend_growth_rate" not in data_prep.columns:
        data_prep["dividend_growth_rate"] = np.nan

    def _div_growth_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(bare_symbol(symbol) + ticker_extension)

            current_year = datetime.now().year
            # Retry the two history fetches (HTTP) with backoff.
            dividend_last_1_year = dividend_current = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    dividend_last_1_year = ticker.history(
                        start=f"{current_year - 1}-01-01",
                        end=f"{current_year - 1}-12-31"
                    )["Dividends"].sum()
                    dividend_current = ticker.history(
                        start=f"{current_year}-01-01",
                        end=f"{current_year}-12-31"
                    )["Dividends"].sum()
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)

            dividend_growth_rate = safe_relative_diff(dividend_current, dividend_last_1_year)
            data_prep.loc[index, "dividend_growth_rate"] = dividend_growth_rate

        except Exception as e:
            print(f"error updating dividend growth rate for symbol {symbol} : ", e)
            _FAILED_SYMBOLS.add(bare_symbol(symbol))

    list(_POOL.map(_div_growth_one, data_prep.iterrows()))

    return data_prep

def update_close_history_data(data_prep: pd.DataFrame, country, rates=None):
    """Fetch 30d close history per symbol in parallel (MY daily path)."""
    rates = rates if rates is not None else {}
    date_format = "%Y-%m-%d"
    last_date = (datetime.now() - timedelta(days=31)).strftime(date_format)

    list_dates = [
        (datetime.strptime(last_date, date_format) + timedelta(days=i)).strftime(date_format)
        for i in range(1, 32)
    ]

    new_close = [None] * len(data_prep)
    # Map by symbol (unique in DB) instead of DataFrame index label.
    sym_pos = {row["symbol"]: pos for pos, (_, row) in enumerate(data_prep.iterrows())}

    def _close_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(bare_symbol(row["symbol"]) + ticker_extension)
            # currency lookup must not abort the close fetch: on failure fall
            # back to the row's stored currency. Retry info fetch (HTTP) first.
            currency_info = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    currency_info = ticker.info.get("currency", None)
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        break
                    _retry_backoff(attempt)
            currency = currency_info or row.get("currency")
            country_currency = "MYR" if country == "my" else "SGD"

            # Retry history fetch (HTTP); on final failure fall back to full history.
            yf_data = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    yf_data = ticker.history(period="1mo").reset_index()
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        break
                    _retry_backoff(attempt)
            if yf_data is None or yf_data.empty:
                # Fallback: full history, then filter to the 31-day window.
                # Retried too (429 on the fallback gets re-attempted).
                yf_data = None
                for attempt in range(1, _MAX_RETRIES + 1):
                    try:
                        yf_data = ticker.history(period="max").reset_index()
                        break
                    except Exception:
                        if attempt == _MAX_RETRIES:
                            raise
                        _retry_backoff(attempt)

            close_data = []
            rate_missing = False
            if yf_data is not None and not yf_data.empty:
                for i in range(len(yf_data)):
                    curr = yf_data.iloc[i]
                    curr_date = curr["Date"].strftime(date_format)
                    if curr_date in list_dates:
                        curr_close = float(curr["Close"])
                        if currency != country_currency:
                            try:
                                rate = float(rates[currency][country_currency])
                            except (KeyError, TypeError, ValueError):
                                # No rate for this currency: raw foreign price would
                                # corrupt local-currency history. Fall back to stored close.
                                rate_missing = True
                                break
                            curr_close = curr_close * rate
                        close_data.append({
                            "date": curr_date,
                            "close": curr_close if np.isfinite(curr_close) else None
                        })

            close_data = [close for close in close_data if close["date"] > last_date]
            new_close[sym_pos[symbol]] = (row["close"] if rate_missing else (close_data if close_data else row["close"]))
        except Exception as e:
            print(f"error in symbol {symbol} : ", e)
            _FAILED_SYMBOLS.add(bare_symbol(symbol))
            new_close[sym_pos[symbol]] = row["close"]

    list(_POOL.map(_close_one, data_prep.iterrows()))

    try:
        data_prep = data_prep.assign(close=new_close)
        if "ocf" in data_prep.columns:
            data_prep = data_prep.drop("ocf", axis="columns")
    except Exception as e:
        print(f"[DEBUG] Error assigning close data: {e}")
        data_prep = data_prep.assign(close=new_close)

    return data_prep

HISTORY_YEARS = 5       # keep only the most recent 5 calendar years of dividends


def update_historical_dividends(data_prep: pd.DataFrame, country):
    date_format = "%Y-%m-%d"
    if "historical_dividends" not in data_prep.columns:
        data_prep["historical_dividends"] = None

    def _hist_div_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(bare_symbol(row["symbol"]) + ticker_extension)

            # Retry the history + dividends fetches (HTTP) with backoff.
            full_history = dividends_series = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    full_history = ticker.history(period="max").reset_index()
                    dividends_series = ticker.dividends
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)
            if full_history.empty:
                raise ValueError("No historical data available")
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)
            latest_close = full_history.iloc[-1]["Close"]

            if dividends_series.empty:
                # Genuinely none, or transient empty from a rate-limited Yahoo.
                # Pre-created col holds None here -> NULL would wipe stored history:
                # mark failed so main() strips cols (DB keeps its value).
                _FAILED_SYMBOLS.add(bare_symbol(symbol))
                return
            dividends_df = dividends_series.reset_index()
            dividends_df.columns = ["Date", "Dividend"]
            dividends_df["year"] = dividends_df["Date"].dt.year
            # Cap history to the last 5 calendar years.
            min_year = datetime.now().year - (HISTORY_YEARS - 1)
            dividends_df = dividends_df[dividends_df["year"] >= min_year]

            # Closes on the same split-adjusted basis as dividends -> yield is
            # unit-consistent. auto_adjust=False -> split-adjusted ONLY (matches
            # .dividends basis). Fetch only the 5y span needed.
            hist = ticker.history(start=f"{min_year}-01-01", auto_adjust=False)
            close_map = {} if hist.empty else {
                d.strftime(date_format): float(c) for d, c in zip(hist.index, hist["Close"])
            }

            historical_dividends = []
            for year, group in dividends_df.groupby("year"):
                breakdown = []
                year_yields = []
                total_dividend = round(group["Dividend"].sum(), 5)
                for _, row_div in group.iterrows():
                    ex_date = row_div["Date"].strftime(date_format)
                    close = close_map.get(ex_date)
                    div_yield = round(row_div["Dividend"] / close, 5) if close else np.nan
                    if pd.notna(div_yield):
                        year_yields.append(div_yield)
                    breakdown.append({
                        "date": ex_date,
                        "total": round(row_div["Dividend"], 5),
                        "yield": div_yield
                    })
                historical_dividends.append({
                    "year": int(year),
                    "breakdown": breakdown,
                    "total_yield": round(sum(year_yields), 5) if year_yields else np.nan,
                    "total_dividend": total_dividend
                })
            # null (not empty list) when no dividends in the 5y window
            data_prep.at[index, "historical_dividends"] = historical_dividends if historical_dividends else None

        except Exception as e:
            print(f"[DEBUG] Error processing historical_dividends for {symbol}: {e}")
            # Fetch failed: mark failed so main() strips cols (no NULL wipe).
            _FAILED_SYMBOLS.add(bare_symbol(symbol))
            return

    list(_POOL.map(_hist_div_one, data_prep.iterrows()))
    return data_prep

def update_all_time_price(data_prep: pd.DataFrame, country: str):
    date_format = "%Y-%m-%d"
    if "all_time_price" not in data_prep.columns:
        data_prep["all_time_price"] = None

    def _alltime_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        ticker_extension = ".KL" if country.lower() == "my" else ".SI"
        ticker_full = bare_symbol(symbol) + ticker_extension

        try:
            ticker = yf.Ticker(ticker_full)

            # Retry full-history fetch (HTTP) with backoff.
            full_history = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    full_history = ticker.history(period="max").reset_index()
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)

            if full_history.empty:
                raise ValueError("No historical data available")

            latest_date = full_history.iloc[-1]["Date"]
            latest_close = full_history.iloc[-1]["Close"]

            all_time_low_row = full_history.loc[full_history["Close"].idxmin()]
            all_time_high_row = full_history.loc[full_history["Close"].idxmax()]
            all_time_low = {"date": all_time_low_row["Date"].strftime(date_format), "price": all_time_low_row["Close"]}
            all_time_high = {"date": all_time_high_row["Date"].strftime(date_format), "price": all_time_high_row["Close"]}

            current_year = datetime.now().year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            if not current_year_data.empty:
                ytd_low_row = full_history.loc[current_year_data["Close"].idxmin()]
                ytd_high_row = full_history.loc[current_year_data["Close"].idxmax()]
                ytd_low = {"date": ytd_low_row["Date"].strftime(date_format), "price": ytd_low_row["Close"]}
                ytd_high = {"date": ytd_high_row["Date"].strftime(date_format), "price": ytd_high_row["Close"]}
            else:
                ytd_low = ytd_high = None

            start_52w = latest_date - timedelta(days=365)
            data_52w = full_history[full_history["Date"] >= start_52w]
            if not data_52w.empty:
                w52_low_row = full_history.loc[data_52w["Close"].idxmin()]
                w52_high_row = full_history.loc[data_52w["Close"].idxmax()]
                w52_low = {"date": w52_low_row["Date"].strftime(date_format), "price": w52_low_row["Close"]}
                w52_high = {"date": w52_high_row["Date"].strftime(date_format), "price": w52_high_row["Close"]}
            else:
                w52_low = w52_high = None

            start_90d = latest_date - timedelta(days=90)
            data_90d = full_history[full_history["Date"] >= start_90d]
            if not data_90d.empty:
                d90_low_row = full_history.loc[data_90d["Close"].idxmin()]
                d90_high_row = full_history.loc[data_90d["Close"].idxmax()]
                d90_low = {"date": d90_low_row["Date"].strftime(date_format), "price": d90_low_row["Close"]}
                d90_high = {"date": d90_high_row["Date"].strftime(date_format), "price": d90_high_row["Close"]}
            else:
                d90_low = d90_high = None

            data_prep.at[index, "all_time_price"] = {
                "ytd_low": ytd_low,
                "ytd_high": ytd_high,
                "52_w_low": w52_low,
                "52_w_high": w52_high,
                "90_d_low": d90_low,
                "90_d_high": d90_high,
                "all_time_low": all_time_low,
                "all_time_high": all_time_high
            }

        except Exception as e:
            print(f"[DEBUG] Error processing all_time_price for {ticker_full}: {e}")
            # Fetch failed: mark failed so main() strips cols (no NULL wipe).
            _FAILED_SYMBOLS.add(bare_symbol(symbol))
            return

    list(_POOL.map(_alltime_one, data_prep.iterrows()))
    return data_prep

def update_change_data(data_prep: pd.DataFrame, country):
    # PRE-CREATE written columns (concurrent .loc needs them to exist).
    write_cols = ["change_3y"] + (["change_ytd", "change_1y"] if country != "sg" else [])
    for col in write_cols:
        if col not in data_prep.columns:
            data_prep[col] = np.nan

    def _change_one(index_row):
        index, row = index_row
        symbol = row["symbol"]
        try:
            ticker_extension = ".KL" if country == "my" else ".SI"
            ticker = yf.Ticker(bare_symbol(row["symbol"]) + ticker_extension)

            # Retry full-history fetch (HTTP) with backoff.
            full_history = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    full_history = ticker.history(period="max").reset_index()
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)
            if full_history.empty:
                raise ValueError("No historical data available")
            full_history["Date"] = pd.to_datetime(full_history["Date"])
            full_history.sort_values("Date", inplace=True)

            latest_date = full_history.iloc[-1]["Date"]
            latest_close = full_history.iloc[-1]["Close"]

            def get_close_on_or_before(target_dt):
                subset = full_history[full_history["Date"] <= target_dt]
                return subset.iloc[-1]["Close"] if not subset.empty else None

            current_year = latest_date.year
            current_year_data = full_history[full_history["Date"].dt.year == current_year]
            ytd_close = current_year_data.iloc[0]["Close"] if not current_year_data.empty else None
            close_1y = get_close_on_or_before(latest_date - timedelta(days=365))
            close_3y = get_close_on_or_before(latest_date - timedelta(days=3 * 365))

            def compute_change(latest, past):
                if past is None or past == 0:
                    return np.nan
                return (latest - past) / past

            if country != "sg":
                data_prep.loc[index, "change_ytd"] = compute_change(latest_close, ytd_close)
                data_prep.loc[index, "change_1y"]  = compute_change(latest_close, close_1y)
            data_prep.loc[index, "change_3y"]  = compute_change(latest_close, close_3y)

        except Exception as e:
            print(f"[DEBUG] Error calculating change metrics for {symbol}: {e}")
            _FAILED_SYMBOLS.add(bare_symbol(symbol))

    list(_POOL.map(_change_one, data_prep.iterrows()))
    return data_prep

def employee_updater(data_final, country):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    iv_data_dict = {
        "symbol": [],
        "employee_num_sgx": []
    }
    yf_data_dict = {
        "symbol": [],
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
    def _sgx_emp_one(sym):
        ticker_extension = ".KL" if country == "my" else ".SI"
        sym_with_ext = bare_symbol(sym) + ticker_extension
        mapped = special_case.get(sym_with_ext)
        ric = mapped if mapped is not None else sym_with_ext
        url = f"https://api.sgx.com/companygeneralinformation/v1.0/countryCode/SG/ricCode/{ric}?lang=en-US&params=companyDescription%2CstreetAddress1%2CstreetAddress2%2CstreetAddress3%2Ccity%2Cstate%2CpostalCode%2Ccountry%2Cemail%2Cwebsite%2CincorporatedDate%2CincorporatedCountry%2CpublicDate%2CnoOfEmployees%2CnoOfEmployeesLastUpdated"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                try:
                    emp = response.json()["data"][0]["noOfEmployees"]
                except Exception:
                    emp = np.nan
            else:
                emp = np.nan
        except Exception:
            emp = np.nan
        return sym, emp

    def _yf_emp_one(sym):
        try:
            # MY stocks have no .SI ticker; use the country-appropriate suffix.
            ext = ".KL" if country == "my" else ".SI"
            temp = yf.Ticker(bare_symbol(sym) + ext)
            return sym, temp.info["fullTimeEmployees"]
        except Exception:
            return sym, np.nan

    iv_pairs = list(_POOL.map(_sgx_emp_one, data_final["symbol"].tolist()))
    yf_pairs = list(_POOL.map(_yf_emp_one, data_final["symbol"].tolist()))

    iv_data_dict["symbol"] = [p[0] for p in iv_pairs]
    iv_data_dict["employee_num_sgx"] = [p[1] for p in iv_pairs]
    yf_data_dict["symbol"] = [p[0] for p in yf_pairs]
    yf_data_dict["employee_num"] = [p[1] for p in yf_pairs]

    employee_sgx = pd.DataFrame(iv_data_dict)
    employee_yf = pd.DataFrame(yf_data_dict)
    new_en = []
    for en_yf, en_sgx in zip(employee_yf["employee_num"].tolist(), employee_sgx["employee_num_sgx"].tolist()):
        # Coerce string responses (SGX/Yahoo can return e.g. "39892").
        try:
            en_sgx = float(en_sgx)
        except (TypeError, ValueError):
            en_sgx = np.nan
        try:
            en_yf = float(en_yf)
        except (TypeError, ValueError):
            en_yf = np.nan
        if en_sgx > 0:
            new_en.append(en_sgx)
        else:
            if en_yf > 0:
                new_en.append(en_yf)
            else:
                # Both sources failed: preserve the existing DB value (no NULL wipe).
                old_val = data_final.iloc[len(new_en)].get('employee_num', np.nan)
                new_en.append(old_val if old_val is not None else np.nan)
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


# Verified home/primary listings for SGX secondary/DRC lines whose .SI quote carries no
# +1y analyst estimate on Yahoo. All entries identity-checked: same legal entity (income
# statements identical within 1%) and same security (share ratio 1.0000 where quoted).
# Do NOT add unverified guesses (e.g. 7995.T is Valqua, not Maruwa; 0867.HK needs the zero).
HOME_TICKER_MAP = {
    "TATD": "AOT.BK",    # Airports of Thailand PCL DRC
    "TPED": "PTTEP.BK",  # PTT Exploration & Production PCL DRC
    "TCPD": "CPALL.BK",  # CP All PCL DRC
    "O6Z": "LONN.SW",    # Lonza Group
    "K6S": "PRU.L",      # Prudential plc
    "N33": "8604.T",     # Nomura Holdings
    "NIO": "NIO",        # NIO Inc (NYSE primary)
    "M12": "5344.T",     # Maruwa Co Ltd
    "8A8": "0867.HK",    # China Medical System Holdings (zero-padded HK code)
    "T14": "600329.SS",  # Tianjin Pharmaceutical Da Ren Tang (A-shares)
    "Z77": "Z74.SI",     # Singtel secondary line (primary Z74)
    "SO7": "BS6.SI",     # Yangzijiang secondary line (primary BS6)
}


def _growth_1y(ticker_str: str):
    """+1y forward analyst EPS growth (stockTrend) for a Yahoo ticker, or np.nan."""
    # Shared curl_cffi session; retry the growth_estimates fetch (HTTP).
    ge = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            ge = yf.Ticker(ticker_str).growth_estimates
            break
        except Exception:
            if attempt == _MAX_RETRIES:
                return np.nan
            _retry_backoff(attempt)
    if isinstance(ge, pd.DataFrame) and "+1y" in ge.index:
        val = ge.at["+1y", "stockTrend"]
        if pd.notna(val):
            return val
    return np.nan


def update_estimate_growth_data(data_prep: pd.DataFrame, country: str) -> pd.DataFrame:
    # PRE-CREATE: workers write one_year_eps_growth concurrently.
    if "one_year_eps_growth" not in data_prep.columns:
        data_prep["one_year_eps_growth"] = np.nan
    # Symbols with NO forward estimate anywhere: their column must be EXCLUDED
    # from the upsert payload (never NULL-overwrite the existing DB value).
    global _NO_ESTIMATE_SYMBOLS
    _NO_ESTIMATE_SYMBOLS = set()

    def _estimate_one(idx_row):
        idx, row = idx_row
        symbol = row["symbol"]
        ext = ".KL" if country.lower() == "my" else ".SI"

        eps_1y = np.nan
        source = None

        # 1) PRIMARY: +1y forward estimate on the local line (.SI / .KL)
        try:
            ticker = yf.Ticker(bare_symbol(symbol) + ext)

            # Retry the growth_estimates fetch (HTTP) with backoff.
            ge = None
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    ge = ticker.growth_estimates
                    break
                except Exception:
                    if attempt == _MAX_RETRIES:
                        raise
                    _retry_backoff(attempt)

            eps_1y = ge.at["+1y", "stockTrend"] if "+1y" in ge.index else np.nan

        except Exception as e:
            print(f"[DEBUG] Failed to fetch estimates for {symbol + ext} after {_MAX_RETRIES} retries: {e}")
            # Network failure (429/etc), NOT "no estimate": mark failed so the
            # column is stripped (DB preserved) without polluting _NO_ESTIMATE_SYMBOLS.
            _FAILED_SYMBOLS.add(bare_symbol(symbol))
            return

        # 2) FALLBACK: verified home/primary listing for secondary/DRC lines.
        #    DB symbols are suffixed ("Z77.SI") but the map keys are bare ("Z77").
        if pd.isna(eps_1y):
            home = HOME_TICKER_MAP.get(bare_symbol(symbol))
            if home:
                try:
                    eps_1y = _growth_1y(home)
                    source = f"home:{home}"
                except Exception as e:
                    print(f"[DEBUG] Failed to fetch estimates for {symbol} via home {home}: {e}")
                    source = f"home:{home}:error"

        # 3) NO forward estimate anywhere: keep the previous DB value (never write
        #    NULL over it). Record the symbol so main() strips its payload column.
        if pd.isna(eps_1y):
            print(f"[growth] {symbol}: no +1y forward estimate (.SI -> {source}) - keeping previous value")
            _NO_ESTIMATE_SYMBOLS.add(bare_symbol(symbol))
            return

        data_prep.loc[idx, "one_year_eps_growth"] = eps_1y
        print(f"[growth] {symbol}: +1y eps growth = {eps_1y} (source={source})")

    list(_POOL.map(_estimate_one, data_prep.iterrows()))

    return data_prep

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update sg or my data. If no argument is specified, the sg data will be updated.")
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

    country = "my" if args.malaysia else "sg"

    if country == "sg":
        print("Attempting to use local compact_rates.json for SGX...")
        try:
            with open('compact_rates.json', 'r') as f:
                data = json.load(f)
            print("...Success! Loaded conversion rates from local compact_rates.json file.")
        except Exception as e:
            print(f"...Warning: Could not load local file due to an error ({e}).")
            print("...Falling back to fetching current rates from the live URL.")
            url_currency = 'https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json'
            response = requests.get(url_currency)
            data = response.json()
            print("...Successfully fetched current rates from URL as a fallback.")
    else:
        print("Fetching current conversion rates from URL for KLSE data.")
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
    elif args.daily:
        # Reset per-run failure tracking.
        _FAILED_SYMBOLS.clear()
        db = "klse_companies" if args.malaysia else "sgx_companies"
        if args.singapore:
            data_db = supabase.table(db).select("*").eq("is_active", True).execute()
        else:
            data_db = supabase.table(db).select("*").execute()
        data_db = pd.DataFrame(data_db.data)
        drop_cols = ['market_cap', 'volume', 'pe', 'revenue', 'beta', 'weekly_signal', 'monthly_signal', 'earnings']
        data_db.drop(drop_cols, axis=1, inplace=True, errors='ignore')
        data_final = yf_data_updater(data_db, country, rates=data)
        data_final = update_change_data(data_final, country)
        data_final = update_dividend_growth_rate(data_final, country)
        if not args.singapore:
            data_final = update_close_history_data(data_final, country, rates=data)

        if args.singapore:
            data_final = update_historical_dividends(data_final, country)
            data_final = update_all_time_price(data_final, country)
            data_final = update_estimate_growth_data(data_final, country)

    invalid_yf_symbol = ['KIPR', 'PREI', 'YTLR', 'IGRE', 'ALQA', 'TWRE', 'AMFL', 'UOAR', 'AMRY', 'HEKR', 'SENT', 'AXSR',
                         'CAMA', 'SUNW', 'ATRL', 'PROL', 'KLCC', '5270']
    # The blocklist is written as bare codes; stored symbols are suffixed.
    data_final = data_final[~data_final["symbol"].map(bare_symbol).isin(invalid_yf_symbol)]
    data_final.to_csv("data_my.csv", index=False) if args.malaysia else data_final.to_csv("data_sg.csv", index=False)

    records_before_cleaning = data_final.to_dict("records")
    problematic_records_before = []
    for record in records_before_cleaning:
        try:
            json.dumps(record, allow_nan=False, default=str)
        except ValueError:
            problematic_records_before.append(record)

    if problematic_records_before:
        bad_record = problematic_records_before[0]

    data_final.replace({np.nan: None}, inplace=True)

    json_like_cols = ['close', 'historical_dividends', 'all_time_price']
    for col in json_like_cols:
        if col in data_final.columns:
            data_final[col] = data_final[col].apply(recursively_clean_nans)

    # Normalize symbols to the stored (suffixed) form before writing.
    data_final["symbol"] = data_final["symbol"].map(lambda s: with_suffix(s, country))

    # Symbols with no +1y estimate must NOT have one_year_eps_growth in their
    # payload (writing NULL would wipe the existing DB value).
    records = data_final.to_dict("records")
    if _NO_ESTIMATE_SYMBOLS:
        est_col = "one_year_eps_growth"
        for rec in records:
            if bare_symbol(rec["symbol"]) in _NO_ESTIMATE_SYMBOLS:
                rec.pop(est_col, None)

    # Symbols whose Yahoo fetch failed (429/network): strip recomputed columns
    # so NaN -> NULL does NOT wipe stored values. Pass-through DB cols stay.
    if _FAILED_SYMBOLS:
        recomputed_cols = {"short_name", "dividend_yield_5y_avg", "change_3y",
                           "change_ytd", "change_1y", "dividend_growth_rate",
                           "market_cap", "volume", "pe", "ps_ttm", "pb", "beta",
                           "pcf", "close", "historical_dividends", "all_time_price",
                           "one_year_eps_growth"}
        for rec in records:
            if bare_symbol(rec["symbol"]) in _FAILED_SYMBOLS:
                for col in recomputed_cols:
                    rec.pop(col, None)

    # NaN/Inf that survives cleaning (e.g. Yahoo raw inf for beta/volume) would
    # make Postgres reject the whole batch. NULL non-finite floats in place
    # instead of dropping the record wholesale.
    clean_records = []
    for record in records:
        for k, v in record.items():
            if isinstance(v, float) and not np.isfinite(v):
                record[k] = None
        try:
            json.dumps(record, allow_nan=False, default=str)
            clean_records.append(record)
        except ValueError:
            print(f"Dropping record with NaN/Inf: {record.get('symbol')}")

    supabase.table(db).upsert(clean_records, returning='minimal').execute()
    print("Upsert operation successful.")
