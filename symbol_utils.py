"""Shared symbol normalization for SG/MY scripts (repo root).

sgx_companies stores symbols with the exchange suffix ("D05.SI"); klse_companies
stores the bare Bursa code ("1155", no suffix). Yahoo tickers are built from
bare codes ("1155.KL"). db_symbol() normalizes to the DB write form at every
write boundary; bare_symbol() strips suffixes for Yahoo calls.
"""

import numpy as np
import pandas as pd

EXCHANGE_SUFFIXES = (".SI", ".KL")

# A second SGX counter of one trust -> the counter the pipeline reads the
# trust-level data from. Only amounts already converted to SGD and ratios are
# copied; prices, volume and price history stay per counter.
SECONDARY_COUNTERS = {
    "MENU.SI": "MXNU.SI",  # Elite UK REIT: SGD counter <- GBP counter
}


def is_valid_number(x) -> bool:
    """True for a real, finite numeric value (excludes None, NaN, inf, and
    non-numeric strings). Guards divisions and arithmetic against Yahoo's
    mixed None/NaN/str/0 responses."""
    if x is None or isinstance(x, bool):
        return False
    try:
        return bool(np.isfinite(float(x)))
    except (TypeError, ValueError):
        return False


def bare_symbol(symbol) -> str:
    """The ticker code without its exchange suffix ("D05.SI" -> "D05",
    "D05.SI.SI" -> "D05"). Strips repeated trailing suffixes. Idempotent."""
    symbol = str(symbol)
    changed = True
    while changed:
        changed = False
        for suffix in EXCHANGE_SUFFIXES:
            if symbol.endswith(suffix):
                symbol = symbol[: -len(suffix)]
                changed = True
                break
    return symbol


def db_symbol(symbol, country) -> str:
    """Normalize to the DB write form: SGX -> ".SI" suffix, KLSE -> bare code.
    Idempotent; strips any existing exchange suffix first."""
    if country.upper() == "SG":
        return bare_symbol(symbol) + ".SI"
    return bare_symbol(symbol)


def mirror_frame(df: pd.DataFrame, columns) -> pd.DataFrame:
    """Copy `columns` from each primary counter's row to its secondary counter's
    row (SECONDARY_COUNTERS). A pair with either row missing is left unchanged."""
    if df.empty or "symbol" not in df.columns:
        return df
    df = df.copy()
    keys = df["symbol"].map(lambda s: db_symbol(s, "SG"))
    for secondary, primary in SECONDARY_COUNTERS.items():
        secondary_rows = keys == secondary
        primary_rows = df[keys == primary]
        if not secondary_rows.any() or primary_rows.empty:
            continue
        source = primary_rows.iloc[-1]
        for col in columns:
            if col in df.columns:
                df[col] = df[col].astype(object)
                for idx in df.index[secondary_rows]:
                    df.at[idx, col] = source[col]
    return df


def mirror_records(records, columns):
    """Same as mirror_frame, for a list of row dicts about to be upserted. A
    column the primary does not send (stripped after a failed fetch) is also
    not sent for the secondary, so its stored value is kept."""
    by_symbol = {db_symbol(r["symbol"], "SG"): r for r in records}
    for secondary, primary in SECONDARY_COUNTERS.items():
        target, source = by_symbol.get(secondary), by_symbol.get(primary)
        if target is None or source is None:
            continue
        for col in columns:
            if col in source:
                target[col] = source[col]
            else:
                target.pop(col, None)
    return records


def with_suffix(symbol, country) -> str:
    """Deprecated alias kept for SGX-only callers: normalizes to "D05.SI".
    Use db_symbol() at write boundaries (it knows KLSE stores bare codes)."""
    return db_symbol(symbol, country)
