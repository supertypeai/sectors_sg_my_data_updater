"""Shared symbol normalization for SG/MY scripts (repo root).

sgx_companies stores symbols with the exchange suffix ("D05.SI"); klse_companies
stores the bare Bursa code ("1155", no suffix). Yahoo tickers are built from
bare codes ("1155.KL"). db_symbol() normalizes to the DB write form at every
write boundary; bare_symbol() strips suffixes for Yahoo calls.
"""

import numpy as np
import pandas as pd

EXCHANGE_SUFFIXES = (".SI", ".KL")


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


def with_suffix(symbol, country) -> str:
    """Deprecated alias kept for SGX-only callers: normalizes to "D05.SI".
    Use db_symbol() at write boundaries (it knows KLSE stores bare codes)."""
    return db_symbol(symbol, country)
