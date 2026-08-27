"""Shared SGX symbol normalization.

sgx_companies (and the derived sgx_daily_data / sgx_metrics_daily /
sgx_financials_annual / sgx_periodic_financial tables) store symbols with the
exchange suffix ("D05.SI"). The SGX APIs and Yahoo Finance use the bare code
("D05"). All diffing is done in bare space; symbols are re-suffixed at every
write boundary — otherwise every SGX symbol looks new and every DB symbol looks
delisted, and Yahoo calls become "D05.SI.SI".
"""

SYMBOL_SUFFIX = ".SI"


def bare_symbol(symbol) -> str:
    """The ticker code without the exchange suffix ("D05.SI" -> "D05",
    "D05.SI.SI" -> "D05"). Strips repeated trailing suffixes. Idempotent."""
    symbol = str(symbol)
    while symbol.endswith(SYMBOL_SUFFIX):
        symbol = symbol[: -len(SYMBOL_SUFFIX)]
    return symbol


def with_suffix(symbol) -> str:
    """Normalize a bare or already-suffixed code to the stored form ("D05" -> "D05.SI").
    Idempotent; strips any existing suffix first so a doubled suffix is corrected
    ("D05.SI.SI" -> "D05.SI")."""
    return bare_symbol(symbol) + SYMBOL_SUFFIX


def stored_symbols(symbols) -> list:
    """Map bare codes to the form stored in sgx_companies."""
    return [with_suffix(s) for s in symbols]
