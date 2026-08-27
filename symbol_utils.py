"""Shared symbol normalization for SG/MY scripts (repo root).

DB symbols are stored with the exchange suffix ("D05.SI", "1155.KL"); Yahoo
tickers are built from the bare code. All Yahoo calls strip via bare_symbol();
every DB write boundary re-applies the suffix via with_suffix() so a bare
source can never write a bare row, and a suffixed source never becomes
"D05.SI.SI".
"""

EXCHANGE_SUFFIXES = (".SI", ".KL")


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


def with_suffix(symbol, country) -> str:
    """Normalize to the DB form (SG -> ".SI", MY -> ".KL"). Idempotent; strips
    any existing exchange suffix first so a wrong/doubled suffix is corrected
    ("D05.SI.KL" -> "D05.SI", "D05.SI.SI" -> "D05.SI")."""
    symbol = str(symbol)
    suffix = ".SI" if country.upper() == "SG" else ".KL"
    return bare_symbol(symbol) + suffix
