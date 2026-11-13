import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

# Loaded by path: sgx_scrape/ has its own symbol_utils module.
_spec = importlib.util.spec_from_file_location(
    "root_symbol_utils", Path(__file__).resolve().parents[1] / "symbol_utils.py")
_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utils)
SECONDARY_COUNTERS, mirror_frame, mirror_records = (
    _utils.SECONDARY_COUNTERS, _utils.mirror_frame, _utils.mirror_records)


def test_only_menu_is_mapped():
    assert SECONDARY_COUNTERS == {"MENU.SI": "MXNU.SI"}


def test_mirror_frame_copies_listed_columns_only():
    df = pd.DataFrame([
        {"symbol": "MXNU.SI", "dividend_ttm": 0.0495, "eps": 0.16},
        {"symbol": "MENU.SI", "dividend_ttm": 0.0247, "eps": np.nan},
        {"symbol": "D05.SI", "dividend_ttm": 2.4, "eps": 3.9},
    ])
    out = mirror_frame(df, ["dividend_ttm"]).set_index("symbol")
    assert out.loc["MENU.SI", "dividend_ttm"] == 0.0495
    assert pd.isna(out.loc["MENU.SI", "eps"])          # not listed: untouched
    assert out.loc["D05.SI", "dividend_ttm"] == 2.4
    assert out.loc["MXNU.SI", "dividend_ttm"] == 0.0495


def test_mirror_frame_accepts_bare_symbols_and_json_values():
    hist = '[{"period":"TTM","earnings":1.0}]'
    df = pd.DataFrame([
        {"symbol": "MXNU", "historical_earnings": hist, "earnings": 1.0},
        {"symbol": "MENU", "historical_earnings": np.nan, "earnings": np.nan},
    ])
    out = mirror_frame(df, ["historical_earnings", "earnings"]).set_index("symbol")
    assert out.loc["MENU", "historical_earnings"] == hist
    assert out.loc["MENU", "earnings"] == 1.0


def test_mirror_frame_without_primary_is_unchanged():
    df = pd.DataFrame([{"symbol": "MENU.SI", "dividend_ttm": 0.0247}])
    assert mirror_frame(df, ["dividend_ttm"]).loc[0, "dividend_ttm"] == 0.0247


def test_mirror_frame_empty():
    df = pd.DataFrame()
    assert mirror_frame(df, ["dividend_ttm"]).empty


def test_mirror_records_copies_and_follows_stripped_columns():
    records = [
        {"symbol": "MXNU.SI", "dividend_yield_5y_avg": 0.0913, "dividend_growth_rate": -0.03},
        {"symbol": "MENU.SI", "dividend_yield_5y_avg": None, "dividend_growth_rate": 0.0247,
         "one_year_eps_growth": None, "close": 0.515},
    ]
    out = {r["symbol"]: r for r in mirror_records(records, [
        "dividend_yield_5y_avg", "dividend_growth_rate", "one_year_eps_growth"])}
    menu = out["MENU.SI"]
    assert menu["dividend_yield_5y_avg"] == 0.0913
    assert menu["dividend_growth_rate"] == -0.03
    # MXNU did not send one_year_eps_growth: MENU does not send it either.
    assert "one_year_eps_growth" not in menu
    assert menu["close"] == 0.515                       # price data stays per counter


def test_mirror_records_other_symbols_untouched():
    records = [{"symbol": "D05.SI", "dividend_growth_rate": 0.1}]
    assert mirror_records(records, ["dividend_growth_rate"]) == [{"symbol": "D05.SI", "dividend_growth_rate": 0.1}]
