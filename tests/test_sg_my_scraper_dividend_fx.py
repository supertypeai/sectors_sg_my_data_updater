"""Dividend FX: yahoo amounts must be stored in SGD, yields must stay on the raw pair.

Locks down the second half of the currency bug (the first half is
tests/test_sg_my_scraper_alltime_fx.py): H78 2022 used to be stored as 0.22 USD
next to a 2020 row of 0.2064 SGD for the same payout.

Run: ~/miniconda3/envs/yf-verify/bin/python -m pytest tests/test_sg_my_scraper_dividend_fx.py -q
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sg_my_scraper as m


class _FakeTicker:
    def __init__(self, dividends: dict, closes: dict):
        self._div = pd.Series(dividends, name="Dividends")
        idx = pd.DatetimeIndex(sorted(closes), name="Date")
        self._hist = pd.DataFrame({"Close": [closes[d] for d in sorted(closes)]}, index=idx)

    @property
    def dividends(self):
        return self._div

    def history(self, **kwargs):
        return self._hist


DIVIDENDS = {pd.Timestamp("2024-03-21"): 0.16, pd.Timestamp("2024-08-22"): 0.06}
CLOSES = {pd.Timestamp("2024-03-21"): 4.0, pd.Timestamp("2024-08-22"): 3.0}


def _run(monkeypatch, currency, rates):
    monkeypatch.setattr(m, "data", rates)
    monkeypatch.setattr(m.yf, "Ticker", lambda _t: _FakeTicker(DIVIDENDS, CLOSES))
    m._FAILED_SYMBOLS.clear()
    df = pd.DataFrame([{"symbol": "TEST.SI", "currency": currency}])
    out = m.update_historical_dividends(df, "sg")
    assert not m._FAILED_SYMBOLS, m._FAILED_SYMBOLS
    (entry,) = out.at[0, "historical_dividends"]
    return entry


def test_non_sgd_amounts_are_converted_yields_are_not(monkeypatch):
    entry = _run(monkeypatch, "USD", {"USD": {"SGD": 1.28}})
    totals = {e["date"]: e["total"] for e in entry["breakdown"]}
    yields = {e["date"]: e["yield"] for e in entry["breakdown"]}
    # amounts scaled by the USD rate
    assert totals["2024-03-21"] == round(0.16 * 1.28, 5)
    assert totals["2024-08-22"] == round(0.06 * 1.28, 5)
    assert entry["total_dividend"] == round((0.16 + 0.06) * 1.28, 5)
    # yields stay raw-dividend / raw-close: currency-neutral, so unscaled
    assert yields["2024-03-21"] == 0.04
    assert yields["2024-08-22"] == 0.02
    assert entry["total_yield"] == 0.06


def test_sgd_counter_is_untouched(monkeypatch):
    entry = _run(monkeypatch, "SGD", {"USD": {"SGD": 1.28}})
    totals = {e["date"]: e["total"] for e in entry["breakdown"]}
    assert totals == {"2024-03-21": 0.16, "2024-08-22": 0.06}
    assert entry["total_dividend"] == 0.22


def test_missing_rate_marks_symbol_failed(monkeypatch):
    monkeypatch.setattr(m, "data", {})
    monkeypatch.setattr(m.yf, "Ticker", lambda _t: _FakeTicker(DIVIDENDS, CLOSES))
    m._FAILED_SYMBOLS.clear()
    df = pd.DataFrame([{"symbol": "TEST.SI", "currency": "CNY"}])
    out = m.update_historical_dividends(df, "sg")
    assert "TEST" in m._FAILED_SYMBOLS, "no rate must fail closed, not store raw amounts"
    assert out.at[0, "historical_dividends"] is None
