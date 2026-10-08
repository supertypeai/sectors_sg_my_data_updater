import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sgx_scrape"))

import sgx_screener_scraper_daily as scraper

ALL_KINDS = {"stocks", "reits", "businesstrusts", "adrs"}


def _bar(close: str, date: str = "20260916_170000"):
    return {"trading_time": date, "o": close, "h": close, "l": close, "lt": close, "vl": "10"}


@pytest.mark.parametrize("is_reit, first", [(False, "stocks"), (True, "reits")])
def test_ladder_tries_every_type(is_reit, first):
    kinds = scraper._historic_kinds(is_reit)
    assert kinds[0] == first
    assert set(kinds) == ALL_KINDS
    assert len(kinds) == len(ALL_KINDS)


def test_ladder_falls_back_to_the_type_that_serves(monkeypatch):
    """A trust filed under a non-REIT sector is only served by businesstrusts."""
    served = {"businesstrusts": [_bar(close="1.5")]}
    calls = []

    def fake_get(url, headers=None, timeout=None):
        kind = url.split("/historic/")[1].split("/")[0]
        calls.append(kind)
        return _Response(served.get(kind, []))

    monkeypatch.setattr(scraper.requests, "get", fake_get)
    df = scraper._fetch_historic_single("CJLU", "1m")

    assert calls == ["stocks", "businesstrusts"]
    assert df["symbol"].tolist() == ["CJLU"]
    assert df["close"].tolist() == [1.5]


def test_one_failing_type_does_not_abort_the_symbol(monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if "/stocks/" in url:
            raise RuntimeError("503")
        return _Response([_bar(close="2")])

    monkeypatch.setattr(scraper.requests, "get", fake_get)
    assert not scraper._fetch_historic_single("ANY", "1m").empty


def test_metrics_payload_only_has_columns_the_table_accepts(monkeypatch):
    """Guards the PGRST204 class of failure: a column added to the symbol frame
    must not reach the upsert."""
    base_df = pd.DataFrame(
        [{"symbol": "D05.SI", "api_symbol": "D05", "sector": "Financial Services",
          "is_reit": False, "market": "MAINBOARD", "anything_new": "x"}]
    )
    monkeypatch.setattr(
        scraper, "_fetch_screener",
        lambda: pd.DataFrame([{"symbol": "D05", "pe": 10.0, "pb": 1.2}]),
    )
    monkeypatch.setattr(
        scraper, "_fetch_ratios_single",
        lambda symbol: {"symbol": symbol, "beta": 0.9},
    )

    df = scraper.build_metrics_df(base_df, {})

    allowed = set(scraper.METRICS_COLUMNS) | set(scraper.NUMERIC_METRICS) | {"_failed"}
    assert set(df.columns) <= allowed
    assert df["symbol"].tolist() == ["D05.SI"]


def test_currency_metrics_use_snapshot_reporting_currency(monkeypatch):
    """revenue_ttm/eps arrive in the reporting currency; the snapshot
    tradedCurrency must win over the DB trading currency (both NIO and D05 are
    SGD-traded). Ratios are currency-independent and stay untouched."""
    monkeypatch.setattr(scraper, "_load_rates", lambda: {"USD": {"SGD": 1.25}})
    base_df = pd.DataFrame([
        {"symbol": "NIO.SI", "api_symbol": "NIO", "sector": "Consumer Cyclicals",
         "is_reit": False, "currency": "SGD"},
        {"symbol": "D05.SI", "api_symbol": "D05", "sector": "Financial Services",
         "is_reit": False, "currency": "SGD"},
    ])
    # NIO reports in USD (snapshot) even though it trades in SGD (DB).
    currency_maps = {"NIO": ("USD", "USD"), "D05": ("SGD", "SGD")}
    monkeypatch.setattr(
        scraper, "_fetch_screener",
        lambda: pd.DataFrame([
            {"symbol": "NIO", "revenue_ttm": 1000.0, "pe": 12.0},
            {"symbol": "D05", "revenue_ttm": 2000.0, "pe": 10.0},
        ]),
    )
    monkeypatch.setattr(
        scraper, "_fetch_ratios_single",
        lambda symbol: {"symbol": symbol, "eps": 4.0, "beta": 1.0},
    )

    out = scraper.build_metrics_df(base_df, currency_maps).set_index("symbol")

    assert out.loc["NIO.SI", "revenue_ttm"] == 1250.0
    assert out.loc["NIO.SI", "eps"] == 5.0
    assert out.loc["D05.SI", "revenue_ttm"] == 2000.0
    assert out.loc["D05.SI", "eps"] == 4.0
    # ratios are currency-independent and must be untouched
    assert out.loc["NIO.SI", "pe"] == 12.0
    assert out.loc["NIO.SI", "beta"] == 1.0


def test_clean_empties_are_not_retried(monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        return _Response([])

    monkeypatch.setattr(scraper.requests, "get", fake_get)
    assert scraper._fetch_historic_single("DEAD", "1m").empty
    assert len(calls) == len(ALL_KINDS)


def test_ladder_is_retried_after_an_error(monkeypatch):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        if len(calls) <= len(ALL_KINDS):
            raise RuntimeError("503")
        return _Response([_bar(close="3")])

    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)
    monkeypatch.setattr(scraper.requests, "get", fake_get)
    assert not scraper._fetch_historic_single("FLAKY", "1m").empty
    assert len(calls) > len(ALL_KINDS)


def test_latest_rows_without_market_cap_are_not_written():
    client = _Client()
    price = pd.DataFrame([{"symbol": "D05.SI", "date": "2026-09-16", "close": 1.0}])
    latest = pd.DataFrame(
        [{"symbol": "D05.SI", "date": "2026-09-16", "close": 1.0, "market_cap": 5.0},
         {"symbol": "TCPD.SI", "date": "2026-09-16", "close": 2.0, "market_cap": float("nan")}]
    )

    scraper.upsert_daily(price, latest, client)

    assert [r["symbol"] for r in client.upserts[-1]] == ["D05.SI"]
    assert client.upserts[0][0]["symbol"] == "D05.SI"


def test_missing_market_cap_column_does_not_crash_the_write():
    """build_daily_df returns a latest frame with no market_cap column when the
    market cap API answers empty."""
    client = _Client()
    price = pd.DataFrame([{"symbol": "D05.SI", "date": "2026-09-16", "close": 1.0}])

    scraper.upsert_daily(price, price.copy(), client)

    assert len(client.upserts) == 1
    assert client.upserts[0][0]["symbol"] == "D05.SI"


class _Client:
    def __init__(self):
        self.upserts = []

    def table(self, _name):
        return self

    def upsert(self, records, **_kwargs):
        self.upserts.append(records)
        return self

    def execute(self):
        return self


class _Response:
    def __init__(self, historic):
        self._historic = historic

    def raise_for_status(self):
        return None

    def json(self):
        return {"data": {"historic": self._historic}}


def test_rate_is_one_for_sgd_and_scales_the_rest():
    assert scraper._rate({"EUR": {"SGD": 1.5}}, "SGD") == 1.0
    assert scraper._rate({"EUR": {"SGD": 1.5}}, "EUR") == 1.5
    assert scraper._rate({}, "EUR") == 1.0
    assert scraper._rate({"EUR": {"SGD": 1.5}}, None) == 1.0


def test_historic_ohlc_is_scaled_to_sgd(monkeypatch):
    """A EUR counter (fx != 1) must be stored in SGD like market_cap."""
    monkeypatch.setattr(
        scraper.requests, "get",
        lambda url, headers=None, timeout=None: _Response([_bar(close="1.5")]),
    )
    df = scraper._fetch_historic_single("SET", "1m", False, 2.0)
    assert df["close"].tolist() == [3.0]
    assert df["open"].tolist() == [3.0]
    assert df["high"].tolist() == [3.0]
    assert df["low"].tolist() == [3.0]


def test_market_cap_guard_overrides_a_leg_double_count(monkeypatch):
    monkeypatch.setattr(scraper, "_reit_units_by_symbol", lambda: {"SET.SI": 556_884_000})
    base = pd.DataFrame([
        {"symbol": "SEB.SI", "name": "Stoneweg Europe Stapled Trust"},
        {"symbol": "SET.SI", "name": "Stoneweg Europe Stapled Trust"},
        {"symbol": "D05.SI", "name": "DBS Group Holdings"},
    ])
    latest = pd.DataFrame([
        {"symbol": "SEB.SI", "close": 2.13, "market_cap": 2_386_034_577.0},
        {"symbol": "SET.SI", "close": 1.49, "market_cap": 829_757_160.0},
        {"symbol": "D05.SI", "close": 40.0, "market_cap": 100_000_000.0},
    ])

    out = scraper._apply_market_cap_guard(latest, base).set_index("symbol")

    # SEB has no units row of its own; it borrows SET's by company name.
    assert out.loc["SEB.SI", "market_cap"] == 1_186_162_920
    assert out.loc["SET.SI", "market_cap"] == 829_757_160.0
    assert out.loc["D05.SI", "market_cap"] == 100_000_000.0


def test_market_cap_guard_keeps_small_drift(monkeypatch):
    monkeypatch.setattr(scraper, "_reit_units_by_symbol", lambda: {"X.SI": 100})
    base = pd.DataFrame([{"symbol": "X.SI", "name": "X"}])
    latest = pd.DataFrame([{"symbol": "X.SI", "close": 1.0, "market_cap": 108.0}])
    assert scraper._apply_market_cap_guard(latest, base).loc[0, "market_cap"] == 108.0


def test_market_cap_guard_skips_when_units_unavailable(monkeypatch):
    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(scraper, "_reit_units_by_symbol", boom)
    base = pd.DataFrame([{"symbol": "X.SI", "name": "X"}])
    latest = pd.DataFrame([{"symbol": "X.SI", "close": 1.0, "market_cap": 2_000_000.0}])
    assert scraper._apply_market_cap_guard(latest, base).loc[0, "market_cap"] == 2_000_000.0


def test_market_cap_guard_noop_without_market_cap_column():
    base = pd.DataFrame([{"symbol": "X.SI", "name": "X"}])
    latest = pd.DataFrame([{"symbol": "X.SI", "close": 1.0}])
    out = scraper._apply_market_cap_guard(latest, base)
    assert "market_cap" not in out.columns
