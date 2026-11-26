"""Self-check for the W4 price-jump guard. No framework beyond pytest."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sgx_scrape"))

from sgx_screener_scraper_daily import classify_price_jump as c


def test_ignores_zero_and_missing():
    # the repaired rows: close 0.0 must never be divided by or compared
    assert c(0.0, 3.275264, 100) is None      # XWA.SI 2024-04-17 resume
    assert c(3.275264, 0.0, 100) is None
    assert c(None, 3.0, 100) is None
    assert c(3.0, None, 100) is None
    assert c(float("nan"), 3.0, 100) is None


def test_tiers():
    assert c(1.00, 4.00, 5000) == "warn"            # real trade, abrupt
    assert c(1.00, 4.00, None) == "info"            # no volume: stale quote
    assert c(1.00, 4.00, 0) == "info"
    assert c(4.00, 1.00, 5000) == "warn"            # collapse
    assert c(1.00, 1.50, 5000) is None              # ordinary move


def test_exact_third_boundary():
    # X2UR.SI 2026-06-17 was exactly x3.0 and fell through a strict '>' test
    assert c(0.011, 0.033, 52) == "warn"
    assert c(0.033, 0.011, 52) == "warn"


def test_penny_tick_exempt():
    # 594.SI / FQ7.SI: 0.001 -> 0.004 is 4 minimum ticks, not a discontinuity
    assert c(0.001, 0.004, 100000) is None
    # ... but the same ratio off the tick floor is real
    assert c(0.010, 0.040, 100000) == "warn"
    # a large absolute move from a penny price is still reported
    assert c(0.002, 0.500, 100000) == "warn"


class _FakeQuery:
    """Minimal PostgREST chain. `cap` emulates the server truncating a response."""

    def __init__(self, rows, cap):
        self._rows, self._cap, self._filters = rows, cap, []
        self._desc, self._range = False, (0, 10 ** 6)

    def select(self, *_a, **_k): return self
    def in_(self, col, vals): self._filters.append(lambda r: r[col] in set(vals)); return self
    def gt(self, col, v): self._filters.append(lambda r: r[col] is not None and r[col] > v); return self
    def lt(self, col, v): self._filters.append(lambda r: r[col] is not None and r[col] < v); return self

    def order(self, _col, desc=False):
        self._desc = desc
        return self

    def range(self, start, end):
        self._range = (start, end)
        return self

    def execute(self):
        kept = [r for r in self._rows if all(f(r) for f in self._filters)]
        kept.sort(key=lambda r: r["date"], reverse=self._desc)
        start, end = self._range
        end = min(end, start + self._cap - 1)          # server-side truncation
        return type("R", (), {"data": kept[start : end + 1]})()


class _FakeClient:
    def __init__(self, rows, cap=2):
        self.rows, self.cap = rows, cap

    def table(self, _name):
        return _FakeQuery(list(self.rows), self.cap)


def test_previous_close_paginates_and_picks_newest(monkeypatch):
    import sgx_screener_scraper_daily as mod

    monkeypatch.setattr(mod, "PAGE_SIZE", 2)          # force multi-page pagination
    previous_positive_closes = mod.previous_positive_closes

    rows = [
        {"symbol": "A.SI", "date": "2024-01-05", "close": 0.10},
        {"symbol": "A.SI", "date": "2024-01-04", "close": 0.20},
        {"symbol": "A.SI", "date": "2024-01-03", "close": 0.30},
        {"symbol": "B.SI", "date": "2023-06-01", "close": 5.00},   # dormant, deep page
        {"symbol": "A.SI", "date": "2024-01-06", "close": 0.0},    # untraded
        {"symbol": "A.SI", "date": "2024-01-07", "close": 0.40},   # == batch date
    ]
    client = _FakeClient(rows, cap=2)  # matches the patched PAGE_SIZE
    got = previous_positive_closes(client, {"A.SI": "2024-01-06", "B.SI": "2024-01-06"})

    assert got["A.SI"] == 0.10, f"newest close before A.SI's first batch row: {got.get('A.SI')}"
    assert got["B.SI"] == 5.00, "dormant symbol must be found beyond the first page"


def test_zero_volume_includes_nan():
    from sgx_screener_scraper_daily import classify_price_jump as c
    assert c(1.0, 4.0, float("nan")) == "info"
    assert c(1.0, 4.0, 0.0) == "info"
    assert c(1.0, 4.0, 10.0) == "warn"


def test_multidate_batch_compares_consecutive_not_one_stale_prior(caplog):
    """A 1-month batch must be walked day-by-day, not measured against one prior.

    Regression: comparing every row in the window against the single close from
    before the window reported 2 jumps here instead of 1.
    """
    import logging

    import pandas as pd
    import sgx_screener_scraper_daily as mod

    # one stored close before the batch opens the window
    client = _FakeClient([{"symbol": "A.SI", "date": "2024-01-01", "close": 0.10}], cap=100)
    batch = pd.DataFrame([
        {"symbol": "A.SI", "date": "2024-01-02", "close": 0.10, "volume": 100},
        {"symbol": "A.SI", "date": "2024-01-03", "close": 0.40, "volume": 200},   # ×4 vs prev day
        {"symbol": "A.SI", "date": "2024-01-04", "close": 0.42, "volume": 300},   # normal vs 0.40
    ])

    with caplog.at_level(logging.WARNING, logger="sgx_screener_scraper_daily"):
        mod.audit_price_jumps(batch, client)

    alerts = [r.getMessage() for r in caplog.records if r.getMessage().startswith("data_anomaly:")]
    assert len(alerts) == 1, f"expected exactly one data_anomaly, got {len(alerts)}: {alerts}"
    assert "2024-01-03" in alerts[0]


def test_prior_uses_each_symbols_own_window_start():
    """A batch-wide cut-off hands most symbols a stale prior.

    Regression: batch_date was the global min date across all symbols (one
    outlier symbol pulled it a year back), so X.SI compared 2024-02-10 against
    its 2023-12-01 close instead of its 2024-02-09 close.
    """
    from sgx_screener_scraper_daily import previous_positive_closes

    client = _FakeClient([
        {"symbol": "L.SI", "date": "2024-01-01", "close": 0.10},
        {"symbol": "X.SI", "date": "2023-12-01", "close": 0.05},   # year-stale
        {"symbol": "X.SI", "date": "2024-02-09", "close": 5.00},   # the real prior
    ], cap=100)

    got = previous_positive_closes(
        client, {"L.SI": "2024-01-02", "X.SI": "2024-02-10"}
    )

    assert got["L.SI"] == 0.10
    assert got["X.SI"] == 5.00, f"must be the close before X.SI's own window start, got {got.get('X.SI')}"
