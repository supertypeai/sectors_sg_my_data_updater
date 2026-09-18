import sys
from pathlib import Path

import pandas as pd

ROOT = str(Path(__file__).resolve().parents[1])

# Root and sgx_scrape/ each ship a different symbol_utils. Evict whichever is
# cached so this import gets the root one, then evict again so the SGX tests
# don't inherit it - works in any collection order.
sys.modules.pop("symbol_utils", None)
sys.path.insert(0, ROOT)
import sg_my_scraper as scraper
sys.path.remove(ROOT)
sys.modules.pop("symbol_utils", None)


def _history(rows):
    """Daily bars shaped like yfinance history().reset_index() for a .KL ticker."""
    return pd.DataFrame({
        "Date": pd.to_datetime([d for d, _ in rows]).tz_localize("Asia/Kuala_Lumpur"),
        "Close": [1.0] * len(rows),
        "Volume": [v for _, v in rows],
    })


# Real 1155.KL bars; 16 Sep 2026 was a public holiday (no bar).
BARS = [("2026-09-14", 6491800), ("2026-09-15", 11314800), ("2026-09-17", 11220500)]


def _utc(ts):
    return pd.Timestamp(ts, tz="UTC")


def test_pre_open_run_uses_previous_session():
    """The job's real start (~04:40 MYT): today's bar does not exist yet."""
    assert scraper.last_completed_volume(_history(BARS[:2]), _utc("2026-09-16 20:40")) == 11314800


def test_partial_bar_during_session_is_ignored():
    """14:00 MYT: today's bar is a running count, use the last finished day."""
    assert scraper.last_completed_volume(_history(BARS), _utc("2026-09-17 06:00")) == 11314800


def test_today_counts_after_the_close():
    """19:00 MYT: today's session is finished."""
    assert scraper.last_completed_volume(_history(BARS), _utc("2026-09-17 11:00")) == 11220500


def test_untraded_day_is_a_real_zero():
    bars = [("2026-09-14", 500), ("2026-09-15", 0)]
    assert scraper.last_completed_volume(_history(bars), _utc("2026-09-16 20:40")) == 0


def test_no_finished_day_returns_none():
    assert scraper.last_completed_volume(_history(BARS[2:]), _utc("2026-09-17 06:00")) is None
    assert scraper.last_completed_volume(pd.DataFrame(), _utc("2026-09-17 06:00")) is None


def test_close_history_writes_volume_and_flags_missing(monkeypatch):
    histories = {"1155.KL": _history(BARS[:2]), "9999.KL": _history([])}

    class FakeTicker:
        def __init__(self, ticker):
            self.ticker = ticker

        def history(self, **_):
            return histories[self.ticker].set_index("Date")

    monkeypatch.setattr(scraper.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(scraper, "data", {}, raising=False)
    scraper._NO_VOLUME_SYMBOLS.clear()
    df = pd.DataFrame([{"symbol": "1155", "close": [], "currency": "MYR"},
                       {"symbol": "9999", "close": [], "currency": "MYR"}])

    out = scraper.update_close_history_data(df, "my")

    assert out.loc[0, "volume"] == 11314800
    assert scraper._NO_VOLUME_SYMBOLS == {"9999"}
