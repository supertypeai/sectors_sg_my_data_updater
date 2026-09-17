import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = str(Path(__file__).resolve().parents[1])

# Root and sgx_scrape/ each ship a different symbol_utils. Evict whichever is
# cached so this import gets the root one, then evict again so the SGX tests
# don't inherit it - works in any collection order.
sys.modules.pop("symbol_utils", None)
sys.path.insert(0, ROOT)
import sg_my_scraper as scraper
sys.path.remove(ROOT)
sys.modules.pop("symbol_utils", None)


def _run(monkeypatch, info):
    class FakeTicker:
        def __init__(self, _symbol):
            self.info = {"currency": "MYR", **info}

    monkeypatch.setattr(scraper.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(scraper, "data", {}, raising=False)
    scraper._FAILED_SYMBOLS.clear()
    df = pd.DataFrame([{"symbol": "1155", "close": [], "eps": 1.0}])
    return scraper.yf_data_updater(df, "my").loc[0, "volume"]


def test_pre_open_zero_session_volume_uses_10d_average(monkeypatch):
    """Before Bursa opens Yahoo can report volume=0; that must not zero the row."""
    info = {"volume": 0, "averageDailyVolume10Day": 11923340, "averageVolume": 13950125}
    assert _run(monkeypatch, info) == 11923340


@pytest.mark.parametrize("info, expected", [
    ({"volume": 5, "averageVolume": 13950125}, 13950125),
    ({"volume": 5}, 5),
    ({"averageDailyVolume10Day": 0, "averageVolume": 9}, 0),
])
def test_volume_fallback_order(monkeypatch, info, expected):
    assert _run(monkeypatch, info) == expected


def test_no_volume_fields_marks_symbol_for_strip(monkeypatch):
    """No volume at all -> symbol flagged so main() keeps the stored value."""
    scraper._NO_VOLUME_SYMBOLS.clear()
    assert np.isnan(_run(monkeypatch, {}))
    assert scraper._NO_VOLUME_SYMBOLS == {"1155"}


def test_volume_present_is_not_stripped(monkeypatch):
    scraper._NO_VOLUME_SYMBOLS.clear()
    _run(monkeypatch, {"volume": 0, "averageDailyVolume10Day": 7})
    assert scraper._NO_VOLUME_SYMBOLS == set()
