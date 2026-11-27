"""FX funnel + point scaling used by update_all_time_price.

The bug this locks down: YTD/52w/all-time extremes were stored in the counter's
trading currency (USD/HKD/EUR/...) while the close they are compared against is
SGD, and the series was dividend-adjusted while the close is raw.

Run: ~/miniconda3/envs/yf-verify/bin/python tests/test_sg_my_scraper_alltime_fx.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sg_my_scraper as m

m.data = {"USD": {"SGD": 1.28}, "HKD": {"SGD": 0.17}}

# same currency -> no-op
assert m._fx_rate("SGD", "SGD") == 1.0
# missing currency (DB NULL / yahoo silent) -> assume the country's own currency
assert m._fx_rate(None, "SGD") == 1.0
# known cross rate
assert m._fx_rate("USD", "SGD") == 1.28
assert m._fx_rate("HKD", "SGD") == 0.17
# unknown currency -> None, so callers mark the symbol failed instead of writing
# an unconverted price over the stored one
assert m._fx_rate("CNY", "SGD") is None
# MY path keeps the same table
assert m._fx_rate("USD", "MYR") is None and m._fx_rate("MYR", "MYR") == 1.0

points = {
    "ytd_low": {"date": "2026-01-19", "price": 2.3778},
    "52_w_high": {"date": "2026-02-20", "price": 3.1173},
    "90_d_low": None,
}
scaled = m._scale_points(points, 1.28)
assert scaled["ytd_low"] == {"date": "2026-01-19", "price": 3.043584}, scaled["ytd_low"]
assert scaled["52_w_high"]["price"] == round(3.1173 * 1.28, 6)
assert scaled["90_d_low"] is None, "absent windows must stay None, not become {}"
# rate 1.0 leaves the payload byte-identical
assert m._scale_points(points, 1.0) == points
# argmin/argmax selection is unaffected by a positive constant: after scaling,
# the selected 52w high is still the max of the (scaled) series
assert scaled["52_w_high"]["price"] > scaled["ytd_low"]["price"]

print("all assertions passed")
