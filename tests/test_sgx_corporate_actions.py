"""
The corporate-action mappings are data-driven claims, so they are pinned here.

Every expectation below was measured against the real SGX feed and, where
possible, confirmed against a price jump in sgx_daily_data - see the module
docstring of sgx_corporate_actions.py.
"""
import os
import sys
from pathlib import Path

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.dummy")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sgx_scrape"))

import sgx_corporate_actions as ca


def _factor(pair):
    old, new = pair
    return (old + new) / old


def test_bonus_factor_matches_verified_price_jumps():
    # Verified against 21 real price moves; D05 2024-04-21 is the control.
    cases = {
        "OFFER OF 1 FOR 10": 1.1,
        "OFFER OF 1 FOR 5": 1.2,
        "OFFER OF 3 FOR 1": 4.0,
        "OFFER OF 5 FOR 100": 1.05,
        "Ratio: 100:1": 1.01,
        "Ratio: 5:1": 1.2,
        "Ratio: 1:1": 2.0,
        "Ratio: 4:3": 1.75,
    }
    for text, expected in cases.items():
        assert abs(ca.bonus_factor(text) - expected) < 1e-9, text


def test_bonus_ratio_keeps_sgx_encoding():
    # old_ratio/new_ratio are stored so (new+old)/old reproduces the factor.
    assert ca.bonus_ratios("Ratio: 10:1") == (10.0, 1.0)
    assert ca.bonus_ratios("OFFER OF 1 FOR 10") == (10.0, 1.0)
    assert abs(_factor(ca.bonus_ratios("Ratio: 4:3")) - 1.75) < 1e-9


def test_rights_ratio_is_inverted_relative_to_bonus():
    # The same string means the opposite by type: verified against CACT18's
    # explicit Underlying/Rights fields (VI2 'Ratio: 9:10' = 9 rights for 10).
    assert ca.rights_ratios("Ratio: 9:10") == (10.0, 9.0)
    assert ca.rights_ratios("Ratio: 1:5") == (5.0, 1.0)
    assert ca.rights_ratios("OFFER OF 1 FOR 2 @ SGD 5.42") == (2.0, 1.0)
    assert ca.rights_ratios("OFFER OF 4 FOR 10 @ SGD 1.2") == (10.0, 4.0)


def test_rights_factor_matches_idx_convention():
    # idx_right_issue.factor is (old + new) / old - PACK 5:102 -> 21.4.
    assert abs(ca.rights_factor(5, 102) - 21.4) < 1e-9
    assert abs(ca.rights_factor(3, 1) - 4 / 3) < 1e-9


def test_split_ratio_is_new_over_old():
    for text, expected in {"4:1": 4.0, "1:10": 0.1, "1:830": 1 / 830}.items():
        new, old = ca.cact_ratio(text)
        assert abs(new / old - expected) < 1e-12, text


def test_cash_parser_covers_every_observed_format():
    cases = {
        "Rate: SGD 0.81  Per Security": 0.81,
        "SGD 0.16 LESS TAX": 0.16,
        "SGD 0.2 ONE-TIER TAX": 0.2,
        "SCRIPDIVSCHEME SGD 0.28 ONE-TIER TAX": 0.28,
        "(SGD) 8 CENTS LESS TAX": 0.08,
        "010418 - 300918 SGD 0.0244 TAX EXEMPT": 0.0244,
        "1.5 CENTS": 0.015,
        "USD 0.0121 Per Security": 0.0121,
    }
    for text, expected in cases.items():
        parsed = ca.parse_cash(text)
        assert parsed and abs(parsed[0] - expected) < 1e-12, text


def test_cash_parser_refuses_unpriceable_rows():
    # Refusing is the point: a guessed amount corrupts the dividend history.
    for text in ("3 Cash Options", "15% LESS TAX", "Please see details.",
                 "Rate: NA NA  Per Security", "8", None, ""):
        assert ca.parse_cash(text) is None, text


def test_cash_parser_reads_the_tax_annotation():
    # The tax type is data Yahoo never provides.
    assert ca.parse_cash("SGD 0.16 LESS TAX")[2] == "LESS TAX"
    assert ca.parse_cash("SGD 0.2 ONE-TIER TAX")[2] == "ONE-TIER TAX"
    assert ca.parse_cash("SGD 0.2 TAX EXEMPT")[2] == "TAX EXEMPT"


def test_epoch_millis_not_iso():
    assert ca.epoch_to_date(1786636800000).isoformat() == "2026-08-13"
    assert ca.epoch_to_date(None) is None


def test_dividend_rows_sum_by_ex_date():
    rows = [
        {"anncType": "DIVIDEND", "particulars": "Rate: SGD 0.00414  Per Security",
         "exDate": 1793625600000, "recDate": 1793808000000, "datePaid": 1794672000000,
         "id": 1, "ibmCode": "X"},
        {"anncType": "ENTITLEMENT", "particulars": "Rate: SGD 0.00791 Per Security",
         "exDate": 1793625600000, "recDate": 1793808000000, "datePaid": 1794672000000,
         "id": 2, "ibmCode": "X"},
        {"anncType": "DIVIDEND", "particulars": "3 Cash Options",
         "exDate": 1793625600000, "recDate": None, "datePaid": None,
         "id": 3, "ibmCode": "X"},
    ]
    records = ca.build_dividends("M44U.SI", rows)
    assert len(records) == 1
    # ENTITLEMENT per-share rows are cash components: 0.00414 + 0.00791.
    assert abs(records[0]["dividend"] - 0.01205) < 1e-9


def test_corporate_actions_dates_are_shifted_forward_one_day():
    # `exDate` in corporateactions is the last CUM date, one day before the real
    # ex-date. Verified 75/75 against CACT06's announced Ex Date, and the price
    # drop lands on the CACT06 day (D05 falls 71.00 -> 69.90 on the shifted date).
    import datetime
    raw = int(datetime.datetime(2026, 11, 12, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    assert ca.corporate_actions_date(raw).isoformat() == "2026-11-13"


def test_union_of_sources_does_not_double_count_a_shared_component():
    # M44U 2026-11-04: corporateactions 0.00414 + 0.00791, CACT06 0.00414 +
    # 0.00611. The 0.00414 appears in both and must be counted once:
    # 0.00414 + 0.00611 + 0.00791 = 0.01816, which is Yahoo's figure.
    corp = [{"symbol": "M44U.SI", "date": "2026-11-04", "dividend": 0.01205,
             "_components": [0.00414, 0.00791], "source": "sgx_corporateactions"}]
    cact = [{"symbol": "M44U.SI", "date": "2026-11-04", "dividend": 0.00414,
             "source": "sgx_cact", "payment_type": "Tax Exempted"},
            {"symbol": "M44U.SI", "date": "2026-11-04", "dividend": 0.00611,
             "source": "sgx_cact", "payment_type": "Payment Rate in Gross"}]
    merged = ca.merge_dividend_sources(corp, cact)
    assert len(merged) == 1
    assert abs(merged[0]["dividend"] - 0.01816) < 1e-9


def test_combine_same_day_prefers_a_stated_total_over_components():
    # XZL 2025-03-06 arrives as 0.00248, 0.006 and their total 0.00848.
    assert abs(ca.combine_same_day([0.00248, 0.006, 0.00848]) - 0.00848) < 1e-9
    # With no total row the components must add up.
    assert abs(ca.combine_same_day([0.00414, 0.00791]) - 0.01205) < 1e-9


def test_dual_listed_ids_are_nulled(monkeypatch):
    # NS8U/P7VU share ibmCode 2D00, so the SGX id repeats across symbols and
    # would violate UNIQUE (id). The (symbol, date) PK still identifies the row.
    class Resp:
        status_code = 200
        text = ""

        def json(self):
            return []

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ca.requests, "get", lambda *a, **k: Resp())
    rows = [{"symbol": "NS8U.SI", "date": "2026-07-27", "id": 999},
            {"symbol": "P7VU.SI", "date": "2026-07-27", "id": 999},
            {"symbol": "D05.SI", "date": "2026-07-27", "id": 111}]
    ca.dedupe_ids("sgx_dividend", rows)
    assert rows[0]["id"] is None and rows[1]["id"] is None
    assert rows[2]["id"] == 111


def test_bonus_warrants_are_not_share_capitalisations():
    rows = [{"anncType": "BONUS", "particulars": "BONUS WARRANTS OFFER OF 1 FOR 2",
             "exDate": 1793625600000, "recDate": None, "datePaid": None, "id": 1}]
    assert ca.build_bonus("ABC.SI", rows) == []


def test_rows_drop_prefix_matched_ibmcodes(monkeypatch):
    """The ibmcode filter is a PREFIX match: asking for 543 also returns 543L
    (an Erajaya bond) and 507 returns 507Q/507X (SocGen warrants). Measured 22
    such rows across 4 symbols. They must never reach the dividend builder, or a
    bond coupon is booked as an equity dividend."""
    class FakeResp:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(sess, url, params, timeout, attempts=3):
        return FakeResp({"meta": {"totalItems": 3, "totalPages": 1}, "data": [
            {"ibmCode": "543", "anncType": "DIVIDEND", "particulars": "Rate: SGD 0.003  Per Security"},
            {"ibmCode": "543L", "anncType": "INTEREST", "particulars": "Rate: 4.5%"},
            {"ibmCode": "543L", "anncType": "DIVIDEND", "particulars": "Rate: SGD 9.99  Per Security"},
        ]})

    monkeypatch.setattr(ca, "get_with_retry", fake_get)
    client = ca.CorporateActionsClient.__new__(ca.CorporateActionsClient)
    client._session = object()
    rows = client.rows("543")
    assert [r["ibmCode"] for r in rows] == ["543"]
    assert len(rows) == 1


def test_rows_refuses_an_empty_ibmcode():
    # None / '' makes the API ignore the filter and return all ~40k rows.
    client = ca.CorporateActionsClient.__new__(ca.CorporateActionsClient)
    client._session = object()
    assert client.rows(None) == []
    assert client.rows("") == []


def test_detail_failure_is_fatal_not_silently_partial(monkeypatch):
    """A 403/502 on one announcement page drops that dividend component, which
    changes the total (M44U 2026-11-04 came out as 0.00611 instead of 0.00414
    when a sibling page failed). An incomplete table must never look complete,
    so a failed detail fetch raises instead of being skipped."""
    class Sess:
        def get(self, url, headers=None, timeout=None):
            class R:
                status_code = 403
                text = "Access Denied"
            return R()

    client = ca.AnnouncementClient.__new__(ca.AnnouncementClient)
    client._session = Sess()
    # No real sleeping / retrying in a unit test.
    monkeypatch.setattr(ca.time, "sleep", lambda *_: None)
    try:
        client.detail("https://links.sgx.com/x", attempts=1)
        raise AssertionError("expected a detail fetch to raise on 403")
    except RuntimeError as exc:
        assert "403" in str(exc)


def test_failed_detail_aborts_the_whole_cact_run(monkeypatch):
    """Skipping a failed page must not shrink the table quietly."""
    client = ca.AnnouncementClient.__new__(ca.AnnouncementClient)
    monkeypatch.setattr(client, "list", lambda sub, **kw: [
        {"url": "u1", "title": "New", "submission_date": "20260101",
         "issuers": [{"stock_code": "AAA"}]},
    ])

    def boom(url):
        raise RuntimeError("HTTP 403 (banned)")

    monkeypatch.setattr(client, "detail", boom)
    try:
        ca.cact_records(client, {"CACT06": "dividend"}, {"AAA"}, {"AAA.SI": "SGD"})
        raise AssertionError("expected the run to abort")
    except RuntimeError as exc:
        assert "403" in str(exc)


def test_amounts_are_converted_to_sgd():
    """The table has no currency column and its sibling idx_dividend holds IDR,
    so every stored amount must be SGD. BBW (Azeus) declares HKD: 2.8 HKD is
    0.45 SGD, not 2.8 SGD."""
    rates = {"HKD": 0.1631, "USD": 1.2797}
    assert abs(ca.to_sgd(2.8, "HKD", rates) - 0.45668) < 1e-9
    assert ca.to_sgd(0.81, "SGD", rates) == 0.81
    assert ca.to_sgd(None, "HKD", rates) is None


def test_unknown_currency_is_not_silently_one_to_one():
    """A missing rate must be loud, not treated as 1.0 - that turns a 6x error
    into a plausible-looking number."""
    seen = []
    orig = ca._log.warning
    ca._log.warning = lambda msg, *a, **k: seen.append(msg % a if a else msg)
    try:
        got = ca.to_sgd(5.0, "XXX", {})
    finally:
        ca._log.warning = orig
    assert got == 5.0 and any("no SGD rate" in m for m in seen)


def test_mixed_currency_components_are_converted_before_summing():
    """Components of one distribution can be declared in different currencies;
    summing raw numbers would be meaningless."""
    rates = {"HKD": 0.1631, "USD": 1.2797}
    rows = [
        {"anncType": "DIVIDEND", "particulars": "Rate: HKD 2.8  Per Security",
         "exDate": 1791686400000, "recDate": None, "datePaid": None, "id": 1, "ibmCode": "X"},
        {"anncType": "DIVIDEND", "particulars": "Rate: USD 1  Per Security",
         "exDate": 1791686400000, "recDate": None, "datePaid": None, "id": 2, "ibmCode": "X"},
    ]
    rec = ca.build_dividends("BBW.SI", rows, rates)
    assert len(rec) == 1
    assert rec[0]["currency"] == "SGD"
    assert abs(rec[0]["dividend"] - (0.45668 + 1.2797)) < 1e-9


def test_rates_fall_back_to_the_local_file_for_currencies_the_remote_omits():
    """The daily remote table only requests a fixed currency list, so CHF and JPY
    are missing from it even though SGX pays them. Those must come from the
    local file rather than being left unconverted."""
    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"USD": {"SGD": 1.29}, "HKD": {"SGD": 0.164},
                    "datetime": "2026-11-18 23:49:31"}

    orig = ca.requests.get
    ca.requests.get = lambda *a, **k: Resp()
    try:
        rates = ca.load_sgd_rates(path=ca.RATES_FILE)
    finally:
        ca.requests.get = orig
    assert rates["USD"] == 1.29 and rates["HKD"] == 0.164
    assert rates.get("CHF") and rates.get("JPY")


def test_rates_survive_an_unreachable_remote():
    orig = ca.requests.get
    ca.requests.get = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("offline"))
    try:
        rates = ca.load_sgd_rates(path=ca.RATES_FILE)
    finally:
        ca.requests.get = orig
    assert rates.get("USD") and rates.get("CHF")


def test_write_rows_are_homogeneous_and_known_columns():
    """PostgREST rejects a bulk upsert whose objects have different keys
    (PGRST102) and any unknown key (PGRST204). Dividends arrive from two sources
    with different provenances, so every row must be projected onto the table's
    column list before writing."""
    rows = [
        {"symbol": "A.SI", "date": "2020-01-01", "dividend": 1.0, "source": "sgx_cact",
         "dividend_original": 1.0, "payment_type": "Tax Exempted", "_components": [1.0]},
        {"symbol": "B.SI", "date": "2020-01-01", "dividend": 2.0,
         "source": "sgx_corporateactions"},
    ]
    out = ca.normalise("sgx_dividend", rows)
    keys = [tuple(sorted(r)) for r in out]
    assert len(set(keys)) == 1, "every object must carry identical keys"
    assert "_components" not in out[0]
    assert set(out[0]) == set(ca.TABLE_COLUMNS["sgx_dividend"])
    # A column absent on one source is NULL, not missing.
    assert out[1]["dividend_original"] is None


def test_every_target_table_column_list_covers_its_builders():
    """Guard against a builder emitting a column the writer would silently drop."""
    produced = {
        "sgx_dividend": {"symbol", "date", "dividend", "dividend_original",
                         "recording_date", "cum_date", "payment_date",
                         "payment_type", "currency", "id", "source"},
        "sgx_ca_bonus": {"symbol", "ex_date", "recording_date", "old_ratio",
                         "new_ratio", "cum_date", "payment_date", "id", "source"},
        "sgx_right_issue": {"symbol", "ex_date", "recording_date", "old_ratio",
                            "new_ratio", "price", "factor", "cum_date",
                            "trading_period_start", "trading_period_end",
                            "subscription_date", "id", "source"},
        "sgx_stock_split": {"symbol", "date", "split_ratio", "ratio", "cum_date",
                            "recording_date", "applied_on", "id", "source"},
    }
    for table, cols in produced.items():
        assert cols <= set(ca.TABLE_COLUMNS[table]), table


def test_reference_comes_from_each_entry_not_the_last_fetch(monkeypatch):
    """The forward calendar is keyed on the corporate-action reference. It must
    be read from each entry's own fields: `ref` in the fetch loop's scope is a
    leftover from the final iteration, which stamped every row with the same
    reference (72 rows, 1 ref)."""
    client = ca.AnnouncementClient.__new__(ca.AnnouncementClient)
    monkeypatch.setattr(client, "list", lambda sub, **kw: [
        {"url": "u1", "title": "New", "submission_date": "20260101",
         "issuers": [{"stock_code": "AAA"}]},
    ])
    monkeypatch.setattr(client, "detail", lambda url: {
        "Corporate Action Reference": "REF1", "Ex Date": "01/02/2027",
        "Net Rate (Per Share)": "SGD 1.0", "Gross Rate (Per Share)": "SGD 1.0",
    })
    out = ca.cact_records(client, {"CACT06": "dividend"}, {"AAA"}, {"AAA.SI": "SGD"},
                         rates={"SGD": 1.0})
    assert out["sgx_dividend"][0]["reference"] == "REF1"


def test_split_routes_future_ex_dates_to_the_calendar():
    """A dividend announced today can have an ex-date months out, so the split
    is on ex-date, not announcement date."""
    import datetime
    today = datetime.date.today()
    past = (today - datetime.timedelta(days=10)).isoformat()
    future = (today + datetime.timedelta(days=10)).isoformat()
    rows = [
        {"symbol": "A.SI", "date": past, "dividend": 1.0, "reference": "R1"},
        {"symbol": "B.SI", "date": future, "dividend": 2.0, "reference": "R2"},
    ]
    history, upcoming = [], []
    for r in rows:
        (upcoming if r["date"] > today.isoformat() and r.get("reference") else history).append(r)
    assert len(history) == 1 and len(upcoming) == 1
    assert upcoming[0]["symbol"] == "B.SI"


def _fake_get(status, calls):
    class R:
        status_code = status

        def raise_for_status(self):
            import requests
            raise requests.HTTPError(f"{self.status_code}")

    class S:
        def __init__(self):
            self.calls = 0

        def get(self, *a, **k):
            self.calls += 1
            calls.append(self.status_code)
            return R()

    S.status_code = status
    return S()


def test_retry_does_not_retry_a_permanent_4xx(monkeypatch):
    """A 404 never succeeds on retry, and each attempt sleeps - three sleeps on
    a dead symbol slows the whole run for no benefit."""
    monkeypatch.setattr(ca.time, "sleep", lambda *_: None)
    calls = []
    sess = _fake_get(404, calls)
    try:
        ca.get_with_retry(sess, "u", {}, 30, attempts=3)
        raise AssertionError("expected a failure")
    except Exception:
        pass
    assert sess.calls == 1, f"404 should be attempted once, was {sess.calls}"


def test_retry_does_retry_transient_statuses(monkeypatch):
    monkeypatch.setattr(ca.time, "sleep", lambda *_: None)
    for status in (500, 429):
        calls = []
        sess = _fake_get(status, calls)
        try:
            ca.get_with_retry(sess, "u", {}, 30, attempts=3)
            raise AssertionError("expected a failure")
        except Exception:
            pass
        assert sess.calls == 3, f"{status} should retry 3x, was {sess.calls}"


def test_windowed_cact_never_downgrades_a_stored_dividend():
    """A windowed CACT read covers only recent filings, so 'no CACT row' does not
    mean 'CACT has nothing'. Writing corporateactions alone silently LOWERED
    correct values: N2IU 2025-10-29 is 0.0201 from the union but 0.0047 from
    corporateactions alone (measured: 124 rows across 7 REITs)."""
    corp = [{"symbol": "N2IU.SI", "date": "2025-10-29", "dividend": 0.0047,
             "_components": [0.0047], "source": "sgx_corporateactions"}]
    cact = [{"symbol": "N2IU.SI", "date": "2026-11-20", "dividend": 0.02,
             "source": "sgx_cact"}]

    # windowed: the 2025 date has no CACT row, so it must be skipped entirely
    out = ca.merge_dividend_sources(corp, cact, complete_cact_dates=False)
    assert [r["date"] for r in out] == ["2026-11-20"]
    assert out[0]["dividend"] == 0.02

    # full read: CACT was read in its entirety, so corporateactions-only dates
    # are genuinely complete and must be written
    out = ca.merge_dividend_sources(corp, cact, complete_cact_dates=True)
    assert {r["date"] for r in out} == {"2025-10-29", "2026-11-20"}


# ---------------------------------------------------------------- daily runner

def _load_daily_run():
    """Import sgx_daily_run.py without executing main()."""
    import importlib.util, os
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "sgx_scrape", "sgx_daily_run.py")
    spec = importlib.util.spec_from_file_location("sgx_daily_run", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_daily_command_never_touches_the_forward_calendar():
    """sgx_upcoming_dividend is owned by sgx_buyback_pipeline. --split-calendar
    and --prune-calendar would give the table two writers (and prune DELETEs),
    so the scheduler must never pass them."""
    run = _load_daily_run()
    for days in (None, 30, 180):
        cmd = run.build_command(days, write=True, yes=True)
        assert "--split-calendar" not in cmd, cmd
        assert "--prune-calendar" not in cmd, cmd


def test_daily_window_selection():
    """daily 30d, seed 180d, weekly = full history."""
    run = _load_daily_run()
    assert run.window(weekly=False, seeding=False) == 30
    assert run.window(weekly=False, seeding=True) == 180
    assert run.window(weekly=True, seeding=True) is run.FULL_HISTORY


def test_weekly_command_reads_full_history():
    run = _load_daily_run()
    cmd = run.build_command(run.FULL_HISTORY, write=True, yes=True)
    assert "--notices-window" not in cmd, "weekly must read the full 2014+ history"
    assert "--all" in cmd and "--write" in cmd and "--yes" in cmd


def test_yes_without_write_is_rejected(monkeypatch):
    """`--yes` alone used to dry-run and exit 0, so a typo'd cron looked green
    while writing nothing."""
    import sys
    run = _load_daily_run()
    monkeypatch.setattr(sys, "argv", ["sgx_daily_run.py", "--yes"])
    try:
        run.main()
        raise AssertionError("expected argparse to reject --yes without --write")
    except SystemExit as exc:
        assert exc.code == 2, f"expected exit 2, got {exc.code}"


def test_updated_on_is_sent_because_a_default_never_fires_on_conflict(monkeypatch):
    """A column DEFAULT only applies on INSERT, so ON CONFLICT DO UPDATE left
    `updated_on` stale. Verified against the live table: re-upserting an
    identical row did not move it. The payload must carry the timestamp."""
    sent = {}

    class Resp:
        status_code = 201
        text = ""

        def raise_for_status(self):
            pass

    def fake_post(url, headers=None, json=None, timeout=None):
        sent["rows"] = json
        return Resp()

    monkeypatch.setattr(ca.requests, "post", fake_post)
    rows = [{"symbol": "A.SI", "date": "2020-01-01", "dividend": 1.0,
             "source": "sgx_cact"}]
    ca.write_table("sgx_dividend", rows)

    assert sent["rows"], "nothing was posted"
    for r in sent["rows"]:
        assert "updated_on" in r, "updated_on missing - default will not refresh"
        assert r["updated_on"], "updated_on empty"


def test_id_held_by_a_sibling_symbol_is_nulled(monkeypatch):
    """CACT filings name only ONE side of a dual-listed pair (measured: 168
    filings, never both), so a windowed run can see a single symbol whose SGX id
    already belongs to its sibling in the table. Writing it as-is rejects the
    whole batch: 23505 duplicate key on sgx_dividend_id_key (reproduced).
    Batch-local counting cannot see the sibling, so the stored owners are read."""
    class Resp:
        status_code = 200
        text = ""

        def json(self):
            return [{"symbol": "NS8U.SI", "id": 777}]   # sibling already holds it

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ca.requests, "get", lambda *a, **k: Resp())
    rows = [{"symbol": "P7VU.SI", "date": "2020-01-01", "dividend": 1.0, "id": 777}]
    ca.dedupe_ids("sgx_dividend", rows)
    assert rows[0]["id"] is None

    # the same id on the SAME symbol is its own row, not a collision
    rows = [{"symbol": "NS8U.SI", "date": "2020-01-01", "dividend": 1.0, "id": 777}]
    ca.dedupe_ids("sgx_dividend", rows)
    assert rows[0]["id"] == 777


def test_ids_shared_inside_one_batch_are_nulled(monkeypatch):
    """The original case: both sides of a pair in a full-history run."""
    class Resp:
        status_code = 200
        text = ""

        def json(self):
            return []

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ca.requests, "get", lambda *a, **k: Resp())
    rows = [{"symbol": "A.SI", "date": "2020-01-01", "dividend": 1.0, "id": 5},
            {"symbol": "B.SI", "date": "2020-01-01", "dividend": 1.0, "id": 5}]
    ca.dedupe_ids("sgx_dividend", rows)
    assert [r["id"] for r in rows] == [None, None]


def test_two_equal_components_are_not_collapsed_into_one():
    """Two genuine components of the same size look exactly like one component
    reported twice if you dedupe with a set. N02 2009-05-06 is two filings of
    0.10 (ids 12154/12123); Yahoo reports 0.20, and the set-based version stored
    0.10 - half the real distribution. Measured: 132 such dates."""
    assert ca.combine_same_day([0.1, 0.1]) == 0.2
    assert ca.combine_same_day([0.25, 0.25]) == 0.5
    assert ca.combine_same_day([0.02, 0.02, 0.02]) == 0.06


def test_a_stated_total_is_only_used_when_it_appears_once():
    """XZL 2025-03-06 is 0.00248 + 0.006 with a 0.00848 total row. But if the
    largest amount appears twice it cannot be the total of the others, or
    [0.1, 0.1] would wrongly return 0.1."""
    assert abs(ca.combine_same_day([0.00248, 0.006, 0.00848]) - 0.00848) < 1e-9
    assert abs(ca.combine_same_day([0.1, 0.1]) - 0.2) < 1e-9


def test_the_same_component_from_both_sources_counts_once():
    """M44U 2026-11-04 reports 0.00414 in corporateactions AND in CACT06; the
    real distribution is 0.00414 + 0.00611 + 0.00791 = 0.01816. Equal amounts
    from DIFFERENT sources are one component, equal amounts from ONE source are
    two - that is why the source is part of the identity."""
    union = [(0.00414, "corp"), (0.00791, "corp"),
             (0.00414, "cact"), (0.00611, "cact")]
    assert abs(ca.combine_same_day(union) - 0.01816) < 1e-9

    # same amount, same source, twice  -> two components
    assert ca.combine_same_day([(0.1, "corp"), (0.1, "corp")]) == 0.2
    # same amount, one per source      -> one component
    assert ca.combine_same_day([(0.1, "corp"), (0.1, "cact")]) == 0.1
