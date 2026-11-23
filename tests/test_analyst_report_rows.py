"""sgx_analyst_report.source is nullable, the previous-target join must survive it.

add_stored_history() decides whether the stored row or the new one wins by
looking for SGX's host in both source URLs. A stored row written by a source
that had no URL - or a row amended by hand - leaves `source` NULL, and
`SGX_HOST not in None` is a TypeError that kills the whole CLEAN stage, so no
rows are written for any stock in that batch.
"""
import os
import sys
from pathlib import Path

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.dummy")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from future_analyst_report.pipeline.table_rows import SGX_HOST, add_stored_history, build_table_rows


def new_row(**overrides):
    row = {
        "symbol": "D05.SI",
        "contributor": "DBS",
        "title": "DBS - DBS Group Holdings",
        "published_date": "2026-11-20",
        "analyst": "Analyst",
        "source_url": "https://www.dbs.com/research/D05.pdf",
        "rating": "BUY",
        "original_target_price": 40.0,
        "previous_target_price": None,
        "original_currency": "SGD",
        "fx_rate": 1.0,
        "target_basis": None,
        "forecasts": [],
        "forecast_basis": {},
    }
    row.update(overrides)
    return row


def stored_row(**overrides):
    row = {
        "symbol": "D05.SI",
        "contributor": "DBS",
        "date": "2026-11-20",
        "analyst": "Analyst",
        "title": "DBS - DBS Group Holdings",
        "source": "https://www.dbs.com/research/D05.pdf",
        "rating": "BUY",
        "target_price": 39.0,
        "previous_target_price": None,
        "target_basis": None,
        "forecast": None,
    }
    row.update(overrides)
    return row


def test_stored_row_with_null_source_does_not_crash():
    """Same date, stored source NULL: the new report wins, nothing is appended."""
    rows = {("D05.SI", "DBS"): [new_row()]}
    add_stored_history(rows, [stored_row(source=None)], None)
    assert len(rows[("D05.SI", "DBS")]) == 1, rows


def test_new_row_with_null_source_does_not_crash():
    """Same date, both sources NULL: still no TypeError, new report wins."""
    rows = {("D05.SI", "DBS"): [new_row(source_url=None)]}
    add_stored_history(rows, [stored_row(source=None)], None)
    assert len(rows[("D05.SI", "DBS")]) == 1, rows


def test_sgx_copy_beats_the_site_copy_with_a_stored_source():
    """The guard still does its job when both sources are present."""
    rows = {("D05.SI", "DBS"): [new_row(source_url="https://www.dbs.com/x.pdf")]}
    add_stored_history(
        rows,
        [stored_row(source=f"https://{SGX_HOST}/api/reports/D05.pdf")],
        None,
    )
    # SGX's copy is stored, the site's copy is dropped and the stored row kept
    assert [row.get("stored") for row in rows[("D05.SI", "DBS")]] == [True], rows


def test_build_table_rows_skips_the_stored_row_with_a_null_source():
    """End to end: only the new report is written, never the stored one."""
    table_rows = build_table_rows(
        [new_row()],
        [stored_row(source=None, date="2026-11-19")],
        {},
    )
    assert [row["date"] for row in table_rows] == ["2026-11-20"], table_rows
