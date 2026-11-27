from datetime import datetime
from io import BytesIO

import os

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiJ9.dummy")

import openpyxl
import pytest

from institutional_flow.pipeline.extract_data import is_weekly_workbook
from institutional_flow.pipeline.parse_workbook import (
    parse_week,
    parse_workbook,
    SECTORS,
    WorkbookLayoutError,
)
from institutional_flow.pipeline.table_rows import build_table_rows


XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def add_rows(sheet, rows):
    for row in rows:
        sheet.append(row)


def top_10_rows(week_text):
    rows = [
        [f"Week of {week_text}"],
        ["Institutional investors net buy (+S$38.0m) vs. (-S$50.2m) a week ago"],
        ["Retail investors net buy (+S$257.4m) vs. (+S$204.2m) a week ago"],
        ["Top 10 Institution Net Buy (+) Stocks (S$M)", "Stock Code", "Week of", "Top 10 Institution Net Sell (-) Stocks (S$M)", "Stock Code", "Week of"],
        ["CapitaLand Ascendas REIT", "A17U", 84.842197, "UOB", "U11", -40.884196],
        ["AEM ", "AWX", 1.5, "Seatrium", "5E2", -30.482753],
        ["Top 10 Retail Net Buy (+) Stocks (S$M)", "Stock Code", "Week of", "Top 10 Retail Net Sell (-) Stocks (S$M)", "Stock Code", "Week of"],
        ["DBS", "D05", 51.7079436, "Singtel", "Z74", -25.5663985],
        ["Source: SGX."],
    ]
    return rows


def workbook_2021(week_text="22 Nov 2021", first_overall=30.54, last_overall=-173.16, extra_week=None):
    workbook = openpyxl.Workbook()
    add_rows(workbook.active, top_10_rows(week_text))
    workbook.active.title = "Weekly Top 10"

    weeks = [datetime(2021, 11, 15), datetime(2021, 11, 22)]

    if extra_week:
        weeks = weeks[1:] + [extra_week]

    for name, overall in (("Institutional", last_overall), ("Retail", 311.89)):
        sheet = workbook.create_sheet(name)
        add_rows(
            sheet,
            [
                ["Overall", "Week of", "SGX Sector Classification"],
                [None, f"{name} Investors net buy/sell (S$M)", "Energy/\nOil & Gas", "REITs"],
                [first_overall, weeks[0], 0.12, 6.47],
                [overall, weeks[1], 4.09, 4.17],
                [None, "Source: Singapore Exchange."],
            ],
        )

    sti = workbook.create_sheet("STI Constituents")
    add_rows(
        sti,
        [
            ["STI Constituents - Week of 22 Nov 2021", "Stock Code", "Institution\nNet Buy (+) / Net Sell (-) (S$M)", "Retail\nNet Buy (+) / Net Sell (-) (S$M)"],
            ["DBS", "D05", -42.05, 51.7079436],
            ["City Developments", "C09", 0.98, 6.09],
            [None],
        ],
    )

    return save(workbook)


def workbook_2019():
    workbook = openpyxl.Workbook()
    add_rows(workbook.active, top_10_rows("22 July 2019"))
    workbook.active.title = "Weekly Top 10"

    for name in ("Institutional", "Retail"):
        sheet = workbook.create_sheet(name)
        add_rows(
            sheet,
            [
                ["Overall", "Week of", None, "Sectors - MSCI Global Industry Classification Standard (GICS®)"],
                [None, f"{name} Investors net buy/sell (S$M)", "Communication Services", "REITs*"],
                [-64.9, datetime(2019, 7, 22), -25.83, -47.73],
                [None, "Source: Singapore Exchange "],
            ],
        )

    return save(workbook)


def save(workbook) -> bytes:
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_parse_week_reads_every_title_style():
    assert parse_week("Week of 16 November 2026").isoformat() == "2026-11-16"
    assert parse_week("SGX Fund Flow Weekly Tracker (Week of 9 Feb2026)").isoformat() == "2026-02-09"
    assert parse_week("Week of 21 Sept 2020").isoformat() == "2020-09-21"
    assert parse_week("Fund FlowTracker - Week of 18 June 2018").isoformat() == "2018-06-18"
    assert parse_week("Monthly Tracker - July 2019") is None


def test_weekly_filter_skips_monthly_and_pdf_trackers():
    assert is_weekly_workbook({"title": "SGX Fund Flow Weekly Tracker (Week of 9 Nov 2026)", "document_mime": XLSX})
    assert is_weekly_workbook({"title": "SGX Institutional and Retail Fund FlowTracker - Week of 18 June 2018", "document_mime": XLSX})
    assert not is_weekly_workbook({"title": "SGX Institutional and Retail fund flow monthly tracker (May 2022)", "document_mime": XLSX})
    assert not is_weekly_workbook({"title": "SGX Institutional Fund Flow Monthly Tracker - July 2019", "document_mime": "application/pdf"})


def test_2021_layout_sectors_top_10_and_sti():
    parsed = parse_workbook(workbook_2021())

    assert parsed["week_start"] == "2021-11-22"

    sectors = {
        (row["date"], row["investor_type"], row["sector"]): row
        for row in parsed["sector_rows"]
    }
    assert sectors[("2021-11-22", "institutional", "Overall")]["value"] == -173_160_000
    # SGX's names are mapped to ours, the original stays in sgx_sector
    assert sectors[("2021-11-15", "retail", "REIT")]["value"] == 6_470_000
    assert sectors[("2021-11-15", "retail", "REIT")]["sgx_sector"] == "REITs"
    assert sectors[("2021-11-22", "institutional", "Energy")]["sgx_sector"] == "Energy/Oil & Gas"

    stocks = [(row["symbol"], row["investor_type"], row["flow_type"], row["rank"]) for row in parsed["stock_rows"]]
    assert ("A17U.SI", "institutional", "net_buy", 1) in stocks
    assert ("5E2.SI", "institutional", "net_sell", 2) in stocks
    assert ("D05.SI", "retail", "net_buy", 1) in stocks
    # STI-sheet rows have no rank, the sign gives the side
    assert ("C09.SI", "retail", "net_buy", None) in stocks
    assert ("D05.SI", "institutional", "net_sell", None) in stocks

    names = {row["symbol"]: row["name"] for row in parsed["stock_rows"]}
    assert names["AWX.SI"] == "AEM"


def test_2019_layout_gics_behind_spacer_column():
    parsed = parse_workbook(workbook_2019())

    sectors = {(row["investor_type"], row["sector"]): row for row in parsed["sector_rows"]}
    assert sectors[("institutional", "Communication Services")]["value"] == -25_830_000
    assert sectors[("retail", "REIT")]["value"] == -47_730_000
    assert sectors[("retail", "REIT")]["sgx_sector"] == "REITs"
    assert all(row["rank"] for row in parsed["stock_rows"])


def test_every_sgx_and_gics_sector_maps_to_one_of_ours():
    ours = {
        "Basic Materials", "Communication Services", "Consumer Cyclicals", "Consumer Defensive",
        "Energy", "Financial Services", "Healthcare", "Industrials", "Properties & Real Estate",
        "REIT", "Technology", "Utilities",
    }
    assert set(SECTORS.values()) <= ours


def test_unmapped_sector_is_skipped_not_fatal():
    workbook = openpyxl.load_workbook(BytesIO(workbook_2021()))
    workbook["Institutional"]["E2"] = "Space Mining"
    parsed = parse_workbook(save(workbook))

    institutional = {row["sgx_sector"] for row in parsed["sector_rows"] if row["investor_type"] == "institutional"}
    assert institutional == {"Overall", "Energy/Oil & Gas", "REITs"}


def test_name_printed_beside_two_codes_is_flagged(caplog):
    workbook = openpyxl.load_workbook(BytesIO(workbook_2021()))
    # SGX's 9 May 2022 slip: a name copied beside another stock's code
    workbook["Weekly Top 10"]["D8"] = "UOB"

    with caplog.at_level("WARNING"):
        parsed = parse_workbook(save(workbook))

    assert any("'UOB' is printed beside U11.SI, Z74.SI" in message for message in caplog.messages)
    assert {row["symbol"] for row in parsed["stock_rows"] if row["name"] == "UOB"} == {"U11.SI", "Z74.SI"}


def test_distinct_names_raise_no_flag(caplog):
    with caplog.at_level("WARNING"):
        parse_workbook(workbook_2021())

    assert not any("is printed beside" in message for message in caplog.messages)


def test_missing_sector_sheet_is_a_layout_error():
    workbook = openpyxl.load_workbook(BytesIO(workbook_2021()))
    del workbook["Retail"]

    with pytest.raises(WorkbookLayoutError):
        parse_workbook(save(workbook))


def result(content, report_date, source):
    parsed = parse_workbook(content)

    for row in parsed["sector_rows"] + parsed["stock_rows"]:
        row["source"] = source

    return {"status": "parsed", "report_date": report_date, **parsed}


def test_newest_workbook_wins_a_revised_week_and_top_10_beats_sti():
    older = result(workbook_2021(), "2021-11-29", "older.xlsx")
    newer = result(
        workbook_2021("29 Nov 2021", first_overall=-170.0, last_overall=12.0, extra_week=datetime(2021, 11, 29)),
        "2021-12-06",
        "newer.xlsx",
    )

    sector_rows, stock_rows = build_table_rows([older, newer])
    sectors = {(row["date"], row["investor_type"], row["sector"]): row for row in sector_rows}

    # The week of 22 Nov is in both, the newer workbook's revision is kept
    assert sectors[("2021-11-22", "institutional", "Overall")]["value"] == -170_000_000
    assert sectors[("2021-11-22", "institutional", "Overall")]["source"] == "newer.xlsx"
    assert sectors[("2021-11-15", "institutional", "Overall")]["source"] == "older.xlsx"

    dbs_retail = [
        row for row in stock_rows
        if row["symbol"] == "D05.SI" and row["investor_type"] == "retail" and row["date"] == "2021-11-22"
    ]
    assert len(dbs_retail) == 1
    assert dbs_retail[0]["flow_type"] == "net_buy"
    assert dbs_retail[0]["rank"] == 1
    assert "created_at" not in dbs_retail[0]
    assert set(dbs_retail[0]) == {"symbol", "date", "investor_type", "flow_type", "value", "rank", "source"}
    assert set(sector_rows[0]) == {"date", "investor_type", "sector", "value", "source"}


def test_week_range_title_and_holiday_tuesday():
    assert parse_week("Week of 11 to 14 December 2018").isoformat() == "2018-12-11"
    assert parse_week("Week of 26 May 2020").isoformat() == "2020-05-26"


def test_sector_weeks_dated_from_workbook_week_despite_typo():
    # The sheet prints a Thursday for its first week, the rows are still weekly Mondays
    parsed = parse_workbook(workbook_2021(extra_week=None, week_text="22 Nov 2021"))
    assert sorted({row["date"] for row in parsed["sector_rows"]}) == ["2021-11-15", "2021-11-22"]

    workbook = openpyxl.load_workbook(BytesIO(workbook_2021()))
    workbook["Institutional"]["B3"] = datetime(2021, 11, 18)
    parsed = parse_workbook(save(workbook))
    assert {row["date"] for row in parsed["sector_rows"]} == {"2021-11-15", "2021-11-22"}


def test_week_printed_as_text_is_still_read():
    workbook = openpyxl.load_workbook(BytesIO(workbook_2021()))
    workbook["Institutional"]["B4"] = "11 to 14 Dec-18"
    parsed = parse_workbook(save(workbook))

    sectors = {(row["date"], row["investor_type"], row["sector"]): row for row in parsed["sector_rows"]}
    assert sectors[("2021-11-22", "institutional", "Overall")]["value"] == -173_160_000
    assert sectors[("2021-11-15", "institutional", "Overall")]["value"] == 30_540_000
