import logging


LOGGER = logging.getLogger(__name__)

SECTOR_KEY = ("date", "investor_type", "sector")
STOCK_KEY = ("symbol", "date", "investor_type")

# created_at is left out, its default keeps the time a row was first written
# sgx_sector stays on the parsed rows for checks, the table keeps our sector only
SECTOR_COLUMNS = ("date", "investor_type", "sector", "value", "source")
# name stays on the parsed rows for checks, the site reads it from sgx_company_report
STOCK_COLUMNS = ("symbol", "date", "investor_type", "flow_type", "value", "rank", "source")

# Some weeks print one list to S$0.1M, so copies within S$50k are the same figure
SAME_FLOW_TOLERANCE = 50_000


def newest_first(results: list[dict]) -> list[dict]:
    return sorted(
        (result for result in results if result["status"] == "parsed"),
        key=lambda result: (result["week_start"], result["report_date"] or ""),
        reverse=True,
    )


def build_sector_rows(results: list[dict]) -> list[dict]:
    """
    Each sheet repeats the last 4 weeks, so a week is in 4 workbooks. The
    newest workbook carries SGX's latest figure for it and wins
    """
    rows = {}

    for result in newest_first(results):
        for row in result["sector_rows"]:
            key = tuple(row[column] for column in SECTOR_KEY)
            rows.setdefault(key, {column: row[column] for column in SECTOR_COLUMNS})

    return sorted(rows.values(), key=lambda row: tuple(row[column] for column in SECTOR_KEY))


def build_stock_rows(results: list[dict]) -> list[dict]:
    """
    A stock on a Top 10 list and on the STI sheet has one row, the Top 10 copy
    keeps its rank
    """
    rows = {}

    for result in newest_first(results):
        # The Top 10 rows come first in each result, so they win a key they share
        for row in result["stock_rows"]:
            key = tuple(row[column] for column in STOCK_KEY)
            stored = rows.get(key)

            if stored is None:
                rows[key] = {column: row[column] for column in STOCK_COLUMNS}

            elif (
                stored["source"] == row["source"]
                and abs(stored["value"] - row["value"]) > SAME_FLOW_TOLERANCE
            ):
                LOGGER.warning(
                    "[Clean] %s %s week of %s: Top 10 says %s, STI sheet says %s",
                    row["symbol"],
                    row["investor_type"],
                    row["date"],
                    stored["value"],
                    row["value"],
                )

    return sorted(rows.values(), key=lambda row: tuple(row[column] for column in STOCK_KEY))


def build_table_rows(results: list[dict]) -> tuple[list[dict], list[dict]]:
    return build_sector_rows(results), build_stock_rows(results)
