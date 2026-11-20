from periodic_financial.utils.currency import load_rates
from .table_rows import build_table_rows
from ..utils.supabase_helper import get_stored_reports, upsert_reports, REPORT_TABLE

import logging


LOGGER = logging.getLogger(__name__)


def drop_stored_documents(documents: list[dict], refresh: bool) -> list[dict]:
    """
    The daily window overlaps a week of runs. A second read of a stored report
    can differ in a cell or two and would overwrite a checked row, so a stored
    report is only read again with refresh
    """
    if refresh or not documents:
        return documents

    symbols = sorted({
        symbol 
        for document in documents 
        for symbol in document["symbols"]}
    )
    
    stored = {
        (row["symbol"], row["source"]) 
        for row in get_stored_reports(symbols)
    }
    
    unread = []

    for document in documents:
        symbols_left = [
            symbol
            for symbol in document["symbols"]
            if (symbol, document["report"]["document_url"]) not in stored
        ]

        if symbols_left:
            unread.append({**document, "symbols": symbols_left})

    LOGGER.info(
        "[List] %d of %d documents already stored, skipped",
        len(documents) - len(unread),
        len(documents),
    )

    return unread


def prepare_table_rows(results: list[dict]) -> tuple[list[dict], set[tuple]]:
    """
    The rows to write for this run's extracted reports, and the keys already in
    the table, so the summary can tell a new row from an updated one
    """
    rows_by_report = {}

    # A report SGX lists under two filings is extracted twice, the first wins
    for result in results:
        if result["status"] == "extracted":
            row = result["row"]
            rows_by_report.setdefault(
                (row["symbol"], row["contributor"], row["published_date"]), row
            )

    if not rows_by_report:
        return [], set()

    symbols = sorted({symbol for symbol, _, _ in rows_by_report})
    stored_rows = get_stored_reports(symbols)

    LOGGER.info(
        "[Clean] %d reports, %d stored rows for their %d stocks",
        len(rows_by_report),
        len(stored_rows),
        len(symbols),
    )

    rows = build_table_rows(
        list(rows_by_report.values()), 
        stored_rows, 
        load_rates()
    )
    
    stored_keys = {
        (row["symbol"], row["contributor"], row["date"]) 
        for row in stored_rows
    }

    return rows, stored_keys


def write_rows(rows: list[dict], dry_run: bool) -> None:
    if not rows:
        LOGGER.info("[Upsert] nothing to write")

    elif dry_run:
        LOGGER.info("[Upsert] dry run, %d rows not written", len(rows))

    else:
        # Upserted on symbol, contributor and date, so a rerun overwrites
        upsert_reports(rows)
        LOGGER.info("[Upsert] %d rows written to %s", len(rows), REPORT_TABLE)
