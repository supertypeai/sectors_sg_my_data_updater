from ..fund_flow_report.api import download_document, XLSX_MIME
from .parse_workbook import parse_workbook, WorkbookLayoutError

import logging
import random
import requests
import time


LOGGER = logging.getLogger(__name__)


def is_weekly_workbook(report: dict) -> bool:
    """
    The same list carries the 2016-2019 monthly PDF trackers and, until 2023,
    a monthly Excel tracker most months. Only the weekly workbook is read
    """
    title = report["title"].lower()

    # The first workbooks were titled "Fund FlowTracker - Week of 18 June 2018"
    return (
        report["document_mime"] == XLSX_MIME
        and "monthly" not in title
        and ("weekly" in title or "week of" in title)
    )


def process_report(session: requests.Session, report: dict) -> dict:
    result = {
        **report,
        "week_start": None,
        "sector_rows": [],
        "stock_rows": [],
        "error": None,
    }

    if not is_weekly_workbook(report):
        return {**result, "status": "not_weekly"}

    content = download_document(session, report["document_url"])

    if not content:
        return {**result, "status": "download_failed"}

    try:
        parsed = parse_workbook(content, report["title"])

    except WorkbookLayoutError as error:
        LOGGER.warning("[Parse] %s: layout changed, %s", report["title"], error)
        return {**result, "status": "layout_changed", "error": str(error)}

    except Exception as error:
        LOGGER.exception("[Parse] %s: could not read the workbook", report["title"])
        return {**result, "status": "error", "error": str(error)}

    for row in parsed["sector_rows"] + parsed["stock_rows"]:
        row["source"] = report["document_url"]

    LOGGER.info(
        "[Parse] week of %s: %d sector rows, %d stock rows",
        parsed["week_start"],
        len(parsed["sector_rows"]),
        len(parsed["stock_rows"]),
    )

    return {**result, **parsed, "status": "parsed"}


def process_reports(session: requests.Session, reports: list[dict]) -> list[dict]:
    results = []

    for index, report in enumerate(reports):
        result = process_report(session, report)
        results.append(result)

        if result["status"] != "not_weekly" and index < len(reports) - 1:
            time.sleep(random.uniform(0.3, 1))

    return results
