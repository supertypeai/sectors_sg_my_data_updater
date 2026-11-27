from datetime import date, datetime
from typing import Iterator

from future_analyst_report.main_analyst_report.api import (
    create_session,
    download_document,
    get_with_retry,
    to_unix,
    unwrap,
    APP_CONFIG_URL,
    SG_TIMEZONE,
)

import json
import logging
import random
import requests
import time


LOGGER = logging.getLogger(__name__)

# The CMS query behind sgx.com/stock-exchange/data-reports
FUND_FLOW_QUERY = "funds_flow_reports_list"

# reportType on the page, 203 is the Fund Flow Tracker. 204 Institutional
# Newsletter, 355 Daily Momentum and 356 Outstanding Short Sell share the query
FUND_FLOW_TRACKER_TYPE = "203"

PAGE_SIZE = 100

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

__all__ = ["create_session", "download_document", "iter_reports"]


def get_cms_query_url(session: requests.Session) -> tuple[str, str]:
    # CMS_VERSION changes when SGX deploys, so it is read from the app config every run
    response = get_with_retry(session, APP_CONFIG_URL)
    response.raise_for_status()
    config = response.json()

    cms_url = config["endpoints"]["CMS_API_URL"]
    cms_version = config["CMS_VERSION"]

    return f"{cms_url}/", f"{cms_version}:{FUND_FLOW_QUERY}"


def build_variables(
    period_start: date | None = None,
    period_end: date | None = None,
    report_type: str = FUND_FLOW_TRACKER_TYPE,
) -> dict:
    # Every filter only applies when its FilterEnabled flag is sent with it
    variables = {
        "lang": "EN",
        "reportType": report_type,
        "reportTypeFilterEnabled": True,
    }

    if period_start:
        variables["fromDate"] = to_unix(period_start)
        variables["fromDateFilterEnabled"] = True

    if period_end:
        variables["toDate"] = to_unix(period_end, end_of_day=True)
        variables["toDateFilterEnabled"] = True

    return variables


def fetch_reports_page(
    session: requests.Session,
    query_url: str,
    query_id: str,
    variables: dict,
) -> tuple[int, list[dict]]:
    params = {"queryId": query_id, "variables": json.dumps(variables)}

    response = get_with_retry(session, query_url, params=params)
    response.raise_for_status()

    report_list = (response.json().get("data") or {}).get("list") or {}

    return report_list.get("count") or 0, report_list.get("results") or []


def parse_report(raw_report: dict) -> dict:
    report = unwrap(raw_report)
    document = unwrap(report.get("report"))
    file = unwrap(document.get("file"))

    # reportDate is the day SGX published the file, a Monday or Tuesday after
    # the week it covers. The week itself is read from the workbook
    report_timestamp = report.get("reportDate")
    report_date = (
        datetime.fromtimestamp(report_timestamp, SG_TIMEZONE).date().isoformat()
        if report_timestamp
        else None
    )

    return {
        "title": (report.get("title") or "").strip(),
        "report_date": report_date,
        "document_url": file.get("url"),
        "document_mime": file.get("filemime"),
    }


def iter_reports(
    session: requests.Session,
    period_start: date | None = None,
    period_end: date | None = None,
) -> Iterator[dict]:
    query_url, query_id = get_cms_query_url(session)
    variables = build_variables(period_start, period_end)

    offset = 0

    while True:
        total, reports = fetch_reports_page(
            session,
            query_url,
            query_id,
            {**variables, "limit": PAGE_SIZE, "offset": offset},
        )

        if not reports:
            break

        LOGGER.debug("[Fund Flow] offset %d: %d records of %d", offset, len(reports), total)

        for raw_report in reports:
            yield parse_report(raw_report)

        offset += len(reports)

        if offset >= total:
            break

        time.sleep(random.uniform(0.5, 2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    for report in iter_reports(create_session(), period_start=date(2026, 11, 1)):
        print(report)


# uv run -m institutional_flow.fund_flow_report.api
