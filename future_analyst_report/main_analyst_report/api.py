from datetime import date, datetime, timedelta, timezone
from requests.exceptions import RequestException
from typing import Iterator

import json
import logging
import random
import requests
import time


LOGGER = logging.getLogger(__name__)

APP_CONFIG_URL = "https://www.sgx.com/config/appconfig.json"

# Analyst Research, Margin Schedule and the OTC FX reports all share this CMS query
GENERIC_REPORT_QUERY = "generic_report_list"

# genericReportType id the analyst research page always filters on
ANALYST_RESEARCH_TYPE = "497"

# category filter on the page, 25 is Derivatives
SECURITIES_CATEGORY = "26"

PAGE_SIZE = 100

MAX_ATTEMPTS = 3

# reportDate is midnight sg time stored as unix seconds
SG_TIMEZONE = timezone(timedelta(hours=8))

HEADERS = {
    "accept": "*/*",
    "origin": "https://www.sgx.com",
    "referer": "https://www.sgx.com/",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
}


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    return session


def get_with_retry(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    timeout: int = 60,
) -> requests.Response:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)

            if response.status_code >= 500:
                response.raise_for_status()

            return response

        except RequestException as error:
            if attempt == MAX_ATTEMPTS:
                raise

            LOGGER.warning(
                "Request failed on attempt %d/%d, retrying: %s",
                attempt,
                MAX_ATTEMPTS,
                error,
            )
            time.sleep(attempt * 5)


def get_cms_query_url(session: requests.Session) -> tuple[str, str]:
    # CMS_VERSION changes when SGX deploys, so it is read from the app config every run
    response = get_with_retry(session, APP_CONFIG_URL)
    response.raise_for_status()
    config = response.json()

    cms_url = config["endpoints"]["CMS_API_URL"]
    cms_version = config["CMS_VERSION"]

    return f"{cms_url}/", f"{cms_version}:{GENERIC_REPORT_QUERY}"


def to_unix(day: date, end_of_day: bool = False) -> str:
    moment = datetime.combine(day, datetime.min.time(), tzinfo=SG_TIMEZONE)

    if end_of_day:
        moment += timedelta(days=1, seconds=-1)

    return str(int(moment.timestamp()))


def build_variables(
    security_code: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    report_contributor: str | None = None,
    category: str | None = SECURITIES_CATEGORY,
) -> dict:
    # Every filter only applies when its FilterEnabled flag is sent with it
    variables = {
        "lang": "EN",
        "genericReportType": ANALYST_RESEARCH_TYPE,
        "genericReportTypeFilterEnabled": True,
    }

    if category:
        variables["category"] = category
        variables["categoryFilterEnabled"] = True

    if security_code:
        # The site sends the stock code lower case, e.g. ?value=d05 for DBS
        variables["securityCode"] = security_code.lower()
        variables["securityCodeFilterEnabled"] = True

    if period_start:
        variables["fromDate"] = to_unix(period_start)
        variables["fromDateFilterEnabled"] = True

    if period_end:
        variables["toDate"] = to_unix(period_end, end_of_day=True)
        variables["toDateFilterEnabled"] = True

    if report_contributor:
        variables["reportContributor"] = report_contributor
        variables["reportContributorFilterEnabled"] = True

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


def unwrap(node: dict | None) -> dict:
    # The CMS nests every reference as {"data": {"data": {...}}}
    while isinstance(node, dict) and set(node) == {"data"}:
        node = node["data"]

    return node or {}


def get_names(nodes: list[dict] | None) -> list[str]:
    return [unwrap(node).get("name") for node in nodes or [] if unwrap(node)]


def parse_report(raw_report: dict) -> dict:
    report = unwrap(raw_report)
    title = report.get("title") or ""
    document = unwrap(report.get("report"))
    file = unwrap(document.get("file"))

    report_timestamp = report.get("reportDate")
    report_date = (
        datetime.fromtimestamp(report_timestamp, SG_TIMEZONE).date().isoformat()
        if report_timestamp
        else None
    )

    return {
        "title": report.get("title"),
        "report_date": report_date,
        # A few entries leave it empty, their titles start "DBS - ..."
        "contributor": unwrap(report.get("reportContributor")).get("name")
        or (title.split(" - ")[0].strip() if " - " in title else None),
        "category": unwrap(report.get("marketUpdateCategory")).get("name"),
        "asset_classes": get_names(report.get("assetClass")),
        "sectors": get_names(report.get("sector")),
        "document_name": document.get("name"),
        "document_url": file.get("url"),
        "document_mime": file.get("filemime"),
    }


def iter_reports(
    session: requests.Session,
    security_code: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    report_contributor: str | None = None,
    category: str | None = SECURITIES_CATEGORY,
) -> Iterator[dict]:
    query_url, query_id = get_cms_query_url(session)

    variables = build_variables(
        security_code=security_code,
        period_start=period_start,
        period_end=period_end,
        report_contributor=report_contributor,
        category=category,
    )

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

        LOGGER.debug(
            "[Analyst Research] offset %d: %d records of %d",
            offset,
            len(reports),
            total,
        )

        for raw_report in reports:
            yield parse_report(raw_report)

        offset += len(reports)

        if offset >= total:
            break

        time.sleep(random.uniform(0.5, 2))


def download_document(session: requests.Session, document_url: str) -> bytes | None:
    response = get_with_retry(session, document_url, timeout=120)

    if response.status_code != 200 or not response.content:
        LOGGER.warning(
            "Download failed (%d): %s",
            response.status_code,
            document_url,
        )
        return None

    return response.content


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    session = create_session()

    # security_code is the page's ?value=, e.g. ?category=26&value=d05 for DBS
    for report in iter_reports(session, security_code="D05"):
        print(report)


# uv run -m future_analyst_report.analyst_report.api 