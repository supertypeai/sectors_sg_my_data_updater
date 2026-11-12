from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from typing import Iterator
from urllib.parse import urljoin

import codecs
import logging
import random
import re
import time


LOGGER = logging.getLogger(__name__)

APP_CONFIG_URL = "https://www.sgx.com/config/appconfig.json"
ANNOUNCEMENTS_URL = "https://api.sgx.com/announcements/v1.1/company"
ANNOUNCEMENT_HOST = "https://links.sgx.com"

# Financial Statements and Related Announcement
FINANCIAL_STATEMENTS_CATEGORY = "ANNC17"

# The API returns data null for very large pages
PAGE_SIZE = 100

MAX_ATTEMPTS = 3

# links.sgx.com sits behind Akamai and bans the IP for a burst of requests,
# so detail pages and PDFs are fetched one at a time with a pause between
LINKS_DELAY_SECONDS = 1.2

LINKS_HEADERS = {"Referer": "https://www.sgx.com/"}

# Detail page <dt> labels the pipeline reads, mapped to our own names
DETAIL_FIELDS = {
    "status": "status",
    "announcement sub title": "sub_title",
    "announcement reference": "reference",
    "for financial period ended": "period_ended",
    "date time of broadcast": "broadcast_datetime",
    "description": "description",
}


class BlockedError(Exception):
    """links.sgx.com answered 403, every later request in the run would fail too"""


def get_with_retry(
    url: str,
    headers: dict[str, str] | None = None,
    params: dict | None = None,
    timeout: int = 60,
) -> cffi_requests.Response:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # Akamai fingerprints the TLS handshake, plain requests gets a 403
            response = cffi_requests.get(
                url,
                headers=headers,
                params=params,
                impersonate="chrome",
                timeout=timeout,
            )

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


def get_token() -> str:
    # SGX web app reads the token from its CMS and ROT13 decodes it, we do the same
    config_response = get_with_retry(APP_CONFIG_URL)
    config_response.raise_for_status()
    config = config_response.json()

    cms_url = config["endpoints"]["CMS_API_URL"]
    cms_version = config["CMS_VERSION"]
    token_url = f"{cms_url}/?queryId={cms_version}:we_chat_qr_validator"

    token_response = get_with_retry(token_url)
    token_response.raise_for_status()

    raw_token = token_response.json()["data"]["qrValidator"]

    return codecs.encode(raw_token, "rot13")


def get_auth_headers() -> dict[str, str]:
    return {
        "accept": "*/*",
        "origin": "https://www.sgx.com",
        "referer": "https://www.sgx.com/",
        "authorizationtoken": get_token(),
    }


def fetch_announcements_page(params: dict, headers: dict[str, str]) -> list[dict]:
    response = get_with_retry(ANNOUNCEMENTS_URL, headers=headers, params=params)

    # Token expired, refresh once and update headers in place so next pages reuse it
    if response.status_code == 401:
        LOGGER.info("Token expired, refreshing")
        headers["authorizationtoken"] = get_token()
        response = get_with_retry(ANNOUNCEMENTS_URL, headers=headers, params=params)

    response.raise_for_status()

    return response.json().get("data") or []


def iter_announcements(
    period_start: date,
    period_end: date,
    sub_category: str = FINANCIAL_STATEMENTS_CATEGORY,
) -> Iterator[dict]:
    # SGX day D runs from (D-1) 16:00 to D 15:59:59, so the range starts on the previous evening
    start_evening = period_start - timedelta(days=1)

    params = {
        "periodstart": f"{start_evening:%Y%m%d}_160000",
        "periodend": f"{period_end:%Y%m%d}_155959",
        "cat": "ANNC",
        "sub": sub_category,
        "exactsearch": "true",
        "pagesize": PAGE_SIZE,
    }

    headers = get_auth_headers()
    page_start = 0

    while True:
        announcements = fetch_announcements_page(
            {**params, "pagestart": page_start},
            headers,
        )

        if not announcements:
            break

        LOGGER.info(
            "[Announcements] page %d: %d records",
            page_start,
            len(announcements),
        )

        yield from announcements

        page_start += 1
        time.sleep(random.uniform(0.5, 2))


def get_sub_title(announcement: dict) -> str:
    # The API title ends in the same sub title the detail page reports,
    # e.g. "REPL::Financial Statements and Related Announcement::Full Yearly Results"
    return (announcement.get("title") or "").split("::")[-1].strip()


def normalise_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", label.casefold()).strip()


def is_month_end(parsed_date: date) -> bool:
    return (parsed_date + timedelta(days=1)).day == 1


def parse_period_ended(text: str | None) -> str | None:
    # Printed as 31/12/2025, but some issuers type it US style as 09/30/2025. A period
    # usually ends on a month end, day 28 or later, so the reading that gives
    # one wins. A 52-week year closing on a weekday keeps the day/month reading
    if not text:
        return None

    parsed_dates = []

    # H22 types it as 30-Jun-2026
    for date_format in ("%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            parsed_dates.append(datetime.strptime(text.strip(), date_format).date())
        except ValueError:
            continue

    if not parsed_dates:
        LOGGER.warning("Unreadable period ended: %r", text)
        return None

    month_ends = [
        parsed_date for parsed_date in parsed_dates if is_month_end(parsed_date)
    ]

    return (month_ends or parsed_dates)[0].isoformat()


def parse_broadcast(text: str | None) -> str | None:
    # Printed as 26-Feb-2026 16:53:29 on the detail page
    if not text:
        return None

    try:
        return datetime.strptime(text.strip(), "%d-%b-%Y %H:%M:%S").isoformat()

    except ValueError:
        return None


def fetch_announcement_detail(announcement_url: str) -> dict:
    response = get_with_retry(announcement_url, headers=LINKS_HEADERS)
    time.sleep(LINKS_DELAY_SECONDS)

    if response.status_code == 403:
        raise BlockedError(f"403 Access Denied for {announcement_url}")

    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    detail = {"announcement_url": announcement_url}

    for label_tag in soup.select("dl dt"):
        value_tag = label_tag.find_next_sibling("dd")

        if value_tag is None:
            continue

        label = normalise_label(label_tag.get_text(" ", strip=True))

        # The description label carries a long instruction sentence after it
        field_name = next(
            (
                name
                for prefix, name in DETAIL_FIELDS.items()
                if label == prefix or label.startswith(f"{prefix} ")
            ),
            None,
        )

        if field_name and field_name not in detail:
            detail[field_name] = value_tag.get_text(" ", strip=True)

    detail["period_ended"] = parse_period_ended(detail.get("period_ended"))
    detail["broadcast_datetime"] = parse_broadcast(detail.get("broadcast_datetime"))

    detail["attachments"] = [
        {
            "name": link.get_text(" ", strip=True),
            "url": urljoin(ANNOUNCEMENT_HOST, link["href"]),
        }
        for link in soup.select("a.announcement-attachment")
        if link.get("href")
    ]

    return detail


def download_document(document_url: str) -> bytes | None:
    response = get_with_retry(document_url, headers=LINKS_HEADERS, timeout=120)
    time.sleep(LINKS_DELAY_SECONDS)

    if response.status_code == 403:
        raise BlockedError(f"403 Access Denied for {document_url}")

    if response.status_code != 200 or not response.content:
        LOGGER.warning(
            "Download failed (%d): %s",
            response.status_code,
            document_url,
        )
        return None

    return response.content
