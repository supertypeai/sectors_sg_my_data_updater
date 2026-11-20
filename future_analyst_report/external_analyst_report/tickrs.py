from ..main_analyst_report.api import create_session, get_with_retry
from .symbols import build_company_patterns, match_symbols

from bs4 import BeautifulSoup
from datetime import date, datetime
from urllib.parse import unquote

import logging
import re


LOGGER = logging.getLogger(__name__)

LIST_URL = "https://www.tickrs.com.sg/research-reports/"

# The name SGX's research list uses, so both sources join one house history
CONTRIBUTOR = "Tickrs"

MAX_LIST_PAGES = 30

# The PDF name starts with its date, e.g. 20260413FY2025 UPDATE - ...
FILE_DATE_PATTERN = re.compile(r"/(\d{8})")


def list_documents(
    companies: list[dict],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    session = create_session()
    company_patterns = build_company_patterns(companies)

    documents = []

    for page_number in range(1, MAX_LIST_PAGES + 1):
        list_url = LIST_URL if page_number == 1 else f"{LIST_URL}page/{page_number}/"
        page = get_with_retry(session, list_url)

        if page.status_code != 200:
            break

        soup = BeautifulSoup(page.text, "html.parser")
        report_links = {
            anchor["href"]: anchor.get_text(" ", strip=True)
            for anchor in soup.find_all("a", href=True)
            if "infiwm.com/sgxreports/" in anchor["href"] and anchor.get_text(strip=True)
        }

        if not report_links:
            break

        page_dates = []

        for pdf_url, title in report_links.items():
            date_match = FILE_DATE_PATTERN.search(unquote(pdf_url))

            if not date_match:
                continue

            report_date = datetime.strptime(date_match.group(1), "%Y%m%d").date()
            page_dates.append(report_date)

            if (period_start and report_date < period_start) or (
                period_end and report_date > period_end
            ):
                continue

            # "Tickrs – OKP Holdings Update : Record Earnings ..."
            company_part = title.split(" : ")[0].split("–")[-1]
            symbols = match_symbols(company_part, company_patterns)

            if not symbols:
                continue

            documents.append(
                {
                    "report": {
                        "title": title,
                        "report_date": report_date.isoformat(),
                        "contributor": CONTRIBUTOR,
                        "document_url": pdf_url,
                    },
                    "symbols": symbols,
                }
            )

        # Newest first, so a page that ends before the period ends the list
        if period_start and page_dates and max(page_dates) < period_start:
            break

    LOGGER.info("[Tickrs] %d reports on target companies", len(documents))

    return documents
