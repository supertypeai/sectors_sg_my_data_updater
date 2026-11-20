from ..main_analyst_report.api import create_session, get_with_retry
from .symbols import build_company_patterns, match_symbols

from datetime import date, datetime
from email.utils import parsedate_to_datetime

import html
import logging
import re


LOGGER = logging.getLogger(__name__)

LIST_URL = "https://fpafinancial.com/investment-perspective/"

# The name SGX's research list uses, so both sources join one house history
CONTRIBUTOR = "FPA Financial"

# "Pan-United Corporation Ltd – Initiating Coverage – October 2026"
REPORT_LINK_PATTERN = re.compile(
    r'href="(https://fpafinancial\.com/wp-content/uploads/[^"]+\.pdf)"[^>]*>([^<]+)<'
)

MONTH_PATTERN = re.compile(r"([A-Z][a-z]+) (\d{4})\s*$")


def list_documents(
    companies: list[dict],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    session = create_session()
    company_patterns = build_company_patterns(companies)

    page = get_with_retry(session, LIST_URL)
    page.raise_for_status()

    documents = []

    for pdf_url, raw_title in dict.fromkeys(REPORT_LINK_PATTERN.findall(page.text)):
        title = html.unescape(raw_title).strip()
        month_match = MONTH_PATTERN.search(title)

        if not month_match:
            continue

        report_month = datetime.strptime(" ".join(month_match.groups()), "%B %Y").date()

        if (period_end and report_month > period_end) or (
            period_start
            and (report_month.year, report_month.month)
            < (period_start.year, period_start.month)
        ):
            continue

        symbols = match_symbols(re.split(r"\s+[–-]\s+", title)[0], company_patterns)

        if not symbols:
            continue

        # The title gives only the month, the upload date is closer to the day
        # the report came out
        head = session.head(pdf_url, timeout=30)
        last_modified = head.headers.get("Last-Modified")
        
        report_date = (
            parsedate_to_datetime(last_modified).date()
            if last_modified
            else report_month
        )

        documents.append(
            {
                "report": {
                    "title": f"FPA Financial - {title}",
                    "report_date": report_date.isoformat(),
                    "contributor": CONTRIBUTOR,
                    "document_url": pdf_url,
                },
                "symbols": symbols,
            }
        )

    LOGGER.info("[FPA Financial] %d reports on target companies", len(documents))

    return documents
