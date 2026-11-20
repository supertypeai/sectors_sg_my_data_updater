from datetime import date, datetime
from urllib.parse import urljoin

from ..main_analyst_report.api import create_session, get_with_retry
from .symbols import build_company_patterns, match_symbols

import html
import logging
import re


LOGGER = logging.getLogger(__name__)

SITE_URL = "https://uobkh.com.sg"
SITEMAP_URL = f"{SITE_URL}/sitemap.xml"

# The name SGX's research list uses, so both sources join one house history
CONTRIBUTOR = "UOB KayHian"

# Singapore company reports, e.g. company-sg-dbs-group-holdings-06112026
REPORT_URL_PATTERN = re.compile(
    rf"<loc>({re.escape(SITE_URL)}/en/research/company-sg-[^<]*?-(\d{{8}}))</loc>"
)

TITLE_PATTERN = re.compile(r"<title>(.*?)</title>", re.S)
DATE_PATTERN = re.compile(r'"datePublished":"(\d{4}-\d{2}-\d{2})')
PDF_PATTERN = re.compile(r'href="(/strapi-uploads/[^"]+\.pdf)"')


def list_documents(
    companies: list[dict],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    session = create_session()
    company_patterns = build_company_patterns(companies)

    sitemap = get_with_retry(session, SITEMAP_URL)
    sitemap.raise_for_status()

    documents = []

    for page_url, slug_date in dict.fromkeys(REPORT_URL_PATTERN.findall(sitemap.text)):
        report_day = datetime.strptime(slug_date, "%d%m%Y").date()

        if (period_start and report_day < period_start) or (
            period_end and report_day > period_end
        ):
            continue

        page = get_with_retry(session, page_url)

        if page.status_code != 200:
            LOGGER.warning("[UOB Kay Hian] %d on %s", page.status_code, page_url)
            continue

        title_match = TITLE_PATTERN.search(page.text)
        pdf_match = PDF_PATTERN.search(page.text)

        if not title_match or not pdf_match:
            LOGGER.warning("[UOB Kay Hian] no title or PDF on %s", page_url)
            continue

        # "DBS Group Holdings (DBS SP) - 3Q26: ... | Research | UOB Kay Hian"
        title = html.unescape(title_match.group(1)).split(" | ")[0].strip()
        symbols = match_symbols(title.split(" - ")[0], company_patterns)

        if not symbols:
            continue

        date_match = DATE_PATTERN.search(page.text)

        documents.append(
            {
                "report": {
                    "title": f"UOB Kay Hian - {title}",
                    "report_date": date_match.group(1)
                    if date_match
                    else report_day.isoformat(),
                    "contributor": CONTRIBUTOR,
                    "document_url": urljoin(SITE_URL, pdf_match.group(1)),
                    "page_url": page_url,
                },
                "symbols": symbols,
            }
        )

    LOGGER.info("[UOB Kay Hian] %d reports on target companies", len(documents))

    return documents
