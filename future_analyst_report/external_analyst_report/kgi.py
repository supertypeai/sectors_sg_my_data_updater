from ..main_analyst_report.api import create_session, get_with_retry

from datetime import date

import html
import logging
import re


LOGGER = logging.getLogger(__name__)

SITE_URL = "https://www.kgieworld.sg/research"
SITEMAP_URLS = [f"{SITE_URL}/post-sitemap.xml", f"{SITE_URL}/post-sitemap2.xml"]

# The name SGX's research list uses, so both sources join one house history
CONTRIBUTOR = "KGI Securities (Singapore)"

# company-report-okp-holdings-limited-okp-sp-5cf-si-2/ carries the SGX code
REPORT_URL_PATTERN = re.compile(
    rf"({re.escape(SITE_URL)}/company-report-[a-z0-9-]*?-([a-z0-9]+)-si(?:-\d+)?/)"
)

TITLE_PATTERN = re.compile(r"<title>(.*?)</title>", re.S)
DATE_PATTERN = re.compile(r'"datePublished":"(\d{4}-\d{2}-\d{2})')
PDF_PATTERN = re.compile(r'href="([^"]+/docs/research/[^"]+\.pdf)"')


def list_documents(
    companies: list[dict],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    session = create_session()
    target_symbols = {company["symbol"] for company in companies}

    report_urls = {}

    for sitemap_url in SITEMAP_URLS:
        sitemap = get_with_retry(session, sitemap_url)

        if sitemap.status_code != 200:
            continue

        for page_url, security_code in REPORT_URL_PATTERN.findall(sitemap.text):
            symbol = f"{security_code.upper()}.SI"

            if symbol in target_symbols:
                report_urls[page_url] = symbol

    documents = []

    for page_url, symbol in report_urls.items():
        page = get_with_retry(session, page_url)

        if page.status_code != 200:
            LOGGER.warning("[KGI] %d on %s", page.status_code, page_url)
            continue

        date_match = DATE_PATTERN.search(page.text)
        pdf_match = PDF_PATTERN.search(page.text)

        if not date_match or not pdf_match:
            LOGGER.warning("[KGI] no date or PDF on %s", page_url)
            continue

        report_date = date_match.group(1)

        if (period_start and report_date < period_start.isoformat()) or (
            period_end and report_date > period_end.isoformat()
        ):
            continue

        title_match = TITLE_PATTERN.search(page.text)
        title = html.unescape(title_match.group(1)).split(" • ")[0] if title_match else ""

        documents.append(
            {
                "report": {
                    "title": f"KGI - {title}",
                    "report_date": report_date,
                    "contributor": CONTRIBUTOR,
                    "document_url": html.unescape(pdf_match.group(1)),
                    "page_url": page_url,
                },
                "symbols": [symbol],
            }
        )

    LOGGER.info("[KGI] %d reports on target companies", len(documents))

    return documents
