from bs4 import BeautifulSoup
from datetime import date, datetime

from ..main_analyst_report.api import create_session, get_with_retry
from .symbols import build_company_patterns, match_symbols

import logging
import re


LOGGER = logging.getLogger(__name__)

COVERAGE_URL = "https://www.poems.com.sg/stock-research/"
SITEMAP_URL = "https://www.poems.com.sg/stocks-articles-sitemap.xml"

# The name SGX's research list uses, so both sources join one house history
CONTRIBUTOR = "POEMS by PhillipCapital"

# Only the SG table's links end in .SG, US names in the same table have bare codes
STOCK_PAGE_PATTERN = re.compile(r"/stock-research/([A-Z0-9]+\.SG)/$")

ARTICLE_URL_PATTERN = re.compile(r"<loc>(https://www\.poems\.com\.sg/stocks/articles/[^<]+)</loc>")

# Most of the sitemap is investing guides, a research note names its call
# in the slug, e.g. dbs-q3-2026-results-...-accumulate-rating-target-price
CALL_SLUG_PATTERN = re.compile(
    r"(?:^|-)(?:target-price|rating|buy|accumulate|neutral|reduce|sell|upgraded?|downgraded?)(?:-|/|$)"
)

DATE_PATTERN = re.compile(r'"datePublished":"(\d{4}-\d{2}-\d{2})')


def in_period(
    report_date: str, 
    period_start: date | None, 
    period_end: date | None
) -> bool:
    return not (
        (period_start and report_date < period_start.isoformat())
        or (period_end and report_date > period_end.isoformat())
    )


def list_stock_pages(
    session,
    company_patterns: list,
    period_start: date | None,
    period_end: date | None,
) -> tuple[list[dict], dict[str, str]]:
    # The SG coverage table holds the 50 newest notes as plain HTML. The
    # older pages of POEMS's report list come from a script robots.txt
    # disallows, so a daily run picks each note up while it is in the table
    coverage = get_with_retry(session, COVERAGE_URL)
    coverage.raise_for_status()

    table = BeautifulSoup(coverage.text, "html.parser").select_one("#tbl-sg-coverage")

    if table is None:
        LOGGER.warning("[POEMS] SG coverage table not found on %s", COVERAGE_URL)
        return [], {}

    stock_pages = {}
    # Every stock's latest note date, inside the period or not, so an article
    # retelling a note from before the period is recognised
    latest_note_dates = {}

    for row in table.select("tbody tr"):
        link = row.find("a", href=True)
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
        code_match = STOCK_PAGE_PATTERN.search(link["href"]) if link else None

        if code_match is None or len(cells) < 3:
            continue

        report_date = datetime.strptime(cells[2], "%d-%b-%y").date().isoformat()

        for symbol in match_symbols(cells[0], company_patterns):
            latest_note_dates[symbol] = max(report_date, latest_note_dates.get(symbol, ""))

        if in_period(report_date, period_start, period_end):
            stock_pages[link["href"]] = report_date

    documents = []

    for page_url, report_date in stock_pages.items():
        page = get_with_retry(session, page_url)

        if page.status_code != 200:
            LOGGER.warning("[POEMS] %d on %s", page.status_code, page_url)
            continue

        content = BeautifulSoup(page.text, "html.parser").select_one("div.content-reports")
        heading = content.find("h2") if content else None

        if heading is None:
            LOGGER.warning("[POEMS] no report on %s", page_url)
            continue

        title = heading.get_text(" ", strip=True)
        symbols = match_symbols(title, company_patterns)

        if not symbols:
            LOGGER.debug("[POEMS] no target company in %r", title)
            continue

        documents.append(
            {
                "report": {
                    "title": f"POEMS - {title}",
                    "report_date": report_date,
                    "contributor": CONTRIBUTOR,
                    "document_url": page_url,
                    "text": content.get_text("\n", strip=True),
                },
                "symbols": symbols,
            }
        )

    LOGGER.info(
        "[POEMS] %d SG stock pages in the period, %d on target companies",
        len(stock_pages),
        len(documents),
    )

    return documents, latest_note_dates


def list_articles(
    session,
    company_patterns: list,
    period_start: date | None,
    period_end: date | None,
) -> list[dict]:
    sitemap = get_with_retry(session, SITEMAP_URL)
    sitemap.raise_for_status()

    article_urls = [
        url
        for url in dict.fromkeys(ARTICLE_URL_PATTERN.findall(sitemap.text))
        if CALL_SLUG_PATTERN.search(url.rstrip("/").rsplit("/", 1)[-1])
    ]

    documents = []

    for article_url in article_urls:
        page = get_with_retry(session, article_url)

        if page.status_code != 200:
            LOGGER.warning("[POEMS] %d on %s", page.status_code, article_url)
            continue

        soup = BeautifulSoup(page.text, "html.parser")
        heading = soup.find("h1")

        if heading is None:
            continue

        title = heading.get_text(" ", strip=True)
        symbols = match_symbols(title, company_patterns)

        # The page carries the site's own 2021 date too, the article's is the latest
        published_dates = DATE_PATTERN.findall(page.text)

        if not symbols or not published_dates:
            continue

        report_date = max(published_dates)

        if not in_period(report_date, period_start, period_end):
            continue

        body = soup.select_one("div.bond-details") or soup.body

        documents.append(
            {
                "report": {
                    "title": f"POEMS - {title}",
                    "report_date": report_date,
                    "contributor": CONTRIBUTOR,
                    "document_url": article_url,
                    "text": body.get_text("\n", strip=True),
                },
                "symbols": symbols,
            }
        )

    LOGGER.info(
        "[POEMS] %d research articles, %d on target companies",
        len(article_urls),
        len(documents),
    )

    return documents


def list_documents(
    companies: list[dict],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    session = create_session()
    company_patterns = build_company_patterns(companies)

    stock_page_documents, latest_note_dates = list_stock_pages(
        session, company_patterns, period_start, period_end
    )

    # A stock page shows POEMS's latest note, so an article dated on or after
    # it retells that note, often a week later. Its text joins the note, since
    # the article can print figures the page shows only as an image, such as
    # DPS forecasts. Earlier articles are older notes and stay separate
    latest_notes = {
        symbol: document
        for document in stock_page_documents
        for symbol in document["symbols"]
    }

    article_documents = []

    for document in list_articles(session, company_patterns, period_start, period_end):
        report = document["report"]
        older_note_symbols = []

        for symbol in document["symbols"]:
            note = latest_notes.get(symbol)

            if note is None and report["report_date"] >= latest_note_dates.get(symbol, "9999"):
                # The note it retells is from before the period, so an earlier
                # run already read it
                LOGGER.debug(
                    "[POEMS][%s] %s article retells the %s note, left out",
                    symbol,
                    report["report_date"],
                    latest_note_dates[symbol],
                )

            elif note is None or report["report_date"] < note["report"]["report_date"]:
                older_note_symbols.append(symbol)

            elif report["document_url"] not in note["report"]["text"]:
                note["report"]["text"] += (
                    f"\n\nPOEMS article of {report['report_date']} on this note"
                    f" ({report['document_url']}):\n{report['text']}"
                )

        if older_note_symbols:
            article_documents.append({**document, "symbols": older_note_symbols})

    return stock_page_documents + article_documents
