from datetime import date
from pathlib import Path

from ..main_analyst_report.api import create_session, download_document, iter_reports
from periodic_financial.pipeline.parse_pdf import load_pages

import hashlib
import logging
import requests


LOGGER = logging.getLogger(__name__)

PDF_CACHE_DIR = Path("future_analyst_report/pdf_cache")

# Charts cover half of many text pages, so a page is an image by its text alone
MIN_TEXT_CHARS = 120


def download_pdf(
    session: requests.Session,
    document_url: str,
    use_cache: bool = True,
) -> bytes | None:
    url_hash = hashlib.sha1(document_url.encode("utf-8")).hexdigest()[:16]
    pdf_path = PDF_CACHE_DIR / f"{url_hash}.pdf"

    if use_cache and pdf_path.exists():
        return pdf_path.read_bytes()

    pdf_bytes = download_document(session, document_url)

    if pdf_bytes is None:
        return None

    if not pdf_bytes.startswith(b"%PDF"):
        LOGGER.warning("[Download] not a PDF, skipped: %s", document_url)
        return None

    LOGGER.debug("[Download] %d bytes from %s", len(pdf_bytes), document_url)

    if use_cache:
        PDF_CACHE_DIR.mkdir(exist_ok=True)
        pdf_path.write_bytes(pdf_bytes)

    return pdf_bytes


def parse_document(
    session: requests.Session,
    report: dict,
) -> tuple[dict, bytes | None] | None:
    # A web article (POEMS) arrives as text and reads as a single page
    if report.get("text"):
        pdf_bytes = None
        pages = [{"page_number": 1, "text": report["text"], "image_coverage": 0.0}]

    else:
        pdf_bytes = download_pdf(session, report["document_url"])

        if pdf_bytes is None:
            return None

        pages = load_pages(pdf_bytes)

    for page in pages:
        page["is_image"] = len(page["text"]) < MIN_TEXT_CHARS

    parser_result = {
        **report,
        "page_count": len(pages),
        "image_pages": [page["page_number"] for page in pages if page["is_image"]],
        "pages": pages,
    }

    LOGGER.debug(
        "[Parse][%s %s] %d pages, %d image pages",
        report["contributor"],
        report["report_date"],
        len(pages),
        len(parser_result["image_pages"]),
    )

    return parser_result, pdf_bytes


def list_documents(
    symbols: list[str],
    period_start: date | None = None,
    period_end: date | None = None,
) -> list[dict]:
    # A sector report is listed under every stock it covers, grouped so one
    # task downloads it once and extracts it for each of them
    session = create_session()
    documents = {}

    for symbol in symbols:
        for report in iter_reports(
            session=session,
            security_code=symbol.removesuffix(".SI"),
            period_start=period_start,
            period_end=period_end,
        ):
            document_url = report["document_url"]

            if not document_url:
                LOGGER.warning(
                    "[List][%s] no document on %r, skipped", symbol, report["title"]
                )
                continue

            document = documents.setdefault(
                document_url, {"report": report, "symbols": []}
            )
            document["symbols"].append(symbol)

    return list(documents.values())
