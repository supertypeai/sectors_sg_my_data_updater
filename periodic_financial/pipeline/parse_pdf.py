from pathlib import Path


import hashlib
import pymupdf
import requests
import logging
import re


LOGGER = logging.getLogger(__name__)

PDF_CACHE_DIR = Path("periodic_financial/pdf_cache")

REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}

# The period line ("Year ended ...") can sit below a running header and a
# two-line title, e.g. line 6
HEADING_LINE_COUNT = 8

# A statement title can sit below boilerplate (line 10) or start halfway
# down a page after another statement ends, so
# title lines anywhere on the page are added to the heading too
MAX_TITLE_CHARS = 90

FIGURE_PATTERN = re.compile(r"\d{2,}")

# Only a line that starts with a statement title, so sentences in the notes
# that mention "profit or loss" or "balance sheet" are not picked up
# A section number like "1(b)(i)", "1(c)", "A(i)" or "A (i)" comes before
# the title in SGX's Appendix 7.2 format
SECTION_NUMBER = r"(?:(?:\d{1,2}|[A-Z])\s?(?:\([a-z0-9]{1,4}\))+\.?|[A-Z0-9]{1,4}[.)])"

STATEMENT_TITLE_PATTERN = re.compile(
    rf"^(?:{SECTION_NUMBER}\s*)?"
    r"(?:(?:a|an|unaudited|condensed|interim|consolidated|group|the)\s+)*"
    r"(?:statements?\s+of\s+(?:income|profit|comprehensive\s+income|financial\s+position"
    r"|cash\s*flows?|total\s+return)|income\s+statement|balance\s+sheets?"
    r"|cash\s+flow\s+statement|profit\s+or\s+loss\s+statement)",
    re.IGNORECASE,
)

# Below this a page is treated as image-only (a scan), not as a blank page
MIN_TEXT_CHARS = 120

# Some filings paste each statement as one image with only a text header and
# footer, so a page mostly covered by an image is a scan even with some text
MIN_IMAGE_COVERAGE = 0.25

# Enough for the model to read every digit on H22's image statements; Claude
# downscales larger images anyway, so a higher dpi only adds upload size
PAGE_IMAGE_DPI = 150

# sort=True pads columns with long runs of spaces; two are enough to keep columns apart
COLUMN_GAP_PATTERN = re.compile(r" {3,}")


def get_cache_path(symbol: str, date: str, source_url: str) -> Path:
    # Keyed by the URL too: when a filing's document is corrected (slides
    # replaced by the statements) the old copy must not be read again
    url_hash = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:10]
    return PDF_CACHE_DIR / f"{symbol}_{date}_{url_hash}.pdf"


def download_pdf(
    symbol: str,
    date: str,
    source_url: str,
    use_cache: bool = True,
) -> bytes:
    # The cache is for testing, reruns would otherwise download from SGX again
    # and its CDN bans bursts of requests. The weekly job reads each filing once
    pdf_path = get_cache_path(symbol, date, source_url)

    if use_cache and pdf_path.exists():
        return pdf_path.read_bytes()

    response = requests.get(source_url, headers=REQUEST_HEADERS, timeout=60)
    response.raise_for_status()

    LOGGER.info(
        "[Download][%s %s] %d bytes from %s",
        symbol,
        date,
        len(response.content),
        source_url,
    )

    if use_cache:
        PDF_CACHE_DIR.mkdir(exist_ok=True)
        pdf_path.write_bytes(response.content)

    return response.content


def compact_text(page_text: str) -> str:
    lines = []

    for line in page_text.splitlines():
        line = COLUMN_GAP_PATTERN.sub("  ", line).strip()

        if line:
            lines.append(line)

    return "\n".join(lines)


def get_image_coverage(page: pymupdf.Page) -> float:
    page_area = page.rect.width * page.rect.height

    if not page_area:
        return 0.0

    image_area = 0.0

    for image_info in page.get_image_info():
        image_rect = pymupdf.Rect(image_info["bbox"]) & page.rect
        image_area += image_rect.width * image_rect.height

    return min(image_area / page_area, 1.0)


def load_pages(pdf_bytes: bytes) -> list[dict]:
    pages = []

    with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
        for page_index, page in enumerate(document):
            # Reading order keeps each table row's label and numbers on one line
            page_text = compact_text(page.get_text("text", sort=True))
            image_coverage = get_image_coverage(page)

            pages.append(
                {
                    # 1-based so it matches the [PDF_PAGE n] label the LLM sees
                    "page_number": page_index + 1,
                    "text": page_text,
                    "image_coverage": round(image_coverage, 2),
                    "is_scanned": (
                        len(page_text) < MIN_TEXT_CHARS
                        or image_coverage >= MIN_IMAGE_COVERAGE
                    ),
                }
            )

    return pages


def get_page_heading(page_text: str, line_count: int = HEADING_LINE_COUNT) -> str:
    lines = page_text.splitlines()
    heading_lines = lines[:line_count]

    for line in lines[line_count:]:
        # A numbered title can run on into a long description, e.g.
        # "1(c) A statement of cash flows (for the group), together with a
        # comparative statement ...", so only its start is kept
        if re.match(SECTION_NUMBER, line) and STATEMENT_TITLE_PATTERN.match(line):
            heading_lines.append(line[:MAX_TITLE_CHARS])
            continue

        # A title is a short capitalised line: contents entries and table rows
        # carry page numbers or figures, sentences in the notes end with ":"
        if (
            len(line) > MAX_TITLE_CHARS
            or not (line[0].isupper() or line[0].isdigit())
            or line.endswith(":")
            or line[-1].isdigit()
            or FIGURE_PATTERN.search(line)
        ):
            continue

        if STATEMENT_TITLE_PATTERN.match(line):
            heading_lines.append(line)

    return "\n".join(heading_lines)


def build_page_headings(pages: list[dict]) -> str:
    page_count = len(pages)
    headings = []

    for page in pages:
        # Our own index, because the number printed in the footer can differ
        label = f"[PDF_PAGE {page['page_number']} of {page_count}]"

        heading = get_page_heading(page["text"]) or "(no text)"

        # The title often survives as text even when the table is an image
        if page["is_scanned"]:
            heading = f"{heading}\n(page content is an image)"

        headings.append(f"{label}\n{heading}")

    return "\n\n".join(headings)


def select_pages(pages: list[dict], page_numbers: list[int]) -> list[dict]:
    page_count = len(pages)
    selected = []

    for page_number in page_numbers:
        if not 1 <= page_number <= page_count:
            LOGGER.warning(
                "Page %d is outside the PDF (1-%d), skipping",
                page_number,
                page_count,
            )
            continue

        selected.append(pages[page_number - 1])

    return selected


def render_pages(pages: list[dict]) -> str:
    return "\n\n".join(
        f"[PDF_PAGE {page['page_number']}]\n{page['text']}" for page in pages
    )


def render_page_images(
    pdf_bytes: bytes,
    page_numbers: list[int],
    dpi: int = PAGE_IMAGE_DPI,
) -> list[dict]:
    page_images = []

    with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
        for page_number in page_numbers:
            pixmap = document[page_number - 1].get_pixmap(dpi=dpi)

            page_images.append(
                {
                    "page_number": page_number,
                    "png": pixmap.tobytes("png"),
                }
            )

    return page_images


def parse_document(filing: dict) -> tuple[dict, bytes]:
    pdf_bytes = download_pdf(filing["symbol"], filing["date"], filing["source_url"])
    pages = load_pages(pdf_bytes)

    parser_result = {
        "symbol": filing["symbol"],
        "date": filing["date"],
        "period": filing["period"],
        "source_url": filing["source_url"],
        "page_count": len(pages),
        "scanned_pages": [page["page_number"] for page in pages if page["is_scanned"]],
        "page_headings": build_page_headings(pages),
        "pages": pages,
    }

    LOGGER.info(
        "[Parse][%s %s] %s, %d pages, %d scanned",
        filing["symbol"],
        filing["date"],
        filing["period"],
        len(pages),
        len(parser_result["scanned_pages"]),
    )

    return parser_result, pdf_bytes
