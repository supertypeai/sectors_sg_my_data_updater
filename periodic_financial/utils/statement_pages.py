from ..pipeline.parse_pdf import select_pages

import re


# The weighted average share count sits in the EPS note, not on the statement,
# and step 1 only sees page headings, so the note page is found by its wording,
# also as "weighted-average number" or "weighted average no."
SHARE_COUNT_PATTERN = re.compile(
    r"weighted[\s-]+average[\s-]+(number|no\b)", re.IGNORECASE
)

# Some statements print the profit split on the following comprehensive income
# page instead, so that page is sent with the note pages for the split
ATTRIBUTABLE_PATTERN = re.compile(r"attributable\s+to", re.IGNORECASE)
PROFIT_ATTRIBUTABLE_PATTERN = re.compile(
    r"profit[^\n]{0,60}attributable\s+to", re.IGNORECASE
)

# Some notes paste the EPS table as an image under a text heading, so a
# page with that heading and an image is the fallback, sent as an image too
EPS_NOTE_PATTERN = re.compile(
    r"earnings\s+per\s+(ordinary\s+)?(share|unit)", re.IGNORECASE
)

# Big enough for a pasted table, so a logo in the header does not count
MIN_TABLE_IMAGE_COVERAGE = 0.05
MAX_SHARE_PAGES = 2

# A statement page carries dozens of figures. One whose text holds almost
# none is drawn as vector graphics
MIN_STATEMENT_FIGURES = 10
FIGURE_IN_TEXT_PATTERN = re.compile(r"\d[\d,.]{2,}")

# A font missing its digit glyphs leaves figures as "7,  , 66" or "  , 48" in
# the text layer, or drops the last digit, "EBITDA 2,056,65" for 2,056,659,
# so the page is sent as an image as well
GARBLED_FIGURE_PATTERN = re.compile(
    r"\d,\s+,|(?:\s{2}|\()\s*,\s*\d|\d{1,3},\d{3},\d{1,2}(?![\d,.])"
)


def render_statement_pages(pages: list[dict]) -> str:
    rendered_pages = []

    for page in pages:
        # A scanned page only has its title as text, the figures are sent as
        # an image, so the text would only repeat the title
        if page["is_scanned"]:
            rendered_pages.append(
                f"[PDF_PAGE {page['page_number']}]\n(this page is an image, attached below with the same label)"
            )
            continue

        rendered_page = f"[PDF_PAGE {page['page_number']}]\n{page['text']}"

        if has_garbled_figures(page):
            rendered_page += (
                "\n(some figures in this text are garbled, the page is attached "
                "below as an image with the same label, read the figures from the image)"
            )

        rendered_pages.append(rendered_page)

    return "\n\n".join(rendered_pages)


def find_share_pages(
    pages: list[dict], statement_page_numbers: list[int]
) -> list[dict]:
    share_pages = []

    for page in pages:
        if page["page_number"] in statement_page_numbers or page["is_scanned"]:
            continue

        if SHARE_COUNT_PATTERN.search(page["text"]):
            share_pages.append(page)

        if len(share_pages) == MAX_SHARE_PAGES:
            break

    if share_pages:
        return share_pages

    for page in pages:
        if page["page_number"] in statement_page_numbers or page["is_scanned"]:
            continue

        if page[
            "image_coverage"
        ] >= MIN_TABLE_IMAGE_COVERAGE and EPS_NOTE_PATTERN.search(page["text"]):
            return [page]

    return []


def find_attributable_page(
    pages: list[dict], statement_page_numbers: list[int]
) -> dict | None:
    statement_pages = select_pages(pages, statement_page_numbers)

    if not statement_pages or any(
        ATTRIBUTABLE_PATTERN.search(page["text"]) for page in statement_pages
    ):
        return None

    next_page_number = max(statement_page_numbers) + 1

    if next_page_number > len(pages):
        return None

    next_page = pages[next_page_number - 1]

    if PROFIT_ATTRIBUTABLE_PATTERN.search(next_page["text"]):
        return next_page

    return None


def get_image_share_pages(share_pages: list[dict]) -> list[int]:
    # A share page found only by its EPS heading has the table as an image
    return [
        page["page_number"]
        for page in share_pages
        if not SHARE_COUNT_PATTERN.search(page["text"])
    ]


def render_share_pages(share_pages: list[dict]) -> str:
    image_page_numbers = get_image_share_pages(share_pages)
    rendered_pages = []

    for page in share_pages:
        rendered_page = f"[PDF_PAGE {page['page_number']}]\n{page['text']}"

        if page["page_number"] in image_page_numbers:
            rendered_page += "\n(the share count table on this page is an image, attached below with the same label)"

        rendered_pages.append(rendered_page)

    return "\n\n".join(rendered_pages)


def has_readable_figures(page: dict) -> bool:
    return len(FIGURE_IN_TEXT_PATTERN.findall(page["text"])) >= MIN_STATEMENT_FIGURES


def has_garbled_figures(page: dict) -> bool:
    return bool(GARBLED_FIGURE_PATTERN.search(page["text"]))


def get_image_page_numbers(
    statement_key: str,
    pages: list[dict],
    statement_page_numbers: list[int],
) -> list[int]:
    # Only the statement's own image pages are rendered, never the whole PDF
    image_page_numbers = [
        page["page_number"]
        for page in select_pages(pages, statement_page_numbers)
        if page["is_scanned"]
        or not has_readable_figures(page)
        or has_garbled_figures(page)
    ]

    if statement_key == "income_statement":
        image_page_numbers += get_image_share_pages(
            find_share_pages(pages, statement_page_numbers)
        )

    return image_page_numbers
