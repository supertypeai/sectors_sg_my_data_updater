from ..llm.caller import invoke_structured_llm, invoke_structured_vision_llm
from ..llm.usage import track_usage, summarise_usage
from ..prompts.pages import SYSTEM_PROMPT, USER_PROMPT, TARGET_PERIODS, StatementPages
from .parse_pdf import STATEMENT_TITLE_PATTERN, download_pdf

from datetime import date
import logging
import pymupdf
import re


LOGGER = logging.getLogger(__name__)


STATEMENT_KEYS = ["income_statement", "balance_sheet", "cash_flow"]

WHITESPACE_PATTERN = re.compile(r"\s+")

# The model writes straight quotes while PDFs print curly ones, e.g. "cont'd" vs "cont’d"
QUOTE_TRANSLATION = str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"'})

# The model joins a title that wraps over several lines with " / "
EVIDENCE_SEPARATOR = " / "

# Image pages sent when the text pass finds nothing, at the document check's
# low resolution: titles and column headers are all the finder needs
MAX_VISION_PAGES = 20
VISION_DPI = 100

# What a statement's title says, to tell which statement a title line starts
STATEMENT_TITLE_WORDS = {
    "income_statement": re.compile(
        r"income|profit|comprehensive|total return", re.IGNORECASE
    ),
    "balance_sheet": re.compile(r"financial position|balance sheet", re.IGNORECASE),
    "cash_flow": re.compile(r"cash ?flow", re.IGNORECASE),
}


# A table row: a label and at least two figures, wherever they sit on the line
# (rows can end in a "2%" change column)
FIGURE_PATTERN = re.compile(r"\d[\d,.]{2,}")

MIN_TABLE_ROWS = 3


def is_table_row(line: str) -> bool:
    return (
        bool(re.match(r"\s*[A-Za-z(]", line)) and len(FIGURE_PATTERN.findall(line)) >= 2
    )


def normalise_text(text: str) -> str:
    text = text.translate(QUOTE_TRANSLATION)
    return WHITESPACE_PATTERN.sub(" ", text).strip().lower()


def is_evidence_on_page(evidence: str, page_text: str) -> bool:
    normalised_page = normalise_text(page_text)
    evidence_parts = [
        normalise_text(part)
        for part in evidence.split(EVIDENCE_SEPARATOR)
        if part.strip()
    ]

    return bool(evidence_parts) and all(
        evidence_part in normalised_page for evidence_part in evidence_parts
    )


def format_period_end(period_end: str) -> str:
    parsed_date = date.fromisoformat(period_end)
    return f"{parsed_date.day} {parsed_date:%B %Y}"


def format_input(parser_result: dict) -> dict:
    target_period = TARGET_PERIODS[parser_result["period"]].format(
        date=format_period_end(parser_result["date"])
    )

    return {
        "target_period": target_period,
        "page_count": parser_result["page_count"],
        "page_headings": parser_result["page_headings"],
    }


def check_target_pages(target_pages: list[dict], pages: list[dict]) -> list[dict]:
    page_count = len(pages)
    checked_pages = []

    for target_page in target_pages:
        page_number = target_page.get("page")
        in_range = isinstance(page_number, int) and 1 <= page_number <= page_count

        evidence_found = in_range and is_evidence_on_page(
            target_page.get("evidence") or "",
            pages[page_number - 1]["text"],
        )

        checked_pages.append(
            {
                **target_page,
                "in_range": in_range,
                "evidence_found": evidence_found,
            }
        )

    return checked_pages


def get_starting_title(page_text: str) -> str | None:
    # The last statement title on the page with a table under it, e.g.
    # "1(b)(i) Condensed interim statements of financial position" halfway
    # down a page. A contents entry ends in its page number ("Statements of
    # Financial Position 2") and has no table under it, so it is never taken
    lines = page_text.splitlines()
    title_index = None

    for index, line in enumerate(lines):
        line = line.strip()

        if line and STATEMENT_TITLE_PATTERN.match(line) and not line[-1].isdigit():
            title_index = index

    if title_index is None:
        return None

    table_rows = sum(1 for line in lines[title_index + 1 :] if is_table_row(line))

    return lines[title_index].strip() if table_rows >= MIN_TABLE_ROWS else None


def add_title_page(
    statement_key: str, target_pages: list[dict], pages: list[dict]
) -> list[dict]:
    # A statement that starts halfway down a page and runs onto the next: the
    # model can pick only the next page
    page_numbers = [
        target_page["page"]
        for target_page in target_pages
        if target_page.get("in_range")
    ]

    if not page_numbers:
        return target_pages

    first_page = min(page_numbers)

    if first_page < 2:
        return target_pages

    title = get_starting_title(pages[first_page - 2]["text"])

    if title is None or not STATEMENT_TITLE_WORDS[statement_key].search(title):
        return target_pages

    first_target = next(
        target_page
        for target_page in target_pages
        if target_page.get("page") == first_page
    )

    return [
        {
            **first_target,
            "page": first_page - 1,
            "evidence": title,
            "in_range": True,
            "evidence_found": True,
            "added_by_code": "title_on_previous_page",
        },
        *target_pages,
    ]


def has_no_statements(extraction: dict) -> bool:
    return not any(extraction.get(statement_key) for statement_key in STATEMENT_KEYS)


def render_labelled_pages(pdf_bytes: bytes, page_numbers: list[int]) -> list[dict]:
    # The page number is drawn on the image itself: with many images the model
    # paired the separate text labels with the wrong pages
    page_images = []

    with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
        for page_number in page_numbers:
            page = document[page_number - 1]
            page.insert_text(
                (12, 24), f"PDF_PAGE {page_number}", fontsize=16, color=(1, 0, 0)
            )
            page_images.append(
                {
                    "page_number": page_number,
                    "png": page.get_pixmap(dpi=VISION_DPI).tobytes("png"),
                }
            )

    return page_images


def find_pages_on_images(
    parser_result: dict, models: list[str], effort: str, log_name: str
) -> dict | None:
    # Statements printed as images, the text pass sees only
    # "(page content is an image)" and cannot tell the statements apart
    if not any(page["is_scanned"] for page in parser_result["pages"]):
        return None

    # Every front page, not only the image ones: a statement can be drawn
    # as vector graphics, neither text nor an image
    image_pages = [page["page_number"] for page in parser_result["pages"]][
        :MAX_VISION_PAGES
    ]

    pdf_bytes = download_pdf(
        parser_result["symbol"], parser_result["date"], parser_result["source_url"]
    )

    LOGGER.info(
        "[Page Finder][%s] no statements in the text, reading %d image pages",
        log_name,
        len(image_pages),
    )

    return invoke_structured_vision_llm(
        pydantic_output=StatementPages,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=USER_PROMPT,
        log_name=log_name,
        input_data=format_input(parser_result),
        page_images=render_labelled_pages(pdf_bytes, image_pages),
        models=models,
        effort=effort,
    )


def find_statement_pages(
    parser_result: dict, models: list[str], effort: str
) -> dict | None:
    log_name = f"{parser_result['symbol']} {parser_result['date']}"

    extraction = invoke_structured_llm(
        pydantic_output=StatementPages,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=USER_PROMPT,
        log_name=log_name,
        input_data=format_input(parser_result),
        models=models,
        effort=effort,
    )

    if extraction is not None and has_no_statements(extraction):
        image_extraction = find_pages_on_images(parser_result, models, effort, log_name)

        if image_extraction is not None and not has_no_statements(image_extraction):
            extraction = {**image_extraction, "found_on_images": True}

    if extraction is None:
        LOGGER.error("[Page Finder][%s] every model failed", log_name)
        return None

    LOGGER.info("[Page Finder][%s] note: %s", log_name, extraction["notes"])

    for statement_key in STATEMENT_KEYS:
        extraction[statement_key] = add_title_page(
            statement_key,
            check_target_pages(
                extraction.get(statement_key) or [],
                parser_result["pages"],
            ),
            parser_result["pages"],
        )

    return extraction


def summarise_pages(target_pages: list[dict]) -> str:
    summary = []

    for target_page in target_pages:
        flag = "" if target_page["evidence_found"] else "?"

        # Full-year-only pages have no figures for the target period, so mark them
        if target_page.get("coverage") == "full_year":
            flag += "(FY)"

        summary.append(f"{target_page['page']}{flag}")

    return ",".join(summary) or "-"


def find_pages(
    parser_result: dict,
    models: list[str] = ["claude-haiku-5.5"],
    effort: str = "medium",
) -> dict | None:
    # One filing's statement pages, None when it has none
    log_name = f"{parser_result['symbol']} {parser_result['date']}"

    with track_usage() as usage_records:
        statement_pages = find_statement_pages(
            parser_result=parser_result, models=models, effort=effort
        )

    if statement_pages is None:
        return None

    record = {
        "symbol": parser_result["symbol"],
        "date": parser_result["date"],
        **statement_pages,
        "llm_usage": summarise_usage(usage_records),
    }

    LOGGER.info(
        "[Page Finder][%s] statements %s | income %s | balance %s | cash flow %s | cost $%.4f",
        log_name,
        statement_pages.get("has_financial_statements"),
        summarise_pages(statement_pages["income_statement"]),
        summarise_pages(statement_pages["balance_sheet"]),
        summarise_pages(statement_pages["cash_flow"]),
        record["llm_usage"]["cost"],
    )

    if statement_pages.get("has_financial_statements") is False:
        LOGGER.warning("[Page Finder][%s] no financial statements, skipped", log_name)
        return None

    return record
