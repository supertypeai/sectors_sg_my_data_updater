from ..pipeline.parse_pdf import (
    get_image_coverage,
    render_page_images,
    MIN_IMAGE_COVERAGE,
    MIN_TEXT_CHARS,
)
from ..llm.caller import invoke_structured_vision_llm
from ..prompts.document import ScannedDocument, SYSTEM_PROMPT, USER_PROMPT

from calendar import monthrange
from datetime import date
from urllib.parse import unquote

import logging
import pymupdf
import re


LOGGER = logging.getLogger(__name__)

# Filenames only decide the order PDFs are opened in, never whether one is
# used: DBS ships its statements as "4Q25_performance_summary.pdf"
STATEMENT_NAME_PATTERN = re.compile(
    r"financial\s*statement|\bfs\b|condensed|interim",
    re.IGNORECASE,
)

# A results announcement often carries the statements too, but after the
# statements file, e.g. "3. ...financial results" then "4. ...interim
# financial statements", where only 4 is the full set
RESULTS_NAME_PATTERN = re.compile(
    r"unaudited|audited|result|announcement|annc|annt|sgx",
    re.IGNORECASE,
)

SUPPORTING_NAME_PATTERN = re.compile(
    r"present|slide|deck|ppt|briefing|transcript|webcast|"
    r"press|news|media|release|\bnr\b|\bpr\b|"
    r"corrigend|errat|clarif|supplement|"
    r"md\s*&?\s*a|mda|management\s+discussion|operational\s+data|"
    r"auditor|audit\s+report|factsheet|infographic|highlight|letter|"
    r"circular|notice|summary|"
    # The parent company's own accounts, not the group's (EMI's "Audited
    # Financial Statements 2024 Parent" next to its annual report)
    r"\bparent\b|company[\s_-]*only|separate\s+financial|stand[\s-]*alone",
    re.IGNORECASE,
)

NAME_RANK_STATEMENTS = 0
NAME_RANK_RESULTS = 1
NAME_RANK_UNKNOWN = 2
NAME_RANK_SUPPORTING = 3

# Searched anywhere in the line, titles carry prefixes like "(A) Condensed",
# "1(a)(i) Statements of" or "AUDITED CONSOLIDATED"
STATEMENT_KINDS = {
    "income_statement": re.compile(
        r"statements?\s+of\s+(?:\w+\s+)?(?:income|profit|comprehensive\s+income|total\s+return|operations)"
        r"|income\s+statements?|profit\s+(?:or|and)\s+loss\s+statements?",
        re.IGNORECASE,
    ),
    "balance_sheet": re.compile(
        r"statements?\s+of\s+(?:\w+\s+)?financial\s+position|balance\s+sheets?",
        re.IGNORECASE,
    ),
    "cash_flow": re.compile(
        r"statements?\s+of\s+(?:\w+\s+)?cash\s*flows?|cash\s*flows?\s+statements?",
        re.IGNORECASE,
    ),
}

# A statement title in the auditor's opinion or the notes sits in a sentence,
# a real one is a short line on a page full of figures
MAX_TITLE_CHARS = 100
HEADING_LINE_COUNT = 4
NOTES_PATTERN = re.compile(r"^\W*notes?\b", re.IGNORECASE)
# Statements in millions print small decimals (SATS "45.6", ESR-REIT), so a
# one or two decimal figure counts too
FIGURE_PATTERN = re.compile(
    r"\(?\d{1,3}(?:,\d{3})+(?:\.\d+)?\)?|\b\d{3,}\b|\(?\b\d{1,3}\.\d{1,2}\b\)?"
)
MIN_FIGURES_PER_STATEMENT_PAGE = 25

WHITESPACE_PATTERN = re.compile(r"\s+")

# Income statement + balance sheet is enough, some interim filings leave the
# cash flow out
MIN_STATEMENT_KINDS = 2

# Half the pages being images makes the text check unreliable
MIN_SCANNED_SHARE = 0.5

# How far the year can sit after the day and month in a split column header
MAX_SPLIT_PERIOD_CHARS = 80

# Only the top of a page holds its column headers, and only the top of the
# cover its title, a cover can also carry a forecast date
HEADER_LINE_COUNT = 15
COVER_TITLE_LINE_COUNT = 10

# Statements sit near the front of a results filing. A low dpi still shows the
# titles and dates, which is all this check reads, at a fraction of the tokens
VISION_PAGE_COUNT = 12
VISION_DPI = 100
VISION_MODELS = ["claude-haiku-5.5"]

MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]


def get_name_rank(document_name: str, document_url: str) -> int:
    file_name = unquote(document_url.rsplit("/", 1)[-1])
    name = f"{document_name} {file_name}"

    if SUPPORTING_NAME_PATTERN.search(name):
        return NAME_RANK_SUPPORTING

    if STATEMENT_NAME_PATTERN.search(name):
        return NAME_RANK_STATEMENTS

    if RESULTS_NAME_PATTERN.search(name):
        return NAME_RANK_RESULTS

    return NAME_RANK_UNKNOWN


def is_pdf(document_name: str, document_url: str) -> bool:
    return document_name.casefold().endswith(".pdf") or unquote(
        document_url
    ).casefold().endswith(".pdf")


def build_period_pattern(period_end: str) -> re.Pattern:
    # An announcement can type the last working day, 29/09/2023 while the
    # statements print 30 September, so the month end counts too
    parsed = date.fromisoformat(period_end)
    month_end = date(
        parsed.year, parsed.month, monthrange(parsed.year, parsed.month)[1]
    )

    if parsed == month_end:
        return build_one_period_pattern(period_end)

    return re.compile(
        f"{build_one_period_pattern(period_end).pattern}|{build_one_period_pattern(month_end.isoformat()).pattern}",
        re.IGNORECASE,
    )


def build_one_period_pattern(period_end: str) -> re.Pattern:
    # The period end is printed in many ways: "31 December 2025", "31st Dec 2025",
    # "December 31, 2025", "31/12/2025", "31.12.25", "2025-12-31"
    parsed = date.fromisoformat(period_end)
    month_name = MONTH_NAMES[parsed.month - 1]
    month = f"(?:{month_name}|{month_name[:3]})\\.?"
    day = f"0?{parsed.day}(?:st|nd|rd|th)?"
    year = f"(?:{parsed.year}|'?{parsed.year % 100:02d})"
    numeric_month = f"0?{parsed.month}"

    return re.compile(
        "|".join(
            [
                rf"\b{day}\s*{month}\s*,?\s*{parsed.year}\b",
                rf"\b{month}\s*{day}\s*,?\s*{parsed.year}\b",
                rf"\b0?{parsed.day}\s*[./-]\s*{numeric_month}\s*[./-]\s*{year}\b",
                rf"\b{day}[\s-]*{month}[\s-]*{year}\b",
                rf"\b{parsed.isoformat()}\b",
            ]
        ),
        re.IGNORECASE,
    )


def find_statement_kinds(page: pymupdf.Page, page_text: str) -> set[str]:
    # Some filings paste each table as an image under a text title
    has_table = (
        len(FIGURE_PATTERN.findall(page_text)) >= MIN_FIGURES_PER_STATEMENT_PAGE
        or get_image_coverage(page) >= MIN_IMAGE_COVERAGE
    )

    if not has_table:
        return set()

    kinds = set()
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]

    # A title can wrap, "CONSOLIDATED INCOME" then "STATEMENT FOR THE..." (BTM),
    # so the opening lines are also read as one heading
    heading = " ".join(lines[:HEADING_LINE_COUNT])
    title_lines = [heading] if not NOTES_PATTERN.match(heading) else []

    for line in [*title_lines, *lines]:
        if line is not heading and (
            len(line) > MAX_TITLE_CHARS or NOTES_PATTERN.match(line)
        ):
            continue

        for kind, kind_pattern in STATEMENT_KINDS.items():
            if kind_pattern.search(line):
                kinds.add(kind)

    return kinds


def get_top_lines(page_text: str, line_count: int) -> str:
    return "\n".join(
        [line for line in page_text.splitlines() if line.strip()][:line_count]
    )


def build_split_period_pattern(period_end: str) -> re.Pattern:
    # A column header prints the day and month on one line and the year on the
    # next, "At 30 June / 2025", "31 March 30 September / 2024".
    # Only read on statement pages, where the year that follows is the column's
    parsed = date.fromisoformat(period_end)
    month_name = MONTH_NAMES[parsed.month - 1]

    return re.compile(
        rf"\b0?{parsed.day}(?:st|nd|rd|th)?\s*(?:{month_name}|{month_name[:3]})\.?\b"
        rf"[\s\S]{{0,{MAX_SPLIT_PERIOD_CHARS}}}?\b{parsed.year}\b",
        re.IGNORECASE,
    )


def is_period_on_page(
    page: pymupdf.Page,
    page_text: str,
    period_pattern: re.Pattern,
    split_period_pattern: re.Pattern | None = None,
) -> bool:
    if period_pattern.search(page_text):
        return True

    # Column headers sit at the top, a note further down can pair a date with
    # a later year ("as at 31 December 2024, effective ... 2025")
    if split_period_pattern is not None and split_period_pattern.search(
        get_top_lines(page_text, HEADER_LINE_COUNT)
    ):
        return True

    # A column header split over lines ("ended 31 / March / 2026") only
    # reads whole in the unsorted text, where each column stays together
    plain_text = WHITESPACE_PATTERN.sub(" ", page.get_text("text"))

    return period_pattern.search(plain_text) is not None


def check_document(pdf_bytes: bytes, period_end: str) -> dict:
    period_pattern = build_period_pattern(period_end)
    split_period_pattern = build_split_period_pattern(period_end)
    statement_pages = {}
    has_period = False
    is_period_on_cover = False
    scanned_page_count = 0
    front_scanned_page_count = 0

    try:
        with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
            page_count = len(document)

            for page_index, page in enumerate(document):
                page_text = page.get_text("text", sort=True)
                kinds = find_statement_kinds(page, page_text)

                # Same rule as parse_pdf, so both steps agree on what a scan is
                if (
                    len(page_text.strip()) < MIN_TEXT_CHARS
                    or get_image_coverage(page) >= MIN_IMAGE_COVERAGE
                ):
                    scanned_page_count += 1

                    if page_index < VISION_PAGE_COUNT:
                        front_scanned_page_count += 1

                # The cover states the report's own period, "for the six months
                # ended 31 March 2024", when the statement headers split it
                if page_index == 0:
                    is_period_on_cover = (
                        period_pattern.search(
                            get_top_lines(page_text, COVER_TITLE_LINE_COUNT)
                        )
                        is not None
                    )

                # Only a statement page counts, the notes of an older report
                # mention later dates too, e.g. a loan maturing on 30 June 2025
                if kinds and not has_period:
                    has_period = is_period_on_page(
                        page, page_text, period_pattern, split_period_pattern
                    )

                for kind in kinds:
                    statement_pages.setdefault(kind, []).append(page_index + 1)

    except Exception as error:
        LOGGER.warning("Unreadable PDF: %s", error)
        return {"is_readable": False, "is_statements": False, "is_period": False}

    # Statements sit near the front, so a report whose front pages carry no
    # text (statement pages drawn as shapes) goes to the vision check
    # even when its later pages are text
    front_page_count = min(page_count, VISION_PAGE_COUNT)

    return {
        "is_readable": True,
        "page_count": page_count,
        # A scan has little or no text to check, so it goes to the vision check
        "is_scanned": (
            scanned_page_count / max(page_count, 1) >= MIN_SCANNED_SHARE
            or front_scanned_page_count / max(front_page_count, 1) >= MIN_SCANNED_SHARE
        ),
        "statement_pages": statement_pages,
        "is_statements": len(statement_pages) >= MIN_STATEMENT_KINDS,
        "is_period": has_period or (bool(statement_pages) and is_period_on_cover),
    }


def check_scanned_document(
    pdf_bytes: bytes,
    period_end: str,
    log_name: str,
    page_count: int,
) -> dict:
    page_numbers = list(range(1, min(page_count, VISION_PAGE_COUNT) + 1))

    extraction = invoke_structured_vision_llm(
        pydantic_output=ScannedDocument,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=USER_PROMPT,
        log_name=log_name,
        input_data={
            "period_end": date.fromisoformat(period_end).strftime("%d %B %Y"),
            "page_count": page_count,
            "sent_page_count": len(page_numbers),
        },
        page_images=render_page_images(pdf_bytes, page_numbers, dpi=VISION_DPI),
        models=VISION_MODELS,
        temperature=0,
        effort="low",
    )

    if extraction is None:
        LOGGER.error("[Document Check][%s] vision check failed", log_name)
        return {
            "is_statements": False,
            "is_period": False,
            "checked_by": "vision_failed",
        }

    return {
        "is_statements": len(set(extraction["statements_found"]))
        >= MIN_STATEMENT_KINDS,
        "is_period": extraction["is_period_shown"],
        "checked_by": "vision",
        "vision": extraction,
    }
