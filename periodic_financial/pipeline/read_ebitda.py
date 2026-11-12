from ..llm.caller import ainvoke_structured_llm, ainvoke_structured_vision_llm
from ..llm.usage import track_usage, summarise_usage
from ..prompts.ebitda import SYSTEM_PROMPT, USER_PROMPT, EbitdaReading
from ..prompts.pages import TARGET_PERIODS, COMPARATIVE_NOTE
from .page_finder import format_period_end
from .parse_pdf import download_pdf, render_page_images
from ..utils.statement_pages import has_garbled_figures
from ..utils.statement_values import scale_value
from ..utils.half_year import get_first_half_date

import logging
import pymupdf
import re


LOGGER = logging.getLogger(__name__)

# EBITDA by name or written out, "Earnings before interest, taxes,
# depreciation and amortisation decreased by 10% to HK$1,078,308,000"
EBITDA_PATTERN = r"(?:\bEBITDA\b|before interest,? tax(?:es|ation)?,? depreciation and amorti[sz]ation)"

# A line that names EBITDA and carries a figure, e.g. "EBITDA  44,022  39,459"
EBITDA_FIGURE_PATTERN = re.compile(
    rf"^[^\n]*{EBITDA_PATTERN}[^\n]*\d[\d,.]{{2,}}", re.MULTILINE | re.IGNORECASE
)

# A page that names EBITDA away from its figure: a label wrapped over four
# lines with the figure on the first, or a heading like
# "Adjusted EBITDA is reconciled to loss before income tax as follows"
EBITDA_MENTION_PATTERN = re.compile(EBITDA_PATTERN, re.IGNORECASE)
FIGURE_PATTERN = re.compile(r"\d[\d,.]{2,}")

# Highlights come first in a results announcement, so the earliest pages are kept
MAX_EBITDA_PAGES = 5


def find_ebitda_pages(pages: list[dict]) -> list[dict]:
    text_pages = [page for page in pages if not page["is_scanned"]]
    figure_pages = [
        page for page in text_pages if EBITDA_FIGURE_PATTERN.search(page["text"])
    ]
    mention_pages = [
        page
        for page in text_pages
        if page not in figure_pages
        and EBITDA_MENTION_PATTERN.search(page["text"])
        and FIGURE_PATTERN.search(page["text"])
    ]
    selected = (figure_pages + mention_pages)[:MAX_EBITDA_PAGES]

    return sorted(selected, key=lambda page: page["page_number"])


def is_sideways(page: pymupdf.Page) -> bool:
    # A table printed turned 90 degrees, the text layer then
    # reads down the columns and only the page image keeps the table
    if page.rotation:
        return True

    directions = [
        line["dir"]
        for block in page.get_text("dict")["blocks"]
        if block.get("type") == 0
        for line in block["lines"]
    ]

    return (
        bool(directions)
        and sum(abs(direction[0]) < 0.5 for direction in directions)
        > len(directions) / 2
    )


def find_image_pages(pdf_bytes: bytes, ebitda_pages: list[dict]) -> list[int]:
    # Pages whose text layer cannot be read: turned sideways, or with
    # digits missing from the figures ("EBITDA 2,056,65")
    with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
        return [
            page["page_number"]
            for page in ebitda_pages
            if is_sideways(document[page["page_number"] - 1])
            or has_garbled_figures(page)
        ]


def render_ebitda_pages(ebitda_pages: list[dict], image_pages: list[int]) -> str:
    rendered_pages = []

    for page in ebitda_pages:
        if page["page_number"] in image_pages:
            rendered_pages.append(
                f"[PDF_PAGE {page['page_number']}]\n(this page's text cannot be read, it is attached below as an image with the same label)"
            )
            continue

        rendered_pages.append(f"[PDF_PAGE {page['page_number']}]\n{page['text']}")

    return "\n\n".join(rendered_pages)


def get_target_period(parser_result: dict) -> str:
    target_period = TARGET_PERIODS[parser_result["period"]].format(
        date=format_period_end(parser_result["date"])
    )
    document_date = parser_result.get("document_date")

    if document_date and document_date[:7] != parser_result["date"][:7]:
        target_period += COMPARATIVE_NOTE.format(
            document_date=format_period_end(document_date),
            date=format_period_end(parser_result["date"]),
        )

    return target_period


async def read_ebitda(
    parser_result: dict, models: list[str], effort: str
) -> dict | None:
    ebitda_pages = find_ebitda_pages(parser_result["pages"])

    if not ebitda_pages:
        return None

    pdf_bytes = download_pdf(
        parser_result["symbol"], parser_result["date"], parser_result["source_url"]
    )
    image_pages = find_image_pages(pdf_bytes, ebitda_pages)

    call = {
        "pydantic_output": EbitdaReading,
        "system_prompt": SYSTEM_PROMPT,
        "user_prompt": USER_PROMPT,
        "log_name": f"{parser_result['symbol']} {parser_result['date']} ebitda",
        "input_data": {
            "target_period": get_target_period(parser_result),
            "pages": render_ebitda_pages(ebitda_pages, image_pages),
        },
        "models": models,
        "temperature": 0,
        "effort": effort,
    }

    if image_pages:
        return await ainvoke_structured_vision_llm(
            **call, page_images=render_page_images(pdf_bytes, image_pages)
        )

    return await ainvoke_structured_llm(**call)


def get_report_value(reading: dict, result: dict) -> float | None:
    # In the report's currency, scaled. A figure in another currency than the
    # statements cannot use their SGD rate, so it is left out
    if reading.get("value") is None or result.get("sgd_rate") is None:
        return None

    if reading.get("currency") not in (None, result.get("currency")):
        return None

    return scale_value(reading["value"], reading.get("unit"))


def get_basis(reading: dict) -> tuple[bool, bool]:
    # Two EBITDA figures subtract only on the same basis: adjusted or not, and
    # continuing operations or the whole group, a full year can print
    # "EBITDA from continuing operations" while its H1 is the Group's EBITDA
    return bool(reading.get("is_adjusted")), "continuing" in (
        reading.get("label") or ""
    ).lower()


def get_ebitda(
    result: dict, reading: dict | None, results: dict, readings: dict
) -> tuple[int | None, str]:
    # The SGD EBITDA for one row and how it was found. An H2 whose report only
    # prints the twelve months is the full year minus the H1, like the
    # statements, and only when both halves are the same kind of EBITDA
    if reading is None:
        return None, "no_ebitda_figure"

    report_value = get_report_value(reading, result)

    if report_value is None:
        return None, "not_printed_for_period"

    if reading.get("coverage") == "six_months":
        return int(round(report_value * result["sgd_rate"])), "printed"

    if result["period"] != "H2" or reading.get("coverage") != "twelve_months":
        return None, f"printed_for_{reading.get('coverage')}"

    first_half_key = (result["symbol"], get_first_half_date(result["date"]))
    first_half = results.get(first_half_key)
    first_half_reading = readings.get(first_half_key)

    if (
        first_half is None
        or first_half_reading is None
        or first_half.get("sgd_rate") is None
    ):
        return None, "full_year_only_no_h1"

    if first_half_reading.get("ebitda") is None or get_basis(
        first_half_reading
    ) != get_basis(reading):
        return None, "full_year_only_h1_not_comparable"

    first_half_report_value = first_half_reading["ebitda"] / first_half["sgd_rate"]

    return int(
        round((report_value - first_half_report_value) * result["sgd_rate"])
    ), "full_year_minus_h1"


async def add_ebitda(
    result: dict,
    parser_result: dict,
    results: dict[tuple[str, str], dict],
    readings: dict[tuple[str, str], dict],
    models: list[str] = ["claude-haiku-5.5"],
    effort: str = "medium",
) -> None:
    # Printed on the statement it is already read, otherwise from the
    # highlights or review pages that print it
    log_name = f"{result['symbol']} {result['date']}"
    key = (result["symbol"], result["date"])
    values = result["income_statement"]["values"]

    if values.get("ebitda") is not None:
        readings[key] = {
            "ebitda": values["ebitda"],
            "is_adjusted": False,
            "label": result["income_statement"]["labels"].get("ebitda"),
        }
        LOGGER.info(
            "[EBITDA][%s] %s, printed on the statement",
            log_name,
            f"{values['ebitda']:,}",
        )
        return

    if not find_ebitda_pages(parser_result["pages"]):
        LOGGER.info("[EBITDA][%s] not printed in the report", log_name)
        return

    with track_usage() as usage_records:
        reading = await read_ebitda(parser_result, models=models, effort=effort)

    result["ebitda_llm_usage"] = summarise_usage(usage_records)

    # An H2 printed only as the twelve months needs its H1 from the same run
    ebitda, how = get_ebitda(result, reading, results, readings)
    values["ebitda"] = ebitda

    readings[key] = {
        "ebitda": ebitda,
        "is_adjusted": (reading or {}).get("is_adjusted"),
        "label": (reading or {}).get("label"),
    }

    LOGGER.info(
        "[EBITDA][%s] %s, %s | cost $%.4f",
        log_name,
        f"{ebitda:,}" if ebitda is not None else None,
        how,
        result["ebitda_llm_usage"]["cost"],
    )
