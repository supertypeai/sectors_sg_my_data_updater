from ..llm.caller import ainvoke_structured_llm, ainvoke_structured_vision_llm
from ..prompts.statement_common import RECHECK_NOTE
from ..llm.usage import track_usage, summarise_usage
from .parse_pdf import (
    select_pages,
    render_page_images,
)
from .page_finder import format_period_end, STATEMENT_KEYS
from ..prompts.pages import TARGET_PERIODS, FULL_YEAR_TARGET_PERIOD, COMPARATIVE_NOTE
from ..prompts import income_statement, balance_sheet, cash_flow
from ..utils.statement_pages import (
    render_statement_pages,
    find_share_pages,
    find_attributable_page,
    render_share_pages,
    get_image_page_numbers,
)
from ..utils.statement_values import drop_property_purchases
from ..utils.statement_values import (
    get_field_names,
    build_statement_result,
    build_skipped_result,
    derive_balance_sheet,
    derive_cash_flow,
    derive_income_statement,
)
from ..utils.currency import convert_to_sgd
from ..checks.reporting_basis import (
    get_reporting_basis,
    fill_reporting_basis,
    clear_guessed_basis,
)
from ..checks.printed_currency import resolve_mixed_currency, fill_missing_currency
from ..utils.half_year import derive_second_half
from ..checks.statement_checks import (
    apply_checks,
    summarise_checks,
    find_identity_breaks,
    IDENTITY_RULES,
)
from ..checks.second_half_check import check_second_half

import logging
import re
import asyncio


LOGGER = logging.getLogger(__name__)


# Reading printed figures needs no variety, and above 0 the same pages gave
# different answers between runs
EXTRACTION_TEMPERATURE = 0

STATEMENT_PROMPTS = {
    "income_statement": (
        income_statement.IncomeStatement,
        income_statement.SYSTEM_PROMPT,
        income_statement.USER_PROMPT,
    ),
    "balance_sheet": (
        balance_sheet.BalanceSheet,
        balance_sheet.SYSTEM_PROMPT,
        balance_sheet.USER_PROMPT,
    ),
    "cash_flow": (
        cash_flow.CashFlow,
        cash_flow.SYSTEM_PROMPT,
        cash_flow.USER_PROMPT,
    ),
}

FLOW_STATEMENT_KEYS = ["income_statement", "cash_flow"]

TWELVE_MONTH_COLUMN_PATTERN = re.compile(
    r"12[- ]?(?:months?|mths?)|twelve months|full[- ]year|year ended|financial year|\bFY ?\d{2,4}\b",
    re.IGNORECASE,
)
OTHER_SPAN_COLUMN_PATTERN = re.compile(
    r"\b(?:3|6|9|15|18)[- ]?(?:months?|mths?)|\b(?:three|six|nine|fifteen|eighteen) months"
    r"|quarter|\bhalf\b|\b[1-4][HQ]|\b[HQ][1-4]\b",
    re.IGNORECASE,
)


def is_full_year_only(target_pages: list[dict]) -> bool:
    return any(
        target_page.get("coverage") == "full_year" for target_page in target_pages
    )


def get_skip_reason(target_pages: list[dict], period: str) -> str | None:
    if not target_pages:
        return "no_pages_found"

    # Only an H2 can come from the twelve months, as full year minus H1. The
    # twelve months stored as a half would double count H1 in the TTM sums
    if is_full_year_only(target_pages) and period != "H2":
        return "full_year_only"

    return None


def format_input(
    statement_key: str,
    record: dict,
    parser_result: dict,
    target_pages: list[dict],
) -> dict:
    target_period_template = TARGET_PERIODS[parser_result["period"]]

    if is_full_year_only(target_pages):
        target_period_template = FULL_YEAR_TARGET_PERIOD

    target_period = target_period_template.format(
        date=format_period_end(record["date"])
    )

    document_date = parser_result.get("document_date")

    # Compared by month, a half can be dated 29/09 and its row 30/09
    if document_date and document_date[:7] != record["date"][:7]:
        target_period += COMPARATIVE_NOTE.format(
            document_date=format_period_end(document_date),
            date=format_period_end(record["date"]),
        )

    period_lines = []

    for target_page in target_pages:
        if target_page["period_covered"] not in period_lines:
            period_lines.append(target_page["period_covered"])

    page_numbers = [target_page["page"] for target_page in target_pages]

    input_data = {
        "target_period": target_period,
        "period_covered": "; ".join(period_lines),
        "statement_pages": render_statement_pages(
            select_pages(parser_result["pages"], page_numbers)
        ),
    }

    if statement_key == "income_statement":
        share_pages = find_share_pages(parser_result["pages"], page_numbers)
        attributable_page = find_attributable_page(parser_result["pages"], page_numbers)

        if attributable_page is not None and attributable_page not in share_pages:
            share_pages = [attributable_page, *share_pages]

        input_data["share_pages"] = render_share_pages(share_pages) or "(none found)"

    return input_data


def format_printed(extracted_value) -> str:
    # A list field holds several printed lines, shown as the model read them
    if isinstance(extracted_value, list):
        printed = [
            item.get("value")
            for item in extracted_value
            if item.get("value") is not None
        ]
        return " + ".join(f"{value:,.0f}" for value in printed) or "null"

    value = (extracted_value or {}).get("value")
    return "null" if value is None else f"{value:,.0f}"


def format_recheck_note(identity_breaks: list[dict], extraction: dict) -> str:
    lines = []

    for identity_break in identity_breaks:
        read_values = ", ".join(
            f"{field_name} {format_printed(extraction.get(field_name))}"
            for field_name in identity_break["fields"]
        )

        lines.append(
            f" - {IDENTITY_RULES.get(identity_break['check'], identity_break['check'])}\n"
            f" The earlier reading had: {read_values}"
        )

    return RECHECK_NOTE.format(breaks="\n".join(lines))


async def call_model(
    statement_key: str,
    log_name: str,
    input_data: dict,
    pdf_bytes: bytes,
    image_page_numbers: list[int],
    models: list[str],
    effort: str,
) -> dict | None:
    pydantic_output, system_prompt, user_prompt = STATEMENT_PROMPTS[statement_key]

    # Empty on the first reading, filled only when a recheck is asked for
    user_prompt = user_prompt + "\n{recheck_note}"

    if image_page_numbers:
        return await ainvoke_structured_vision_llm(
            pydantic_output=pydantic_output,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            log_name=log_name,
            input_data=input_data,
            page_images=render_page_images(pdf_bytes, image_page_numbers),
            models=models,
            temperature=EXTRACTION_TEMPERATURE,
            effort=effort,
        )

    return await ainvoke_structured_llm(
        pydantic_output=pydantic_output,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        log_name=log_name,
        input_data=input_data,
        models=models,
        temperature=EXTRACTION_TEMPERATURE,
        effort=effort,
    )


def read_statement(
    statement_key: str,
    extraction: dict,
    field_names: list[str],
    basis: dict | None = None,
) -> dict:
    extraction = fill_reporting_basis(extraction, basis)

    if statement_key == "cash_flow":
        extraction = drop_property_purchases(extraction)

    statement_result = build_statement_result(extraction, field_names)

    if extraction.get("dropped_capex_lines"):
        statement_result["dropped_capex_lines"] = extraction["dropped_capex_lines"]

    if statement_key == "balance_sheet":
        statement_result = derive_balance_sheet(statement_result)

    if statement_key == "cash_flow":
        statement_result = derive_cash_flow(statement_result)

    return statement_result


async def extract_one_data(
    statement_key: str,
    record: dict,
    parser_result: dict,
    pdf_bytes: bytes,
    models: list[str],
    effort: str,
    basis: dict | None = None,
) -> dict:
    pydantic_output = STATEMENT_PROMPTS[statement_key][0]
    field_names = get_field_names(pydantic_output)

    target_pages = record.get(statement_key) or []
    log_name = f"{record['symbol']} {record['date']} {statement_key}"

    skip_reason = get_skip_reason(target_pages, parser_result["period"])

    if skip_reason is not None:
        LOGGER.info("[Extract][%s] skipped: %s", log_name, skip_reason)
        return build_skipped_result(skip_reason, field_names)

    input_data = {
        **format_input(statement_key, record, parser_result, target_pages),
        "recheck_note": "",
    }

    image_page_numbers = get_image_page_numbers(
        statement_key,
        parser_result["pages"],
        [target_page["page"] for target_page in target_pages],
    )

    model_inputs = (
        statement_key,
        log_name,
        input_data,
        pdf_bytes,
        image_page_numbers,
        models,
        effort,
    )
    extraction = await call_model(*model_inputs)

    if extraction is None:
        LOGGER.error("[Extract][%s] every model failed", log_name)
        return build_skipped_result("llm_failed", field_names)

    statement_pages = [
        page
        for page in parser_result["pages"]
        if page["page_number"] in [target_page["page"] for target_page in target_pages]
    ]

    extraction = clear_guessed_basis(extraction, statement_pages)
    statement_result = read_statement(statement_key, extraction, field_names, basis)
    identity_breaks = find_identity_breaks(statement_key, statement_result)

    # A break on the statement as read is a misread (e.g. a twelve-month figure
    # in a six-month field), so it is read once more with the rule named.
    # The same prompt at temperature 0 would give the same answer
    if identity_breaks:
        LOGGER.info(
            "[Extract][%s] rechecking: %s", log_name, summarise_checks(identity_breaks)
        )

        recheck_extraction = await call_model(
            *model_inputs[:2],
            {
                **input_data,
                "recheck_note": format_recheck_note(identity_breaks, extraction),
            },
            *model_inputs[3:],
        )

        recheck_result = None
        recheck_breaks = identity_breaks

        if recheck_extraction is not None:
            recheck_result = read_statement(
                statement_key,
                clear_guessed_basis(recheck_extraction, statement_pages),
                field_names,
                basis,
            )
            recheck_breaks = find_identity_breaks(statement_key, recheck_result)

        # Only a reading that breaks fewer rules replaces the first one
        use_recheck = recheck_result is not None and len(recheck_breaks) < len(
            identity_breaks
        )

        if use_recheck:
            statement_result = recheck_result

        statement_result["recheck"] = {
            "first_breaks": identity_breaks,
            "recheck_breaks": recheck_breaks if recheck_result is not None else None,
            "used": "recheck" if use_recheck else "first",
        }

        LOGGER.info(
            "[Extract][%s] recheck %s, now: %s",
            log_name,
            "used" if use_recheck else "not used",
            summarise_checks(recheck_breaks),
        )

    # Read by derive_second_half, which turns the twelve months into H2
    statement_result["coverage"] = (
        "full_year" if is_full_year_only(target_pages) else "period"
    )

    return statement_result


def needs_full_year_read(
    statement_key: str, statement_result: dict, record: dict
) -> bool:
    target_pages = record.get(statement_key) or []

    return (
        statement_key in FLOW_STATEMENT_KEYS
        and statement_result["status"] == "extracted"
        and bool(target_pages)
        and not is_full_year_only(target_pages)
        and all(value is None for value in statement_result["values"].values())
    )


def is_twelve_month_column(column: str | None) -> bool:
    return bool(
        column
        and TWELVE_MONTH_COLUMN_PATTERN.search(column)
        and not OTHER_SPAN_COLUMN_PATTERN.search(column)
    )


def as_full_year(record: dict, statement_key: str) -> dict:
    return {
        **record,
        statement_key: [
            {**target_page, "coverage": "full_year"}
            for target_page in record[statement_key]
        ],
    }


async def extract_full_data(
    record: dict,
    parser_result: dict,
    pdf_bytes: bytes,
    rates: dict,
    models: list[str],
    effort: str,
    fresh_results: dict[tuple[str, str], dict] | None = None,
) -> dict:
    # Used only when a statement prints no scale at all
    basis = get_reporting_basis(record["symbol"], record["date"], fresh_results)

    statement_results = await asyncio.gather(
        *[
            extract_one_data(
                statement_key,
                record,
                parser_result,
                pdf_bytes,
                models,
                effort,
                basis,
            )
            for statement_key in STATEMENT_KEYS
        ]
    )

    # A flow statement read as the half year but found empty: its page prints
    # only the twelve months (WJP's cash flow), read that and subtract H1
    if parser_result["period"] == "H2":
        statement_results = list(statement_results)

        for index, statement_key in enumerate(STATEMENT_KEYS):
            if not needs_full_year_read(
                statement_key, statement_results[index], record
            ):
                continue

            LOGGER.info(
                "[Extract][%s %s %s] empty as a half year, reading the twelve months",
                record["symbol"],
                record["date"],
                statement_key,
            )
            full_year_result = await extract_one_data(
                statement_key,
                as_full_year(record, statement_key),
                parser_result,
                pdf_bytes,
                models,
                effort,
                basis,
            )

            # Kept only when the column read is plainly twelve months, so a
            # wrong column (a 15-month transition year, a quarter) stays blank
            if is_twelve_month_column(full_year_result.get("column")):
                statement_results[index] = full_year_result

            else:
                LOGGER.info(
                    "[Extract][%s %s %s] twelve-month read not confirmed (column %r), left empty",
                    record["symbol"],
                    record["date"],
                    statement_key,
                    full_year_result.get("column"),
                )

    result = {
        "symbol": record["symbol"],
        "date": record["date"],
        "period": parser_result["period"],
        **dict(zip(STATEMENT_KEYS, statement_results)),
    }

    result = resolve_mixed_currency(result, record, parser_result["pages"])
    result = fill_missing_currency(result, basis)

    # Still in the report's currency, H1 is converted back to it for the subtraction
    result = derive_second_half(result, rates, fresh_results)
    result = convert_to_sgd(result, rates)

    # The guards' size limits assume SGD, so they run on the converted figures
    result = apply_checks(result)

    # After the guards, so nothing is built on a line they removed, and after
    # the H2 subtraction, so H2's EBIT comes from H2's own pretax income
    result["income_statement"] = derive_income_statement(result["income_statement"])

    return check_second_half(result, rates, fresh_results)


async def extract_filing(
    record: dict,
    parser_result: dict,
    pdf_bytes: bytes,
    rates: dict,
    fresh_results: dict[tuple[str, str], dict],
    models: list[str] = ["claude-haiku-5.5"],
    effort: str = "medium",
) -> dict:
    with track_usage() as usage_records:
        result = await extract_full_data(
            record=record,
            parser_result=parser_result,
            pdf_bytes=pdf_bytes,
            rates=rates,
            models=models,
            effort=effort,
            fresh_results=fresh_results,
        )

    result["llm_usage"] = summarise_usage(usage_records)

    # Kept for the H2 of the same company later in the run
    fresh_results[(result["symbol"], result["date"])] = result

    LOGGER.info(
        "[Extract][%s %s] income %s | balance %s | cash flow %s | checks %s | cost $%.4f",
        result["symbol"],
        result["date"],
        result["income_statement"]["reason"] or "ok",
        result["balance_sheet"]["reason"] or "ok",
        result["cash_flow"]["reason"] or "ok",
        summarise_checks(result["checks"]),
        result["llm_usage"]["cost"],
    )

    return result
