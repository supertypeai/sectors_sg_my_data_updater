from ..main_analyst_report.api import create_session
from ..prompts.report import AnalystView, SYSTEM_PROMPT, USER_PROMPT
from .page_finder import STOCK_ALIASES, find_pages
from .parse_pdf import parse_document
from periodic_financial.llm.caller import (
    ainvoke_structured_llm,
    ainvoke_structured_vision_llm,
)
from periodic_financial.llm.usage import summarise_usage, track_usage
from periodic_financial.pipeline.parse_pdf import (
    render_page_images,
    render_pages,
    select_pages,
)
from periodic_financial.utils.currency import load_rates, sgd_rate

import asyncio
import logging


LOGGER = logging.getLogger(__name__)

EXTRACTION_TEMPERATURE = 0

MAX_CONCURRENT_DOCUMENTS = 5

# A December year reported in February is still a forecast until results
STALE_FORECAST_MONTHS = 6

# Forecasts are stored in SGD, amounts in millions and per-share figures in
# cents, so every house reads the same without its own units
AMOUNT_TO_MILLIONS = {"units": 1e-6, "thousands": 1e-3, "millions": 1, "billions": 1e3}
PER_SHARE_TO_CENTS = {"major": 100, "minor": 1}
AMOUNT_FIELDS = ("revenue", "net_profit")


def build_result(symbol: str, report: dict, status: str, **fields) -> dict:
    return {
        "symbol": symbol,
        "contributor": report["contributor"],
        "report_date": report["report_date"],
        "title": report["title"],
        "document_url": report["document_url"],
        "status": status,
        **fields,
    }


async def extract_report(
    record: dict,
    parser_result: dict,
    pdf_bytes: bytes,
    company_name: str | None,
    rates: dict,
    models: list[str],
    effort: str,
) -> dict:
    symbol = record["symbol"]
    log_name = f"{symbol} {parser_result['contributor']} {parser_result['report_date']}"
    selected_pages = select_pages(parser_result["pages"], record["page_numbers"])

    call_arguments = {
        "pydantic_output": AnalystView,
        "system_prompt": SYSTEM_PROMPT,
        "user_prompt": USER_PROMPT,
        "log_name": log_name,
        "input_data": {
            "company_name": company_name or symbol,
            "security_code": symbol.removesuffix(".SI"),
            "aliases": ", ".join(STOCK_ALIASES.get(symbol.removesuffix(".SI"), []))
            or "-",
            "title": parser_result["title"],
            "contributor": parser_result["contributor"],
            "report_date": parser_result["report_date"],
            "pages": render_pages(selected_pages),
        },
        "models": models,
        "temperature": EXTRACTION_TEMPERATURE,
        "effort": effort,
    }

    with track_usage() as usage_records:
        if record["image_pages"]:
            extraction = await ainvoke_structured_vision_llm(
                **call_arguments,
                page_images=render_page_images(pdf_bytes, record["image_pages"]),
            )

        else:
            extraction = await ainvoke_structured_llm(**call_arguments)

    page_fields = {
        "selection": record["selection"],
        "page_numbers": record["page_numbers"],
        "llm_usage": summarise_usage(usage_records),
    }
    cost = page_fields["llm_usage"]["cost"]

    if extraction is None:
        LOGGER.error("[Extract][%s] every model failed", log_name)
        return build_result(symbol, parser_result, "llm_failed", **page_fields)

    if not extraction["covers_stock"] or extraction["target_price"] is None:
        LOGGER.info("[Extract][%s] no target for this stock | $%.4f", log_name, cost)
        return build_result(
            symbol,
            parser_result,
            "no_target",
            notes=extraction["notes"],
            **page_fields,
        )

    report_year, report_month = map(int, parser_result["report_date"].split("-")[:2])
    basis = extraction["forecast_basis"] or {}

    # A field whose unit or rate is unknown is left out rather than stored in
    # the wrong scale. No currency means SGD
    sgd_multipliers = {}

    for fields, currency, multiplier in [
        (
            AMOUNT_FIELDS,
            basis.get("amounts_currency"),
            AMOUNT_TO_MILLIONS.get(basis.get("amounts_unit")),
        ),
        (
            ("eps",),
            basis.get("eps_currency"),
            PER_SHARE_TO_CENTS.get(basis.get("eps_unit")),
        ),
        (
            ("dps",),
            basis.get("dps_currency"),
            PER_SHARE_TO_CENTS.get(basis.get("dps_unit")),
        ),
    ]:
        rate = sgd_rate(rates, currency or "SGD", parser_result["report_date"])[0]

        for field in fields:
            sgd_multipliers[field] = (
                None if rate is None or multiplier is None else rate * multiplier
            )

    forecasts = []

    for forecast_year in sorted(
        extraction["forecasts"], key=lambda forecast: forecast["period_end"]
    ):
        period_year, period_month = map(int, forecast_year["period_end"].split("-"))
        months_before_report = (
            (report_year - period_year) * 12 + report_month - period_month
        )

        # DBS still prints FY25/26 as "F" in October 2026
        if months_before_report > STALE_FORECAST_MONTHS:
            LOGGER.debug(
                "[Extract][%s] %s ended %s, dropped as past",
                log_name,
                forecast_year["label"],
                forecast_year["period_end"],
            )
            continue

        sgd_figures = {}

        for field, multiplier in sgd_multipliers.items():
            value = forecast_year[field]

            if value is not None and multiplier is None:
                LOGGER.warning(
                    "[Extract][%s] %s %s has no unit or rate, dropped",
                    log_name,
                    forecast_year["label"],
                    field,
                )

            sgd_figures[field] = (
                None
                if value is None or multiplier is None
                else round(value * multiplier, 1 if field in AMOUNT_FIELDS else 2)
            )

        forecasts.append({"year": period_year, **forecast_year, **sgd_figures})

    # Prices on the site are in SGD, so a US$ or EUR target is converted at
    # the last quarter end before the report. No currency means SGD
    original_currency = extraction["target_currency"] or "SGD"
    fx_rate = sgd_rate(rates, original_currency, parser_result["report_date"])[0]

    if fx_rate is None:
        LOGGER.warning(
            "[Extract][%s] no SGD rate for %s | cost $%.4f",
            log_name,
            original_currency,
            cost,
        )
        return build_result(symbol, parser_result, "no_fx_rate", **page_fields)

    previous_target = extraction["previous_target_price"]

    row = {
        "symbol": symbol,
        "contributor": parser_result["contributor"],
        "analyst": extraction["analyst"],
        "published_date": parser_result["report_date"],
        "source_url": parser_result["document_url"],
        "title": parser_result["title"],
        "rating": extraction["rating"],
        "target_price": round(extraction["target_price"] * fx_rate, 3),
        "previous_target_price": (
            None if previous_target is None else round(previous_target * fx_rate, 3)
        ),
        "original_target_price": extraction["target_price"],
        "original_currency": original_currency,
        "fx_rate": fx_rate,
        "target_basis": extraction["target_basis"],
        "forecast_basis": {
            "profit_type": basis.get("profit_type") or "reported",
            "original_currency": basis.get("amounts_currency")
            or basis.get("eps_currency")
            or "SGD",
        },
        "forecasts": forecasts,
    }

    LOGGER.info(
        "[Extract][%s] %s | target %s %s%s | %s | $%.4f",
        log_name,
        row["rating"] or "no rating",
        original_currency,
        row["original_target_price"],
        "" if original_currency == "SGD" else f" = SGD {row['target_price']}",
        f"{len(row['forecasts'])} forecast years" if row["forecasts"] else "no forecast",
        cost,
    )

    return build_result(
        symbol,
        parser_result,
        "extracted",
        row=row,
        notes=extraction["notes"],
        **page_fields,
    )


async def process_document(
    document: dict,
    company_names: dict[str, str],
    rates: dict,
    semaphore: asyncio.Semaphore,
    models: list[str],
    effort: str,
) -> list[dict]:
    report = document["report"]

    async with semaphore:
        try:
            parsed = await asyncio.to_thread(parse_document, create_session(), report)

        except Exception as error:
            LOGGER.exception("[Parse] failed: %s", report["document_url"])
            return [
                build_result(symbol, report, "download_failed", error=repr(error))
                for symbol in document["symbols"]
            ]

        if parsed is None:
            return [
                build_result(symbol, report, "no_pdf")
                for symbol in document["symbols"]
            ]

        parser_result, pdf_bytes = parsed
        results = []

        for symbol in document["symbols"]:
            try:
                record = find_pages(parser_result, symbol, company_names.get(symbol))
                results.append(
                    await extract_report(
                        record,
                        parser_result,
                        pdf_bytes,
                        company_names.get(symbol),
                        rates,
                        models,
                        effort,
                    )
                )

            except Exception as error:
                LOGGER.exception("[Extract][%s] failed: %s", symbol, report["title"])
                results.append(build_result(symbol, report, "error", error=repr(error)))

        return results


async def process_documents(
    documents: list[dict],
    company_names: dict[str, str],
    models: list[str] = ["claude-haiku-5.5"],
    effort: str = "medium",
) -> list[dict]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOCUMENTS)
    rates = load_rates()

    document_results = await asyncio.gather(
        *[
            process_document(
                document, company_names, rates, semaphore, models, effort
            )
            for document in documents
        ]
    )

    return [result for results in document_results for result in results]
