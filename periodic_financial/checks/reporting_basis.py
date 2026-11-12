from datetime import date

from ..utils.statement_values import STATEMENT_KEYS

import re


# Any sign of a currency or a scale on the statement pages
PRINTED_BASIS_PATTERN = re.compile(
    r"pesos?|dollars?|ringgit|rupiah|yuan|renminbi|euros?|yen\b|rupees?|\$|['’]\s*000"
    r"|\bthousands?\b|\bmillions?\b|\bbillions?\b"
    r"|\b(?:SGD|USD|RMB|CNY|MYR|RM|HKD|IDR|Rp|THB|Baht|EUR|GBP|JPY|PHP|INR|AUD|TWD|KRW|CHF|CAD)\b",
    re.IGNORECASE,
)


def get_reporting_basis(
    symbol: str,
    filing_date: str,
    fresh_results: dict[tuple[str, str], dict] | None,
) -> dict | None:
    # A company reports in the same currency and scale from one filing to the
    # next, so the nearest filing that prints them stands in for one that
    # prints neither, e.g. Hafary's FY2025 statements have no "$'000" row
    candidates = []

    for (result_symbol, result_date), result in (fresh_results or {}).items():
        if (
            result_symbol != symbol
            or result_date == filing_date
            or not result.get("currency")
        ):
            continue

        units = {
            result[statement_key].get("unit")
            for statement_key in STATEMENT_KEYS
            if result[statement_key].get("status") == "extracted"
            and not result[statement_key].get("basis_from")
        }
        units.discard(None)
        units.discard("unknown")

        if len(units) != 1:
            continue

        distance = abs(
            (date.fromisoformat(result_date) - date.fromisoformat(filing_date)).days
        )
        candidates.append((distance, result_date, result["currency"], units.pop()))

    if not candidates:
        return None

    _, source_date, currency, unit = min(candidates)

    return {"currency": currency, "unit": unit, "source_date": source_date}


def fill_reporting_basis(extraction: dict, basis: dict | None) -> dict:
    # An unknown unit means no scale and no currency were printed on the pages
    if basis is None or extraction.get("unit") != "unknown":
        return extraction

    return {
        **extraction,
        "unit": basis["unit"],
        "currency": extraction.get("currency") or basis["currency"],
        "basis_from": basis["source_date"],
    }


def clear_guessed_basis(extraction: dict, pages: list[dict]) -> dict:
    # When the pages print no currency and no scale, a unit the model gave is
    # a guess ("units" for a company that reports in US$'000). Set to unknown,
    # the company's other filings fill it
    if not pages or any(page["is_scanned"] for page in pages):
        return extraction

    if extraction.get("unit") == "unknown":
        return extraction

    if any(PRINTED_BASIS_PATTERN.search(page["text"]) for page in pages):
        return extraction

    return {**extraction, "unit": "unknown", "currency": None}
