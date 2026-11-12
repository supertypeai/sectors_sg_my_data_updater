import re

from ..utils.statement_values import STATEMENT_KEYS


# A currency in a unit row, e.g. RMB'000, S$'000, US$m, RM million
CURRENCY_UNIT_PATTERN = re.compile(
    r"(?<![A-Za-z])(RMB|CNY|US\$|USD|S\$|SGD|RM|MYR|HK\$|HKD|A\$|AUD|Rp|IDR|THB|Baht|EUR|GBP|JPY|PHP|INR|TWD)"
    r"\s*['’]?\s*(?:000|m\b|mil|million|billion|bn\b)",
)

CURRENCY_CODES = {
    "RMB": "CNY",
    "CNY": "CNY",
    "US$": "USD",
    "USD": "USD",
    "S$": "SGD",
    "SGD": "SGD",
    "RM": "MYR",
    "MYR": "MYR",
    "HK$": "HKD",
    "HKD": "HKD",
    "A$": "AUD",
    "AUD": "AUD",
    "Rp": "IDR",
    "IDR": "IDR",
    "THB": "THB",
    "Baht": "THB",
    "EUR": "EUR",
    "GBP": "GBP",
    "JPY": "JPY",
    "PHP": "PHP",
    "INR": "INR",
    "TWD": "TWD",
}


def get_printed_currency(page_texts: list[str]) -> str | None:
    # Only when the pages name a single currency, a bare "$'000" names none
    codes = {
        CURRENCY_CODES[match.group(1)]
        for page_text in page_texts
        for match in CURRENCY_UNIT_PATTERN.finditer(page_text)
    }

    return codes.pop() if len(codes) == 1 else None


def resolve_mixed_currency(result: dict, record: dict, pages: list[dict]) -> dict:
    # The statements of one filing share a currency. When the model reads
    # different ones (BWM's income statement as SGD under "RMB'000"), each
    # statement takes the currency printed in its own unit row instead
    currencies = {
        result[statement_key]["currency"]
        for statement_key in STATEMENT_KEYS
        if result[statement_key]["currency"]
    }

    if len(currencies) < 2:
        return result

    for statement_key in STATEMENT_KEYS:
        statement_result = result[statement_key]
        page_numbers = [
            target_page["page"] for target_page in record.get(statement_key) or []
        ]

        printed_currency = get_printed_currency(
            [page["text"] for page in pages if page["page_number"] in page_numbers]
        )

        if printed_currency and printed_currency != statement_result["currency"]:
            statement_result["currency_note"] = (
                f"Model read {statement_result['currency']}, the pages print {printed_currency}"
            )
            statement_result["currency"] = printed_currency

    return result


def fill_missing_currency(result: dict, basis: dict | None) -> dict:
    # A filing that names no currency at all (only "$m") reports
    # in the same currency as the company's other filings
    if basis is None or not basis.get("currency"):
        return result

    if any(result[statement_key]["currency"] for statement_key in STATEMENT_KEYS):
        return result

    for statement_key in STATEMENT_KEYS:
        statement_result = result[statement_key]

        if statement_result["status"] != "extracted":
            continue

        statement_result["currency"] = basis["currency"]
        statement_result["currency_note"] = (
            f"No currency printed, {basis['currency']} from the filing of {basis['source_date']}"
        )

    return result
