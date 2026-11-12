from pathlib import Path

from .statement_values import STATEMENT_KEYS, SHARE_FIELDS, get_values

import json


RATES_PATH = Path("quarterly_rates.json")
# An H2 derived from the twelve months is still in the report's currency, the
# subtraction happens before conversion
CONVERTIBLE_STATUSES = ("extracted", "derived")


def load_rates(path: Path = RATES_PATH) -> dict:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)["quarters"]


def sgd_rate(
    rates: dict,
    currency: str | None,
    period_end: str | None,
) -> tuple[float | None, str | None]:
    # SGD per unit of currency, at the quarter end on or before the period end
    if not currency:
        return None, None

    if currency == "SGD":
        return 1.0, "n/a"

    quarters = sorted(rates)

    usable_quarters = [
        quarter for quarter in quarters if period_end and quarter <= period_end
    ] or quarters

    rate_quarter = usable_quarters[-1]
    rate = (rates[rate_quarter].get(currency) or {}).get("SGD")

    return rate, rate_quarter


def to_sgd(metrics: dict, rate: float) -> dict:
    converted = {}

    for statement_key, values in metrics.items():
        converted[statement_key] = {
            field_name: (
                None
                if value is None
                else int(round(value if field_name in SHARE_FIELDS else value * rate))
            )
            for field_name, value in values.items()
        }

    return converted


def get_filing_currencies(result: dict) -> set[str]:
    # A statement can come back without a currency, the
    # filing's other statements share it, so one currency per filing is used
    return {
        result[statement_key]["currency"]
        for statement_key in STATEMENT_KEYS
        if result[statement_key]["status"] in CONVERTIBLE_STATUSES
        and result[statement_key]["currency"]
    }


def get_filing_currency(result: dict) -> str | None:
    currencies = get_filing_currencies(result)
    return currencies.pop() if len(currencies) == 1 else None


def convert_to_sgd(result: dict, rates: dict) -> dict:
    currencies = get_filing_currencies(result)
    currency = get_filing_currency(result)
    rate, rate_quarter = sgd_rate(rates, currency, result["date"])

    result["currency"] = currency
    result["sgd_rate"] = rate
    result["rate_quarter"] = rate_quarter

    # The table has no currency column, so unconverted figures would pass as
    # SGD, they are dropped like the old pipeline does
    if rate is None:
        reason = "mixed_currency" if len(currencies) > 1 else "no_sgd_rate"

        for statement_key in STATEMENT_KEYS:
            if result[statement_key]["status"] not in CONVERTIBLE_STATUSES:
                continue

            result[statement_key] = {
                **result[statement_key],
                "status": "skipped",
                "reason": reason,
                "values": {
                    field_name: None for field_name in result[statement_key]["values"]
                },
            }

        return result

    sgd_values = to_sgd(get_values(result), rate)

    for statement_key in STATEMENT_KEYS:
        result[statement_key]["values"] = sgd_values[statement_key]

    return result
