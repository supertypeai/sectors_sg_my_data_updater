from .currency import get_filing_currency, sgd_rate
from .statement_values import SHARE_FIELDS
from .supabase_helper import get_periodic_statements

from calendar import monthrange
from datetime import date

import logging


LOGGER = logging.getLogger(__name__)

# Flows add up over the year, the balance sheet is a point in time, so only
# these two can be derived as full year minus the first half
FLOW_STATEMENT_KEYS = ["income_statement", "cash_flow"]

# Never subtracted: a twelve-month share count is not a half-year one, and the
# stored H1 rows from the old pipeline define these two differently (ebit was a
# copy of operating income, non-operating a broad sum of lines). Both are
# calculated from H2's own pretax income afterwards instead
NOT_DERIVED_FIELDS = [*SHARE_FIELDS, "ebit", "non_operating_income_or_loss"]


def get_first_half_date(second_half_date: str) -> str:
    period_end = date.fromisoformat(second_half_date)
    month = period_end.month - 6
    year = period_end.year

    if month < 1:
        month += 12
        year -= 1

    return date(year, month, monthrange(year, month)[1]).isoformat()


def get_first_half(
    symbol: str,
    first_half_date: str,
    fresh_results: dict[tuple[str, str], dict],
) -> tuple[dict, str]:
    # An H1 extracted in this run wins over the stored row, which can be the
    # old pipeline's quarter-only figures (IHH stored Q2 revenue as its H1)
    fresh_result = fresh_results.get((symbol, first_half_date))

    if fresh_result is not None:
        return {
            statement_key: fresh_result[statement_key]["values"]
            for statement_key in FLOW_STATEMENT_KEYS
        }, "this_run"

    stored_row = get_periodic_statements(symbol, first_half_date)

    if stored_row is not None:
        return {
            statement_key: stored_row.get(statement_key) or {}
            for statement_key in FLOW_STATEMENT_KEYS
        }, "database"

    return {}, "missing"


def has_values(statement_values: dict) -> bool:
    return any(value is not None for value in statement_values.values())


def to_report_currency(sgd_value: float | None, rate: float) -> float | None:
    # H1 is stored in SGD, the twelve months are still in the report's currency
    return None if sgd_value is None else sgd_value / rate


def derive_second_half(
    result: dict,
    rates: dict,
    fresh_results: dict[tuple[str, str], dict] | None = None,
) -> dict:
    # A full-year report sometimes prints a statement for the twelve months
    # only. H2 is then the report's own twelve months minus
    # H1. The SGX annual table defines some fields differently (capex,
    # operating expense), so it is only used to check the result, never here
    full_year_keys = [
        statement_key
        for statement_key in FLOW_STATEMENT_KEYS
        if result[statement_key].get("coverage") == "full_year"
        and result[statement_key]["status"] == "extracted"
    ]

    if not full_year_keys:
        return result

    symbol = result["symbol"]
    first_half_date = get_first_half_date(result["date"])
    first_half, source = get_first_half(symbol, first_half_date, fresh_results or {})

    # Subtracted in the report's currency and converted once at the H2 rate.
    # Subtracting two SGD figures would mix the H1 and year-end rates
    currency = get_filing_currency(result)
    first_half_rate, first_half_rate_quarter = sgd_rate(
        rates, currency, first_half_date
    )

    # An H1 row can exist with a statement left empty
    empty_statements = [
        statement_key
        for statement_key in full_year_keys
        if source != "missing" and not has_values(first_half.get(statement_key) or {})
    ]

    # Logged in the result too, so a missing or empty H1 row can be re-extracted
    result["first_half"] = {
        "date": first_half_date,
        "source": source,
        "rate": first_half_rate,
        "rate_quarter": first_half_rate_quarter,
        "empty_statements": empty_statements,
    }

    if source == "missing" or empty_statements:
        LOGGER.warning(
            "%s %s: H1 %s is %s%s, its full-year-only statements stay null",
            symbol,
            result["date"],
            first_half_date,
            source,
            f", empty {empty_statements}" if empty_statements else "",
        )

    for statement_key in full_year_keys:
        first_half_values = first_half.get(statement_key) or {}

        if statement_key == "income_statement":
            first_half_values = merge_split_costs(
                result[statement_key], first_half_values
            )

        result[statement_key] = subtract_first_half(
            result[statement_key],
            first_half_values,
            currency,
            first_half_rate,
            first_half_date,
        )

        result[statement_key]["reason"] = get_derive_reason(
            source, statement_key in empty_statements, first_half_rate
        )

    return result


def add_costs(
    cost_of_revenue: float | None, operating_expense: float | None
) -> float | None:
    if operating_expense is None:
        return None

    return (cost_of_revenue or 0) + operating_expense


def merge_split_costs(full_year_result: dict, first_half_values: dict) -> dict:
    # A half-year report can print one "Operating expenses" total that holds
    # cost of sales while the full-year report prints cost of sales apart
    # (BVA). Subtracting each part then gives a negative H2 operating expense,
    # so both are merged into operating expense, the total cost subtracts
    # cleanly and H2 operating income stays revenue minus total cost
    full_year_values = full_year_result["values"]
    full_year_has_cost = full_year_values.get("cost_of_revenue") is not None
    first_half_has_cost = first_half_values.get("cost_of_revenue") is not None

    if full_year_has_cost == first_half_has_cost:
        return first_half_values

    full_year_values["operating_expense"] = add_costs(
        full_year_values.get("cost_of_revenue"),
        full_year_values.get("operating_expense"),
    )
    full_year_values["cost_of_revenue"] = None
    full_year_values["gross_income"] = None

    LOGGER.info(
        "Cost of revenue printed in only one of H1 and the full year, "
        "subtracting total costs as operating expense"
    )

    return {
        **first_half_values,
        "operating_expense": add_costs(
            first_half_values.get("cost_of_revenue"),
            first_half_values.get("operating_expense"),
        ),
        "cost_of_revenue": None,
        "gross_income": None,
    }


def get_derive_reason(
    source: str, is_empty: bool, first_half_rate: float | None
) -> str:
    if source == "missing":
        return "no_h1_row"

    if is_empty:
        return "h1_statement_empty"

    if first_half_rate is None:
        return "no_h1_rate"

    return "full_year_minus_h1"


def subtract_first_half(
    statement_result: dict,
    first_half_values: dict,
    currency: str | None,
    first_half_rate: float | None,
    first_half_date: str,
) -> dict:
    for field_name, full_year_value in statement_result["values"].items():
        first_half_sgd = first_half_values.get(field_name)

        if (
            field_name in NOT_DERIVED_FIELDS
            or full_year_value is None
            or first_half_sgd is None
            or first_half_rate is None
        ):
            statement_result["values"][field_name] = None
            statement_result["explanations"][field_name] = (
                f"Not derived: full year {full_year_value}, H1 {first_half_sgd} SGD ({first_half_date})"
            )
            continue

        first_half_value = to_report_currency(first_half_sgd, first_half_rate)
        statement_result["values"][field_name] = full_year_value - first_half_value
        statement_result["explanations"][field_name] = (
            f"Full year {full_year_value:,.0f} {currency} from the report minus H1 "
            f"{first_half_value:,.0f} {currency} ({first_half_sgd:,.0f} SGD at "
            f"{first_half_rate}, {first_half_date})"
        )

    statement_result["status"] = "derived"
    return statement_result
