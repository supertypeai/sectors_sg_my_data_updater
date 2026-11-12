from functools import lru_cache
from pathlib import Path

from ..utils.currency import sgd_rate
from ..utils.half_year import get_first_half, get_first_half_date
from ..utils.supabase_helper import get_annual_statements
from ..utils.io_helper import open_json

import logging


LOGGER = logging.getLogger(__name__)

COMPARABLE_FIRST_HALVES_PATH = (
    Path(__file__).parents[1] / "config" / "comparable_first_halves.json"
)

# Only lines the annual table defines like the reports do. Its capex and
# operating expense are its own sums (AWZ capex 382 vs 598), so they are left out.
# Its net_income is the owners' share, net_income_after_taxes the total.
# Operating income is calculated to follow the annual table, so this checks the
# formula over every company. It needs H1 rows calculated the same way, the old
# pipeline's stored H1 defines it differently
ANNUAL_FIELDS = {
    "income_statement": {
        "total_revenue": "total_revenue",
        "pretax_income": "pretax_income",
        "net_income": "net_income_after_taxes",
        "operating_income": "operating_income",
    },
    "cash_flow": {
        "operating_cash_flow": "operating_cash_flow",
    },
}

# A bank's revenue is 0.4% off (DBS 22,297 vs 22,217), so small gaps are expected
MAX_ANNUAL_GAP = 0.02

# The annual table reclassifies some operating cash flow lines, so a correct
# figure can be 2-3% off it. A wrong H1 is off by half or more
#
# Operating income is calculated, not printed, and the annual table treats a few
# lines its own way for companies that list expenses by nature (up to 12% off
# when the expenses include impairments or one-off losses)
MAX_ANNUAL_GAP_BY_FIELD = {
    "operating_cash_flow": 0.05,
    "operating_income": 0.10,
}

# An H1 holding one quarter makes H2 revenue about twice H1 or more (IHH 3.3x,
# TSH 3.2x). Halves of a normal year sit well inside this, seasonal ones can
# reach it, so it is only reported for review
MAX_HALF_YEAR_RATIO = 1.8

# Full year minus an H1 this different is no half year, e.g. after a reverse
# takeover the full year is the new business and the H1 the old one
MAX_DERIVED_HALF_RATIO = 10

FLOW_STATEMENTS = ["income_statement", "cash_flow"]


def get_gap(full_year_value: float, annual_value: float) -> float:
    return abs(full_year_value - annual_value) / max(abs(annual_value), 1)


def add_review_issue(
    result: dict, check_name: str, field_name: str, detail: str
) -> None:
    LOGGER.warning("%s %s: %s %s", result["symbol"], result["date"], check_name, detail)

    result["checks"].append(
        {
            "check": check_name,
            "severity": "review",
            "fields": [field_name],
            "detail": detail,
        }
    )


def find_annual_mismatches(
    result: dict,
    first_half: dict,
    annual: dict,
    first_half_restatement: float,
) -> list[dict]:
    mismatches = []

    for statement_key, field_map in ANNUAL_FIELDS.items():
        for field_name, annual_field_name in field_map.items():
            second_half_value = result[statement_key]["values"].get(field_name)
            first_half_value = (first_half.get(statement_key) or {}).get(field_name)
            annual_value = (annual.get(statement_key) or {}).get(annual_field_name)

            if None in (second_half_value, first_half_value, annual_value):
                continue

            full_year_value = (
                first_half_value * first_half_restatement + second_half_value
            )
            gap = get_gap(full_year_value, annual_value)

            if gap > MAX_ANNUAL_GAP_BY_FIELD.get(field_name, MAX_ANNUAL_GAP):
                mismatches.append(
                    {
                        "field": field_name,
                        "full_year": full_year_value,
                        "annual": annual_value,
                        "gap": round(gap, 4),
                    }
                )

    return mismatches


def get_first_half_restatement(
    result: dict, rates: dict, first_half_date: str
) -> float | None:
    # The annual table converts the whole year at the year-end rate (IHH's RM
    # 25,745m x 0.3165), so H1's SGD figure is restated from its own rate to
    # H2's. Adding the two as stored put IHH and TSH 2% off on rates alone
    first_half_rate, _ = sgd_rate(rates, result.get("currency"), first_half_date)
    second_half_rate = result.get("sgd_rate")

    if not first_half_rate or not second_half_rate:
        return None

    return second_half_rate / first_half_rate


def check_against_annual(
    result: dict,
    first_half: dict,
    rates: dict,
    first_half_date: str,
) -> None:
    # Catches a wrong H1 or H2 when H2 was read from its own six-month column.
    # An H2 derived as full year minus H1 adds back to the report's full year
    # whatever H1 holds, so then it only confirms the full year was read right
    annual = get_annual_statements(result["symbol"], result["date"])

    if annual is None:
        result["second_half_check"]["annual"] = "no_annual_row"
        return

    first_half_restatement = get_first_half_restatement(result, rates, first_half_date)

    if first_half_restatement is None:
        result["second_half_check"]["annual"] = "no_sgd_rate"
        return

    mismatches = find_annual_mismatches(
        result, first_half, annual, first_half_restatement
    )
    result["second_half_check"]["annual"] = "mismatch" if mismatches else "matched"
    result["second_half_check"]["annual_mismatches"] = mismatches

    for mismatch in mismatches:
        add_review_issue(
            result,
            "annual_mismatch",
            mismatch["field"],
            f"{mismatch['field']} H1+H2={mismatch['full_year']:,.0f} vs annual={mismatch['annual']:,.0f}",
        )


def check_half_year_ratio(result: dict, first_half: dict) -> None:
    # Catches the wrong H1 the annual check cannot see, and runs before the
    # annual table has the year, which is when the weekly run sees an H2
    second_half_revenue = result["income_statement"]["values"].get("total_revenue")
    first_half_revenue = (first_half.get("income_statement") or {}).get("total_revenue")

    if not second_half_revenue or not first_half_revenue:
        return

    ratio = second_half_revenue / first_half_revenue
    result["second_half_check"]["revenue_ratio"] = round(ratio, 2)

    if not 1 / MAX_HALF_YEAR_RATIO <= ratio <= MAX_HALF_YEAR_RATIO:
        add_review_issue(
            result,
            "half_year_ratio",
            "total_revenue",
            f"H2 revenue {second_half_revenue:,.0f} is {ratio:.1f}x H1 {first_half_revenue:,.0f}",
        )

    derived = result["income_statement"]["status"] == "derived"
    confirmed = (result["symbol"], result["date"]) in get_comparable_first_halves()

    # A confirmed H1 keeps only the review flag above: an acquisition inside
    # H2 makes a real jump
    if (
        derived
        and not confirmed
        and not 1 / MAX_DERIVED_HALF_RATIO <= ratio <= MAX_DERIVED_HALF_RATIO
    ):
        blank_derived_statements(result, ratio)


@lru_cache(maxsize=1)
def get_comparable_first_halves() -> set[tuple[str, str]]:
    # H2 rows whose H1 was checked against the report and is the same
    # business, kept with the reason in config/comparable_first_halves.json
    return {
        (row["symbol"], row["date"]) for row in open_json(COMPARABLE_FIRST_HALVES_PATH)
    }


def blank_derived_statements(result: dict, ratio: float) -> None:
    for statement_key in FLOW_STATEMENTS:
        statement_result = result[statement_key]

        if statement_result["status"] != "derived":
            continue

        statement_result["reason"] = "h1_not_comparable"
        statement_result["explanations"] = {
            field_name: f"Not derived: full year minus H1 gives {ratio:.1f}x H1 revenue, the H1 is not this business's half"
            for field_name in statement_result["values"]
        }
        statement_result["values"] = {
            field_name: None for field_name in statement_result["values"]
        }


def check_second_half(
    result: dict,
    rates: dict,
    fresh_results: dict[tuple[str, str], dict] | None = None,
) -> dict:
    if result["period"] != "H2":
        return result

    first_half_date = get_first_half_date(result["date"])
    first_half, source = get_first_half(
        result["symbol"], first_half_date, fresh_results or {}
    )
    result["second_half_check"] = {"first_half_source": source}

    if source == "missing":
        return result

    check_half_year_ratio(result, first_half)
    check_against_annual(result, first_half, rates, first_half_date)

    return result
