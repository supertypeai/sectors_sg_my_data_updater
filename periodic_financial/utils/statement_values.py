import re

from ..prompts.statement_common import StatementBase
from ..pipeline.page_finder import STATEMENT_KEYS


UNIT_MULTIPLIERS = {
    "units": 1,
    "thousands": 1_000,
    "millions": 1_000_000,
    "billions": 1_000_000_000,
}

SHARE_FIELDS = ["basic_shares_outstanding", "diluted_shares_outstanding"]

# Read by the model but folded into capital_expenditure in code, never stored
CODE_ONLY_FIELDS = [
    "intangible_asset_purchases",
    "other_capital_purchases",
    "capital_employed",
]

# Describe how the statement is read, they are not figures
DESCRIPTIVE_FIELDS = ["share_unit", "is_bank"]

# (missing section, the other section, their total)
BALANCE_SHEET_PARTS = [
    ("total_current_asset", "total_non_current_asset", "total_asset"),
    ("total_non_current_asset", "total_current_asset", "total_asset"),
    ("total_current_liabilities", "total_non_current_liabilities", "total_liabilities"),
    ("total_non_current_liabilities", "total_current_liabilities", "total_liabilities"),
]

# Rounding allowed when matching the activities to the net change
NET_CASH_TOLERANCE = 0.001

# Buying a property is an acquisition, not capital expenditure ("Purchase of
# investment properties", "Acquisition of interests in investment
# properties"), whatever field the model put it in
PROPERTY_PURCHASE_PATTERN = re.compile(
    r"\b(?:purchases?|acquisitions?|acquire[ds]?)\b.{0,40}investment\s+propert",
    re.IGNORECASE,
)

# A line that also names spending is not only a purchase, e.g.
# "Acquisition of/development expenditure in investment properties"
PROPERTY_SPENDING_PATTERN = re.compile(
    r"expenditure|addition|improvement|enhancement|construction\s+costs?|capitali[sz]ed"
    # "Purchase of property, plant and equipment and investment
    # properties" also holds PPE capex
    r"|plant|equipment",
    re.IGNORECASE,
)


def get_field_names(pydantic_output) -> list[str]:
    # currency, unit and column describe the statement, they are not figures
    return [
        field_name
        for field_name in pydantic_output.model_fields
        if field_name not in StatementBase.model_fields
        and field_name not in DESCRIPTIVE_FIELDS
    ]


def get_values(result: dict) -> dict:
    return {
        statement_key: dict(result[statement_key]["values"])
        for statement_key in STATEMENT_KEYS
    }


def scale_value(value: float | None, unit: str | None) -> float | None:
    multiplier = UNIT_MULTIPLIERS.get(unit)

    # An unknown scale makes every figure on the statement unusable
    if value is None or multiplier is None:
        return None

    return value * multiplier


def combine_values(extracted_values: list[dict]) -> dict:
    printed_values = [
        extracted_value
        for extracted_value in extracted_values
        if extracted_value.get("value") is not None
    ]

    if not printed_values:
        return {}

    return {
        "value": sum(extracted_value["value"] for extracted_value in printed_values),
        "label": "; ".join(
            extracted_value.get("label") or "" for extracted_value in printed_values
        ),
        "explanation": "; ".join(
            extracted_value.get("explanation") or ""
            for extracted_value in printed_values
        ),
    }


def is_property_purchase(label: str) -> bool:
    return bool(
        PROPERTY_PURCHASE_PATTERN.search(label)
        and not PROPERTY_SPENDING_PATTERN.search(label)
    )


def drop_property_purchases(extraction: dict) -> dict:
    dropped = []
    extraction = dict(extraction)
    capex = extraction.get("capital_expenditure") or {}

    if is_property_purchase(capex.get("label") or ""):
        dropped.append(capex.get("label"))
        extraction["capital_expenditure"] = {**capex, "value": None}

    # A PPE or intangibles line printed without brackets is a reversal of
    # earlier additions, not negative spending (BTM 2H2023 "Additions 4,523")
    for field_name in ["capital_expenditure", "intangible_asset_purchases"]:
        item = extraction.get(field_name) or {}

        if (item.get("value") or 0) < 0:
            dropped.append(f"{item.get('label')} (inflow)")
            extraction[field_name] = {**item, "value": None}

    other_purchases = []

    for item in extraction.get("other_capital_purchases") or []:
        if is_property_purchase(item.get("label") or ""):
            dropped.append(item.get("label"))
            continue

        # Capital expenditure is an outflow, a net disposal inflow is not
        # negative spending ("Net disposal/(acquisition of...)")
        if (item.get("value") or 0) < 0:
            dropped.append(f"{item.get('label')} (inflow)")
            continue

        other_purchases.append(item)

    extraction["other_capital_purchases"] = other_purchases
    extraction["dropped_capex_lines"] = dropped
    return extraction


def build_statement_result(extraction: dict, field_names: list[str]) -> dict:
    values = {}
    labels = {}
    explanations = {}
    summed_fields = []

    for field_name in field_names:
        extracted_value = extraction.get(field_name) or {}
        unit = extraction.get("unit")

        # A list field holds several printed lines, which code adds up
        if isinstance(extracted_value, list):
            if sum(item.get("value") is not None for item in extracted_value) > 1:
                summed_fields.append(field_name)

            extracted_value = combine_values(extracted_value)

        if field_name in SHARE_FIELDS:
            unit = extraction.get("share_unit")

        values[field_name] = scale_value(extracted_value.get("value"), unit)
        labels[field_name] = extracted_value.get("label")
        explanations[field_name] = extracted_value.get("explanation")

    return {
        "status": "extracted",
        "reason": None,
        "currency": extraction.get("currency"),
        "unit": extraction.get("unit"),
        "column": extraction.get("column"),
        # Only the income statement asks for it, read by derive_income_statement
        "is_bank": extraction.get("is_bank"),
        # Set when currency and scale came from another filing of the company
        "basis_from": extraction.get("basis_from"),
        # Totals code added up from several printed lines
        "summed_fields": summed_fields,
        "values": values,
        "labels": labels,
        "explanations": explanations,
    }


def build_skipped_result(reason: str, field_names: list[str]) -> dict:
    return {
        "status": "skipped",
        "reason": reason,
        "currency": None,
        "unit": None,
        "column": None,
        "values": {
            field_name: None
            for field_name in field_names
            if field_name not in CODE_ONLY_FIELDS
        },
        "labels": {},
        "explanations": {},
    }


def add_values(first_value: float | None, second_value: float | None) -> float | None:
    if first_value is None or second_value is None:
        return None

    return first_value + second_value


def subtract_values(
    first_value: float | None, second_value: float | None
) -> float | None:
    if first_value is None or second_value is None:
        return None

    return first_value - second_value


def derive_balance_sheet(statement_result: dict) -> dict:
    values = statement_result["values"]
    derived = []

    # Funds employed layout: equity + non-current liabilities is printed,
    # total assets and total liabilities are not
    capital_employed = values.pop("capital_employed", None)
    statement_result["labels"].pop("capital_employed", None)
    statement_result["explanations"].pop("capital_employed", None)

    if capital_employed is not None:
        if (
            values["total_non_current_liabilities"] is None
            and values["total_equity"] is not None
        ):
            values["total_non_current_liabilities"] = (
                capital_employed - values["total_equity"]
            )
            derived.append("total_non_current_liabilities")

        if (
            values["total_asset"] is None
            and values["total_current_liabilities"] is not None
        ):
            values["total_asset"] = (
                capital_employed + values["total_current_liabilities"]
            )
            derived.append("total_asset")

    # The model only reports printed totals, these are derived from printed
    # lines with arithmetic, which is safer done here than by the model
    if values["total_asset"] is None:
        # Liabilities + equity first: current + non-current can miss an
        # "Assets held for sale" line printed outside both sections
        values["total_asset"] = add_values(
            values["total_liabilities"], values["total_equity"]
        )

        if values["total_asset"] is None:
            values["total_asset"] = add_values(
                values["total_current_asset"], values["total_non_current_asset"]
            )

        if values["total_asset"] is not None:
            derived.append("total_asset")

    if values["total_liabilities"] is None:
        values["total_liabilities"] = add_values(
            values["total_current_liabilities"], values["total_non_current_liabilities"]
        )

        if values["total_liabilities"] is None and None not in (
            values["total_asset"],
            values["total_equity"],
        ):
            values["total_liabilities"] = values["total_asset"] - values["total_equity"]

        if values["total_liabilities"] is not None:
            derived.append("total_liabilities")

    # A section without its own subtotal (no total current assets printed)
    # is the total less the other section. An "Assets held for sale" line
    # printed outside both sections lands in the current side, where it belongs
    for part_name, other_part_name, total_name in BALANCE_SHEET_PARTS:
        if values[part_name] is None:
            values[part_name] = subtract_values(
                values[total_name], values[other_part_name]
            )

            if values[part_name] is not None:
                derived.append(part_name)

    if values["working_capital"] is None and None not in (
        values["total_current_asset"],
        values["total_current_liabilities"],
    ):
        values["working_capital"] = (
            values["total_current_asset"] - values["total_current_liabilities"]
        )
        derived.append("working_capital")

    # The model's explanation describes why the line was null, which no longer
    # matches the value once it is derived here
    for field_name in derived:
        statement_result["labels"][field_name] = None
        statement_result["explanations"][field_name] = (
            "Derived in code from the other balance sheet fields"
        )

    statement_result["derived"] = derived
    return statement_result


def derive_income_statement(statement_result: dict) -> dict:
    values = statement_result["values"]
    formulas = {}

    if values["gross_income"] is None:
        values["gross_income"] = subtract_values(
            values["total_revenue"], values["cost_of_revenue"]
        )
        formulas["gross_income"] = "total_revenue - cost_of_revenue"

    # A bank's operating profit is its printed after-allowances line, revenue
    # minus expenses would be the before-allowances figure (DBS 13,279 vs 12,634)
    if not statement_result.get("is_bank"):
        # The SGX annual table defines operating income this way, e.g. net
        # property income less fees. Revenue is the base only for statements
        # listed by nature, where there is no cost of sales line
        operating_base = values["gross_income"]

        if operating_base is None and values["cost_of_revenue"] is None:
            operating_base = values["total_revenue"]

        # Always calculated, even when a line is printed: a printed operating
        # profit is each company's own definition, which can include other
        # income and gains the annual table leaves out
        values["operating_income"] = subtract_values(
            operating_base, values["operating_expense"]
        )
        formulas["operating_income"] = (
            "gross_income (or total_revenue) - operating_expense"
        )

        # Finance costs are the interest here, a bank's is part of its income
        if values["ebit"] is None:
            values["ebit"] = add_values(
                values["pretax_income"], values["interest_expense_non_operating"]
            )
            formulas["ebit"] = "pretax_income + interest_expense_non_operating"

    # Everything between operating profit and pretax: other income, FX, fair
    # value changes, associates and exceptional items. Always calculated, a
    # printed "non-operating items" line is each company's own narrower one
    values["non_operating_income_or_loss"] = add_values(
        subtract_values(values["pretax_income"], values["operating_income"]),
        values["interest_expense_non_operating"] or 0,
    )
    formulas["non_operating_income_or_loss"] = (
        "pretax_income - operating_income + interest_expense_non_operating"
    )

    # With no "Attributable to:" split printed, there are no
    # minorities and the whole profit belongs to the owners
    if (
        values["unitholders"] is None
        and values["minorities"] in (None, 0)
        and values["perpetual_security_holders"] is None
        and values["net_income"] is not None
    ):
        values["unitholders"] = values["net_income"]
        formulas["unitholders"] = "net_income, no attributable split printed"

    derived = [field_name for field_name in formulas if values[field_name] is not None]

    for field_name in derived:
        statement_result["labels"][field_name] = None
        statement_result["explanations"][field_name] = (
            f"Derived in code as {formulas[field_name]}"
        )

    statement_result["derived"] = [*statement_result.get("derived", []), *derived]
    return statement_result


def derive_cash_flow(statement_result: dict) -> dict:
    values = statement_result["values"]
    explanations = statement_result["explanations"]
    derived = []

    # Capex is PPE plus intangibles and other capital lines (exploration, mine
    # properties), like the old pipeline. The model reads each line separately
    # so it never has to add them itself
    for field_name in CODE_ONLY_FIELDS:
        value = values.pop(field_name, None)
        explanation = explanations.pop(field_name, None)
        statement_result["labels"].pop(field_name, None)

        if value is None:
            continue

        values["capital_expenditure"] = (values["capital_expenditure"] or 0) + value

        explanations["capital_expenditure"] = (
            f"{explanations.get('capital_expenditure') or 'No PPE line'}; "
            f"plus {field_name}: {explanation}"
        )

        if "capital_expenditure" not in derived:
            derived.append("capital_expenditure")

    if exclude_exchange_effect(values):
        explanations["net_cash_flow"] = (
            f"{explanations.get('net_cash_flow')}; exchange rate effect taken out, "
            "net change is operating + investing + financing"
        )
        derived.append("net_cash_flow")

    statement_result["derived"] = derived
    return statement_result


def exclude_exchange_effect(values: dict) -> bool:
    # Most statements print the net change before the exchange rate line, a
    # few put the line above it, sometimes only in one period. The full
    # year minus H1 is then off by H1's exchange effect, so the net change is
    # always kept as operating + investing + financing
    activities = [
        values.get("operating_cash_flow"),
        values.get("investing_cash_flow"),
        values.get("financing_cash_flow"),
    ]
    net_change = values.get("net_cash_flow")
    exchange_effect = values.get("effect_of_exchange_rate_changes")

    if None in activities or net_change is None or not exchange_effect:
        return False

    activities_total = sum(activities)
    tolerance = max(abs(value) for value in activities) * NET_CASH_TOLERANCE
    gap_without = abs(net_change - activities_total)
    gap_with = abs(net_change - (activities_total + exchange_effect))

    # The closer reading wins: DBS prints no net change line, and its H1 one
    # differs from the activities by only the 7m exchange line
    if gap_with > tolerance or gap_with >= gap_without:
        return False

    values["net_cash_flow"] = net_change - exchange_effect
    return True
