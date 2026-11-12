from ..utils.statement_values import STATEMENT_KEYS, get_values

from sgx_periodic_financial import financial_guards

import re


# The rules a statement must satisfy, in plain words for the recheck prompt
IDENTITY_RULES = {
    "balance_sheet_balances": "total assets = total liabilities + total equity",
    "assets_split": "total assets = current assets + non-current assets",
    "liabilities_split": "total liabilities = current liabilities + non-current liabilities",
    "working_capital": "working capital = current assets - current liabilities",
    "tax_bridge": "profit before tax - income tax = profit after tax",
    "gross_bridge": "revenue - cost of revenue = gross profit",
    "cash_flow_sums": "operating + investing + financing cash flow, with the exchange rate effect, = net change in cash",
}

# Figures printed in millions carry three or four digits, two statements
# can share one by chance
COARSE_UNITS = {"millions", "billions"}


def flip_exchange_effect(cash_flow_values: dict) -> dict:
    # The guards accept a net change of operating + investing + financing - FX,
    # while statements print the FX line to be added, e.g. WJP and STG
    exchange_effect = cash_flow_values.get("effect_of_exchange_rate_changes")

    if exchange_effect is not None:
        cash_flow_values["effect_of_exchange_rate_changes"] = -exchange_effect

    return cash_flow_values


def find_identity_breaks(statement_key: str, statement_result: dict) -> list[dict]:
    # One statement as read, before any H2 subtraction or conversion, so a break
    # can only be a misread on these pages, not a stored H1 or a rate
    metrics = {key: {} for key in STATEMENT_KEYS}
    metrics[statement_key] = dict(statement_result["values"])

    if statement_key == "cash_flow":
        flip_exchange_effect(metrics["cash_flow"])

    # Impossibilities are left to apply_checks, their size limits assume SGD
    return [
        issue
        for issue in financial_guards.check(metrics)
        if issue["severity"] == "identity"
    ]


def is_coarse(result: dict) -> bool:
    return any(
        result[statement_key].get("unit") in COARSE_UNITS
        for statement_key in STATEMENT_KEYS
    )


def keep_summed_totals(
    result: dict,
    metrics: dict,
    cleaned_metrics: dict,
    issues: list[dict],
) -> list[dict]:
    # A copied figure is one printed line. A total code added up from several
    # lines matching another statement is chance, more so for a statement in
    # millions
    summed_fields = {
        field_name
        for statement_key in STATEMENT_KEYS
        for field_name in result[statement_key].get("summed_fields") or []
    }

    for issue in issues:
        if issue["check"] == "duplicate_value_across_statements" and (
            summed_fields & set(issue["fields"]) or is_coarse(result)
        ):
            issue["severity"] = "review"

        # A property company's profit includes revaluation gains and can pass
        # its revenue, so revenue is kept and only flagged
        if issue["check"] == "earnings_exceed_revenue":
            issue["severity"] = "review"

    still_removed = {
        field_name
        for issue in issues
        if issue["severity"] == "impossible"
        for field_name in issue["fields"]
    }

    for statement_key in STATEMENT_KEYS:
        for field_name, value in metrics[statement_key].items():
            if field_name not in still_removed:
                cleaned_metrics[statement_key][field_name] = value

    return issues


def strip_label_note(label: str | None) -> str | None:
    return re.sub(r"\s*\([^)]*\)", "", label).strip() if label else label


def apply_checks(result: dict) -> dict:
    # The old pipeline's accounting guards: a value a rule proves impossible is
    # nulled, an identity break only says some component is wrong, so it is
    # reported without touching the figures
    metrics = get_values(result)
    flip_exchange_effect(metrics["cash_flow"])

    # Only the printed label is checked, not the model's note in brackets on a
    # bare subtotal: SEG's "Subtotal of Non-current liabilities (bare line
    # after Financial guarantee contract liabilities)" is not a guarantee line
    provenance = {
        statement_key: {
            field_name: {
                "value": value,
                "source_label": strip_label_note(
                    result[statement_key]["labels"].get(field_name)
                ),
            }
            for field_name, value in result[statement_key]["values"].items()
        }
        for statement_key in STATEMENT_KEYS
    }

    cleaned_metrics, issues = financial_guards.apply(metrics, provenance)
    issues = keep_summed_totals(result, metrics, cleaned_metrics, issues)

    for statement_key in STATEMENT_KEYS:
        statement_result = result[statement_key]

        for field_name, value in statement_result["values"].items():
            if value is None or cleaned_metrics[statement_key][field_name] is not None:
                continue

            # The model's own reason is kept, it shows why the line was read
            statement_result["values"][field_name] = None
            statement_result["explanations"][field_name] = (
                f"Removed by the accounting checks, extracted {value:,.0f}. "
                f"Model: {statement_result['explanations'].get(field_name)}"
            )

    result["checks"] = issues
    return result


def summarise_checks(issues: list[dict]) -> str:
    return financial_guards.summarise(issues)
