from ..utils.statement_values import STATEMENT_KEYS


def summarise_statement(statement_result: dict) -> dict:
    recheck = statement_result.get("recheck") or {}

    return {
        "status": statement_result["status"],
        "reason": statement_result["reason"],
        "nulls": [
            name for name, value in statement_result["values"].items() if value is None
        ],
        "recheck_used": recheck.get("used"),
        "recheck_first_breaks": [
            issue.get("check") for issue in recheck.get("first_breaks", [])
        ],
    }


def has_severity(result: dict, severity: str) -> bool:
    return any(issue["severity"] == severity for issue in result["checks"])


def build_run_checks(
    results: list[dict], skipped: list[dict], already_stored: int, cost: float
) -> dict:
    # One file to review a run: what needs a look, and what never got extracted
    return {
        "summary": {
            "extracted": len(results),
            "skipped": len(skipped),
            "already_stored": already_stored,
            "with_identity_issue": sum(
                has_severity(result, "identity") for result in results
            ),
            "with_review_flag": sum(
                has_severity(result, "review") for result in results
            ),
            # H1 + H2 differs from the SGX annual table, also counted as a review flag
            "with_annual_mismatch": sum(
                (result.get("second_half_check") or {}).get("annual") == "mismatch"
                for result in results
            ),
            "rechecks_used": sum(
                (result[key].get("recheck") or {}).get("used") == "recheck"
                for result in results
                for key in STATEMENT_KEYS
            ),
            "cost": round(cost, 4),
        },
        "skipped": skipped,
        "filings": [
            {
                "symbol": result["symbol"],
                "date": result["date"],
                "period": result["period"],
                "currency": result.get("currency"),
                **{key: summarise_statement(result[key]) for key in STATEMENT_KEYS},
                "checks": result["checks"],
                "second_half_check": result.get("second_half_check"),
                "cost": (result.get("llm_usage") or {}).get("cost"),
            }
            for result in results
        ],
    }
