from ..pipeline.page_finder import STATEMENT_KEYS


# Read to correct the cash flow totals, sgx_periodic_financial has no column for it
NOT_STORED_FIELDS = ["effect_of_exchange_rate_changes"]


def get_financial_year(period_end: str, period: str) -> int:
    # Same label as the old pipeline: a year closing in January to June is
    # mostly the previous calendar year (Singtel's 31 Mar 2026 is FY2025),
    # and an H1 is labelled by the year end six months later
    year, month = int(period_end[:4]), int(period_end[5:7])

    if period == "H1":
        month += 6

        if month > 12:
            month -= 12
            year += 1

    return year - 1 if month <= 6 else year


def build_row(result: dict, source_url: str) -> dict:
    # One filing in the sgx_periodic_financial shape
    statements = {
        key: {
            field_name: value
            for field_name, value in (result[key].get("values") or {}).items()
            if field_name not in NOT_STORED_FIELDS
        }
        for key in STATEMENT_KEYS
    }

    return {
        "symbol": result["symbol"],
        "date": result["date"],
        **statements,
        "period": result["period"],
        "financial_year": get_financial_year(result["date"], result["period"]),
        "source_url": source_url,
    }
