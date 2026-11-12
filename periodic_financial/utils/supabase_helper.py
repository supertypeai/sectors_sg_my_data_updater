from supabase import Client, create_client

from ..config.settings import SUPABASE_URL, SUPABASE_KEY

from functools import lru_cache

import logging
import json


LOGGER = logging.getLogger(__name__)

PERIODIC_TABLE = "sgx_periodic_financial"
PERIODIC_CONFLICT_KEY = "symbol,date"


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def select_one(table: str, columns: str, filters: dict) -> dict | None:
    query = get_supabase_client().table(table).select(columns)

    for column, value in filters.items():
        query = query.eq(column, value)

    rows = query.limit(1).execute().data
    return rows[0] if rows else None


def parse_metrics(metrics) -> dict:
    if isinstance(metrics, str):
        return json.loads(metrics)

    return metrics or {}


def get_annual_statements(symbol: str, date: str) -> dict | None:
    row = select_one(
        "sgx_financials_annual",
        "income_stmt_metrics,cash_flow_metrics",
        {"symbol": symbol, "date": date},
    )

    if row is None:
        return None

    return {
        "income_statement": parse_metrics(row["income_stmt_metrics"]),
        "cash_flow": parse_metrics(row["cash_flow_metrics"]),
    }


def get_top_symbols(limit: int) -> set[str]:
    rows = (
        get_supabase_client()
        .table("sgx_company_report")
        .select("symbol")
        .not_.is_("market_cap", "null")
        .order("market_cap", desc=True)
        .limit(limit)
        .execute()
        .data
    )

    return {row["symbol"] for row in rows}


def get_periodic_statements(symbol: str, date: str) -> dict | None:
    return select_one(
        PERIODIC_TABLE,
        "income_statement,cash_flow",
        {"symbol": symbol, "date": date},
    )


def is_stored(symbol: str, date: str) -> bool:
    return (
        select_one(PERIODIC_TABLE, "symbol", {"symbol": symbol, "date": date})
        is not None
    )


def upsert_row(row: dict) -> None:
    get_supabase_client().table(PERIODIC_TABLE).upsert(
        row, on_conflict=PERIODIC_CONFLICT_KEY
    ).execute()
    LOGGER.info(
        "[Upsert][%s %s] written to %s", row["symbol"], row["date"], PERIODIC_TABLE
    )
