from periodic_financial.utils.supabase_helper import get_supabase_client


COMPANY_COLUMNS = "symbol,name,sector,sub_sector,market_cap"

DEFAULT_TOP = 100

REPORT_TABLE = "sgx_analyst_report"
REPORT_CONFLICT_KEY = "symbol,contributor,date"
REPORT_COLUMNS = (
    "symbol,contributor,date,analyst,title,source,rating,target_price,"
    "previous_target_price,target_basis,forecast"
)

# Supabase returns at most 1,000 rows a request and a stock has a few dozen
SYMBOLS_PER_QUERY = 20
UPSERT_BATCH_SIZE = 200


def get_target_companies(top: int, symbols: list[str] | None = None) -> list[dict]:
    client = get_supabase_client()

    top_companies = (
        client.table("sgx_company_report")
        .select(COMPANY_COLUMNS)
        .not_.is_("market_cap", "null")
        .order("market_cap", desc=True)
        .limit(top)
        .execute()
        .data
    )

    reits = (
        client.table("sgx_company_report")
        .select(COMPANY_COLUMNS)
        .eq("sector", "REIT")
        .execute()
        .data
    )

    top_symbols = {company["symbol"] for company in top_companies}
    companies = {company["symbol"]: company for company in top_companies + reits}

    return sorted(
        (
            {
                **company,
                "in_top": company["symbol"] in top_symbols,
                "is_reit": company["sector"] == "REIT",
            }
            for company in companies.values()
            if not symbols or company["symbol"] in symbols
        ),
        key=lambda company: company["market_cap"] or 0,
        reverse=True,
    )


def get_stored_reports(symbols: list[str]) -> list[dict]:
    client = get_supabase_client()
    rows = []

    for start in range(0, len(symbols), SYMBOLS_PER_QUERY):
        rows += (
            client.table(REPORT_TABLE)
            .select(REPORT_COLUMNS)
            .in_("symbol", symbols[start : start + SYMBOLS_PER_QUERY])
            .execute()
            .data
        )

    return rows


def upsert_reports(rows: list[dict]) -> None:
    client = get_supabase_client()

    for start in range(0, len(rows), UPSERT_BATCH_SIZE):
        client.table(REPORT_TABLE).upsert(
            rows[start : start + UPSERT_BATCH_SIZE], on_conflict=REPORT_CONFLICT_KEY
        ).execute()
