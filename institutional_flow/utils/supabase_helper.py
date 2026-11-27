from periodic_financial.utils.supabase_helper import get_supabase_client


SECTOR_TABLE = "sgx_institutional_flow_sector"
SECTOR_CONFLICT_KEY = "date,investor_type,sector"

STOCK_TABLE = "sgx_institutional_flow"
STOCK_CONFLICT_KEY = "symbol,date,investor_type"

UPSERT_BATCH_SIZE = 500


def upsert_rows(table: str, rows: list[dict], conflict_key: str) -> None:
    client = get_supabase_client()

    for start in range(0, len(rows), UPSERT_BATCH_SIZE):
        client.table(table).upsert(
            rows[start : start + UPSERT_BATCH_SIZE], on_conflict=conflict_key
        ).execute()
