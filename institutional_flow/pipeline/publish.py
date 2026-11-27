from ..utils.supabase_helper import (
    upsert_rows,
    SECTOR_CONFLICT_KEY,
    SECTOR_TABLE,
    STOCK_CONFLICT_KEY,
    STOCK_TABLE,
)

import logging


LOGGER = logging.getLogger(__name__)


def write_rows(sector_rows: list[dict], stock_rows: list[dict], dry_run: bool) -> None:
    for table, rows, conflict_key in (
        (SECTOR_TABLE, sector_rows, SECTOR_CONFLICT_KEY),
        (STOCK_TABLE, stock_rows, STOCK_CONFLICT_KEY),
    ):
        if not rows:
            LOGGER.info("[Upsert] nothing to write to %s", table)

        elif dry_run:
            LOGGER.info("[Upsert] dry run, %d rows not written to %s", len(rows), table)

        else:
            # Upserted on the natural key, a rerun or a revised week overwrites
            upsert_rows(table, rows, conflict_key)
            LOGGER.info("[Upsert] %d rows written to %s", len(rows), table)
