"""One-off backfill: realign existing sgx_companies S-REIT rows to the REITAS
sub-sector taxonomy defined in utils.constant.SG_REIT_OVERRIDES.

The daily scraper only classifies rows at insert time, so editing the mapping
alone leaves existing rows untouched. Run this once after the mapping change:

    python reit_subsector_backfill.py --dryrun
    python reit_subsector_backfill.py
"""

from new_sector_scraper_sgx import chunked_list, init_supabase

from utils.constant import *

# SG_REIT_OVERRIDES is keyed by bare code; sgx_companies stores the suffixed
# form, so queries and updates are re-suffixed at the DB boundary.
SYMBOL_SUFFIX = ".SI"


def stored_symbols(symbols) -> list:
    return [s if str(s).endswith(SYMBOL_SUFFIX) else f"{s}{SYMBOL_SUFFIX}" for s in symbols]


def bare_symbol(symbol) -> str:
    symbol = str(symbol)
    return symbol[: -len(SYMBOL_SUFFIX)] if symbol.endswith(SYMBOL_SUFFIX) else symbol


import sys
import traceback
from collections import defaultdict


stats = {
    'overrides_defined': len(SG_REIT_OVERRIDES),
    'found_in_db': 0,
    'missing_in_db': 0,
    'already_correct': 0,
    'to_update': 0,
    'updated': 0
}

global_errors = []


def fetch_reit_state(supabase) -> dict:
    """Current sector/sub_sector for every symbol we have an override for."""
    print("[STEP] Fetch DB State: Grab current classification for overridden S-REIT symbols.")
    current = {}
    symbols = list(SG_REIT_OVERRIDES)

    try:
        for chunk in chunked_list(symbols, 100):
            response = supabase.table("sgx_companies").select("symbol, sector, sub_sector").in_("symbol", stored_symbols(chunk)).execute()
            for r in response.data:
                current[bare_symbol(r['symbol'])] = (r.get('sector'), r.get('sub_sector'))

        stats['found_in_db'] = len(current)
        stats['missing_in_db'] = len(symbols) - len(current)
        print(f"       -> Matched {stats['found_in_db']} of {len(symbols)} overridden symbols.")
    except Exception as e:
        global_errors.append(f"DB Fetch Error: {str(e)}\n{traceback.format_exc()}")

    return current


def build_updates(current: dict) -> dict:
    """Group symbols needing a change by their target sub_sector."""
    print("\n[STEP] Compare: Diff current classification against REITAS overrides.")
    updates = defaultdict(list)

    for symbol, target_sub_sector in SG_REIT_OVERRIDES.items():
        if symbol not in current:
            print(f"  [SKIP] not in DB: symbol:{symbol}")
            continue

        sector, sub_sector = current[symbol]
        if (sector, sub_sector) == ("REIT", target_sub_sector):
            stats['already_correct'] += 1
            continue

        print(f"  [DIFF] symbol:{symbol} {sector} / {sub_sector} -> REIT / {target_sub_sector}")
        updates[target_sub_sector].append(symbol)

    stats['to_update'] = sum(len(v) for v in updates.values())
    return updates


def main():
    dry_run = "--dryrun" in sys.argv
    if dry_run:
        print("\n" + "=" * 50)
        print("!! RUNNING IN DRY-RUN MODE !!")
        print("No changes will be executed on the DB.")
        print("=" * 50 + "\n")

    try:
        supabase = init_supabase()

        current = fetch_reit_state(supabase)

        # Stop before writing if the read failed, so we never blank-slate the table.
        if global_errors:
            raise Exception("Critical errors occurred while fetching DB state. Execution halted.")

        updates = build_updates(current)

        print("\n" + "=" * 50)
        print("[PREVIEW] SUB_SECTOR CHANGES TO BE APPLIED:")
        print("=" * 50)

        if updates:
            for target_sub_sector, symbols in updates.items():
                print(f"\n---> {target_sub_sector} ({len(symbols)} symbols):")
                print(f"     {symbols}")
        else:
            print("\n---> No changes required.")

        print("=" * 50 + "\n")

        print("[STEP] Batch Execution: Push batched updates to Supabase.")

        if dry_run:
            print("  [DRY RUN] Execution skipped.")
            stats['updated'] = stats['to_update']

        else:
            for target_sub_sector, symbols in updates.items():
                payload = {"sector": "REIT", "sub_sector": target_sub_sector}
                for chunk in chunked_list(symbols, 100):
                    try:
                        supabase.table("sgx_companies").update(payload).in_("symbol", stored_symbols(chunk)).execute()
                        stats['updated'] += len(chunk)
                    except Exception as e:
                        global_errors.append(f"Backfill DB Error ({target_sub_sector}): {str(e)}")

    except Exception as e:
        global_errors.append(f"Main Process Crash: {str(e)}\n{traceback.format_exc()}")

    print("\n-- Backfill Summary --")
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")

    if global_errors:
        print("\n" + "!" * 50)
        print("THE FOLLOWING ERRORS OCCURRED DURING EXECUTION:")
        print("!" * 50)
        for err in global_errors:
            print(f"- {err}\n")
        sys.exit("Script completed, but with errors (see above).")


if __name__ == "__main__":
    main()
