from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .financial_announcement.api import BlockedError
from .financial_announcement.fetch_documents import (
    find_filings,
    fetch_document,
    DEFAULT_TOP,
    DEFAULT_DAYS,
)
from .pipeline.parse_pdf import parse_document
from .pipeline.page_finder import find_pages
from .pipeline.extract_data import extract_filing
from .pipeline.read_ebitda import add_ebitda
from .llm.usage import summarise_usage, RUN_RECORDS
from .utils.currency import load_rates
from .utils.run_output import save_run_output
from .utils.supabase_helper import (
    get_top_symbols,
    is_stored,
    upsert_row,
    PERIODIC_TABLE,
)
from .utils.table_row import build_row

import asyncio
import logging
import requests
import sys
import typer


LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)

app = typer.Typer(help="Periodic financial pipeline")


@app.callback()
def main() -> None:
    pass


@app.command(name="run_periodic_financial")
def run_periodic_financial_pipeline(
    days: int = typer.Option(
        DEFAULT_DAYS, help="Days back from today to scan, when --start is not given"
    ),
    start: str = typer.Option(None, help="First announcement day, YYYY-MM-DD"),
    end: str = typer.Option(
        None, help="Last announcement day, YYYY-MM-DD, default today"
    ),
    top: int = typer.Option(DEFAULT_TOP, help="Issuers by market cap to include"),
    symbols: list[str] = typer.Option(
        None, help="Only these symbols, e.g. --symbols D05.SI --symbols O39.SI"
    ),
    overwrite: bool = typer.Option(
        False, help="Also redo filings already in the table"
    ),
    dry_run: bool = typer.Option(False, help="Extract but do not write to the table"),
):
    # SGX dates its announcements in sg time
    now = datetime.now(timezone(timedelta(hours=8)))
    end_date = date.fromisoformat(end) if end else now.date()
    start_date = date.fromisoformat(start) if start else end_date - timedelta(days=days)

    # Every filing's full result and the rows, one folder per run
    run_dir = (
        Path("periodic_financial/data/live") / f"{start_date}_{end_date}_{now:%H%M%S}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    # The run's log sits next to its results, same format as the console
    log_file = logging.FileHandler(run_dir / "run.log", encoding="utf-8")
    log_file.setFormatter(logging.getLogger().handlers[0].formatter)
    logging.getLogger().addHandler(log_file)

    top_symbols = set(symbols) if symbols else get_top_symbols(top)
    LOGGER.info(
        "[Announcements] fetching %s to %s for %d issuers",
        start_date,
        end_date,
        len(top_symbols),
    )

    try:
        filings = find_filings(start_date, end_date, top_symbols)

    except BlockedError as error:
        LOGGER.error("[Error] blocked by links.sgx.com, stopping early: %s", error)
        raise typer.Exit(code=1)

    LOGGER.info("[Announcements] %d filings found", len(filings))

    rates = load_rates()
    results, readings, rows = {}, {}, []
    skipped, already_stored = [], 0

    # Oldest first, so a company's H1 is processed before the H2 that subtracts it
    pending = sorted(filings.items(), key=lambda item: (item[0][1], item[0][0]))

    for index, ((symbol, period_end), versions) in enumerate(pending, start=1):
        LOGGER.info(
            "Processing symbol: %s %s | %d/%d", symbol, period_end, index, len(pending)
        )

        if not overwrite and is_stored(symbol, period_end):
            LOGGER.info(
                "[Fetch][%s %s] already in %s, skipped",
                symbol,
                period_end,
                PERIODIC_TABLE,
            )
            already_stored += 1
            continue

        try:
            filing = fetch_document(symbol, period_end, versions)

            if filing is None:
                skipped.append(
                    {"symbol": symbol, "date": period_end, "reason": "no_document"}
                )
                continue

            parser_result, pdf_bytes = parse_document(filing)
            record = find_pages(parser_result)

            if record is None:
                skipped.append(
                    {"symbol": symbol, "date": period_end, "reason": "no_statements"}
                )
                continue

            result = asyncio.run(
                extract_filing(record, parser_result, pdf_bytes, rates, results)
            )
            asyncio.run(add_ebitda(result, parser_result, results, readings))

            row = build_row(result, filing["source_url"])

            if dry_run:
                LOGGER.info(
                    "[Upsert][%s %s] dry run, not written", row["symbol"], row["date"]
                )

            else:
                upsert_row(row)

            rows.append(row)

        except BlockedError as error:
            LOGGER.error("[Error] blocked by links.sgx.com, stopping early: %s", error)
            skipped.append({"symbol": symbol, "date": period_end, "reason": "blocked"})
            break

        except requests.RequestException as error:
            LOGGER.warning(
                "[Error][%s %s] download failed, skipped: %s", symbol, period_end, error
            )
            skipped.append(
                {
                    "symbol": symbol,
                    "date": period_end,
                    "reason": "download_failed",
                    "error": str(error),
                }
            )
            continue

        except Exception as error:
            # One bad filing never stops the others, the next run retries it
            LOGGER.exception("[Error][%s %s] failed, skipped", symbol, period_end)
            skipped.append(
                {
                    "symbol": symbol,
                    "date": period_end,
                    "reason": "error",
                    "error": repr(error),
                }
            )
            continue

        finally:
            save_run_output(run_dir, results, rows, skipped, already_stored)

    save_run_output(run_dir, results, rows, skipped, already_stored)

    LOGGER.info(
        "[Summary] %d filings, %d rows %s, LLM usage %s",
        len(pending),
        len(rows),
        "extracted (dry run)" if dry_run else f"written to {PERIODIC_TABLE}",
        summarise_usage(RUN_RECORDS),
    )


if __name__ == "__main__":
    app()
