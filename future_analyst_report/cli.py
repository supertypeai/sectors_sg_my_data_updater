from collections import Counter

from .external_analyst_report import SOURCES
from .pipeline.extract_data import process_documents
from .pipeline.parse_pdf import list_documents
from .pipeline.publish import drop_stored_documents, prepare_table_rows, write_rows
from .utils.log_setup import setup_logging, stage
from .utils.run_output import report_window, DEFAULT_DAYS
from .utils.run_report import finish_run, RunReport
from .utils.supabase_helper import get_target_companies, DEFAULT_TOP

import asyncio
import logging
import typer


LOGGER = logging.getLogger(__name__)

app = typer.Typer(help="Analyst report pipeline")


@app.callback()
def main() -> None:
    pass


@app.command(name="run_analyst_report")
def run_analyst_report(
    days: int = typer.Option(
        DEFAULT_DAYS, help="Days back from today to scan, when --start is not given"
    ),
    start: str = typer.Option(None, help="First report day, YYYY-MM-DD"),
    end: str = typer.Option(None, help="Last report day, YYYY-MM-DD, default today"),
    top: int = typer.Option(DEFAULT_TOP, help="Issuers by market cap, plus every REIT"),
    symbols: list[str] = typer.Option(
        None, help="Only these symbols, e.g. --symbols D05.SI --symbols C38U.SI"
    ),
    dry_run: bool = typer.Option(False, help="Extract but do not write to Supabase"),
    refresh: bool = typer.Option(False, help="Read reports already in the table again"),
):
    warnings = setup_logging()

    start_date, end_date = report_window(days, start, end)
    companies = get_target_companies(top, symbols)
    
    report = RunReport(
        title="SGX analyst reports",
        window=f"{start_date} to {end_date}",
        companies=len(companies),
        dry_run=dry_run,
    )

    try:
        with stage(1, 4, "LIST", LOGGER):
            documents = list_documents(
                [company["symbol"] for company in companies], start_date, end_date
            )
            report.documents = len(documents)
            LOGGER.info(
                "[List] %d documents on SGX for %d companies", len(documents), len(companies)
            )
            documents = drop_stored_documents(documents, refresh)
            report.already_stored = report.documents - len(documents)

        with stage(2, 4, "EXTRACT", LOGGER):
            report.results = asyncio.run(
                process_documents(
                    documents,
                    {company["symbol"]: company["name"] for company in companies},
                )
            )

        with stage(3, 4, "CLEAN", LOGGER):
            report.rows, report.stored_keys = prepare_table_rows(report.results)

        with stage(4, 4, "UPSERT", LOGGER):
            write_rows(report.rows, dry_run)

    finally:
        report.warnings = warnings.messages
        finish_run(report, "sgx")


@app.command(name="run_external_analyst_report")
def run_external_analyst_report(
    source: str = typer.Option("all", help=f"One of {', '.join(SOURCES)}, or all"),
    days: int = typer.Option(
        DEFAULT_DAYS, help="Days back from today to scan, when --start is not given"
    ),
    start: str = typer.Option(None, help="First report day, YYYY-MM-DD"),
    end: str = typer.Option(None, help="Last report day, YYYY-MM-DD, default today"),
    top: int = typer.Option(DEFAULT_TOP, help="Issuers by market cap, plus every REIT"),
    symbols: list[str] = typer.Option(
        None, help="Only these symbols, e.g. --symbols D05.SI --symbols C38U.SI"
    ),
    dry_run: bool = typer.Option(False, help="Extract but do not write to Supabase"),
    refresh: bool = typer.Option(False, help="Read reports already in the table again"),
):
    if source != "all" and source not in SOURCES:
        raise typer.BadParameter(f"source must be one of {', '.join(SOURCES)}")

    warnings = setup_logging()

    start_date, end_date = report_window(days, start, end)
    companies = get_target_companies(top, symbols)

    report = RunReport(
        title="External analyst reports",
        window=f"{start_date} to {end_date}",
        companies=len(companies),
        dry_run=dry_run,
    )

    documents_by_source = {}

    try:
        with stage(1, 4, "LIST", LOGGER):
            for source_name, list_source_documents in SOURCES.items():
                if source not in ("all", source_name):
                    continue

                try:
                    documents_by_source[source_name] = list_source_documents(
                        companies, start_date, end_date
                    )

                except Exception:
                    # A site that changed its layout never stops the other sites
                    LOGGER.exception("[List %s] listing failed, skipped", source_name)
                    report.sources.append({"name": source_name, "listing_failed": True})

            report.documents = sum(
                len(documents) for documents in documents_by_source.values()
            )
            documents_by_source = {
                source_name: drop_stored_documents(documents, refresh)
                for source_name, documents in documents_by_source.items()
            }
            report.already_stored = report.documents - sum(
                len(documents) for documents in documents_by_source.values()
            )

        with stage(2, 4, "EXTRACT", LOGGER):
            for source_name, documents in documents_by_source.items():
                results = asyncio.run(
                    process_documents(
                        documents,
                        {company["symbol"]: company["name"] for company in companies},
                    )
                )

                report.results += results
                report.sources.append(
                    {
                        "name": source_name,
                        "listing_failed": False,
                        "documents": len(documents),
                        "statuses": Counter(result["status"] for result in results),
                        "cost": sum(
                            (result.get("llm_usage") or {}).get("cost", 0)
                            for result in results
                        ),
                    }
                )

        with stage(3, 4, "CLEAN", LOGGER):
            report.rows, report.stored_keys = prepare_table_rows(report.results)

        with stage(4, 4, "UPSERT", LOGGER):
            write_rows(report.rows, dry_run)

    finally:
        report.warnings = warnings.messages
        finish_run(report, "external")


if __name__ == "__main__":
    app()
