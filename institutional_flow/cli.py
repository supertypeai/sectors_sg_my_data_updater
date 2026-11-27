from future_analyst_report.utils.log_setup import setup_logging, stage

from .fund_flow_report.api import create_session, iter_reports
from .pipeline.extract_data import process_reports
from .pipeline.publish import write_rows
from .pipeline.table_rows import build_table_rows
from .utils.run_output import report_window, DEFAULT_DAYS
from .utils.run_report import finish_run, RunReport

import logging
import typer


LOGGER = logging.getLogger(__name__)

app = typer.Typer(help="SGX institutional and retail fund flow pipeline")


@app.callback()
def main() -> None:
    pass


@app.command(name="run_fund_flow")
def run_fund_flow(
    days: int = typer.Option(DEFAULT_DAYS, help="Days back from today to scan"),
    dry_run: bool = typer.Option(False, help="Parse but do not write to Supabase"),
):
    warnings = setup_logging()

    start_date, end_date = report_window(days)

    report = RunReport(window=f"{start_date} to {end_date}", dry_run=dry_run)
    session = create_session()

    try:
        with stage(1, 4, "LIST", LOGGER):
            reports = list(iter_reports(session, start_date, end_date))
            LOGGER.info("[List] %d Fund Flow Tracker reports on SGX", len(reports))

        with stage(2, 4, "PARSE", LOGGER):
            report.results = process_reports(session, reports)

        with stage(3, 4, "CLEAN", LOGGER):
            report.sector_rows, report.stock_rows = build_table_rows(report.results)
            LOGGER.info(
                "[Clean] %d sector rows, %d stock rows over %d weeks",
                len(report.sector_rows),
                len(report.stock_rows),
                len(report.weeks),
            )

        with stage(4, 4, "UPSERT", LOGGER):
            write_rows(report.sector_rows, report.stock_rows, dry_run)

    finally:
        report.warnings = warnings.messages
        finish_run(report)


if __name__ == "__main__":
    app()


# python -m institutional_flow.cli run_fund_flow --dry-run
