from collections import Counter
from dataclasses import dataclass, field

from .run_output import write_json, RUN_DIR

import logging
import os


LOGGER = logging.getLogger(__name__)

# Statuses that need no look, the rest are failures
READ_STATUSES = ("parsed", "not_weekly")

PROBLEMS = {
    "download_failed": "download failed",
    "layout_changed": "workbook layout changed",
    "error": "parse error",
}

# The card stays readable on a wide --days run, the full lists are in the run artifact
MAX_CARD_ROWS = 60


@dataclass
class RunReport:
    window: str
    dry_run: bool
    results: list[dict] = field(default_factory=list)
    sector_rows: list[dict] = field(default_factory=list)
    stock_rows: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def statuses(self) -> Counter:
        return Counter(result["status"] for result in self.results)

    @property
    def failed(self) -> list[dict]:
        return [result for result in self.results if result["status"] not in READ_STATUSES]

    @property
    def weeks(self) -> list[str]:
        return sorted({row["date"] for row in self.sector_rows})


def millions(value: int | None) -> str:
    return "-" if value is None else f"{value / 1_000_000:+,.1f}"


def log_run_summary(report: RunReport, logger: logging.Logger = LOGGER) -> None:
    statuses = report.statuses
    weeks = report.weeks

    logger.info(
        "RUN SUMMARY  %s\n"
        "    reports %d | parsed %d | not weekly %d | failed %d\n"
        "    weeks %s | sector rows %d | stock rows %d | %s\n"
        "    warnings %d",
        report.window,
        len(report.results),
        statuses["parsed"],
        statuses["not_weekly"],
        len(report.failed),
        f"{weeks[0]} to {weeks[-1]}" if weeks else "none",
        len(report.sector_rows),
        len(report.stock_rows),
        "dry run, nothing written" if report.dry_run else "written",
        len(report.warnings),
    )


def overall_lines(report: RunReport) -> list[str]:
    overall = {
        (row["date"], row["investor_type"]): row["value"]
        for row in report.sector_rows
        if row["sector"] == "Overall"
    }

    lines = [
        "",
        "### Net flow by week (S$M)",
        "",
        "| Week of | Institutional | Retail |",
        "|---|---|---|",
    ]

    for week in list(reversed(report.weeks))[:MAX_CARD_ROWS]:
        lines.append(
            f"| {week} | {millions(overall.get((week, 'institutional')))} "
            f"| {millions(overall.get((week, 'retail')))} |"
        )

    return lines


def write_job_summary(report: RunReport) -> None:
    """
    Markdown shown at the top of the GitHub run page, no log reading needed
    """
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")

    if not summary_path:
        return

    statuses = report.statuses
    lines = [
        "## SGX institutional and retail fund flow",
        "",
        f"**{report.window}**" + (" · **dry run, nothing written**" if report.dry_run else ""),
        "",
        "| Reports | Parsed | Not weekly | Failed | Weeks | Sector rows | Stock rows |",
        "|---|---|---|---|---|---|---|",
        f"| {len(report.results)} | {statuses['parsed']} | {statuses['not_weekly']} "
        f"| {len(report.failed)} | {len(report.weeks)} | {len(report.sector_rows)} "
        f"| {len(report.stock_rows)} |",
    ]

    if report.sector_rows:
        lines += overall_lines(report)

    if report.failed or report.warnings:
        lines += ["", "### Needs a look", ""]

    if report.failed:
        lines += ["| Report | Published | Problem |", "|---|---|---|"]

        for result in report.failed[:MAX_CARD_ROWS]:
            lines.append(
                f"| [{result['title']}]({result['document_url']}) | {result['report_date']} "
                f"| {PROBLEMS.get(result['status'], result['status'])} |"
            )

        lines.append("")

    lines += [f"- {message}" for message in report.warnings[:MAX_CARD_ROWS]]

    try:
        with open(summary_path, "a") as file:
            file.write("\n".join(lines) + "\n")

    except OSError as error:
        LOGGER.warning("could not write the GitHub job summary: %s", error)


def finish_run(report: RunReport) -> None:
    """
    Saves the run's results and rows for the artifact, then writes the summary
    to the log and the run page
    """
    write_json(
        RUN_DIR / "results.json",
        [
            {key: value for key, value in result.items() if key not in ("sector_rows", "stock_rows")}
            for result in report.results
        ],
    )
    write_json(RUN_DIR / "sector_rows.json", report.sector_rows)
    write_json(RUN_DIR / "stock_rows.json", report.stock_rows)
    log_run_summary(report)
    write_job_summary(report)
