from collections import Counter
from dataclasses import dataclass, field

from .run_output import write_json, RUN_DIR

import logging
import os


LOGGER = logging.getLogger(__name__)

# Statuses that mean the report was read, the rest are failures to look at
READ_STATUSES = ("extracted", "no_target")

PROBLEMS = {
    "download_failed": "download failed",
    "no_pdf": "no PDF at the link",
    "llm_failed": "every model failed",
    "no_fx_rate": "no SGD rate for the target's currency",
    "error": "extraction error",
}

# The card stays readable on a backfill, the full lists are in the run artifact
MAX_CARD_ROWS = 60


@dataclass
class RunReport:
    title: str
    window: str
    companies: int
    dry_run: bool
    documents: int = 0
    # Listed again inside the overlapping window but already in the table
    already_stored: int = 0
    results: list[dict] = field(default_factory=list)
    rows: list[dict] = field(default_factory=list)
    stored_keys: set[tuple] = field(default_factory=set)
    # One entry per external site: name, documents, statuses, cost, listing_failed
    sources: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def statuses(self) -> Counter:
        return Counter(result["status"] for result in self.results)

    @property
    def cost(self) -> float:
        return sum(
            (result.get("llm_usage") or {}).get("cost", 0) for result in self.results
        )

    @property
    def failed(self) -> list[dict]:
        return [
            result for result in self.results if result["status"] not in READ_STATUSES
        ]

    @property
    def updated(self) -> int:
        return sum(
            (row["symbol"], row["contributor"], row["date"]) in self.stored_keys
            for row in self.rows
        )


def price(value: float | None) -> str:
    if value is None:
        return "-"

    # Two decimals, or three for a target such as 0.283
    text = f"{value:.3f}"
    return text[:-1] if text.endswith("0") else text


def log_run_summary(report: RunReport, logger: logging.Logger = LOGGER) -> None:
    statuses = report.statuses
    written = "dry run, nothing written" if report.dry_run else "written to sgx_analyst_report"

    logger.info(
        "RUN SUMMARY  %s | %d companies\n"
        "    documents %d (%d already stored) | extracted %d | no target %d | failed %d | cost $%.4f\n"
        "    rows new %d | updated %d | %s\n"
        "    warnings %d",
        report.window,
        report.companies,
        report.documents,
        report.already_stored,
        statuses["extracted"],
        statuses["no_target"],
        len(report.failed),
        report.cost,
        len(report.rows) - report.updated,
        report.updated,
        written,
        len(report.warnings),
    )


def write_job_summary(report: RunReport) -> None:
    """
    Markdown shown at the top of the GitHub run page, no log reading needed
    """
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")

    if not summary_path:
        return

    statuses = report.statuses
    lines = [
        f"## {report.title}",
        "",
        f"**{report.window}** · {report.companies} companies"
        + (" · **dry run, nothing written**" if report.dry_run else ""),
        "",
        "| Documents | Already stored | Extracted | No target | Failed | New rows | Updated rows | Cost |",
        "|---|---|---|---|---|---|---|---|",
        f"| {report.documents} | {report.already_stored} | {statuses['extracted']} | {statuses['no_target']} "
        f"| {len(report.failed)} | {len(report.rows) - report.updated} "
        f"| {report.updated} | ${report.cost:.4f} |",
    ]

    if report.sources:
        lines += [
            "",
            "| Site | Documents | Extracted | No target | Failed | Cost |",
            "|---|---|---|---|---|---|",
        ]

        for source in report.sources:
            if source["listing_failed"]:
                lines.append(f"| {source['name']} | listing failed | - | - | - | - |")
                continue

            source_statuses = source["statuses"]
            lines.append(
                f"| {source['name']} | {source['documents']} "
                f"| {source_statuses.get('extracted', 0)} "
                f"| {source_statuses.get('no_target', 0)} "
                f"| {sum(count for status, count in source_statuses.items() if status not in READ_STATUSES)} "
                f"| ${source['cost']:.4f} |"
            )

    if report.rows:
        lines += [
            "",
            "### Rows that would be written" if report.dry_run else "### Rows written",
            "",
            "| Stock | House | Date | Rating | Target (SGD) | Previous | Forecast years | Report |",
            "|---|---|---|---|---|---|---|---|",
        ]

        for row in report.rows[:MAX_CARD_ROWS]:
            key = (row["symbol"], row["contributor"], row["date"])
            lines.append(
                f"| {row['symbol']}{' (updated)' if key in report.stored_keys else ''} "
                f"| {row['contributor']} | {row['date']} | {(row['rating'] or '-').title()} "
                f"| {price(row['target_price'])} | {price(row['previous_target_price'])} "
                f"| {len(row['forecast']) if row['forecast'] else '-'} "
                f"| [link]({row['source']}) |"
            )

        if len(report.rows) > MAX_CARD_ROWS:
            lines.append(f"\n{len(report.rows) - MAX_CARD_ROWS} more rows in the run artifact.")

    if report.failed or report.warnings:
        lines += ["", "### Needs a look", ""]

    if report.failed:
        lines += [
            "| Stock | House | Date | Problem | Report |",
            "|---|---|---|---|---|",
        ]

        for result in report.failed[:MAX_CARD_ROWS]:
            lines.append(
                f"| {result['symbol']} | {result['contributor'] or '-'} "
                f"| {result['report_date']} | {PROBLEMS.get(result['status'], result['status'])} "
                f"| [link]({result['document_url']}) |"
            )

        lines.append("")

    lines += [f"- {message}" for message in report.warnings[:MAX_CARD_ROWS]]

    try:
        with open(summary_path, "a") as file:
            file.write("\n".join(lines) + "\n")

    except OSError as error:
        LOGGER.warning("could not write the GitHub job summary: %s", error)


def finish_run(report: RunReport, run_name: str) -> None:
    """
    Saves the run's results and rows for the artifact, then writes the summary
    to the log and the run page
    """
    write_json(RUN_DIR / f"{run_name}_results.json", report.results)
    write_json(RUN_DIR / f"{run_name}_rows.json", report.rows)
    log_run_summary(report)
    write_job_summary(report)
