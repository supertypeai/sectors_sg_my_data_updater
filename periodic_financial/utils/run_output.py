from pathlib import Path

from ..checks.run_report import build_run_checks
from ..llm.usage import summarise_usage, RUN_RECORDS
from .io_helper import write_json


def save_run_output(
    run_dir: Path,
    results: dict,
    rows: list[dict],
    skipped: list[dict],
    already_stored: int,
) -> None:
    # Rewritten after every filing, so a run that dies keeps what it finished
    write_json(run_dir / "extract_result.json", list(results.values()))
    write_json(run_dir / "rows.json", rows)
    write_json(
        run_dir / "checks.json",
        build_run_checks(
            list(results.values()),
            skipped,
            already_stored,
            summarise_usage(RUN_RECORDS)["cost"],
        ),
    )
