from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import json


DATA_DIR = Path("institutional_flow/data")
# Results and rows of the last run, uploaded as the workflow's artifact
RUN_DIR = DATA_DIR / "runs"

# The window counts its first and last day, so 6 days back from a weekly run
# covers the 7 days since the last one and stops short of the file it already read.
# A run that starts a day late still reaches Monday's workbook
DEFAULT_DAYS = 6


def write_json(path: Path, content) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, indent=2, ensure_ascii=False, default=str))


def report_window(days: int) -> tuple[date, date]:
    # SGX dates its reports in Singapore time
    today = datetime.now(timezone(timedelta(hours=8))).date()

    return today - timedelta(days=days), today
