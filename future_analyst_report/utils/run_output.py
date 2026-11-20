from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import json


DATA_DIR = Path("future_analyst_report/data")
# Results and rows of the last run, uploaded as the workflow's artifact
RUN_DIR = DATA_DIR / "runs"

# The daily run looks back a week, so a report SGX lists late is still caught
DEFAULT_DAYS = 7


def write_json(path: Path, content) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, indent=2, ensure_ascii=False))


def report_window(days: int, start: str | None, end: str | None) -> tuple[date, date]:
    # SGX dates its reports in Singapore time
    today = datetime.now(timezone(timedelta(hours=8))).date()
    end_date = date.fromisoformat(end) if end else today
    start_date = date.fromisoformat(start) if start else end_date - timedelta(days=days)

    return start_date, end_date
