from contextlib import contextmanager
from time import monotonic

import logging
import os
import sys


IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

NOISY_LOGGERS = (
    "httpx",
    "httpcore",
    "urllib3",
    "requests",
    "langchain",
    "langsmith",
    "openai",
)


class WarningCollector(logging.Handler):
    """
    Keeps every warning and error of the run, so the job summary can list what
    needs a look without anyone opening the log
    """
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage().splitlines()[0][:300])


class GitHubErrorAnnotation(logging.Handler):
    """
    Stages are collapsed groups in the GitHub log, which would hide an error
    inside them. An ::error:: annotation also shows it on the run page
    """
    def emit(self, record: logging.LogRecord) -> None:
        print(f"::error::{record.getMessage().splitlines()[0][:300]}", flush=True)


def setup_logging() -> WarningCollector:
    console_level = getattr(
        logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO
    )

    logging.addLevelName(logging.WARNING, "WARN")

    # GitHub stamps every line with the full UTC time, the time of day is enough
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(console_level)
    console.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    )

    collector = WarningCollector()
    handlers = [console, collector]

    if IS_GITHUB_ACTIONS:
        annotation = GitHubErrorAnnotation()
        annotation.setLevel(logging.ERROR)
        handlers.append(annotation)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    for handler in handlers:
        root.addHandler(handler)

    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    # The shared LLM caller and PDF parser log each call, this pipeline logs
    # one outcome per report instead. LOG_LEVEL=DEBUG brings them back
    if console_level > logging.DEBUG:
        logging.getLogger("periodic_financial").setLevel(logging.WARNING)

    return collector


@contextmanager
def stage(number: int, total: int, name: str, logger: logging.Logger):
    """
    One collapsible group per stage in GitHub Actions, a plain header elsewhere
    """
    title = f"[{number}/{total}] {name}"
    started = monotonic()

    if IS_GITHUB_ACTIONS:
        print(f"::group::{title}", flush=True)

    else:
        logger.info("=== %s ===", title)

    try:
        yield

    finally:
        logger.info("%s took %.0fs", name, monotonic() - started)

        if IS_GITHUB_ACTIONS:
            print("::endgroup::", flush=True)
