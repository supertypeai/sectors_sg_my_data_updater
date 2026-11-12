from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

import logging


LOGGER = logging.getLogger(__name__)

# The calls of the filing being processed. asyncio tasks and asyncio.to_thread
# copy it, so the three statement calls of one filing land in the same list
CURRENT_RECORDS: ContextVar[list[dict] | None] = ContextVar(
    "current_records", default=None
)

# Every call of this process, for the run total
RUN_RECORDS: list[dict] = []


@contextmanager
def track_usage() -> Generator[list[dict], None, None]:
    records = []
    token = CURRENT_RECORDS.set(records)

    try:
        yield records

    finally:
        CURRENT_RECORDS.reset(token)


def read_usage(message) -> dict:
    usage = getattr(message, "usage_metadata", None) or {}
    output_details = usage.get("output_token_details") or {}

    return {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "reasoning_tokens": output_details.get("reasoning", 0),
        # OpenRouter's own charge already in USD
        "cost": message.response_metadata.get("cost") or 0.0,
    }


class UsageTracker(BaseCallbackHandler):
    def __init__(self, model_name: str):
        self.model_name = model_name

    def on_llm_end(self, response: LLMResult, **kwargs) -> None:
        for generations in response.generations:
            for generation in generations:
                message = getattr(generation, "message", None)

                if message is None:
                    continue

                record = {"model": self.model_name, **read_usage(message)}
                RUN_RECORDS.append(record)

                current_records = CURRENT_RECORDS.get()

                if current_records is not None:
                    current_records.append(record)

                # Each step's log line carries its filing's total, per call is for debugging
                LOGGER.debug(
                    "[LLM Usage] %s input=%d output=%d reasoning=%d cost=$%.5f",
                    self.model_name,
                    record["input_tokens"],
                    record["output_tokens"],
                    record["reasoning_tokens"],
                    record["cost"],
                )


def summarise_usage(records: list[dict]) -> dict:
    return {
        "calls": len(records),
        "input_tokens": sum(record["input_tokens"] for record in records),
        "output_tokens": sum(record["output_tokens"] for record in records),
        "reasoning_tokens": sum(record["reasoning_tokens"] for record in records),
        "cost": round(sum(record["cost"] for record in records), 6),
    }
