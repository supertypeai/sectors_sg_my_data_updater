from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.messages import HumanMessage

from pydantic import BaseModel, ValidationError
from pathlib import Path

from .client import get_llm
from .constant import DEFAULT_MODELS

import asyncio
import logging
import time
import base64


LOGGER = logging.getLogger(__name__)


def encode_image(image_path: str) -> str:
    image_bytes = Path(image_path).read_bytes()
    return base64.b64encode(image_bytes).decode("utf-8")


def build_image_content(page_images: list[dict]) -> list[dict]:
    content = []

    for page_image in page_images:
        # The label keeps the model citing our page index, without it the
        # explanation uses the number printed in the page footer
        content.append(
            {
                "type": "text",
                "text": f"[PDF_PAGE {page_image['page_number']}]",
            }
        )

        image_data = base64.b64encode(page_image["png"]).decode("utf-8")
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{image_data}"},
            }
        )

    return content


def build_structured_input(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    input_data: dict[str],
) -> tuple:
    parser = JsonOutputParser(pydantic_object=pydantic_output)

    prompt = ChatPromptTemplate.from_messages(
        [
            ('system', system_prompt),
            ('user', user_prompt),
        ]
    )

    input_data = {
        **input_data,
        "format_instructions": parser.get_format_instructions(),
    }

    return prompt, parser, input_data


def unwrap_extraction(extraction, model: str) -> dict | None:
    # the model sometimes wraps the object in a JSON array, unwrap to a dict
    if isinstance(extraction, list):
        extraction = next((item for item in extraction if isinstance(item, dict)), None)

    if not isinstance(extraction, dict):
        LOGGER.warning("[fallback] %s returned non-object extraction, skipping", model)
        return None

    return extraction


def validate_extraction(
    extraction: dict | None,
    pydantic_output: type[BaseModel],
    model: str,
    log_name: str,
) -> dict | None:
    if extraction is None:
        return None

    # JsonOutputParser accepts any JSON, so a model that echoes the schema back
    # instead of filling it in would otherwise pass as a success
    try:
        return pydantic_output.model_validate(extraction).model_dump()

    except ValidationError as error:
        LOGGER.warning(
            "[LLM Caller][%s] %s returned output that does not match %s: %s",
            log_name,
            model,
            pydantic_output.__name__,
            error.error_count(),
        )
        return None


def invoke_structured_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    input_data: dict[str],
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: float = 0.3,
    effort: str = "high",
) -> dict | list | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )

    for model in models:
        attempt = 1

        while attempt <= max_retry:
            try:
                llm = get_llm(model, temperature=temperature, effort=effort)

                if llm is None:
                    attempt += 1
                    continue

                LOGGER.info(
                    "[LLM Caller][%s] model used %s (attempt %d/%d)",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                )

                extraction = (prompt | llm | parser).invoke(input_data)

                LOGGER.debug(
                    "[LLM Caller][%s] raw extraction: %s", log_name, extraction
                )

                extraction = validate_extraction(
                    unwrap_extraction(extraction, model),
                    pydantic_output,
                    model,
                    log_name,
                )

                # Falls through to attempt += 1 on failure, a bare continue
                # here would retry the same attempt forever
                if extraction is not None:
                    return extraction

            except Exception as error:
                LOGGER.warning("[fallback] model %s failed: %s", model, error)

            attempt += 1

            if attempt <= max_retry:
                time.sleep(2.5)

    return None


async def ainvoke_structured_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    input_data: dict[str],
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: float = 0.3,
    effort: str = "high",
) -> dict | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )

    for model in models:
        for attempt in range(1, max_retry + 1):
            llm = get_llm(model, temperature=temperature, effort=effort)

            if llm is None:
                continue

            try:
                extraction = await (prompt | llm | parser).ainvoke(input_data)
            except Exception as error:
                LOGGER.warning(
                    "[LLM Caller][%s] model %s failed (attempt %d/%d): %s",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                    error,
                )
                await asyncio.sleep(2.5)
                continue

            extraction = validate_extraction(
                unwrap_extraction(extraction, model),
                pydantic_output,
                model,
                log_name,
            )

            if extraction is not None:
                return extraction

    return None


def build_vision_messages(
    prompt: ChatPromptTemplate,
    input_data: dict,
    page_images: list[dict],
) -> list:
    # Formatted once, then the page images are appended after the user text
    messages = prompt.format_messages(**input_data)

    messages[-1] = HumanMessage(
        content=[
            {"type": "text", "text": messages[-1].content},
            *build_image_content(page_images),
        ]
    )

    return messages


def invoke_structured_vision_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    input_data: dict[str],
    page_images: list[dict],
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: float = 0.3,
    effort: str = "high",
) -> dict | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )
    messages = build_vision_messages(prompt, input_data, page_images)

    for model in models:
        for attempt in range(1, max_retry + 1):
            llm = get_llm(model, temperature=temperature, effort=effort)

            if llm is None:
                continue

            try:
                extraction = (llm | parser).invoke(messages)
            except Exception as error:
                LOGGER.warning(
                    "[LLM Caller][%s] model %s failed (attempt %d/%d): %s",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                    error,
                )
                time.sleep(2.5)
                continue

            extraction = validate_extraction(
                unwrap_extraction(extraction, model),
                pydantic_output,
                model,
                log_name,
            )

            if extraction is not None:
                return extraction

    return None


async def ainvoke_structured_vision_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    input_data: dict[str],
    page_images: list[dict],
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: float = 0.3,
    effort: str = "high",
) -> dict | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )

    messages = build_vision_messages(prompt, input_data, page_images)

    for model in models:
        for attempt in range(1, max_retry + 1):
            llm = get_llm(model, temperature=temperature, effort=effort)

            if llm is None:
                continue

            try:
                extraction = await (llm | parser).ainvoke(messages)
            except Exception as error:
                LOGGER.warning(
                    "[LLM Caller][%s] model %s failed (attempt %d/%d): %s",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                    error,
                )
                await asyncio.sleep(2.5)
                continue

            extraction = validate_extraction(
                unwrap_extraction(extraction, model),
                pydantic_output,
                model,
                log_name,
            )

            if extraction is not None:
                return extraction

    return None
