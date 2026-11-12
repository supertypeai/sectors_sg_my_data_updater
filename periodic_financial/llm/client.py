from langchain.chat_models import init_chat_model

from .constant import MODEL_CONFIG
from .usage import UsageTracker
from ..config.settings import OPENROUTER_API_KEY

import logging


LOGGER = logging.getLogger(__name__)

# ChatOpenRouter takes the timeout in milliseconds. Without one, a stalled
# request is retried silently by the OpenRouter SDK for up to an hour
REQUEST_TIMEOUT_MS = 120_000

# The SDK retries for max_retries x 150 seconds on its own. invoke_structured_llm
# already retries and falls back to other models, so one SDK retry is enough
SDK_MAX_RETRIES = 1

MAX_OUTPUT_TOKENS = 55_000


def get_llm(
    model_name: str,
    temperature: float = 0.5,
    effort: str = "low",
    max_retries: int = SDK_MAX_RETRIES,
    request_timeout_ms: int = REQUEST_TIMEOUT_MS,
):
    config_model = MODEL_CONFIG.get(model_name)

    if config_model is None:
        LOGGER.error(
            "Unknown model name: %s. Available models: %s",
            model_name,
            ", ".join(MODEL_CONFIG),
        )
        return None

    if not OPENROUTER_API_KEY:
        LOGGER.error("OPENROUTER_API_KEY is not set")
        return None

    try:
        return init_chat_model(
            config_model["model"],
            model_provider=config_model["provider"],
            api_key=OPENROUTER_API_KEY,
            temperature=temperature,
            max_retries=max_retries,
            max_tokens=MAX_OUTPUT_TOKENS,
            request_timeout=request_timeout_ms,
            reasoning={"effort": effort},
            callbacks=[UsageTracker(model_name)],
        )

    except Exception as error:
        LOGGER.error("Error initialising %s: %s", model_name, error)
        return None
