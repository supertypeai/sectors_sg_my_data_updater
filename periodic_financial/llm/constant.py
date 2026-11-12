MODEL_CONFIG = {
    "gpt-6-luna": {
        "model": "openai/gpt-6-luna",
        "provider": "openrouter",
    },
    "glm-5.3-flash": {"model": "z-ai/glm-5.3-flash", "provider": "openrouter"},
    "claude-haiku-5.5": {
        "model": "anthropic/claude-haiku-5.5",
        "provider": "openrouter",
    },
}


DEFAULT_MODELS = ["claude-haiku-5.5", "gpt-6-luna", "glm-5.3-flash"]
