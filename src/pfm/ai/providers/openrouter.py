"""OpenRouter LLM provider."""

from __future__ import annotations

from pfm.ai.providers.openai_compat import OpenAICompatibleProvider
from pfm.ai.providers.registry import register_provider


@register_provider
class OpenRouterProvider(OpenAICompatibleProvider):
    """OpenRouter multi-model proxy."""

    name = "openrouter"
    default_model = "anthropic/claude-sonnet-4"
    default_base_url = "https://openrouter.ai/api"
