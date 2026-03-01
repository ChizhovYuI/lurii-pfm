"""OpenRouter LLM provider."""

from __future__ import annotations

from pfm.ai.providers.openai_compat import OpenAICompatibleProvider
from pfm.ai.providers.registry import register_provider


@register_provider
class OpenRouterProvider(OpenAICompatibleProvider):
    """OpenRouter multi-model proxy."""

    name = "openrouter"
    description = "OpenRouter — multi-model proxy, one API key for Claude/GPT/Gemini/etc."
    default_model = "qwen/qwen3-235b-a22b-thinking-2507"
    default_base_url = "https://openrouter.ai/api"
    models: tuple[tuple[str, str], ...] = (
        ("qwen/qwen3-235b-a22b-thinking-2507", "free, 235B MoE, reasoning"),
        ("arcee-ai/trinity-large-preview:free", "free, 400B MoE, creative"),
        ("google/gemini-2.5-flash-preview", "free, fast, 1M context"),
        ("anthropic/claude-sonnet-4", "paid, best quality"),
        ("openai/gpt-4.1-mini", "paid, fast, cheap"),
    )
