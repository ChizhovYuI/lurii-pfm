"""Grok (xAI) LLM provider."""

from __future__ import annotations

from pfm.ai.providers.openai_compat import OpenAICompatibleProvider
from pfm.ai.providers.registry import register_provider


@register_provider
class GrokProvider(OpenAICompatibleProvider):
    """Grok via xAI API."""

    name = "grok"
    default_model = "grok-3-mini"
    default_base_url = "https://api.x.ai"
