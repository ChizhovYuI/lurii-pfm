"""Tests for OpenRouter provider."""

from pfm.ai.providers.openrouter import OpenRouterProvider


def test_openrouter_defaults():
    assert OpenRouterProvider.name == "openrouter"
    assert OpenRouterProvider.default_model == "qwen/qwen3-235b-a22b-thinking-2507"
    assert "openrouter.ai" in OpenRouterProvider.default_base_url


def test_openrouter_stores_api_key():
    provider = OpenRouterProvider(api_key="or-key-123")
    assert provider._api_key == "or-key-123"
