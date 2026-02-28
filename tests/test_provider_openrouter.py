"""Tests for OpenRouter provider."""

from pfm.ai.providers.openrouter import OpenRouterProvider


def test_openrouter_defaults():
    assert OpenRouterProvider.name == "openrouter"
    assert OpenRouterProvider.default_model == "anthropic/claude-sonnet-4"
    assert "openrouter.ai" in OpenRouterProvider.default_base_url


def test_openrouter_auth_header():
    provider = OpenRouterProvider(api_key="or-key-123")
    headers = provider._build_headers()
    assert headers["Authorization"] == "Bearer or-key-123"
