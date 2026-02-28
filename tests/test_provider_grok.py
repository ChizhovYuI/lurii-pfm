"""Tests for Grok provider."""

from pfm.ai.providers.grok import GrokProvider


def test_grok_defaults():
    assert GrokProvider.name == "grok"
    assert GrokProvider.default_model == "grok-3-mini"
    assert "api.x.ai" in GrokProvider.default_base_url


def test_grok_auth_header():
    provider = GrokProvider(api_key="xai-key-456")
    headers = provider._build_headers()
    assert headers["Authorization"] == "Bearer xai-key-456"
