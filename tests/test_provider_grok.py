"""Tests for Grok provider."""

from pfm.ai.providers.grok import GrokProvider


def test_grok_defaults():
    assert GrokProvider.name == "grok"
    assert GrokProvider.default_model == "grok-3-mini"
    assert "api.x.ai" in GrokProvider.default_base_url


def test_grok_stores_api_key():
    provider = GrokProvider(api_key="xai-key-456")
    assert provider._api_key == "xai-key-456"
