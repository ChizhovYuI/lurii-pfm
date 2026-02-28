"""Tests for the LLM provider registry."""

import pytest

from pfm.ai.base import CommentaryResult, LLMProvider, ProviderName
from pfm.ai.providers.registry import PROVIDER_REGISTRY, get_provider_names, register_provider


def test_registry_contains_all_four_providers():
    assert ProviderName.gemini in PROVIDER_REGISTRY
    assert ProviderName.ollama in PROVIDER_REGISTRY
    assert ProviderName.openrouter in PROVIDER_REGISTRY
    assert ProviderName.grok in PROVIDER_REGISTRY


def test_get_provider_names_sorted():
    names = get_provider_names()
    assert names == sorted(names)
    assert "gemini" in names
    assert "ollama" in names


def test_register_provider_rejects_missing_name():
    class _BadProvider(LLMProvider):
        async def generate_commentary(self, system_prompt, user_prompt, *, max_output_tokens=4096):
            return CommentaryResult(text="", model=None)

        async def close(self):
            pass

    with pytest.raises(ValueError, match="must define a 'name'"):
        register_provider(_BadProvider)
