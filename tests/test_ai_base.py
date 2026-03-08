"""Tests for AI base types and LLMProvider ABC."""

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, LLMProvider, ProviderName


def test_provider_name_enum_values():
    assert ProviderName.gemini == "gemini"
    assert ProviderName.deepseek == "deepseek"
    assert ProviderName.ollama == "ollama"
    assert ProviderName.openrouter == "openrouter"
    assert ProviderName.grok == "grok"


def test_commentary_result_frozen():
    result = CommentaryResult(text="hello", model="test-model")
    assert result.text == "hello"
    assert result.model == "test-model"


def test_commentary_result_none_model():
    result = CommentaryResult(text="fallback", model=None)
    assert result.model is None


def test_commentary_result_supports_optional_generation_metadata():
    result = CommentaryResult(
        text="fallback",
        model=None,
        provider="deepseek",
        finish_reason="length",
        reasoning_text="thinking...",
        generation_meta={"provider": "deepseek"},
    )
    assert result.provider == "deepseek"
    assert result.finish_reason == "length"
    assert result.reasoning_text == "thinking..."
    assert result.generation_meta == {"provider": "deepseek"}


def test_fallback_commentary_is_non_empty():
    assert FALLBACK_COMMENTARY
    assert "unavailable" in FALLBACK_COMMENTARY


def test_llm_provider_is_abstract():
    import pytest

    with pytest.raises(TypeError):
        LLMProvider()  # type: ignore[abstract]
