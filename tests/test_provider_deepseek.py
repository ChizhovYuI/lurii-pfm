"""Tests for the first-class DeepSeek provider."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from openai import APIError

from pfm.ai.base import FALLBACK_COMMENTARY
from pfm.ai.providers.deepseek import DeepSeekProvider


def _mock_client(response=None, side_effect=None):
    client = MagicMock()
    create = AsyncMock(return_value=response, side_effect=side_effect)
    client.chat.completions.create = create
    return client


def _response(*, content: str = "", reasoning_content: str = "", finish_reason: str | None = "stop") -> object:
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason=finish_reason,
                message=SimpleNamespace(content=content, reasoning_content=reasoning_content),
            )
        ]
    )


def test_deepseek_defaults():
    assert DeepSeekProvider.name == "deepseek"
    assert DeepSeekProvider.default_model == "deepseek-chat"
    assert DeepSeekProvider.default_base_url == "https://api.deepseek.com"


async def test_validate_connection_uses_models_endpoint():
    client = _mock_client()
    raw_client = MagicMock()
    raw_client.models.list = AsyncMock(return_value=[{"id": "deepseek-chat"}])
    provider = DeepSeekProvider(api_key="ds-key", client=client, raw_client=raw_client)

    await provider.validate_connection()

    raw_client.models.list.assert_awaited_once()


async def test_generate_commentary_extracts_standard_content():
    provider = DeepSeekProvider(
        api_key="ds-key",
        client=_mock_client(response=_response(content="Compact markdown response")),
    )

    result = await provider.generate_commentary("system", "user")

    assert result.text == "Compact markdown response"
    assert result.model == "deepseek-chat"
    assert result.provider == "deepseek"
    assert result.finish_reason == "stop"
    assert result.reasoning_text is None


async def test_generate_commentary_keeps_reasoning_metadata_for_reasoner():
    provider = DeepSeekProvider(
        api_key="ds-key",
        model="deepseek-reasoner",
        client=_mock_client(
            response=_response(
                content="Short final answer",
                reasoning_content="Long chain of thought",
                finish_reason="stop",
            )
        ),
    )

    result = await provider.generate_commentary("system", "user")

    assert result.text == "Short final answer"
    assert result.model == "deepseek-reasoner"
    assert result.reasoning_text == "Long chain of thought"
    assert result.finish_reason == "stop"


async def test_generate_commentary_classifies_empty_content_with_reasoning_budget_failure():
    provider = DeepSeekProvider(
        api_key="ds-key",
        model="deepseek-reasoner",
        client=_mock_client(
            response=_response(
                content="",
                reasoning_content="Reasoning happened but final answer never arrived",
                finish_reason="length",
            )
        ),
    )

    result = await provider.generate_commentary("system", "user")

    assert result.text == FALLBACK_COMMENTARY
    assert result.model is None
    assert result.error == "deepseek API returned no final answer before reasoning budget was exhausted"
    assert result.reasoning_text == "Reasoning happened but final answer never arrived"
    assert result.finish_reason == "length"


async def test_generate_commentary_surfaces_api_error():
    err = APIError(message="bad auth", request=MagicMock(), body=None)
    provider = DeepSeekProvider(api_key="ds-key", client=_mock_client(side_effect=err))

    result = await provider.generate_commentary("system", "user")

    assert result.text == FALLBACK_COMMENTARY
    assert result.model is None
    assert result.error == "deepseek API request failed"


async def test_generate_commentary_json_uses_json_mode():
    client = _mock_client(response=_response(content='{"sections": []}'))
    provider = DeepSeekProvider(api_key="ds-key", client=client)

    result = await provider.generate_commentary_json("system", "user", max_output_tokens=1234)

    assert result.text == '{"sections": []}'
    assert result.model == "deepseek-chat"
    client.chat.completions.create.assert_awaited_once()
    kwargs = client.chat.completions.create.await_args.kwargs
    assert kwargs["response_format"] == {"type": "json_object"}
    assert kwargs["max_tokens"] == 1234


async def test_generate_commentary_json_classifies_empty_content_with_reasoning_budget_failure():
    provider = DeepSeekProvider(
        api_key="ds-key",
        model="deepseek-chat",
        client=_mock_client(
            response=_response(
                content="",
                reasoning_content="Reasoning happened but final answer never arrived",
                finish_reason="length",
            )
        ),
    )

    result = await provider.generate_commentary_json("system", "user")

    assert result.text == ""
    assert result.model is None
    assert result.error == "deepseek API returned no final answer before reasoning budget was exhausted"
    assert result.reasoning_text == "Reasoning happened but final answer never arrived"
    assert result.finish_reason == "length"
