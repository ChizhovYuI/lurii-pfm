"""Tests for OpenAI-compatible base provider using raw chat completions."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from openai import APIError

from pfm.ai.base import FALLBACK_COMMENTARY
from pfm.ai.providers.openai_compat import OpenAICompatibleProvider


class _TestProvider(OpenAICompatibleProvider):
    name = "test"
    default_model = "test-model"
    default_base_url = "http://localhost:9999"


def _mock_client(response=None, side_effect=None):
    client = MagicMock()
    create = AsyncMock(return_value=response, side_effect=side_effect)
    client.chat.completions.create = create
    return client


def _response_with_text(text: str) -> object:
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=text))])


async def test_generate_commentary_success():
    client = _mock_client(response=_response_with_text("Portfolio looks stable.\n- Concentration remains high."))
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("system", "user prompt")

    assert result.model == "test-model"
    assert result.sections == ()
    assert "Portfolio looks stable." in result.text

    call_kwargs = client.chat.completions.create.call_args
    assert call_kwargs.kwargs["model"] == "test-model"
    assert "messages" in call_kwargs.kwargs


async def test_generate_commentary_error_returns_fallback():
    err = APIError(message="API timeout", request=MagicMock(), body=None)
    client = _mock_client(side_effect=err)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == FALLBACK_COMMENTARY
    assert result.model is None
    assert result.error is not None


async def test_generate_commentary_custom_model():
    client = _mock_client(response=_response_with_text("Custom model response"))
    provider = _TestProvider(model="custom-model", client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "custom-model"
    call_kwargs = client.chat.completions.create.call_args
    assert call_kwargs.kwargs["model"] == "custom-model"


async def test_generate_commentary_empty_text_returns_fallback():
    client = _mock_client(response=_response_with_text(""))
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == FALLBACK_COMMENTARY
    assert result.error is not None


async def test_close_does_not_close_injected_client():
    client = _mock_client()
    provider = _TestProvider(client=client)

    await provider.close()

    assert not hasattr(client, "close") or not getattr(client.close, "called", False)


async def test_validate_connection_uses_raw_models_endpoint():
    client = _mock_client()
    raw_client = MagicMock()
    raw_client.models.list = AsyncMock(return_value=[{"id": "test-model"}])
    provider = _TestProvider(client=client, raw_client=raw_client)

    await provider.validate_connection()

    raw_client.models.list.assert_awaited_once()


async def test_validate_connection_propagates_models_error():
    client = _mock_client()
    raw_client = MagicMock()
    raw_client.models.list = AsyncMock(side_effect=APIError(message="bad auth", request=MagicMock(), body=None))
    provider = _TestProvider(client=client, raw_client=raw_client)

    import pytest

    with pytest.raises(APIError):
        await provider.validate_connection()
