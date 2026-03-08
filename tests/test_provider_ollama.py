"""Tests for Ollama provider using raw chat completions + httpx for pull."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
from openai import APIError

from pfm.ai.providers.ollama import OllamaProvider


@dataclass
class _FakeTransport(httpx.AsyncBaseTransport):
    responses: list[httpx.Response] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.responses:
            resp = self.responses.pop(0)
            resp.request = request
            resp.stream = httpx.ByteStream(resp.content)
            return resp
        return httpx.Response(200, json={"status": "success"}, request=request)


@dataclass
class _FailingTransport(httpx.AsyncBaseTransport):
    error: Exception

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise self.error


def _mock_openai_client(response=None, side_effect=None):
    client = MagicMock()
    create = AsyncMock(return_value=response, side_effect=side_effect)
    client.chat.completions.create = create
    return client


def _response(text: str) -> object:
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=text))])


async def test_ollama_success():
    openai_client = _mock_openai_client(response=_response("BTC up and allocations remain concentrated."))
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "qwen3:14b"
    assert result.sections == ()
    assert "BTC up" in result.text
    assert len(transport.requests) == 0
    await provider.close()


async def test_ollama_auto_pull_on_empty_result():
    err = APIError(message="model not found", request=MagicMock(), body=None)
    openai_client = _mock_openai_client(side_effect=[err, _response("Works now after pull.")])
    transport = _FakeTransport(responses=[httpx.Response(200, json={"status": "success"})])
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.text == "Works now after pull."
    assert len(transport.requests) == 1
    assert "/api/pull" in str(transport.requests[0].url)
    await provider.close()


async def test_ollama_error_returns_empty():
    err = APIError(message="connection refused", request=MagicMock(), body=None)
    openai_client = _mock_openai_client(side_effect=err)
    transport = _FakeTransport(responses=[httpx.Response(500, json={"error": "pull failed"})])
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.text == ""
    assert result.model == "qwen3:14b"
    await provider.close()


async def test_ollama_custom_model_and_url():
    openai_client = _mock_openai_client(response=_response("Custom response"))
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(
        model="mistral:7b",
        base_url="http://gpu:11434",
        openai_client=openai_client,
        http_client=http_client,
    )
    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "mistral:7b"
    assert result.text == "Custom response"
    await provider.close()


def test_ollama_default_base_url():
    assert OllamaProvider.default_base_url == "http://localhost:11434"


async def test_ollama_close_does_not_close_injected_clients():
    openai_client = _mock_openai_client()
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    await provider.close()

    assert not http_client.is_closed
    await http_client.aclose()


async def test_ollama_validate_connection_success():
    transport = _FakeTransport(responses=[httpx.Response(200, json={"models": [{"name": "qwen3:14b"}]})])
    http_client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(http_client=http_client)

    await provider.validate_connection()

    assert len(transport.requests) == 1
    assert "/api/tags" in str(transport.requests[0].url)
    await provider.close()


async def test_ollama_validate_connection_missing_model():
    transport = _FakeTransport(responses=[httpx.Response(200, json={"models": [{"name": "llama3.1:8b"}]})])
    http_client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(model="qwen3:14b", http_client=http_client)

    import pytest

    with pytest.raises(ValueError, match="not installed"):
        await provider.validate_connection()

    await provider.close()


async def test_ollama_validate_connection_unreachable():
    http_client = httpx.AsyncClient(transport=_FailingTransport(error=httpx.ConnectError("connection refused")))
    provider = OllamaProvider(http_client=http_client)

    import pytest

    with pytest.raises(httpx.ConnectError):
        await provider.validate_connection()

    await provider.close()
