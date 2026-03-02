"""Tests for Ollama provider using instructor + httpx for pull."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import httpx
from instructor.core import InstructorRetryException
from openai import APIError

from pfm.ai.base import FALLBACK_COMMENTARY
from pfm.ai.providers.ollama import OllamaProvider
from pfm.ai.schemas import CommentaryResponse, ReportSection


@dataclass
class _FakeTransport(httpx.AsyncBaseTransport):
    """Fake HTTP transport for testing the pull endpoint."""

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


def _mock_openai_client(response=None, side_effect=None):
    """Create a mock instructor-patched OpenAI client."""
    client = MagicMock()
    create = AsyncMock(return_value=response, side_effect=side_effect)
    client.chat.completions.create = create
    return client


async def test_ollama_success():
    response = CommentaryResponse(sections=[ReportSection(title="Market", description="BTC up.")])
    openai_client = _mock_openai_client(response=response)
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "qwen3:14b"
    assert len(result.sections) == 1
    assert result.sections[0].title == "Market"
    assert "BTC up." in result.text
    # No pull attempt on success
    assert len(transport.requests) == 0
    await provider.close()


async def test_ollama_auto_pull_on_empty_result():
    """When first call fails, pull model, then retry successfully."""
    response_ok = CommentaryResponse(sections=[ReportSection(title="After Pull", description="Works now.")])
    # First call raises (empty result), second call succeeds
    err = APIError(message="model not found", request=MagicMock(), body=None)
    openai_client = _mock_openai_client(
        side_effect=[err, response_ok],
    )
    transport = _FakeTransport(
        responses=[httpx.Response(200, json={"status": "success"})],
    )
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.text != ""
    assert result.sections[0].title == "After Pull"
    # Pull was attempted via httpx
    assert len(transport.requests) == 1
    assert "/api/pull" in str(transport.requests[0].url)
    await provider.close()


async def test_ollama_error_returns_empty():
    """When both call and pull fail, return empty result."""
    err = APIError(message="connection refused", request=MagicMock(), body=None)
    openai_client = _mock_openai_client(side_effect=err)
    transport = _FakeTransport(
        responses=[httpx.Response(500, json={"error": "pull failed"})],
    )
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.text == ""
    assert result.model == "qwen3:14b"
    await provider.close()


async def test_ollama_validation_failure_skips_pull():
    """When model responds but generates invalid JSON, return fallback without pulling."""
    err = InstructorRetryException(
        n_attempts=3,
        messages=[],
        total_usage=0,
        last_completion=None,
    )
    openai_client = _mock_openai_client(side_effect=err)
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    result = await provider.generate_commentary("sys", "usr")

    assert result.text == FALLBACK_COMMENTARY
    assert result.error == "structured_output_failed"
    # No pull attempt — model exists, just generated bad output
    assert len(transport.requests) == 0
    await provider.close()


async def test_ollama_custom_model_and_url():
    response = CommentaryResponse(sections=[ReportSection(title="Custom", description="Done.")])
    openai_client = _mock_openai_client(response=response)
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
    await provider.close()


def test_ollama_default_base_url():
    assert OllamaProvider.default_base_url == "http://localhost:11434"


async def test_ollama_close_does_not_close_injected_clients():
    openai_client = _mock_openai_client()
    transport = _FakeTransport()
    http_client = httpx.AsyncClient(transport=transport)

    provider = OllamaProvider(openai_client=openai_client, http_client=http_client)
    await provider.close()

    # Injected clients should not be closed
    assert not http_client.is_closed
    await http_client.aclose()
