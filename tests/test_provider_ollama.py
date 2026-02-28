"""Tests for Ollama provider."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

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
        body = {"message": {"role": "assistant", "content": "ollama response"}}
        return httpx.Response(200, json=body, request=request)


async def test_ollama_success():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == "ollama response"
    assert result.model == "qwen3:14b"
    assert len(transport.requests) == 1
    assert "/api/chat" in str(transport.requests[0].url)
    await provider.close()


async def test_ollama_auto_pull_on_empty_result():
    chat_empty = httpx.Response(200, json={"message": {"role": "assistant", "content": ""}})
    pull_ok = httpx.Response(200, json={"status": "success"})
    chat_ok = httpx.Response(200, json={"message": {"role": "assistant", "content": "after pull"}})

    transport = _FakeTransport(responses=[chat_empty, pull_ok, chat_ok])
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == "after pull"
    assert len(transport.requests) == 3
    urls = [str(r.url) for r in transport.requests]
    assert any("/api/pull" in u for u in urls)
    await provider.close()


async def test_ollama_returns_empty_on_http_error():
    error_resp = httpx.Response(500, json={"error": "server error"})
    pull_fail = httpx.Response(500, json={"error": "pull failed"})

    transport = _FakeTransport(responses=[error_resp, pull_fail])
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == ""
    assert result.model == "qwen3:14b"
    await provider.close()


async def test_ollama_custom_model_and_url():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(model="mistral:7b", base_url="http://gpu:11434", client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "mistral:7b"
    assert "gpu:11434" in str(transport.requests[0].url)
    await provider.close()


def test_ollama_default_base_url():
    assert OllamaProvider.default_base_url == "http://localhost:11434"


async def test_ollama_close_does_not_close_external_client():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    await provider.close()
    assert not client.is_closed
    await client.aclose()
