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
        body = {"choices": [{"message": {"content": "ollama response"}}]}
        return httpx.Response(200, json=body, request=request)


async def test_ollama_no_auth_headers():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    await provider.generate_commentary("sys", "usr")

    assert "authorization" not in transport.requests[0].headers
    await provider.close()


async def test_ollama_success():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == "ollama response"
    assert result.model == "llama3.1:8b"
    await provider.close()


async def test_ollama_auto_pull_on_empty_result():
    chat_empty = httpx.Response(200, json={"choices": [{"message": {"content": ""}}]})
    pull_ok = httpx.Response(200, json={"status": "success"})
    chat_ok = httpx.Response(200, json={"choices": [{"message": {"content": "after pull"}}]})

    transport = _FakeTransport(responses=[chat_empty, pull_ok, chat_ok])
    client = httpx.AsyncClient(transport=transport)
    provider = OllamaProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == "after pull"
    # Should have: 1 chat, 1 pull, 1 chat = 3 requests
    assert len(transport.requests) == 3
    urls = [str(r.url) for r in transport.requests]
    assert any("/api/pull" in u for u in urls)
    await provider.close()


def test_ollama_default_base_url():
    assert OllamaProvider.default_base_url == "http://localhost:11434"
