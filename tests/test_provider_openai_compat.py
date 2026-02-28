"""Tests for OpenAI-compatible base provider."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx

from pfm.ai.providers.openai_compat import OpenAICompatibleProvider


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
        body = {"choices": [{"message": {"content": "test response"}}]}
        return httpx.Response(200, json=body, request=request)


class _TestProvider(OpenAICompatibleProvider):
    name = "test"
    default_model = "test-model"
    default_base_url = "http://localhost:9999"


async def test_generate_commentary_success():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("system", "user prompt")

    assert result.text == "test response"
    assert result.model == "test-model"
    assert len(transport.requests) == 1

    body = json.loads(transport.requests[0].content)
    assert body["model"] == "test-model"
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][1]["role"] == "user"
    await provider.close()


async def test_generate_commentary_with_api_key():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(api_key="my-secret", client=client)

    await provider.generate_commentary("sys", "usr")

    auth = transport.requests[0].headers.get("authorization")
    assert auth == "Bearer my-secret"
    await provider.close()


async def test_generate_commentary_empty_choices():
    transport = _FakeTransport(responses=[httpx.Response(200, json={"choices": []})])
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == ""
    await provider.close()


async def test_generate_commentary_custom_model_and_url():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(model="custom-model", base_url="http://custom:8080", client=client)

    await provider.generate_commentary("sys", "usr")

    url = str(transport.requests[0].url)
    assert "custom:8080" in url
    body = json.loads(transport.requests[0].content)
    assert body["model"] == "custom-model"
    await provider.close()


async def test_generate_commentary_http_error_returns_fallback():
    transport = _FakeTransport(responses=[httpx.Response(402, text="Payment Required")])
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert "unavailable" in result.text
    assert result.model is None
    await provider.close()


async def test_generate_commentary_network_error_returns_fallback():
    transport = _FakeTransport(responses=[httpx.Response(500, text="Internal Server Error")])
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert "unavailable" in result.text
    assert result.model is None
    await provider.close()


async def test_close_does_not_close_external_client():
    transport = _FakeTransport()
    client = httpx.AsyncClient(transport=transport)
    provider = _TestProvider(client=client)

    await provider.close()
    # Client should still be usable since we didn't own it
    assert not client.is_closed
    await client.aclose()
