"""Tests for Gemini provider."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from google.genai import errors

from pfm.ai.providers.gemini import (
    GEMINI_MODELS,
    GeminiProvider,
    _extract_text,
    _field_value,
    _log_token_usage,
    _retry_delay_seconds,
)


@dataclass
class _FakeAsyncModels:
    responses: list[object] = field(default_factory=list)
    calls: list[dict[str, object]] = field(default_factory=list)
    get_response: object | None = None
    get_calls: list[str] = field(default_factory=list)

    async def generate_content(self, *, model: str, contents: str, config: dict[str, object]) -> object:
        self.calls.append({"model": model, "contents": contents, "config": config})
        if self.responses:
            next_response = self.responses.pop(0)
            if isinstance(next_response, Exception):
                raise next_response
            return next_response
        return SimpleNamespace(text="")

    async def get(self, *, model: str) -> object:
        self.get_calls.append(model)
        if isinstance(self.get_response, Exception):
            raise self.get_response
        return self.get_response or SimpleNamespace(name=model)


@dataclass
class _FakeAioClient:
    models: _FakeAsyncModels
    closed: bool = False

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class _FakeClient:
    aio: _FakeAioClient
    closed: bool = False

    def close(self) -> None:
        self.closed = True


async def test_gemini_provider_success():
    fake_models = _FakeAsyncModels(responses=[SimpleNamespace(text="Portfolio looks stable.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    result = await provider.generate_commentary("system prompt", "user prompt")
    await provider.close()

    assert result.text == "Portfolio looks stable."
    assert result.model == GEMINI_MODELS[0]
    assert len(fake_models.calls) == 1


async def test_gemini_provider_model_failover():
    response = httpx.Response(
        500,
        json={"error": {"message": "boom"}},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    error = errors.ServerError(500, {"error": {"status": "INTERNAL", "message": "boom"}}, response)
    fake_models = _FakeAsyncModels(responses=[error, SimpleNamespace(text="Recovered.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    result = await provider.generate_commentary("sys", "usr")
    await provider.close()

    assert result.text == "Recovered."
    assert result.model == GEMINI_MODELS[1]
    assert len(fake_models.calls) == 2


async def test_gemini_provider_all_models_fail():
    response = httpx.Response(
        500,
        json={"error": {"message": "boom"}},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    error = errors.ServerError(500, {"error": {"status": "INTERNAL", "message": "boom"}}, response)
    fake_models = _FakeAsyncModels(responses=[error] * len(GEMINI_MODELS))
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    result = await provider.generate_commentary("sys", "usr")
    await provider.close()

    assert result.text == ""
    assert result.model is None


async def test_gemini_provider_429_switches_model():
    limited_response = httpx.Response(
        429,
        headers={"Retry-After": "0.01"},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    limited_error = errors.ClientError(429, {"error": {"status": "RESOURCE_EXHAUSTED"}}, limited_response)
    success = SimpleNamespace(text="Recovered after retry.")
    fake_models = _FakeAsyncModels(responses=[limited_error, success])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    result = await provider.generate_commentary("sys", "usr")
    await provider.close()

    assert result.text == "Recovered after retry."
    assert len(fake_models.calls) == 2


async def test_gemini_provider_single_model():
    fake_models = _FakeAsyncModels(responses=[SimpleNamespace(text="Single model.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", model="gemini-2.5-flash", client=fake_client)  # type: ignore[arg-type]
    result = await provider.generate_commentary("sys", "usr")
    await provider.close()

    assert result.text == "Single model."
    assert result.model == "gemini-2.5-flash"
    assert fake_models.calls[0]["model"] == "gemini-2.5-flash"


async def test_gemini_provider_closes_owned_client():
    fake_models = _FakeAsyncModels(responses=[SimpleNamespace(text="ok")])
    fake_aio = _FakeAioClient(models=fake_models)
    fake_client = _FakeClient(aio=fake_aio)

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    # Simulate ownership
    provider._owns_client = True
    await provider.close()
    assert fake_aio.closed
    assert fake_client.closed


async def test_gemini_validate_connection_uses_models_get():
    fake_models = _FakeAsyncModels(get_response=SimpleNamespace(name="gemini-2.5-pro"))
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]
    await provider.validate_connection()

    assert fake_models.get_calls == ["gemini-2.5-pro"]


async def test_gemini_validate_connection_propagates_api_error():
    error_response = httpx.Response(
        401,
        json={"error": {"message": "bad api key"}},
        request=httpx.Request("GET", "https://example.invalid/models"),
    )
    error = errors.ClientError(401, {"error": {"status": "UNAUTHENTICATED"}}, error_response)
    fake_models = _FakeAsyncModels(get_response=error)
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    provider = GeminiProvider(api_key="key", client=fake_client)  # type: ignore[arg-type]

    import pytest

    with pytest.raises(errors.ClientError):
        await provider.validate_connection()


def test_retry_delay_applies_model_minimum():
    assert _retry_delay_seconds("0.01", 1, "gemini-2.5-pro") == 30.0
    assert _retry_delay_seconds("0.01", 1, "gemini-2.5-flash") == 7.0


def test_extract_text_from_simple_response():
    resp = SimpleNamespace(text="Hello world")
    assert _extract_text(resp) == "Hello world"


def test_extract_text_from_candidates():
    resp = {"candidates": [{"content": {"parts": [{"text": "Part 1"}, {"text": "Part 2"}]}}]}
    assert _extract_text(resp) == "Part 1\nPart 2"


def test_field_value_from_mapping():
    assert _field_value({"key": "val"}, "key") == "val"
    assert _field_value({"key": "val"}, "missing") is None


def test_field_value_from_object():
    obj = SimpleNamespace(key="val")
    assert _field_value(obj, "key") == "val"
    assert _field_value(obj, "missing") is None


def test_log_token_usage_with_usage_metadata():
    resp = SimpleNamespace(
        usage_metadata=SimpleNamespace(
            prompt_token_count=100,
            candidates_token_count=50,
            total_token_count=150,
        )
    )
    with patch("pfm.ai.providers.gemini.logger.info") as log_info:
        _log_token_usage(resp, model="gemini-2.5-pro")
    assert log_info.call_count == 1
    assert "gemini_usage" in log_info.call_args.args[0]
    assert log_info.call_args.args[1:] == ("gemini-2.5-pro", 100, 50, 150)
