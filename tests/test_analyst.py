"""Tests for Gemini analyst client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from google.genai import errors
from pydantic import SecretStr

from pfm.ai.analyst import (
    FALLBACK_COMMENTARY,
    GEMINI_MAX_OUTPUT_TOKENS,
    GEMINI_MODEL,
    GEMINI_MODELS,
    _retry_delay_seconds,
    generate_commentary,
)
from pfm.ai.prompts import AnalyticsSummary
from pfm.db.gemini_store import GeminiStore
from pfm.db.models import init_db


def _sample_analytics() -> AnalyticsSummary:
    return AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(1000),
        allocation_by_asset="[]",
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
        pnl="{}",
        weekly_pnl_by_asset="[]",
    )


@dataclass
class _FakeAsyncModels:
    responses: list[object] = field(default_factory=list)
    calls: list[dict[str, object]] = field(default_factory=list)

    async def generate_content(self, *, model: str, contents: str, config: dict[str, object]) -> object:
        self.calls.append({"model": model, "contents": contents, "config": config})
        if self.responses:
            next_response = self.responses.pop(0)
            if isinstance(next_response, Exception):
                raise next_response
            return next_response
        return SimpleNamespace(text="")


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


async def test_generate_commentary_success_with_mock_client():
    fake_models = _FakeAsyncModels(responses=[SimpleNamespace(text="Portfolio looks stable.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == "Portfolio looks stable."
    assert len(fake_models.calls) == 1
    call = fake_models.calls[0]
    assert call["model"] == GEMINI_MODEL
    config = call["config"]
    assert isinstance(config, dict)
    assert config["max_output_tokens"] == GEMINI_MAX_OUTPUT_TOKENS


async def test_generate_commentary_fallback_to_next_model():
    response = httpx.Response(
        500,
        json={"error": {"message": "boom"}},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    error = errors.ServerError(500, {"error": {"status": "INTERNAL", "message": "boom"}}, response)
    fake_models = _FakeAsyncModels(responses=[error, SimpleNamespace(text="Recovered on fallback model.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == "Recovered on fallback model."
    assert len(fake_models.calls) == 2
    assert fake_models.calls[0]["model"] == GEMINI_MODELS[0]
    assert fake_models.calls[1]["model"] == GEMINI_MODELS[1]


async def test_generate_commentary_fallback_on_api_error():
    response = httpx.Response(
        500,
        json={"error": {"message": "boom"}},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    error = errors.ServerError(500, {"error": {"status": "INTERNAL", "message": "boom"}}, response)
    fake_models = _FakeAsyncModels(responses=[error])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_retries_on_429_then_succeeds():
    limited_response = httpx.Response(
        429,
        headers={"Retry-After": "0.01"},
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    limited_error = errors.ClientError(429, {"error": {"status": "RESOURCE_EXHAUSTED"}}, limited_response)
    success = SimpleNamespace(text="Recovered after retry.")
    fake_models = _FakeAsyncModels(responses=[limited_error, success])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")
    sleep_mock = AsyncMock()

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst.asyncio.sleep", sleep_mock),
    ):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == "Recovered after retry."
    assert len(fake_models.calls) == 2
    assert sleep_mock.await_count == 0


async def test_generate_commentary_fallback_after_429_retries_exhausted():
    limited_response = httpx.Response(
        429,
        request=httpx.Request("POST", "https://example.invalid/gemini"),
    )
    limited_error = errors.ClientError(429, {"error": {"status": "RESOURCE_EXHAUSTED"}}, limited_response)
    fake_models = _FakeAsyncModels(responses=[limited_error] * len(GEMINI_MODELS))
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")
    sleep_mock = AsyncMock()

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst.asyncio.sleep", sleep_mock),
    ):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == FALLBACK_COMMENTARY
    assert len(fake_models.calls) == len(GEMINI_MODELS)
    assert sleep_mock.await_count == 0


async def test_generate_commentary_fallback_on_unexpected_exception():
    class _BrokenModels:
        async def generate_content(self, *, model: str, contents: str, config: dict[str, object]) -> object:
            raise RuntimeError("unexpected")

    broken_client = _FakeClient(aio=_FakeAioClient(models=_BrokenModels()))  # type: ignore[arg-type]
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(
            _sample_analytics(),
            api_key="gemini-key",
            client=broken_client,  # type: ignore[arg-type]
        )

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_when_key_missing(tmp_path):
    db_path = tmp_path / "empty.db"
    await init_db(db_path)
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("")
    settings.database_path = db_path

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_uses_db_key_when_env_missing(tmp_path):
    db_path = tmp_path / "ai.db"
    await init_db(db_path)
    await GeminiStore(db_path).set("gemini-db-key")

    fake_models = _FakeAsyncModels(responses=[SimpleNamespace(text="From DB key.")])
    fake_client = _FakeClient(aio=_FakeAioClient(models=fake_models))

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), client=fake_client)

    assert result == "From DB key."


def test_retry_delay_applies_model_minimum_even_with_small_retry_after():
    assert _retry_delay_seconds("0.01", 1, "gemini-2.5-pro") == 30.0
    assert _retry_delay_seconds("0.01", 1, "gemini-2.5-flash") == 7.0
