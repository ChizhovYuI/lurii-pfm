"""Tests for Gemini analyst client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pydantic import SecretStr

from pfm.ai.analyst import FALLBACK_COMMENTARY, GEMINI_MAX_OUTPUT_TOKENS, GEMINI_MODEL, generate_commentary
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
        yield_metrics="[]",
    )


@dataclass
class _FakeClient:
    responses: list[httpx.Response] = field(default_factory=list)
    calls: list[tuple[str, dict[str, str], dict[str, object]]] = field(default_factory=list)

    async def post(self, url: str, *, params: dict[str, str], json: dict[str, object]) -> httpx.Response:
        self.calls.append((url, params, json))
        if self.responses:
            return self.responses.pop(0)
        return httpx.Response(200, json={"candidates": []}, request=httpx.Request("POST", url, params=params))


async def test_generate_commentary_success_with_mock_client():
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    response = httpx.Response(
        200,
        json={
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": "Portfolio looks stable."}],
                    }
                }
            ]
        },
        request=httpx.Request("POST", endpoint),
    )
    fake_client = _FakeClient(responses=[response])
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == "Portfolio looks stable."
    assert len(fake_client.calls) == 1
    url, params, payload = fake_client.calls[0]
    assert url.endswith(f"/models/{GEMINI_MODEL}:generateContent")
    assert params["key"] == "gemini-key"
    generation_config = payload.get("generationConfig")
    assert isinstance(generation_config, dict)
    assert generation_config["maxOutputTokens"] == GEMINI_MAX_OUTPUT_TOKENS


async def test_generate_commentary_fallback_on_http_error():
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    response = httpx.Response(
        500,
        json={"error": {"message": "boom"}},
        request=httpx.Request("POST", endpoint),
    )
    fake_client = _FakeClient(responses=[response])
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_retries_on_429_then_succeeds():
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    limited = httpx.Response(
        429,
        headers={"Retry-After": "0.01"},
        request=httpx.Request("POST", endpoint),
    )
    success = httpx.Response(
        200,
        json={"candidates": [{"content": {"parts": [{"text": "Recovered after retry."}]}}]},
        request=httpx.Request("POST", endpoint),
    )
    fake_client = _FakeClient(responses=[limited, success])
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")
    sleep_mock = AsyncMock()

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst.asyncio.sleep", sleep_mock),
    ):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == "Recovered after retry."
    assert len(fake_client.calls) == 2
    sleep_mock.assert_awaited_once()


async def test_generate_commentary_fallback_after_429_retries_exhausted():
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    limited = httpx.Response(
        429,
        request=httpx.Request("POST", endpoint),
    )
    fake_client = _FakeClient(responses=[limited, limited, limited])
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")
    sleep_mock = AsyncMock()

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst.asyncio.sleep", sleep_mock),
    ):
        result = await generate_commentary(_sample_analytics(), api_key="gemini-key", client=fake_client)

    assert result == FALLBACK_COMMENTARY
    assert len(fake_client.calls) == 3
    assert sleep_mock.await_count == 2


async def test_generate_commentary_fallback_on_unexpected_exception():
    class _BrokenClient:
        async def post(self, url: str, *, params: dict[str, str], json: dict[str, object]) -> httpx.Response:
            raise RuntimeError("unexpected")

    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(
            _sample_analytics(),
            api_key="gemini-key",
            client=_BrokenClient(),  # type: ignore[arg-type]
        )

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_when_key_missing():
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics())

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_uses_db_key_when_env_missing(tmp_path):
    db_path = tmp_path / "ai.db"
    await init_db(db_path)
    await GeminiStore(db_path).set("gemini-db-key")

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    response = httpx.Response(
        200,
        json={"candidates": [{"content": {"parts": [{"text": "From DB key."}]}}]},
        request=httpx.Request("POST", endpoint),
    )
    fake_client = _FakeClient(responses=[response])

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), client=fake_client)

    assert result == "From DB key."
    _, params, _ = fake_client.calls[0]
    assert params["key"] == "gemini-db-key"
