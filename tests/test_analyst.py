"""Tests for Gemini analyst client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import httpx
from pydantic import SecretStr

from pfm.ai.analyst import FALLBACK_COMMENTARY, GEMINI_MAX_OUTPUT_TOKENS, GEMINI_MODEL, generate_commentary
from pfm.ai.prompts import AnalyticsSummary


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
        result = await generate_commentary(_sample_analytics(), client=fake_client)

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
        result = await generate_commentary(_sample_analytics(), client=fake_client)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_on_unexpected_exception():
    class _BrokenClient:
        async def post(self, url: str, *, params: dict[str, str], json: dict[str, object]) -> httpx.Response:
            raise RuntimeError("unexpected")

    settings = MagicMock()
    settings.gemini_api_key = SecretStr("gemini-key")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), client=_BrokenClient())  # type: ignore[arg-type]

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_when_key_missing():
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics())

    assert result == FALLBACK_COMMENTARY
