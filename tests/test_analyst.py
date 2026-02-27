"""Tests for Claude analyst client."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from anthropic import APIError
from anthropic.types import Message, TextBlock, Usage
from pydantic import SecretStr

from pfm.ai.analyst import CLAUDE_MAX_TOKENS, CLAUDE_MODEL, FALLBACK_COMMENTARY, generate_commentary
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


def _message(text: str) -> Message:
    return Message(
        id="msg_1",
        content=[TextBlock(type="text", text=text)],
        model=CLAUDE_MODEL,
        role="assistant",
        stop_reason="end_turn",
        stop_sequence=None,
        type="message",
        usage=Usage(input_tokens=111, output_tokens=222),
    )


async def test_generate_commentary_success_with_mock_client():
    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=_message("Portfolio looks stable."))

    result = await generate_commentary(_sample_analytics(), client=mock_client)

    assert result == "Portfolio looks stable."
    assert mock_client.messages.create.await_count == 1
    kwargs = mock_client.messages.create.await_args.kwargs
    assert kwargs["model"] == CLAUDE_MODEL
    assert kwargs["max_tokens"] == CLAUDE_MAX_TOKENS
    assert kwargs["messages"][0]["role"] == "user"


async def test_generate_commentary_fallback_on_api_error():
    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    mock_client.messages.create = AsyncMock(side_effect=APIError("boom", request=request, body=None))

    result = await generate_commentary(_sample_analytics(), client=mock_client)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_on_unexpected_exception():
    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = AsyncMock(side_effect=RuntimeError("unexpected"))

    result = await generate_commentary(_sample_analytics(), client=mock_client)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_fallback_when_key_missing():
    settings = MagicMock()
    settings.anthropic_api_key = SecretStr("")

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics())

    assert result == FALLBACK_COMMENTARY
