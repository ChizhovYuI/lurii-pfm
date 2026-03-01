"""Tests for the AI analyst orchestrator."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import SecretStr

from pfm.ai.analyst import (
    GEMINI_MAX_OUTPUT_TOKENS,
    _finalize_commentary_text,
    generate_commentary,
)
from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult
from pfm.ai.prompts import AnalyticsSummary
from pfm.db.ai_store import AIProviderStore
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
    )


async def test_generate_commentary_uses_provider(tmp_path):
    db_path = tmp_path / "test.db"
    await init_db(db_path)
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="test-key", activate=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(text="Provider response.", model="test-model")
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == "Provider response."
    mock_provider.generate_commentary.assert_awaited_once()
    mock_provider.close.assert_awaited_once()


async def test_generate_commentary_fallback_when_no_config(tmp_path):
    db_path = tmp_path / "empty.db"
    await init_db(db_path)
    settings = MagicMock()
    settings.gemini_api_key = SecretStr("")
    settings.database_path = db_path

    with patch("pfm.ai.analyst.get_settings", return_value=settings):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == FALLBACK_COMMENTARY


async def test_generate_commentary_env_fallback(tmp_path):
    db_path = tmp_path / "env.db"
    await init_db(db_path)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(text="Env key response.", model="gemini-2.5-pro")
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("env-gemini-key")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == "Env key response."


async def test_generate_commentary_uses_db_key_via_migration(tmp_path):
    db_path = tmp_path / "migrate.db"
    await init_db(db_path)
    await GeminiStore(db_path).set("gemini-db-key")

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(text="From DB key.", model="gemini-2.5-pro")
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == "From DB key."


async def test_generate_commentary_fallback_on_empty_provider_text(tmp_path):
    db_path = tmp_path / "empty_text.db"
    await init_db(db_path)
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="key", activate=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(return_value=CommentaryResult(text="", model=None))
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary(_sample_analytics(), db_path=db_path)

    assert result == FALLBACK_COMMENTARY


def test_finalize_commentary_text_preserves_incomplete_tail_line():
    text = "Market context.\nPortfolio health is stable.\nReview your target"
    assert _finalize_commentary_text(text).endswith("Review your target")


def test_finalize_commentary_text_normalizes_line_endings_and_whitespace():
    text = "Market context.\r\nPortfolio health is stable.\r\n"
    assert _finalize_commentary_text(text) == "Market context.\nPortfolio health is stable."


def test_finalize_commentary_text_preserves_section_header_tail():
    text = "Health looks stable.\n### 5) Actionable recommendations for next 7 days"
    finalized = _finalize_commentary_text(text)
    assert finalized.endswith("### 5) Actionable recommendations for next 7 days")


def test_gemini_max_output_tokens_constant():
    assert GEMINI_MAX_OUTPUT_TOKENS == 4096
