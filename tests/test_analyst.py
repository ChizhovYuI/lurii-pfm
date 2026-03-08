"""Tests for the AI analyst orchestrator."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import SecretStr

from pfm.ai.analyst import (
    GEMINI_MAX_OUTPUT_TOKENS,
    _escape_newlines_in_json_strings,
    _finalize_commentary_text,
    _parse_sections,
    generate_commentary,
)
from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, CommentarySection, flatten_sections
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
    await store.add("gemini", api_key="test-key", active=True)

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
    await store.add("gemini", api_key="key", active=True)

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


def test_parse_sections_valid_json():
    raw = '[{"title": "Market Context", "description": "BTC at **$95k**."}]'
    sections = _parse_sections(raw)
    assert len(sections) == 1
    assert sections[0] == CommentarySection(title="Market Context", description="BTC at **$95k**.")


def test_parse_sections_with_code_fence():
    raw = '```json\n[{"title": "Risk Alerts", "description": "High concentration."}]\n```'
    sections = _parse_sections(raw)
    assert len(sections) == 1
    assert sections[0].title == "Risk Alerts"


def test_parse_sections_plain_text_returns_empty():
    raw = "This is just plain text commentary."
    sections = _parse_sections(raw)
    assert sections == ()


def test_parse_sections_skips_missing_fields():
    raw = '[{"title": "Good", "description": "OK"}, {"title": "", "description": "no title"}, {"other": 1}]'
    sections = _parse_sections(raw)
    assert len(sections) == 1
    assert sections[0].title == "Good"


def test_flatten_sections():
    sections = (
        CommentarySection(title="Market Context", description="BTC is up."),
        CommentarySection(title="Risk Alerts", description="Low diversification."),
    )
    flat = flatten_sections(sections)
    assert "Market Context" in flat
    assert "BTC is up." in flat
    assert "Risk Alerts" in flat
    assert "Low diversification." in flat


def test_finalize_strips_think_blocks():
    text = '<think>\nLet me analyze the portfolio...\n</think>\n[{"title": "A", "description": "B"}]'
    result = _finalize_commentary_text(text)
    assert "<think>" not in result
    assert result == '[{"title": "A", "description": "B"}]'


def test_finalize_strips_multiple_think_blocks():
    text = "<think>first</think>Hello<think>second</think> world"
    result = _finalize_commentary_text(text)
    assert result == "Hello world"


def test_parse_sections_with_preamble():
    raw = 'Here is my analysis:\n[{"title": "Market", "description": "BTC up."}]'
    sections = _parse_sections(raw)
    assert len(sections) == 1
    assert sections[0].title == "Market"


def test_parse_sections_think_block_then_json():
    """Combined scenario: <think> block + preamble + JSON array."""
    raw = '<think>\nreasoning here\n</think>\nSure, here is the analysis:\n[{"title": "Risk", "description": "Low."}]'
    finalized = _finalize_commentary_text(raw)
    sections = _parse_sections(finalized)
    assert len(sections) == 1
    assert sections[0] == CommentarySection(title="Risk", description="Low.")


def test_parse_sections_preamble_no_json_array():
    """Preamble text with no JSON array still returns empty."""
    raw = "Here is my analysis of the portfolio. It looks good overall."
    sections = _parse_sections(raw)
    assert sections == ()


def test_escape_newlines_in_json_strings_fixes_bare_newlines():
    raw = '{"description": "line1\nline2"}'
    fixed = _escape_newlines_in_json_strings(raw)
    assert fixed == '{"description": "line1\\nline2"}'


def test_escape_newlines_preserves_already_escaped():
    raw = '{"description": "line1\\nline2"}'
    fixed = _escape_newlines_in_json_strings(raw)
    assert fixed == raw


def test_parse_sections_with_newlines_in_strings():
    """Gemini-style JSON with actual newlines inside description values."""
    raw = (
        "```json\n"
        "[\n"
        "  {\n"
        '    "title": "Recommendations",\n'
        '    "description": "1. Buy BTC.\n    2. Hold ETH.\n    3. Sell DOGE."\n'
        "  }\n"
        "]\n"
        "```"
    )
    sections = _parse_sections(raw)
    assert len(sections) == 1
    assert sections[0].title == "Recommendations"
    assert "1. Buy BTC." in sections[0].description
    assert "2. Hold ETH." in sections[0].description


def test_parse_sections_recovers_complete_items_from_truncated_array():
    raw = (
        '[{"title": "Market Context", "description": "BTC at **$95k**."}, '
        '{"title": "Risk Alerts", "description": "High con'
    )
    sections = _parse_sections(raw)
    assert sections == (CommentarySection(title="Market Context", description="BTC at **$95k**."),)


def test_parse_sections_recovers_from_truncated_fenced_json():
    raw = (
        "```json\n"
        "[\n"
        '  {"title": "Market", "description": "BTC up."},\n'
        '  {"title": "Risk", "description": "Sharpe improv'
    )
    sections = _parse_sections(raw)
    assert sections == (CommentarySection(title="Market", description="BTC up."),)


def test_parse_sections_returns_empty_when_first_object_is_incomplete():
    raw = '[{"title": "Market", "description": "BTC'
    sections = _parse_sections(raw)
    assert sections == ()


async def test_generate_commentary_with_model_parses_sections(tmp_path):
    """When LLM returns valid JSON sections, result includes parsed sections."""

    db_path = tmp_path / "sections.db"
    await init_db(db_path)
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="key", active=True)

    sections_json = json.dumps(
        [
            {"title": "Market Context", "description": "BTC at **$95k**."},
            {"title": "Risk Alerts", "description": "High HHI."},
        ]
    )

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(text=sections_json, model="gemini-2.5-flash")
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    from pfm.ai.analyst import generate_commentary_with_model

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert len(result.sections) == 2
    assert result.sections[0].title == "Market Context"
    assert result.sections[1].description == "High HHI."
    assert "Market Context" in result.text
    assert "BTC at **$95k**." in result.text


async def test_generate_commentary_with_model_uses_recovered_sections_from_truncated_array(tmp_path):
    db_path = tmp_path / "truncated.db"
    await init_db(db_path)
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="key", active=True)

    truncated = (
        '[{"title": "Market Context", "description": "BTC at **$95k**."}, '
        '{"title": "Risk Alerts", "description": "High con'
    )

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(text=truncated, model="gemini-2.5-flash")
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    from pfm.ai.analyst import generate_commentary_with_model

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.sections == (CommentarySection(title="Market Context", description="BTC at **$95k**."),)
    assert result.text == "Market Context\nBTC at **$95k**."


async def test_generate_commentary_with_model_returns_preparsed_sections(tmp_path):
    """When provider returns CommentaryResult with pre-populated sections, orchestrator returns as-is."""
    db_path = tmp_path / "preparsed.db"
    await init_db(db_path)
    store = AIProviderStore(db_path)
    await store.add("openrouter", api_key="key", active=True)

    pre_sections = (
        CommentarySection(title="Instructor Parsed", description="Already structured."),
        CommentarySection(title="Risk", description="Low risk."),
    )
    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        return_value=CommentaryResult(
            text="Instructor Parsed\nAlready structured.\n\nRisk\nLow risk.",
            model="qwen3-235b",
            sections=pre_sections,
        )
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    from pfm.ai.analyst import generate_commentary_with_model

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    # Should return the result as-is without re-parsing
    assert result.sections == pre_sections
    assert result.model == "qwen3-235b"
    assert "Instructor Parsed" in result.text
