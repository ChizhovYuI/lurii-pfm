"""Tests for the AI analyst orchestrator."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import SecretStr

from pfm.ai.analyst import (
    GEMINI_MAX_OUTPUT_TOKENS,
    SectionInputContext,
    _escape_newlines_in_json_strings,
    _finalize_commentary_text,
    _is_valid_section_body,
    _normalize_section_body,
    _parse_sections,
    generate_commentary,
    generate_commentary_with_model,
)
from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, CommentarySection
from pfm.ai.prompts import REPORT_SECTION_SPECS, AnalyticsSummary
from pfm.db.ai_report_memory_store import AIReportMemoryStore
from pfm.db.ai_store import AIProviderStore
from pfm.db.models import init_db


def _sample_analytics() -> AnalyticsSummary:
    return AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(1000),
        allocation_by_asset='[{"asset":"BTC","usd_value":"700","asset_type":"crypto","percentage":"70"}]',
        allocation_by_source='[{"source":"okx","usd_value":"700","percentage":"70"}]',
        allocation_by_category='[{"category":"crypto","usd_value":"700","percentage":"70"}]',
        currency_exposure='[{"currency":"USD","usd_value":"900","percentage":"90"}]',
        risk_metrics='{"concentration_percentage":"70"}',
        recent_transactions=(
            '[{"date":"2024-01-14","source":"ibkr-main","type":"trade","asset":"VWRA","amount":"37.20",'
            '"usd_value":"5000","counterparty_asset":"GBP","counterparty_amount":"5000","trade_side":"buy"}]'
        ),
        internal_conversions=(
            '[{"date":"2024-01-14","source":"ibkr-main","from_asset":"GBP","from_amount":"5000","to_asset":"VWRA",'
            '"to_amount":"37.20","usd_value":"5000","trade_side":"buy"}]'
        ),
        currency_flow_bridge=(
            '[{"currency":"GBP","previous_amount":"5000","current_amount":"0","delta_amount":"-5000",'
            '"delta_usd_value":"-6400","explained_by_external_inflows":"0","explained_by_external_outflows":"0",'
            '"explained_by_income":"0","explained_by_trade_spend":"5000","explained_by_trade_proceeds":"0",'
            '"residual_unexplained":"0"}]'
        ),
    )


def _section_text(index: int) -> str:
    spec = REPORT_SECTION_SPECS[index - 1]
    if spec.structure == "paragraph_then_bullets" and spec.title == "Market Context":
        return (
            "Weekly movement was driven mainly by internal redeployment and recent flows rather than by pure FX moves. "
            "The data points to asset purchases funded from existing cash balances.\n\n"
            "- GBP appears to have been redeployed into VWRA purchases.\n"
            "- Remaining valuation noise looks secondary to the conversion itself."
        )
    if spec.structure == "two_paragraphs":
        return (
            "The portfolio remains diversified across several buckets, although concentration still sits above an "
            "ideal benchmark. The largest positions are meaningful but not isolated from the rest of the portfolio.\n\n"
            "Liquidity and yield exposure still fit the stated profile, with cash, stablecoins, and income-bearing "
            "positions providing flexibility without fully crowding out long-term growth assets."
        )
    if spec.structure == "paragraph_then_bullets":
        return (
            "Only a few rebalancing ideas are justified by the current data, and they mostly relate to concentration "
            "and cash deployment.\n\n"
            "- Trim oversized fiat concentration when it is no longer intentional.\n"
            "- Redeploy excess cash into target long-term holdings gradually."
        )
    if spec.structure == "bullets_only":
        return (
            "- GBP concentration remains elevated relative to the rest of the portfolio.\n"
            "- DeFi yield exposure adds counterparty and smart-contract risk.\n"
            "- Stale source data can reduce confidence in short-term conclusions."
        )
    return (
        "Weekly priorities should stay practical and limited to the clearest actions.\n\n"
        "1. Review the biggest concentration risk.\n"
        "2. Confirm liquidity buffers remain adequate.\n"
        "3. Execute only the highest-conviction rebalance."
    )


def _deepseek_json_report() -> str:
    return json.dumps(
        {
            "sections": [
                {
                    "title": "Market Context",
                    "description": (
                        "Weekly movement was mainly transaction-driven.\n\n"
                        "- External flows added capital.\n"
                        "- Internal conversions redeployed GBP into long-term assets.\n"
                        "- Residual market and FX effects were secondary."
                    ),
                },
                {
                    "title": "Portfolio Health Assessment",
                    "description": (
                        "Diversification remains acceptable across the main portfolio buckets. "
                        "Concentration is elevated in a few areas but not extreme.\n\n"
                        "Liquidity and income exposure remain aligned with the stated profile, "
                        "with enough flexibility to keep contributing over time."
                    ),
                },
                {
                    "title": "Rebalancing Opportunities",
                    "description": (
                        "Only a few allocation changes look worth acting on this week.\n\n"
                        "- Trim the largest unintended concentration if it keeps drifting higher.\n"
                        "- Deploy idle cash gradually into target holdings."
                    ),
                },
                {
                    "title": "Risk Alerts",
                    "description": (
                        "- GBP concentration remains a meaningful portfolio risk.\n"
                        "- Yield platforms still add counterparty and smart-contract exposure."
                    ),
                },
                {
                    "title": "Actionable Recommendations for Next 7 Days",
                    "description": (
                        "1. Review the largest concentration risk.\n"
                        "2. Confirm liquidity buffers remain adequate.\n"
                        "3. Make only the clearest high-conviction allocation change."
                    ),
                },
            ]
        }
    )


async def test_generate_commentary_uses_provider(tmp_path):
    db_path = tmp_path / "test.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="test-key", active=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        side_effect=[CommentaryResult(text=_section_text(i), model="test-model") for i in range(1, 6)]
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

    assert "## Market Context" in result
    assert "## Actionable Recommendations for Next 7 Days" in result
    assert mock_provider.generate_commentary.await_count == 5
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
        side_effect=[CommentaryResult(text=_section_text(i), model="gemini-2.5-pro") for i in range(1, 6)]
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

    assert "Portfolio Health Assessment" in result


async def test_generate_commentary_with_model_calls_provider_in_fixed_section_order(tmp_path):
    db_path = tmp_path / "sections.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)
    await AIReportMemoryStore(db_path).set("## Investment Profile\nGoal: FIRE.")

    prompts: list[str] = []

    async def _generate(system_prompt: str, user_prompt: str, *, max_output_tokens: int = 4096) -> CommentaryResult:
        prompts.append(user_prompt)
        return CommentaryResult(text=_section_text(len(prompts)), model="gemini-2.5-flash")

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=_generate)
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert [section.title for section in result.sections] == [spec.title for spec in REPORT_SECTION_SPECS]
    assert len(prompts) == len(REPORT_SECTION_SPECS)
    assert 'Write only the body for the section titled "Market Context".' in prompts[0]
    assert 'Write only the body for the section titled "Portfolio Health Assessment".' in prompts[1]
    assert "<investor_memory>" in prompts[0]
    assert "Goal: FIRE." in prompts[0]
    assert "<prior_sections>" in prompts[1]
    assert "## Market Context" in prompts[1]
    assert result.model == "gemini-2.5-flash"


async def test_generate_commentary_with_model_uses_single_shot_json_for_deepseek_chat(tmp_path):
    db_path = tmp_path / "deepseek-json.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("deepseek", api_key="key", model="deepseek-chat", active=True)

    response_text = _deepseek_json_report()

    mock_provider = MagicMock()
    mock_provider.generate_commentary_json = AsyncMock(
        return_value=CommentaryResult(
            text=response_text,
            model="deepseek-chat",
            provider="deepseek",
            finish_reason="stop",
        )
    )
    mock_provider.generate_commentary = AsyncMock()
    mock_provider.close = AsyncMock()
    mock_provider.name = "deepseek"
    mock_provider._model = "deepseek-chat"

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert len(result.sections) == 5
    assert result.generation_meta["strategy"] == "deepseek_json_single_shot"
    assert result.generation_meta["status"] == "generated"
    assert mock_provider.generate_commentary_json.await_count == 1
    mock_provider.generate_commentary.assert_not_called()


async def test_generate_commentary_with_model_retries_single_shot_json_once(tmp_path):
    db_path = tmp_path / "deepseek-json-retry.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("deepseek", api_key="key", model="deepseek-chat", active=True)

    valid_response = _deepseek_json_report()

    mock_provider = MagicMock()
    mock_provider.generate_commentary_json = AsyncMock(
        side_effect=[
            CommentaryResult(text="{bad json", model="deepseek-chat", provider="deepseek", finish_reason="stop"),
            CommentaryResult(text=valid_response, model="deepseek-chat", provider="deepseek", finish_reason="stop"),
        ]
    )
    mock_provider.generate_commentary = AsyncMock()
    mock_provider.close = AsyncMock()
    mock_provider.name = "deepseek"
    mock_provider._model = "deepseek-chat"

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert len(result.sections) == 5
    assert result.generation_meta["attempts"] == 2
    assert mock_provider.generate_commentary_json.await_count == 2


async def test_generate_commentary_with_model_single_shot_json_hard_fails_after_retry(tmp_path):
    db_path = tmp_path / "deepseek-json-hard-fail.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("deepseek", api_key="key", model="deepseek-chat", active=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary_json = AsyncMock(
        side_effect=[
            CommentaryResult(text="{bad json", model="deepseek-chat", provider="deepseek", finish_reason="stop"),
            CommentaryResult(text="{still bad", model="deepseek-chat", provider="deepseek", finish_reason="stop"),
        ]
    )
    mock_provider.generate_commentary = AsyncMock()
    mock_provider.close = AsyncMock()
    mock_provider.name = "deepseek"
    mock_provider._model = "deepseek-chat"

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.sections == ()
    assert result.text == ""
    assert result.error == "JSON output was invalid after retry."
    assert result.generation_meta["strategy"] == "deepseek_json_single_shot"
    assert result.generation_meta["status"] == "failed"
    assert result.generation_meta["reason"] == "invalid_json"


async def test_generate_commentary_with_model_retries_invalid_section_once(tmp_path):
    db_path = tmp_path / "retry.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    responses = [CommentaryResult(text='{"bad": "json"}', model="gemini-2.5-flash")]
    responses.extend(CommentaryResult(text=_section_text(i), model="gemini-2.5-flash") for i in range(1, 6))

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=responses)
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert len(result.sections) == 5
    assert mock_provider.generate_commentary.await_count == 6
    second_prompt = mock_provider.generate_commentary.await_args_list[1].args[1]
    assert "<retry_instruction>" in second_prompt


async def test_generate_commentary_with_model_falls_back_for_one_failed_section(tmp_path):
    db_path = tmp_path / "partial-fallback.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    responses = [
        CommentaryResult(text=_section_text(1), model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(2), model="gemini-2.5-flash"),
        CommentaryResult(text='{"oops": "json"}', model="gemini-2.5-flash"),
        CommentaryResult(text='{"oops": "still json"}', model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(4), model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(5), model="gemini-2.5-flash"),
    ]

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=responses)
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert len(result.sections) == 5
    assert result.sections[2].title == "Rebalancing Opportunities"
    assert result.sections[2].description == REPORT_SECTION_SPECS[2].fallback_text
    assert "\n\n" in result.sections[2].description
    assert "- " in result.sections[2].description
    assert result.error == "Some sections used fallback text: Rebalancing Opportunities."


async def test_generate_commentary_with_model_returns_global_fallback_when_all_sections_fail(tmp_path):
    db_path = tmp_path / "all-fail.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        side_effect=[CommentaryResult(text="[]", model="gemini-2.5-flash") for _ in range(10)]
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.text == FALLBACK_COMMENTARY
    assert result.sections == ()
    assert result.error == "All report sections fell back to the generic commentary."


async def test_generate_commentary_with_model_reports_progress(tmp_path):
    db_path = tmp_path / "progress.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    progress: list[tuple[int, int, str]] = []
    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        side_effect=[CommentaryResult(text=_section_text(i), model="gemini-2.5-flash") for i in range(1, 6)]
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        await generate_commentary_with_model(
            _sample_analytics(),
            db_path=db_path,
            progress_callback=lambda completed, total, title: progress.append((completed, total, title)),
        )

    assert progress[0] == (0, 5, "Market Context")
    assert progress[1] == (1, 5, "Portfolio Health Assessment")
    assert progress[-1] == (4, 5, "Actionable Recommendations for Next 7 Days")


def test_normalize_section_body_compacts_blank_lines_between_list_items():
    raw = "Short intro paragraph.\n\n* First item\n\n* Second item\n\n* Third item"
    assert _normalize_section_body(raw) == (
        "Short intro paragraph.\n\n- First item\n- Second item\n- Third item"
    )


def test_is_valid_section_body_accepts_dense_market_context():
    body = (
        "GBP dropped sharply this week and created most of the portfolio weakness. This was a major negative move for "
        "the currency and it reduced the portfolio materially.\n- External flows remained positive.\n"
        "- Internal conversions likely funded purchases.\n- Residual market pressure was secondary."
    )
    assert _is_valid_section_body(body, REPORT_SECTION_SPECS[0], context=SectionInputContext()) is True


async def test_generate_commentary_with_model_accepts_first_valid_attempt_without_retry(tmp_path):
    db_path = tmp_path / "semantic-retry.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    responses = [
        CommentaryResult(
            text=(
                "GBP dropped sharply this week and created most of the portfolio weakness. This was a major negative "
                "move for the currency and it reduced the portfolio materially without any notable rebalancing."
            ),
            model="gemini-2.5-flash",
        ),
        CommentaryResult(text=_section_text(2), model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(3), model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(4), model="gemini-2.5-flash"),
        CommentaryResult(text=_section_text(5), model="gemini-2.5-flash"),
    ]

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=responses)
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.sections[0].description.startswith("GBP dropped sharply this week")
    assert mock_provider.generate_commentary.await_count == 5


async def test_generate_commentary_with_model_records_generation_meta_for_fallbacks(tmp_path):
    db_path = tmp_path / "generation-meta.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    responses = [
        CommentaryResult(text=_section_text(1), model="gemini-2.5-flash", provider="gemini"),
        CommentaryResult(text="[]", model="gemini-2.5-flash", provider="gemini", finish_reason="length"),
        CommentaryResult(text="[]", model="gemini-2.5-flash", provider="gemini", finish_reason="length"),
        CommentaryResult(text=_section_text(3), model="gemini-2.5-flash", provider="gemini"),
        CommentaryResult(text=_section_text(4), model="gemini-2.5-flash", provider="gemini"),
        CommentaryResult(text=_section_text(5), model="gemini-2.5-flash", provider="gemini"),
    ]

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=responses)
    mock_provider.close = AsyncMock()
    mock_provider.name = "gemini"
    mock_provider._model = "gemini-2.5-flash"

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.generation_meta is not None
    assert result.generation_meta["provider"] == "gemini"
    assert result.generation_meta["model"] == "gemini-2.5-flash"
    diagnostics = result.generation_meta["sections"]
    assert diagnostics[1] == {
        "title": "Portfolio Health Assessment",
        "status": "fallback",
        "reason": "length_truncated",
        "finish_reason": "length",
    }


async def test_generate_commentary_with_model_reasoner_empty_content_triggers_retry(tmp_path):
    db_path = tmp_path / "deepseek-reasoner.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("deepseek", api_key="key", model="deepseek-reasoner", active=True)

    recorded_max_tokens: list[int] = []
    responses = [
        CommentaryResult(
            text="",
            model="deepseek-reasoner",
            provider="deepseek",
            reasoning_text="Reasoning happened but no final answer arrived.",
            finish_reason="length",
            error="deepseek API returned no final answer before reasoning budget was exhausted",
        ),
        CommentaryResult(text=_section_text(1), model="deepseek-reasoner", provider="deepseek"),
        CommentaryResult(text=_section_text(2), model="deepseek-reasoner", provider="deepseek"),
        CommentaryResult(text=_section_text(3), model="deepseek-reasoner", provider="deepseek"),
        CommentaryResult(text=_section_text(4), model="deepseek-reasoner", provider="deepseek"),
        CommentaryResult(text=_section_text(5), model="deepseek-reasoner", provider="deepseek"),
    ]

    async def _generate(_system_prompt: str, _user_prompt: str, *, max_output_tokens: int = 4096) -> CommentaryResult:
        recorded_max_tokens.append(max_output_tokens)
        return responses[len(recorded_max_tokens) - 1]

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(side_effect=_generate)
    mock_provider.close = AsyncMock()
    mock_provider.name = "deepseek"
    mock_provider._model = "deepseek-reasoner"

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert recorded_max_tokens[0] >= 6000
    assert mock_provider.generate_commentary.await_count == 6
    assert result.generation_meta is not None
    assert result.generation_meta["sections"][0]["status"] == "retried"
    assert result.generation_meta["sections"][0]["reason"] == "empty_content_with_reasoning"


async def test_generate_commentary_with_model_fallback_text_stays_structured(tmp_path):
    db_path = tmp_path / "structured-fallback.db"
    await init_db(db_path)
    await AIProviderStore(db_path).add("gemini", api_key="key", active=True)

    mock_provider = MagicMock()
    mock_provider.generate_commentary = AsyncMock(
        side_effect=[CommentaryResult(text="[]", model="gemini-2.5-flash") for _ in range(10)]
    )
    mock_provider.close = AsyncMock()

    settings = MagicMock()
    settings.database_path = db_path
    settings.gemini_api_key = SecretStr("")

    with (
        patch("pfm.ai.analyst.get_settings", return_value=settings),
        patch("pfm.ai.analyst._build_provider", return_value=mock_provider),
    ):
        result = await generate_commentary_with_model(_sample_analytics(), db_path=db_path)

    assert result.sections == ()
    assert result.text == FALLBACK_COMMENTARY


def test_finalize_commentary_text_preserves_incomplete_tail_line():
    text = "Market context.\nPortfolio health is stable.\nReview your target"
    assert _finalize_commentary_text(text).endswith("Review your target")


def test_finalize_commentary_text_normalizes_line_endings_and_whitespace():
    text = "Market context.\r\nPortfolio health is stable.\r\n"
    assert _finalize_commentary_text(text) == "Market context.\nPortfolio health is stable."


def test_gemini_max_output_tokens_constant():
    assert GEMINI_MAX_OUTPUT_TOKENS == 4096


def test_parse_sections_valid_json():
    raw = '[{"title": "Market Context", "description": "BTC at **$95k**."}]'
    sections = _parse_sections(raw)
    assert sections == (CommentarySection(title="Market Context", description="BTC at **$95k**."),)


def test_parse_sections_with_code_fence():
    raw = '```json\n[{"title": "Risk Alerts", "description": "High concentration."}]\n```'
    sections = _parse_sections(raw)
    assert sections[0].title == "Risk Alerts"


def test_parse_sections_plain_text_returns_empty():
    assert _parse_sections("This is just plain text commentary.") == ()


def test_escape_newlines_in_json_strings_fixes_bare_newlines():
    raw = '{"description": "line1\nline2"}'
    fixed = _escape_newlines_in_json_strings(raw)
    assert fixed == '{"description": "line1\\nline2"}'


def test_parse_sections_recovers_complete_items_from_truncated_array():
    raw = (
        '[{"title": "Market Context", "description": "BTC at **$95k**."}, '
        '{"title": "Risk Alerts", "description": "High con'
    )
    sections = _parse_sections(raw)
    assert sections == (
        CommentarySection(title="Market Context", description="BTC at **$95k**."),
        CommentarySection(title="Risk Alerts", description="High con"),
    )


def test_parse_sections_recovers_from_truncated_fenced_json():
    raw = (
        "```json\n"
        "[\n"
        '  {"title": "Market", "description": "BTC up."},\n'
        '  {"title": "Risk", "description": "Sharpe improv'
    )
    sections = _parse_sections(raw)
    assert sections == (
        CommentarySection(title="Market", description="BTC up."),
        CommentarySection(title="Risk", description="Sharpe improv"),
    )
