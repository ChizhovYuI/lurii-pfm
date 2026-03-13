"""Tests for section-based AI prompt templates."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.ai.base import CommentarySection
from pfm.ai.prompts import (
    GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    AnalyticsSummary,
    render_gemini_weekly_report_json_prompt,
    render_report_section_prompt,
    render_weekly_report_json_prompt,
)


def _sample_analytics() -> AnalyticsSummary:
    return AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal("12345.67"),
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000","asset_type":"crypto","percentage":"56.7"}]',
        allocation_by_source='[{"source":"okx","usd_value":"7000","percentage":"56.7"}]',
        allocation_by_category='[{"category":"crypto","usd_value":"7000","percentage":"56.7"}]',
        currency_exposure='[{"currency":"USD","usd_value":"5000","percentage":"40"}]',
        risk_metrics='{"concentration_percentage":"56.7"}',
        recent_transactions=(
            '[{"date":"2024-01-14","source":"ibkr-main","type":"trade","asset":"VWRA","amount":"37.20",'
            '"usd_value":"5000","counterparty_asset":"GBP","counterparty_amount":"5000","trade_side":"buy"}]'
        ),
        capital_flows='[{"date":"2024-01-13","source":"wise-main","kind":"external_inflow","asset":"USDC","amount":"1500","usd_value":"1500"}]',
        internal_conversions=(
            '[{"date":"2024-01-14","source":"ibkr-main","from_asset":"GBP","from_amount":"5000","to_asset":"VWRA",'
            '"to_amount":"37.20","usd_value":"5000","trade_side":"buy"}]'
        ),
        currency_flow_bridge=(
            '[{"currency":"GBP","previous_amount":"5000","current_amount":"0","delta_amount":"-5000",'
            '"delta_usd_value":"-6400","explained_by_external_inflows":"0","explained_by_external_outflows":"0",'
            '"explained_by_income":"0","explained_by_trade_spend":"5000","explained_by_trade_proceeds":"0",'
            '"residual_unexplained":"0","likely_counterparties":[{"asset":"VWRA","amount":"37.20","direction":"bought"}]}]'
        ),
    )


def test_prompt_templates_have_required_sections():
    assert "personal financial advisor" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Return only the markdown body" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Do not return JSON" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Analyze only the current portfolio snapshot and investor context." in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Do not describe historical changes, trends, or prior-state comparisons" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Separate paragraphs with a blank line" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Put a blank line before the first bullet list or numbered list" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Start every bullet and numbered item on its own new line" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Do not return one long block of text" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Do not leave blank lines between adjacent bullet items or numbered items" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert [spec.title for spec in REPORT_SECTION_SPECS] == [
        "Market Context",
        "Portfolio Health Assessment",
        "Rebalancing Opportunities",
        "Risk Alerts",
        "Actionable Recommendations for Next 7 Days",
    ]
    assert "Return one valid JSON object only" in WEEKLY_REPORT_JSON_SYSTEM_PROMPT
    assert 'The JSON must contain a top-level "sections" array' in WEEKLY_REPORT_JSON_SYSTEM_PROMPT
    assert "Analyze only the current portfolio snapshot and investor context." in WEEKLY_REPORT_JSON_SYSTEM_PROMPT
    assert (
        "Do not leave blank lines between adjacent bullet items or numbered items" in WEEKLY_REPORT_JSON_SYSTEM_PROMPT
    )
    assert "Start every bullet and numbered item on its own new line" in WEEKLY_REPORT_JSON_SYSTEM_PROMPT
    gemini_json_prompt_lower = GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT.lower()
    assert "return structured json only" in gemini_json_prompt_lower
    assert "do not wrap the response in markdown or code fences" in gemini_json_prompt_lower
    assert "analyze only the current portfolio snapshot and investor context." in gemini_json_prompt_lower


def test_render_report_section_prompt_formats_analytics():
    prompt = render_report_section_prompt(REPORT_SECTION_SPECS[0], _sample_analytics())

    assert "As of date: 2024-01-15" in prompt
    assert "Net worth (USD): 12345.67" in prompt
    assert '"asset": "BTC"' in prompt
    assert '"category": "crypto"' in prompt
    assert '"currency": "USD"' in prompt
    assert '"concentration_percentage": "56.70%"' in prompt
    assert "<analytics>" in prompt
    assert "<investor_memory>" not in prompt
    assert "current portfolio positioning" in prompt
    assert "liquidity or currency posture" in prompt
    assert "Do not describe week-over-week changes" in prompt
    assert "Fiat balance bridge" not in prompt
    assert "Recent transactions (audit trail, last 7 days)" not in prompt
    assert "Do not leave blank lines between bullet items" in prompt
    assert "Never place bullets inline after a sentence" in prompt


def test_render_report_section_prompt_includes_investor_memory():
    prompt = render_report_section_prompt(
        REPORT_SECTION_SPECS[1],
        _sample_analytics(),
        investor_memory="## Location & Expenses\nLiving in Thailand.",
    )

    assert "<investor_memory>" in prompt
    assert "Living in Thailand." in prompt


def test_render_report_section_prompt_includes_clipped_prior_sections():
    prior = (
        CommentarySection(title="Market Context", description="A" * 500),
        CommentarySection(title="Portfolio Health Assessment", description="B" * 500),
        CommentarySection(title="Rebalancing Opportunities", description="C" * 500),
    )

    prompt = render_report_section_prompt(
        REPORT_SECTION_SPECS[3],
        _sample_analytics(),
        prior_sections=prior,
    )

    assert "<prior_sections>" in prompt
    assert "## Market Context" in prompt
    assert "## Portfolio Health Assessment" in prompt
    assert "## Rebalancing Opportunities" in prompt
    assert "..." in prompt


def test_render_weekly_report_json_prompt_includes_exact_titles_and_json_contract():
    prompt = render_weekly_report_json_prompt(_sample_analytics())

    assert "Return one valid JSON object only." in prompt
    assert '"title": "Market Context"' in prompt
    assert '"title": "Portfolio Health Assessment"' in prompt
    assert '"title": "Rebalancing Opportunities"' in prompt
    assert '"title": "Risk Alerts"' in prompt
    assert '"title": "Actionable Recommendations for Next 7 Days"' in prompt
    assert "Analyze only the current snapshot and investor context." in prompt
    assert "Do not mention last-7-day changes, prior snapshots, or historical comparisons." in prompt
    assert "No blank lines between bullet items or numbered items." in prompt
    assert "Start every bullet and numbered item on its own new line." in prompt
    assert "<analytics>" in prompt


def test_render_weekly_report_json_prompt_includes_investor_memory():
    prompt = render_weekly_report_json_prompt(
        _sample_analytics(),
        investor_memory="## Investment Profile\nGoal: FIRE.",
    )

    assert "<investor_memory>" in prompt
    assert "Goal: FIRE." in prompt


def test_render_gemini_weekly_report_json_prompt_includes_exact_titles_and_rules():
    prompt = render_gemini_weekly_report_json_prompt(_sample_analytics())

    assert "Return one valid JSON object only." in prompt
    assert '"title": "Market Context"' in prompt
    assert '"title": "Portfolio Health Assessment"' in prompt
    assert '"title": "Rebalancing Opportunities"' in prompt
    assert '"title": "Risk Alerts"' in prompt
    assert '"title": "Actionable Recommendations for Next 7 Days"' in prompt
    assert "Do not include code fences or wrapper text." in prompt
    assert "Do not place bullets or numbered items inline after prose on the same line." in prompt
    assert "Analyze only the current snapshot and investor context." in prompt
