"""Tests for section-based AI prompt templates."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.ai.base import CommentarySection
from pfm.ai.prompts import (
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    AnalyticsSummary,
    render_report_section_prompt,
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
    assert "Separate paragraphs with a blank line" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Do not return one long block of text" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Fiat balance bridge and Internal conversions" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert [spec.title for spec in REPORT_SECTION_SPECS] == [
        "Market Context",
        "Portfolio Health Assessment",
        "Rebalancing Opportunities",
        "Risk Alerts",
        "Actionable Recommendations for Next 7 Days",
    ]


def test_render_report_section_prompt_formats_analytics():
    prompt = render_report_section_prompt(REPORT_SECTION_SPECS[0], _sample_analytics())

    assert "As of date: 2024-01-15" in prompt
    assert "Net worth (USD): 12345.67" in prompt
    assert '"asset": "BTC"' in prompt
    assert '"category": "crypto"' in prompt
    assert '"currency": "USD"' in prompt
    assert '"concentration_percentage": "56.70%"' in prompt
    assert "Fiat balance bridge" in prompt
    assert '"explained_by_trade_spend": "5000.00"' in prompt
    assert "Internal conversions" in prompt
    assert '"from_asset": "GBP"' in prompt
    assert '"to_asset": "VWRA"' in prompt
    assert "External capital and income flows" in prompt
    assert "Recent transactions (audit trail, last 7 days)" in prompt
    assert "<analytics>" in prompt
    assert "<investor_memory>" not in prompt


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
