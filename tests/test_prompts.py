"""Tests for AI prompt templates."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.ai.prompts import (
    WEEKLY_REPORT_SYSTEM_PROMPT,
    WEEKLY_REPORT_USER_PROMPT_TEMPLATE,
    AnalyticsSummary,
    render_weekly_report_user_prompt,
)


def test_prompt_templates_have_required_sections():
    assert "personal financial advisor" in WEEKLY_REPORT_SYSTEM_PROMPT
    assert "Market context" in WEEKLY_REPORT_USER_PROMPT_TEMPLATE
    assert "Portfolio health assessment" in WEEKLY_REPORT_USER_PROMPT_TEMPLATE
    assert "Rebalancing opportunities" in WEEKLY_REPORT_USER_PROMPT_TEMPLATE
    assert "Risk alerts" in WEEKLY_REPORT_USER_PROMPT_TEMPLATE
    assert "Actionable recommendations for next 7 days" in WEEKLY_REPORT_USER_PROMPT_TEMPLATE


def test_render_weekly_report_user_prompt_formats_analytics():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal("12345.67"),
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000"}]',
        allocation_by_source='[{"source":"okx","usd_value":"7000"}]',
        allocation_by_category='[{"category":"crypto","usd_value":"7000"}]',
        currency_exposure='[{"currency":"USD","usd_value":"5000"}]',
        risk_metrics='{"concentration_percentage":"56.7"}',
        pnl='{"weekly":{"absolute_change":"120"}}',
        weekly_pnl_by_asset='[{"asset":"BTC","absolute_change":"120","percentage_change":"1.8"}]',
    )

    prompt = render_weekly_report_user_prompt(analytics)
    assert "2024-01-15" in prompt
    assert "Net worth (USD): 12345.67" in prompt
    assert '"asset": "BTC"' in prompt
    assert '"source": "okx"' in prompt
    assert '"category": "crypto"' in prompt
    assert '"currency": "USD"' in prompt
    assert '"concentration_percentage": "56.7"' in prompt
    assert '"absolute_change": "120"' in prompt
    assert "Weekly PnL by asset" in prompt
