"""Prompt templates for AI weekly commentary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date
    from decimal import Decimal

WEEKLY_REPORT_SYSTEM_PROMPT = """
You are a personal financial advisor. Analyze portfolio analytics and produce concise, practical guidance.
Prioritize risk-aware recommendations and explicitly call out data limitations when confidence is low.
Keep advice specific to the provided portfolio data and avoid generic education content.
""".strip()

WEEKLY_REPORT_USER_PROMPT_TEMPLATE = """
You are given portfolio analytics for {as_of_date}.
Net worth (USD): {net_worth_usd}

Allocation by asset:
{allocation_by_asset}

Allocation by source:
{allocation_by_source}

Allocation by category:
{allocation_by_category}

Currency exposure:
{currency_exposure}

Risk metrics:
{risk_metrics}

PnL:
{pnl}

Weekly PnL by asset:
{weekly_pnl_by_asset}

Write a report with these sections:
1) Market context
2) Portfolio health assessment
3) Rebalancing opportunities
4) Risk alerts
5) Actionable recommendations for next 7 days

Rules:
- Ground every claim in provided data.
- If data is missing or noisy, state that clearly.
- Use concise bullet points and include concrete numbers.
""".strip()


@dataclass(frozen=True, slots=True)
class AnalyticsSummary:
    """Serializable analytics payload used by AI commentary."""

    as_of_date: date
    net_worth_usd: Decimal
    allocation_by_asset: str
    allocation_by_source: str
    allocation_by_category: str
    currency_exposure: str
    risk_metrics: str
    pnl: str
    weekly_pnl_by_asset: str


def render_weekly_report_user_prompt(analytics: AnalyticsSummary) -> str:
    """Render the user prompt from analytics data."""
    return WEEKLY_REPORT_USER_PROMPT_TEMPLATE.format(
        as_of_date=analytics.as_of_date.isoformat(),
        net_worth_usd=str(analytics.net_worth_usd),
        allocation_by_asset=_pretty_json(analytics.allocation_by_asset),
        allocation_by_source=_pretty_json(analytics.allocation_by_source),
        allocation_by_category=_pretty_json(analytics.allocation_by_category),
        currency_exposure=_pretty_json(analytics.currency_exposure),
        risk_metrics=_pretty_json(analytics.risk_metrics),
        pnl=_pretty_json(analytics.pnl),
        weekly_pnl_by_asset=_pretty_json(analytics.weekly_pnl_by_asset),
    )


def _pretty_json(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    return json.dumps(parsed, indent=2, sort_keys=True)
