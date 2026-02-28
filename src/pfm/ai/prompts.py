"""Prompt templates for AI weekly commentary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

WEEKLY_REPORT_SYSTEM_PROMPT = """
You are a personal financial advisor. Analyze portfolio analytics and produce concise, practical guidance.
Prioritize risk-aware recommendations and explicitly call out data limitations when confidence is low.
Keep advice specific to the provided portfolio data and avoid generic education content.
Output plain text only for Telegram:
- no markdown syntax (`#`, `*`, `**`, backticks)
- keep all five requested sections complete
- include concrete numbers from the provided data
- give enough detail to explain reasoning and actions
""".strip()

WEEKLY_REPORT_USER_PROMPT_TEMPLATE = """
You are given portfolio analytics for {as_of_date}.
Net worth (USD): {net_worth_usd}

Top holdings:
{top_holdings}

Allocation by category:
{allocation_by_category}

Risk metrics:
{risk_metrics}

PnL summary:
{pnl_summary}

Top weekly movers by asset:
{weekly_pnl_by_asset}

Write a compact report with these sections in plain text:
1) Market context
2) Portfolio health assessment
3) Rebalancing opportunities
4) Risk alerts
5) Actionable recommendations for next 7 days

Rules:
- Ground every claim in provided data.
- If data is missing or noisy, state that clearly.
- Use concise but complete lines and include concrete numbers.
- Do not use markdown symbols.
- End every line with proper punctuation.
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
    top_holdings = _compact_top_holdings(analytics.allocation_by_asset)
    allocation_by_category = _compact_allocation_by_category(analytics.allocation_by_category)
    risk_metrics = _compact_risk_metrics(analytics.risk_metrics)
    pnl_summary = _compact_pnl_summary(analytics.pnl)
    weekly_movers = _compact_weekly_movers(analytics.weekly_pnl_by_asset)
    return WEEKLY_REPORT_USER_PROMPT_TEMPLATE.format(
        as_of_date=analytics.as_of_date.isoformat(),
        net_worth_usd=_fmt_usd(analytics.net_worth_usd),
        top_holdings=_pretty_json(top_holdings),
        allocation_by_category=_pretty_json(allocation_by_category),
        risk_metrics=_pretty_json(risk_metrics),
        pnl_summary=_pretty_json(pnl_summary),
        weekly_pnl_by_asset=_pretty_json(weekly_movers),
    )


def _pretty_json(raw: str | list[dict[str, object]] | dict[str, object]) -> str:
    parsed: object
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw
    else:
        parsed = raw
    return json.dumps(parsed, indent=2, sort_keys=True)


def _compact_top_holdings(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "asset": str(row.get("asset", "UNKNOWN")),
            "asset_type": str(row.get("asset_type", "other")),
            "usd_value": _fmt_usd(row.get("usd_value", "0")),
            "percentage": _fmt_pct(row.get("percentage", "0")),
        }
        for row in rows
    ]
    compact.sort(key=lambda row: _to_decimal(row.get("usd_value", "0")), reverse=True)
    return compact[:10]


def _compact_allocation_by_category(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = []
    for row in rows:
        category = row.get("category", row.get("bucket", "unknown"))
        compact.append(
            {
                "category": str(category),
                "usd_value": _fmt_usd(row.get("usd_value", "0")),
                "percentage": _fmt_pct(row.get("percentage", "0")),
            }
        )
    compact.sort(key=lambda row: _to_decimal(row.get("usd_value", "0")), reverse=True)
    return compact[:6]


def _compact_risk_metrics(raw: str) -> dict[str, object]:
    parsed = _parse_dict(raw)
    top_rows = parsed.get("top_5_assets", [])
    compact_top: list[dict[str, object]] = []
    if isinstance(top_rows, list):
        for item in top_rows:
            if not isinstance(item, dict):
                continue
            compact_top.append(
                {
                    "asset": str(item.get("asset", "UNKNOWN")),
                    "usd_value": _fmt_usd(item.get("usd_value", "0")),
                    "percentage": _fmt_pct(item.get("percentage", "0")),
                }
            )
    return {
        "concentration_percentage": _fmt_pct(parsed.get("concentration_percentage", "0")),
        "hhi_index": str(_to_decimal(parsed.get("hhi_index", "0")).quantize(Decimal("0.001"))),
        "top_assets": compact_top[:5],
    }


def _compact_pnl_summary(raw: str) -> dict[str, object]:
    parsed = _parse_dict(raw)
    weekly = parsed.get("weekly", {})
    monthly = parsed.get("monthly", {})
    all_time = parsed.get("all_time", {})
    return {
        "weekly": _compact_pnl_period(weekly),
        "monthly": _compact_pnl_period(monthly),
        "all_time": _compact_pnl_period(all_time),
    }


def _compact_pnl_period(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {"absolute_change": "$0.00", "percentage_change": "0.00%"}
    return {
        "absolute_change": _fmt_usd_signed(value.get("absolute_change", "0")),
        "percentage_change": _fmt_pct(value.get("percentage_change", "0")),
    }


def _compact_weekly_movers(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "asset": str(row.get("asset", "UNKNOWN")),
            "absolute_change": _fmt_usd_signed(row.get("absolute_change", "0")),
            "percentage_change": _fmt_pct(row.get("percentage_change", "0")),
        }
        for row in rows
    ]
    compact.sort(key=lambda row: abs(_to_decimal(str(row.get("absolute_change", "0")).replace("$", ""))), reverse=True)
    return compact[:10]


def _parse_list(raw: str) -> list[dict[str, object]]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _parse_dict(raw: str) -> dict[str, object]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)


def _fmt_usd(value: object) -> str:
    """Format USD value rounded to 2 decimals."""
    return str(_to_decimal(value).quantize(Decimal("0.01")))


def _fmt_usd_signed(value: object) -> str:
    """Format USD with sign before '$' (e.g. '-$5.05', '+$30.56')."""
    d = _to_decimal(value).quantize(Decimal("0.01"))
    if d < 0:
        return f"-${abs(d)}"
    return f"${d}"


def _fmt_pct(value: object) -> str:
    """Format percentage with '%' suffix to prevent AI misinterpretation."""
    return f"{_to_decimal(value).quantize(Decimal('0.01'))}%"
