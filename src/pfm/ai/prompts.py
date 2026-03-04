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

Output format: respond ONLY with a valid JSON array of objects, no text before or after.
Each object has two keys:
  "title" — short section heading (plain text, no markdown)
  "description" — section body in GitHub-flavored Markdown (use **bold**, bullet lists, numbers from data)

Rules:
- Ground every claim in provided data.
- If data is missing or noisy, state that clearly.
- Include concrete numbers and percentages from the provided data.
- Give enough detail to explain reasoning and actions.
""".strip()

WEEKLY_REPORT_USER_PROMPT_TEMPLATE = """
You are given portfolio analytics for {as_of_date}.
Net worth (USD): {net_worth_usd}

Top holdings:
{top_holdings}

Allocation by category:
{allocation_by_category}
{extra_sections}
Risk metrics:
{risk_metrics}

Data warnings:
{warnings}

Write a compact report with exactly these 5 sections:
1) Market Context
2) Portfolio Health Assessment
3) Rebalancing Opportunities
4) Risk Alerts
5) Actionable Recommendations for Next 7 Days

Respond with a JSON array of 5 objects, each with "title" and "description" keys.
Example format:
[
  {{"title": "Market Context", "description": "Bitcoin is trading at **$95,432**..."}},
  ...
]
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
    warnings: tuple[str, ...] = ()
    earn_positions: str = ""
    weekly_pnl: str = ""


def render_weekly_report_user_prompt(analytics: AnalyticsSummary) -> str:
    """Render the user prompt from analytics data."""
    top_holdings = _compact_top_holdings(analytics.allocation_by_asset)
    allocation_by_category = _compact_allocation_by_category(analytics.allocation_by_category)
    risk_metrics = _compact_risk_metrics(analytics.risk_metrics)
    warnings_text = "\n".join(analytics.warnings) if analytics.warnings else "None"

    extra_parts: list[str] = []
    if analytics.earn_positions:
        earn = _compact_earn_positions(analytics.earn_positions)
        if earn:
            extra_parts.append(f"DeFi/Earn positions (assets generating yield):\n{_pretty_json(earn)}")
    if analytics.weekly_pnl:
        pnl = _compact_weekly_pnl(analytics.weekly_pnl)
        if pnl:
            extra_parts.append(f"7-Day portfolio change:\n{_pretty_json(pnl)}")
    extra_sections = "\n".join(extra_parts) + "\n" if extra_parts else ""

    return WEEKLY_REPORT_USER_PROMPT_TEMPLATE.format(
        as_of_date=analytics.as_of_date.isoformat(),
        net_worth_usd=_fmt_usd(analytics.net_worth_usd),
        top_holdings=_pretty_json(top_holdings),
        allocation_by_category=_pretty_json(allocation_by_category),
        extra_sections=extra_sections,
        risk_metrics=_pretty_json(risk_metrics),
        warnings=warnings_text,
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


def _fmt_pct(value: object) -> str:
    """Format percentage with '%' suffix to prevent AI misinterpretation."""
    return f"{_to_decimal(value).quantize(Decimal('0.01'))}%"


def _compact_earn_positions(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "asset": str(row.get("asset", "UNKNOWN")),
            "source": str(row.get("source", "")),
            "usd_value": _fmt_usd(row.get("usd_value", "0")),
            "apy": _fmt_pct(row.get("apy", "0")),
            "portfolio_pct": _fmt_pct(row.get("portfolio_pct", "0")),
        }
        for row in rows
    ]
    compact.sort(key=lambda r: _to_decimal(r.get("usd_value", "0")), reverse=True)
    return compact


def _compact_weekly_pnl(raw: str) -> dict[str, object] | None:
    parsed = _parse_dict(raw)
    if not parsed:
        return None
    result: dict[str, object] = {
        "start_date": parsed.get("start_date", ""),
        "end_date": parsed.get("end_date", ""),
        "start_value": _fmt_usd(parsed.get("start_value", "0")),
        "end_value": _fmt_usd(parsed.get("end_value", "0")),
        "absolute_change": _fmt_usd(parsed.get("absolute_change", "0")),
        "percentage_change": _fmt_pct(parsed.get("percentage_change", "0")),
    }
    for key in ("top_gainers", "top_losers"):
        items = parsed.get(key, [])
        if isinstance(items, list):
            result[key] = [
                {
                    "asset": str(item.get("asset", "UNKNOWN")),
                    "absolute_change": _fmt_usd(item.get("absolute_change", "0")),
                    "percentage_change": _fmt_pct(item.get("percentage_change", "0")),
                }
                for item in items
                if isinstance(item, dict)
            ][:3]
    return result
