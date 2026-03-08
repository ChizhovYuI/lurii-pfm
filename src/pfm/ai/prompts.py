"""Prompt templates for section-based AI weekly commentary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.ai.base import CommentarySection

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

REPORT_PROMPT_VERSION = 2
_PRIOR_SECTION_DESCRIPTION_LIMIT = 300
_PRIOR_SECTION_DESCRIPTION_TRUNCATED_LIMIT = _PRIOR_SECTION_DESCRIPTION_LIMIT - 3
_PRIOR_SECTIONS_TOTAL_LIMIT = 1200

WEEKLY_REPORT_SYSTEM_PROMPT = """
You are a personal financial advisor writing one section of a weekly portfolio report.
Ground every claim in the supplied analytics and investor context.
If investor context conflicts with live analytics, trust the live analytics.
Be concise, practical, and risk-aware.

Output contract:
- Return only the markdown body for the requested section.
- Do not return JSON.
- Do not wrap the answer in code fences.
- Do not repeat the section title as a heading or first line.
- Use GitHub-flavored Markdown when it helps clarity.
- If data is missing or noisy, say so explicitly instead of guessing.
""".strip()

REPORT_SECTION_USER_PROMPT_TEMPLATE = """
<section_contract>
Write only the body for the section titled "{title}".
Purpose: {purpose}
Style requirements:
{output_style}
</section_contract>
{investor_memory_block}
<analytics>
{analytics_context}
</analytics>
{prior_sections_block}
<output_example>
- One or two short paragraphs grounded in the data.
- Use bullets only when they improve readability.
- Include concrete numbers or percentages where useful.
</output_example>
""".strip()

# Backward-compat export retained for internal imports/tests.
WEEKLY_REPORT_USER_PROMPT_TEMPLATE = REPORT_SECTION_USER_PROMPT_TEMPLATE


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
    recent_transactions: str = ""


@dataclass(frozen=True, slots=True)
class ReportSectionSpec:
    """Backend-defined weekly report section."""

    slug: str
    title: str
    purpose: str
    output_style: str
    fallback_text: str
    max_output_tokens: int = 1400


REPORT_SECTION_SPECS: tuple[ReportSectionSpec, ...] = (
    ReportSectionSpec(
        slug="market-context",
        title="Market Context",
        purpose="Summarize what the portfolio data suggests about the last week and current positioning backdrop.",
        output_style=(
            "- Explain performance drivers from the portfolio itself.\n"
            "- Mention material 7-day PnL, top movers, or transaction context.\n"
            "- Avoid macro storytelling that is not supported by the data."
        ),
        fallback_text=(
            "Recent portfolio movement was mixed. Use the 7-day PnL, top movers, and recent transfers as the main "
            "context for the week, and treat missing market data cautiously."
        ),
    ),
    ReportSectionSpec(
        slug="portfolio-health",
        title="Portfolio Health Assessment",
        purpose="Assess diversification, liquidity, income mix, and whether the portfolio matches the stated profile.",
        output_style=(
            "- Focus on concentration, cash buffer, category balance, and yield exposure.\n"
            "- Call out strengths before weaknesses.\n"
            "- Relate the assessment to the investor context when available."
        ),
        fallback_text=(
            "Portfolio health should be assessed through concentration risk, liquidity buffer, source diversification, "
            "and whether current holdings still match the intended long-term strategy."
        ),
    ),
    ReportSectionSpec(
        slug="rebalancing",
        title="Rebalancing Opportunities",
        purpose="Highlight concrete rebalancing or allocation adjustments worth considering.",
        output_style=(
            "- Point to specific assets, categories, or currencies.\n"
            "- Prefer actionable reweighting ideas over generic diversification advice.\n"
            "- Skip forced recommendations if the data does not justify rebalancing."
        ),
        fallback_text=(
            "Review outsized positions, category drift, and cash deployment opportunities. Rebalance only where the "
            "current allocation materially diverges from the intended risk profile."
        ),
    ),
    ReportSectionSpec(
        slug="risk-alerts",
        title="Risk Alerts",
        purpose="List the most important portfolio-specific risks that deserve monitoring.",
        output_style=(
            "- Prioritize the top one to three risks.\n"
            "- Mention data quality limits if they affect confidence.\n"
            "- Focus on concentration, liquidity, counterparty, or behavioral risk visible from the data."
        ),
        fallback_text=(
            "The main risks to monitor are concentration, liquidity, and any source-specific exposure that could cause "
            "outsized damage if one position or platform moves sharply."
        ),
    ),
    ReportSectionSpec(
        slug="next-7-days",
        title="Actionable Recommendations for Next 7 Days",
        purpose="End with a short practical checklist for the coming week.",
        output_style=(
            "- Use a short numbered list when helpful.\n"
            "- Keep actions realistic for a one-week horizon.\n"
            "- Separate must-do items from optional improvements."
        ),
        fallback_text=(
            "For the next 7 days, review concentration risk, confirm cash and emergency reserves, and make only the "
            "highest-conviction allocation adjustments supported by current data."
        ),
    ),
)


def render_report_section_prompt(
    spec: ReportSectionSpec,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str = "",
    prior_sections: Sequence[CommentarySection] = (),
) -> str:
    """Render the user prompt for a single report section."""
    investor_memory_block = ""
    normalized_memory = investor_memory.strip()
    if normalized_memory:
        investor_memory_block = f"\n<investor_memory>\n{normalized_memory}\n</investor_memory>\n"

    prior_sections_block = ""
    clipped_prior = _clip_prior_sections(prior_sections)
    if clipped_prior:
        lines = []
        for section in clipped_prior:
            lines.append(f"## {section.title}")
            lines.append(section.description)
        prior_sections_block = f"\n<prior_sections>\n{'\n'.join(lines)}\n</prior_sections>\n"

    return REPORT_SECTION_USER_PROMPT_TEMPLATE.format(
        title=spec.title,
        purpose=spec.purpose,
        output_style=spec.output_style,
        investor_memory_block=investor_memory_block.strip("\n"),
        analytics_context=_render_analytics_context(analytics),
        prior_sections_block=prior_sections_block.strip("\n"),
    )


def render_weekly_report_user_prompt(analytics: AnalyticsSummary) -> str:
    """Legacy compatibility helper used by older imports/tests."""
    return render_report_section_prompt(REPORT_SECTION_SPECS[0], analytics)


def _clip_prior_sections(prior_sections: Sequence[CommentarySection]) -> tuple[CommentarySection, ...]:
    if not prior_sections:
        return ()
    clipped: list[CommentarySection] = []
    total_chars = 0
    for section in prior_sections:
        description = section.description.strip()
        if len(description) > _PRIOR_SECTION_DESCRIPTION_LIMIT:
            description = description[:_PRIOR_SECTION_DESCRIPTION_TRUNCATED_LIMIT].rstrip() + "..."
        section_chars = len(section.title) + len(description)
        if clipped and total_chars + section_chars > _PRIOR_SECTIONS_TOTAL_LIMIT:
            break
        clipped.append(CommentarySection(title=section.title, description=description))
        total_chars += section_chars
    return tuple(clipped)


def _render_analytics_context(analytics: AnalyticsSummary) -> str:
    top_holdings = _compact_top_holdings(analytics.allocation_by_asset)
    allocation_by_category = _compact_allocation_by_category(analytics.allocation_by_category)
    allocation_by_source = _compact_allocation_by_source(analytics.allocation_by_source)
    currency_exposure = _compact_currency_exposure(analytics.currency_exposure)
    risk_metrics = _compact_risk_metrics(analytics.risk_metrics)
    warnings_text = "\n".join(f"- {warning}" for warning in analytics.warnings) if analytics.warnings else "- None"

    parts = [
        f"As of date: {analytics.as_of_date.isoformat()}",
        f"Net worth (USD): {_fmt_usd(analytics.net_worth_usd)}",
        "Top holdings:",
        _pretty_json(top_holdings),
        "Allocation by category:",
        _pretty_json(allocation_by_category),
        "Allocation by source:",
        _pretty_json(allocation_by_source),
        "Currency exposure:",
        _pretty_json(currency_exposure),
        "Risk metrics:",
        _pretty_json(risk_metrics),
    ]

    if analytics.earn_positions:
        earn = _compact_earn_positions(analytics.earn_positions)
        if earn:
            parts.extend(["Earn/yield positions:", _pretty_json(earn)])

    if analytics.weekly_pnl:
        pnl = _compact_weekly_pnl(analytics.weekly_pnl)
        if pnl:
            parts.extend(["7-day portfolio change:", _pretty_json(pnl)])

    if analytics.recent_transactions:
        txs = _compact_recent_transactions(analytics.recent_transactions)
        if txs:
            parts.extend(["Recent transactions (last 7 days):", _pretty_json(txs)])

    parts.extend(["Data warnings:", warnings_text])
    return "\n".join(parts)


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


def _compact_allocation_by_source(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "source": str(row.get("source", "unknown")),
            "usd_value": _fmt_usd(row.get("usd_value", "0")),
            "percentage": _fmt_pct(row.get("percentage", "0")),
        }
        for row in rows
    ]
    compact.sort(key=lambda row: _to_decimal(row.get("usd_value", "0")), reverse=True)
    return compact[:8]


def _compact_currency_exposure(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "currency": str(row.get("currency", "unknown")),
            "usd_value": _fmt_usd(row.get("usd_value", "0")),
            "percentage": _fmt_pct(row.get("percentage", "0")),
        }
        for row in rows
    ]
    compact.sort(key=lambda row: _to_decimal(row.get("usd_value", "0")), reverse=True)
    return compact[:8]


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
    return str(_to_decimal(value).quantize(Decimal("0.01")))


def _fmt_pct(value: object) -> str:
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
    compact.sort(key=lambda row: _to_decimal(row.get("usd_value", "0")), reverse=True)
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


def _compact_recent_transactions(raw: str) -> list[dict[str, object]]:
    """Compact recent transactions for the AI prompt."""
    rows = _parse_list(raw)
    grouped: dict[tuple[str, ...], Decimal] = {}
    for row in rows:
        key = (
            str(row.get("date", "")),
            str(row.get("source", "")),
            str(row.get("type", "")),
            str(row.get("asset", "")),
        )
        grouped[key] = grouped.get(key, Decimal(0)) + _to_decimal(row.get("amount", 0))

    compact: list[dict[str, object]] = []
    for (tx_date, source, tx_type, asset), amount in grouped.items():
        if amount < 10:  # noqa: PLR2004
            continue
        compact.append(
            {
                "date": tx_date,
                "source": source,
                "type": tx_type,
                "asset": asset,
                "amount": str(amount.quantize(Decimal("0.01"))),
            }
        )
    compact.sort(key=lambda row: str(row.get("date", "")), reverse=True)
    return compact[:30]
