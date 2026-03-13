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
_SIGNIFICANT_VALUE_THRESHOLD = Decimal(10)

WEEKLY_REPORT_SYSTEM_PROMPT = """
You are a personal financial advisor writing one section of a weekly portfolio report.
Ground every claim in the supplied analytics and investor context.
If investor context conflicts with live analytics, trust the live analytics.
Be concise, practical, and risk-aware.
Analyze only the current portfolio snapshot and investor context.
Do not describe historical changes, trends, or prior-state comparisons unless they are explicitly provided.

Output contract:
- Return only the markdown body for the requested section.
- Do not return JSON.
- Do not wrap the answer in code fences.
- Do not repeat the section title as a heading or first line.
- Use GitHub-flavored Markdown when it helps clarity.
- Separate paragraphs with a blank line.
- Put a blank line before the first bullet list or numbered list.
- Start every bullet and numbered item on its own new line.
- Never place bullets or numbered items inline after a sentence in the same paragraph.
- Return exactly 2 short paragraphs, or 1 short paragraph followed by a short list when the section asks for it.
- Do not return one long block of text.
- Keep paragraphs to roughly 2-3 sentences each.
- Prefer bullet lists over dense prose when summarizing multiple drivers, risks, or actions.
- Keep bullets compact and, when possible, to one sentence each.
- Do not leave blank lines between adjacent bullet items or numbered items.
- Avoid verbose prefacing before a list.
- If data is missing or noisy, say so explicitly instead of guessing.
""".strip()

WEEKLY_REPORT_JSON_SYSTEM_PROMPT = """
You are a personal financial advisor writing a weekly portfolio report.
Ground every claim in the supplied analytics and investor context.
If investor context conflicts with live analytics, trust the live analytics.
Be concise, practical, and risk-aware.
Analyze only the current portfolio snapshot and investor context.
Do not describe historical changes, trends, or prior-state comparisons unless they are explicitly provided.

JSON output contract:
- Return one valid JSON object only.
- The JSON must contain a top-level "sections" array.
- The array must contain exactly 5 objects in this exact order:
  1. Market Context
  2. Portfolio Health Assessment
  3. Rebalancing Opportunities
  4. Risk Alerts
  5. Actionable Recommendations for Next 7 Days
- Each section object must have exactly two keys: "title" and "description".
- Do not add any other keys, prose, code fences, or wrapper text.
- Use the exact section titles provided above.

Formatting rules for section descriptions:
- Use GitHub-flavored Markdown.
- Do not repeat the section title inside the description.
- Separate paragraphs with a blank line.
- Put a blank line before the first bullet list or numbered list.
- Start every bullet and numbered item on its own new line.
- Never place bullets or numbered items inline after a sentence in the same paragraph.
- Do not leave blank lines between adjacent bullet items or numbered items.
- Keep bullets compact and, when possible, to one sentence each.

Section structure rules:
- Market Context: exactly 1 short paragraph, then exactly 3 bullets covering
  current portfolio positioning, liquidity/currency posture, and key caveats or watchpoints.
- Portfolio Health Assessment: exactly 2 short paragraphs.
- Rebalancing Opportunities: exactly 1 short intro paragraph, then 2-4 compact bullets.
- Risk Alerts: exactly 2-3 bullets.
- Actionable Recommendations for Next 7 Days: exactly 3 numbered items.
""".strip()

GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT = """
You are a personal financial advisor writing a weekly portfolio report.
Ground every claim in the supplied analytics and investor context.
If investor context conflicts with live analytics, trust the live analytics.
Return structured JSON only.
Analyze only the current portfolio snapshot and investor context.
Do not describe historical changes, trends, or prior-state comparisons unless they are explicitly provided.

JSON output contract:
- Return one valid JSON object only.
- Do not wrap the response in markdown or code fences.
- Do not include any text before or after the JSON object.
- The JSON must contain a top-level "sections" array.
- The array must contain exactly 5 objects in this exact order:
  1. Market Context
  2. Portfolio Health Assessment
  3. Rebalancing Opportunities
  4. Risk Alerts
  5. Actionable Recommendations for Next 7 Days
- Each section object must contain exactly two keys: "title" and "description".
- Use those exact titles and order.

Formatting rules for section descriptions:
- Use GitHub-flavored Markdown.
- Do not repeat the title inside the description.
- Do not inline bullets or numbered items after prose on the same line.
- Start every bullet and numbered item on its own new line.
- Do not leave blank lines between adjacent bullet items or numbered items.
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
- Use one or two short paragraphs grounded in the data.
- Separate paragraphs with a blank line.
- Put a blank line before the first bullet list or numbered list.
- Start every bullet and numbered item on its own new line.
- Never place bullets inline after a sentence.
- Use bullets only when they improve readability or when the section explicitly asks for them.
- Do not leave blank lines between bullet items.
- Keep bullet items compact.
- Include concrete numbers or percentages where useful.
</output_example>
""".strip()

WEEKLY_REPORT_JSON_USER_PROMPT_TEMPLATE = """
<json_contract>
Return one valid JSON object only.
The JSON must contain:
{{
  "sections": [
    {{"title": "Market Context", "description": "..."}},
    {{"title": "Portfolio Health Assessment", "description": "..."}},
    {{"title": "Rebalancing Opportunities", "description": "..."}},
    {{"title": "Risk Alerts", "description": "..."}},
    {{"title": "Actionable Recommendations for Next 7 Days", "description": "..."}}
  ]
}}
Use those exact titles and order.
</json_contract>
{investor_memory_block}
<analytics>
{analytics_context}
</analytics>
<section_requirements>
- Market Context: 1 short paragraph, then exactly 3 bullets for current portfolio positioning,
  liquidity/currency posture, and key caveats or watchpoints.
- Portfolio Health Assessment: exactly 2 short paragraphs.
- Rebalancing Opportunities: 1 short intro paragraph, then 2-4 bullets.
- Risk Alerts: exactly 2-3 bullets.
- Actionable Recommendations for Next 7 Days: exactly 3 numbered items.
- Analyze only the current snapshot and investor context.
- Do not mention last-7-day changes, prior snapshots, or historical comparisons.
- Put a blank line before the first bullet list or numbered list.
- Start every bullet and numbered item on its own new line.
- Never place bullets or numbered items inline after a sentence in the same paragraph.
- No blank lines between bullet items or numbered items.
- Return valid JSON only, with no wrapper text.
</section_requirements>
""".strip()

GEMINI_WEEKLY_REPORT_JSON_USER_PROMPT_TEMPLATE = """
<json_contract>
Return one valid JSON object only.
Do not include code fences or wrapper text.
The JSON must contain:
{{
  "sections": [
    {{"title": "Market Context", "description": "..."}},
    {{"title": "Portfolio Health Assessment", "description": "..."}},
    {{"title": "Rebalancing Opportunities", "description": "..."}},
    {{"title": "Risk Alerts", "description": "..."}},
    {{"title": "Actionable Recommendations for Next 7 Days", "description": "..."}}
  ]
}}
Use those exact titles and order.
</json_contract>
{investor_memory_block}
<analytics>
{analytics_context}
</analytics>
<section_requirements>
- Market Context: exactly 1 short intro paragraph, then exactly 3 bullets covering
  current portfolio positioning, liquidity/currency posture, and key caveats or watchpoints.
- Portfolio Health Assessment: exactly 2 short paragraphs.
- Rebalancing Opportunities: exactly 1 short intro paragraph, then 2-4 compact bullets.
- Risk Alerts: exactly 2-3 bullets.
- Actionable Recommendations for Next 7 Days: exactly 3 numbered items.
- Do not place bullets or numbered items inline after prose on the same line.
- Start every bullet and numbered item on its own new line.
- Do not leave blank lines between adjacent bullet items or numbered items.
- Analyze only the current snapshot and investor context.
- Do not mention last-7-day changes, prior snapshots, or historical comparisons.
</section_requirements>
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
    capital_flows: str = ""
    internal_conversions: str = ""
    currency_flow_bridge: str = ""


@dataclass(frozen=True, slots=True)
class ReportSectionSpec:
    """Backend-defined weekly report section."""

    slug: str
    title: str
    purpose: str
    output_style: str
    fallback_text: str
    structure: str
    max_output_tokens: int = 1400


REPORT_SECTION_SPECS: tuple[ReportSectionSpec, ...] = (
    ReportSectionSpec(
        slug="market-context",
        title="Market Context",
        purpose=(
            "Summarize the current portfolio backdrop by highlighting dominant allocations, "
            "liquidity or currency posture, and the clearest caveats visible in the latest snapshot."
        ),
        output_style=(
            "- Start with exactly one short summary paragraph.\n"
            "- Then add exactly 3 compact bullets covering: current portfolio positioning, "
            "liquidity or currency posture, and key caveats or watchpoints.\n"
            "- Keep each bullet to one sentence when possible and do not leave blank lines between bullets.\n"
            "- Ground every point in the current snapshot rather than historical comparisons.\n"
            "- Do not describe week-over-week changes, momentum, or prior-state moves.\n"
            "- Avoid macro storytelling that is not supported by the data."
        ),
        fallback_text=(
            "The current portfolio snapshot should be framed through today's positioning rather than historical "
            "narrative.\n\n"
            "- Highlight the dominant holdings or categories shaping the portfolio.\n"
            "- Note the current liquidity or currency posture.\n"
            "- Call out the clearest caveat, concentration, or data-quality warning."
        ),
        structure="paragraph_then_bullets",
    ),
    ReportSectionSpec(
        slug="portfolio-health",
        title="Portfolio Health Assessment",
        purpose="Assess diversification, liquidity, income mix, and whether the portfolio matches the stated profile.",
        output_style=(
            "- Return at least two short paragraphs.\n"
            "- The first paragraph should assess diversification, concentration, and balance.\n"
            "- The second paragraph should assess liquidity, yield exposure, and fit with the investor profile.\n"
            "- Keep each paragraph to 2-3 sentences.\n"
            "- Call out strengths before weaknesses."
        ),
        fallback_text=(
            "Portfolio health should be assessed through concentration risk, diversification, and whether current "
            "holdings still match the intended long-term strategy.\n\n"
            "Liquidity, income exposure, and portfolio fit should then be reviewed against the investor's stated goals "
            "and risk profile."
        ),
        structure="two_paragraphs",
    ),
    ReportSectionSpec(
        slug="rebalancing",
        title="Rebalancing Opportunities",
        purpose="Highlight concrete rebalancing or allocation adjustments worth considering.",
        output_style=(
            "- Start with exactly one short intro paragraph.\n"
            "- Then use 2-4 compact bullets for the concrete actions.\n"
            "- Do not leave blank lines between bullets.\n"
            "- Point to specific assets, categories, or currencies.\n"
            "- Prefer actionable reweighting ideas over generic diversification advice.\n"
            "- Skip forced recommendations if the data does not justify rebalancing."
        ),
        fallback_text=(
            "Review only the clearest allocation drifts and deployment gaps.\n\n"
            "- Focus first on outsized positions or idle cash.\n"
            "- Rebalance only where the current allocation materially diverges from the intended risk profile."
        ),
        structure="paragraph_then_bullets",
    ),
    ReportSectionSpec(
        slug="risk-alerts",
        title="Risk Alerts",
        purpose="List the most important portfolio-specific risks that deserve monitoring.",
        output_style=(
            "- Use bullets only, one bullet per risk.\n"
            "- Return exactly 2-3 bullets.\n"
            "- Keep bullets compact and do not leave blank lines between them.\n"
            "- Mention data quality limits if they affect confidence.\n"
            "- Focus on concentration, liquidity, counterparty, or behavioral risk visible from the data."
        ),
        fallback_text=(
            "- Monitor the largest concentration and liquidity risks first.\n"
            "- Watch for source-specific or counterparty exposure that could cause outsized damage.\n"
            "- Note any data quality gaps that reduce confidence in the analysis."
        ),
        structure="bullets_only",
    ),
    ReportSectionSpec(
        slug="next-7-days",
        title="Actionable Recommendations for Next 7 Days",
        purpose="End with a short practical checklist for the coming week.",
        output_style=(
            "- Return a numbered list only.\n"
            "- An optional one-line intro is allowed, but keep it short.\n"
            "- Return exactly 3 numbered items.\n"
            "- Do not leave blank lines between numbered items.\n"
            "- Keep actions realistic for a one-week horizon.\n"
            "- Separate must-do items from optional improvements."
        ),
        fallback_text=(
            "1. Review the biggest concentration and liquidity risks.\n"
            "2. Confirm cash reserves and emergency buffers are still adequate.\n"
            "3. Make only the highest-conviction allocation adjustments supported by the data."
        ),
        structure="numbered_list",
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


def render_weekly_report_json_prompt(
    analytics: AnalyticsSummary,
    *,
    investor_memory: str = "",
) -> str:
    """Render the single-shot JSON prompt for DeepSeek chat."""
    investor_memory_block = ""
    normalized_memory = investor_memory.strip()
    if normalized_memory:
        investor_memory_block = f"\n<investor_memory>\n{normalized_memory}\n</investor_memory>\n"

    return WEEKLY_REPORT_JSON_USER_PROMPT_TEMPLATE.format(
        investor_memory_block=investor_memory_block.strip("\n"),
        analytics_context=_render_analytics_context(analytics),
    )


def render_gemini_weekly_report_json_prompt(
    analytics: AnalyticsSummary,
    *,
    investor_memory: str = "",
) -> str:
    """Render the single-shot JSON prompt for Gemini structured output."""
    investor_memory_block = ""
    normalized_memory = investor_memory.strip()
    if normalized_memory:
        investor_memory_block = f"\n<investor_memory>\n{normalized_memory}\n</investor_memory>\n"

    return GEMINI_WEEKLY_REPORT_JSON_USER_PROMPT_TEMPLATE.format(
        investor_memory_block=investor_memory_block.strip("\n"),
        analytics_context=_render_analytics_context(analytics),
    )


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
        "Risk metrics:",
        _pretty_json(risk_metrics),
    ]

    if analytics.earn_positions:
        earn = _compact_earn_positions(analytics.earn_positions)
        if earn:
            parts.extend(["Earn/yield positions:", _pretty_json(earn)])

    parts.extend(["Currency exposure:", _pretty_json(currency_exposure)])

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


def _compact_capital_flows(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = []
    for row in rows:
        usd_value = _to_decimal(row.get("usd_value", "0"))
        amount = _to_decimal(row.get("amount", "0"))
        if abs(usd_value) < _SIGNIFICANT_VALUE_THRESHOLD and abs(amount) < _SIGNIFICANT_VALUE_THRESHOLD:
            continue
        compact.append(
            {
                "date": str(row.get("date", "")),
                "source": str(row.get("source", "")),
                "kind": str(row.get("kind", "")),
                "asset": str(row.get("asset", "")),
                "amount": _fmt_signed(row.get("amount", "0")),
                "usd_value": _fmt_signed(row.get("usd_value", "0")),
            }
        )
    return compact[:20]


def _compact_internal_conversions(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = [
        {
            "date": str(row.get("date", "")),
            "source": str(row.get("source", "")),
            "from_asset": str(row.get("from_asset", "")),
            "from_amount": _fmt_signed(row.get("from_amount", "0")),
            "to_asset": str(row.get("to_asset", "")),
            "to_amount": _fmt_signed(row.get("to_amount", "0")),
            "usd_value": _fmt_usd(row.get("usd_value", "0")),
            "trade_side": str(row.get("trade_side", "")),
        }
        for row in rows
    ]
    return compact[:20]


def _compact_currency_flow_bridge(raw: str) -> list[dict[str, object]]:
    rows = _parse_list(raw)
    compact: list[dict[str, object]] = []
    for row in rows:
        delta_usd = _to_decimal(row.get("delta_usd_value", "0"))
        explained = max(
            abs(_to_decimal(row.get("explained_by_trade_spend", "0"))),
            abs(_to_decimal(row.get("explained_by_trade_proceeds", "0"))),
            abs(_to_decimal(row.get("explained_by_external_inflows", "0"))),
            abs(_to_decimal(row.get("explained_by_external_outflows", "0"))),
            abs(_to_decimal(row.get("explained_by_income", "0"))),
        )
        if abs(delta_usd) < _SIGNIFICANT_VALUE_THRESHOLD and explained < _SIGNIFICANT_VALUE_THRESHOLD:
            continue
        compact.append(
            {
                "currency": str(row.get("currency", "")),
                "previous_amount": _fmt_signed(row.get("previous_amount", "0")),
                "current_amount": _fmt_signed(row.get("current_amount", "0")),
                "delta_amount": _fmt_signed(row.get("delta_amount", "0")),
                "delta_usd_value": _fmt_signed(row.get("delta_usd_value", "0")),
                "explained_by_external_inflows": _fmt_signed(row.get("explained_by_external_inflows", "0")),
                "explained_by_external_outflows": _fmt_signed(row.get("explained_by_external_outflows", "0")),
                "explained_by_income": _fmt_signed(row.get("explained_by_income", "0")),
                "explained_by_trade_spend": _fmt_signed(row.get("explained_by_trade_spend", "0")),
                "explained_by_trade_proceeds": _fmt_signed(row.get("explained_by_trade_proceeds", "0")),
                "residual_unexplained": _fmt_signed(row.get("residual_unexplained", "0")),
                "likely_counterparties": row.get("likely_counterparties", []),
            }
        )
    compact.sort(key=lambda row: abs(_to_decimal(row.get("delta_usd_value", "0"))), reverse=True)
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
    compact: list[dict[str, object]] = []
    for row in rows:
        usd_value = _to_decimal(row.get("usd_value", "0"))
        amount = _to_decimal(row.get("amount", 0))
        counterparty_amount = _to_decimal(row.get("counterparty_amount", "0"))
        if (
            abs(usd_value) < _SIGNIFICANT_VALUE_THRESHOLD
            and abs(amount) < _SIGNIFICANT_VALUE_THRESHOLD
            and abs(counterparty_amount) < _SIGNIFICANT_VALUE_THRESHOLD
        ):
            continue
        compact.append(
            {
                "date": str(row.get("date", "")),
                "source": str(row.get("source", "")),
                "type": str(row.get("type", "")),
                "asset": str(row.get("asset", "")),
                "amount": _fmt_signed(row.get("amount", "0")),
                "usd_value": _fmt_signed(row.get("usd_value", "0")),
                "counterparty_asset": str(row.get("counterparty_asset", "")),
                "counterparty_amount": _fmt_signed(row.get("counterparty_amount", "0")),
                "trade_side": str(row.get("trade_side", "")),
            }
        )
    compact.sort(key=lambda row: str(row.get("date", "")), reverse=True)
    return compact[:30]


def _fmt_signed(value: object) -> str:
    return str(_to_decimal(value).quantize(Decimal("0.01")))
