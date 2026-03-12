"""Telegram report formatter."""

from __future__ import annotations

import html
import json
import re
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.reporting.telegram import WeeklyReport

if TYPE_CHECKING:
    from pfm.ai.prompts import AnalyticsSummary

HOLDING_MIN_DISPLAY_USD = Decimal(10)
_HOLDING_TYPE_ICONS = {
    "crypto": "🪙",
    "fiat": "💵",
    "stocks": "📈",
    "defi": "🏦",
    "deposit": "🔐",
    "other": "📦",
}
_FIAT_ASSETS = {
    "USD",
    "THB",
    "GBP",
    "EUR",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "NZD",
    "SGD",
    "HKD",
}
_KNOWN_CRYPTO_ASSETS = {
    "BTC",
    "ETH",
    "SOL",
    "USDT",
    "USDC",
    "BNB",
    "XRP",
    "ADA",
    "DOGE",
    "LTC",
    "TRX",
    "AVAX",
    "DOT",
    "LINK",
}
_AI_COMMENTARY_HEADING_RE = re.compile(r"^\s*#{1,6}\s*(.+?)\s*$")
_AI_COMMENTARY_BULLET_RE = re.compile(r"^\s*[*-]\s+(.+?)\s*$")
_AI_COMMENTARY_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")


def format_weekly_report(
    analytics: AnalyticsSummary,
    commentary: str,
    *,
    warnings: list[str] | None = None,
) -> WeeklyReport:
    """Build Telegram HTML report from analytics."""
    allocation_rows = _parse_list_json(analytics.allocation_by_asset)

    lines = [
        f"<b>PFM Weekly Report</b> — {analytics.as_of_date.isoformat()}",
        f"Net worth: <b>${_fmt_money(analytics.net_worth_usd)}</b>",
        "",
        "<b>All Holdings</b>",
    ]

    shown_holding = False
    if allocation_rows:
        for row in allocation_rows:
            asset = html.escape(str(row.get("asset", "UNKNOWN")))
            usd_value = _to_decimal(row.get("usd_value", "0"))
            if usd_value < HOLDING_MIN_DISPLAY_USD:
                continue
            icon = _holding_icon(row)
            percentage = _to_decimal(row.get("percentage", "0"))
            lines.append(f"{icon} {asset}: ${_fmt_money(usd_value)} ({percentage}%)")
            shown_holding = True

    if not shown_holding:
        lines.append("• No holdings data available.")

    if warnings:
        lines.extend(["", "<b>Warnings</b>"])
        lines.extend([f"• {html.escape(warning)}" for warning in warnings])

    return WeeklyReport(
        text="\n".join(lines),
        ai_summary_text=format_ai_commentary(commentary),
    )


def format_ai_commentary(commentary: str) -> str:
    """Build a separate Telegram HTML message for AI commentary.

    Handles both plain text and flattened sections (title + markdown body).
    """
    normalized = commentary.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = _normalize_ai_commentary(normalized)
    return f"<b>AI Commentary</b>\n{cleaned}"


def _normalize_ai_commentary(text: str) -> str:
    rendered_lines: list[str] = []
    previous_blank = False
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            if rendered_lines and not previous_blank:
                rendered_lines.append("")
            previous_blank = True
            continue

        previous_blank = False
        heading_match = _AI_COMMENTARY_HEADING_RE.match(line)
        if heading_match:
            heading = _render_inline(heading_match.group(1))
            rendered_lines.append(f"<b>{heading}</b>")
            continue

        bullet_match = _AI_COMMENTARY_BULLET_RE.match(line)
        if bullet_match:
            bullet = _render_inline(bullet_match.group(1))
            rendered_lines.append(f"• {bullet}")
            continue

        rendered_lines.append(_render_inline(line))

    return "\n".join(rendered_lines).strip()


def _render_inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = escaped.replace("`", "")
    return _AI_COMMENTARY_BOLD_RE.sub(r"<b>\1</b>", escaped)


def _parse_list_json(raw_json: str) -> list[dict[str, object]]:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)


def _fmt_money(value: Decimal) -> str:
    return f"{value:,}"


def _holding_icon(row: dict[str, object]) -> str:
    asset_type = str(row.get("asset_type", "")).strip().lower()
    if not asset_type:
        asset_upper = str(row.get("asset", "")).upper()
        if asset_upper in _FIAT_ASSETS:
            asset_type = "fiat"
        elif asset_upper in _KNOWN_CRYPTO_ASSETS:
            asset_type = "crypto"
        else:
            asset_type = "other"
    return _HOLDING_TYPE_ICONS.get(asset_type, _HOLDING_TYPE_ICONS["other"])
