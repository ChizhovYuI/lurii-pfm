"""Telegram report formatter."""

from __future__ import annotations

import html
import json
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.reporting.telegram import WeeklyReport

if TYPE_CHECKING:
    from pfm.ai.prompts import AnalyticsSummary


def format_weekly_report(
    analytics: AnalyticsSummary,
    commentary: str,
    *,
    warnings: list[str] | None = None,
) -> WeeklyReport:
    """Build Telegram HTML report from analytics and AI commentary."""
    allocation_rows = _parse_list_json(analytics.allocation_by_asset)
    pnl = _parse_dict_json(analytics.pnl)
    yield_rows = _parse_list_json(analytics.yield_metrics)

    weekly_pnl = _parse_dict_json(json.dumps(pnl.get("weekly", {})))
    weekly_abs = _to_decimal(weekly_pnl.get("absolute_change", "0"))
    weekly_pct = _to_decimal(weekly_pnl.get("percentage_change", "0"))
    monthly_pnl = _parse_dict_json(json.dumps(pnl.get("monthly", {})))
    monthly_abs = _to_decimal(monthly_pnl.get("absolute_change", "0"))
    monthly_pct = _to_decimal(monthly_pnl.get("percentage_change", "0"))

    lines = [
        f"<b>PFM Weekly Report</b> — {analytics.as_of_date.isoformat()}",
        f"Net worth: <b>${_fmt_money(analytics.net_worth_usd)}</b>",
        "",
        f"<b>PnL (Weekly)</b>: {_pnl_arrow(weekly_abs)} ${_fmt_money(weekly_abs)} "
        f"({weekly_pct.quantize(Decimal('0.01'))}%)",
        f"<b>PnL (Monthly)</b>: {_pnl_arrow(monthly_abs)} ${_fmt_money(monthly_abs)} "
        f"({monthly_pct.quantize(Decimal('0.01'))}%)",
        "",
        "<b>All Holdings</b>",
    ]

    if allocation_rows:
        for row in allocation_rows:
            asset = html.escape(str(row.get("asset", "UNKNOWN")))
            usd_value = _to_decimal(row.get("usd_value", "0"))
            percentage = _to_decimal(row.get("percentage", "0")).quantize(Decimal("0.01"))
            lines.append(f"• {asset}: ${_fmt_money(usd_value)} ({percentage}%)")
    else:
        lines.append("• No holdings data available.")

    lines.extend(["", "<b>Yield</b>"])
    if yield_rows:
        for row in yield_rows:
            source = html.escape(str(row.get("source", "unknown")).upper())
            asset = html.escape(str(row.get("asset", "UNKNOWN")).upper())
            yield_amount = _to_decimal(row.get("yield_amount", "0"))
            yield_pct = _to_decimal(row.get("yield_percentage", "0")).quantize(Decimal("0.01"))
            lines.append(f"• {source}/{asset}: ${_fmt_money(yield_amount)} ({yield_pct}%)")
    else:
        lines.append("• No yield data available.")

    lines.extend(["", "<b>AI Commentary</b>", html.escape(commentary).replace("\n", "<br>")])

    if warnings:
        lines.extend(["", "<b>Warnings</b>"])
        lines.extend([f"• {html.escape(warning)}" for warning in warnings])

    return WeeklyReport(text="\n".join(lines))


def _parse_list_json(raw_json: str) -> list[dict[str, object]]:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _parse_dict_json(raw_json: str) -> dict[str, object]:
    try:
        parsed = json.loads(raw_json)
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


def _fmt_money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01')):,}"


def _pnl_arrow(change: Decimal) -> str:
    if change > 0:
        return "↑"
    if change < 0:
        return "↓"
    return "→"
