"""Tests for report formatter."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.ai.prompts import AnalyticsSummary
from pfm.reporting.formatter import format_ai_commentary, format_weekly_report


def test_format_weekly_report_contains_required_sections():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal("12345.67"),
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000","percentage":"56.7","asset_type":"crypto"}]',
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
    )

    report = format_weekly_report(analytics, "Watch <volatility>.\nRebalance slowly.", warnings=["Data is partial"])

    assert "<b>PFM Weekly Report</b>" in report.text
    assert "Net worth: <b>$12,345.67</b>" in report.text
    assert "<b>All Holdings</b>" in report.text
    assert "🪙 BTC: $7,000 (56.7%)" in report.text
    assert "<b>Warnings</b>" in report.text
    assert "• Data is partial" in report.text
    assert report.ai_summary_text is not None
    assert "Watch &lt;volatility&gt;.\nRebalance slowly." in report.ai_summary_text


def test_format_weekly_report_handles_missing_data_branches():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(100),
        allocation_by_asset="[]",
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
    )

    report = format_weekly_report(analytics, "No major changes.")

    assert "• No holdings data available." in report.text


def test_format_weekly_report_tolerates_invalid_numeric_values():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(100),
        allocation_by_asset='[{"asset":"BTC","usd_value":"not-a-number","percentage":"bad"}]',
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
    )

    report = format_weekly_report(analytics, "Still works.")
    assert "BTC:" not in report.text  # below HOLDING_MIN_DISPLAY_USD threshold
    assert "• No holdings data available." in report.text


def test_format_weekly_report_includes_all_holdings_not_truncated():
    holdings = ",".join(
        f'{{"asset":"A{i}","usd_value":"{i}","percentage":"1","asset_type":"stocks"}}' for i in range(1, 12)
    )
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(100),
        allocation_by_asset=f"[{holdings}]",
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
    )

    report = format_weekly_report(analytics, "All holdings visible.")
    assert "📈 A9: $9 (1%)" not in report.text
    assert "📈 A10: $10 (1%)" in report.text
    assert "📈 A11: $11 (1%)" in report.text


def test_format_ai_commentary_escapes_html_and_preserves_lines():
    message = format_ai_commentary("Watch <volatility>.\nRebalance slowly.")
    assert message.startswith("<b>AI Commentary</b>\n")
    assert "Watch &lt;volatility&gt;.\nRebalance slowly." in message


def test_format_ai_commentary_normalizes_markdown():
    message = format_ai_commentary(
        "### 1) Market Context\n* **Net Worth:** $60,922.81\n* Use `cash` buffer\nPlain line"
    )
    assert "###" not in message
    assert "• <b>Net Worth:</b> $60,922.81" in message
    assert "• Use cash buffer" in message
    assert "<b>1) Market Context</b>" in message
    assert "Plain line" in message


def test_format_ai_commentary_does_not_truncate_long_text():
    long_line = "A" * 2500
    message = format_ai_commentary(f"Line 1.\n{long_line}\nLine 3.")
    assert message.endswith("Line 3.")
    assert "…" not in message
