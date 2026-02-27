"""Tests for report formatter."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.ai.prompts import AnalyticsSummary
from pfm.reporting.formatter import format_weekly_report


def test_format_weekly_report_contains_required_sections():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal("12345.67"),
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000","percentage":"56.7"}]',
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
        pnl='{"weekly":{"absolute_change":"123.45","percentage_change":"1.23"}}',
        yield_metrics='[{"source":"blend","asset":"USDC","yield_amount":"10","yield_percentage":"2.5"}]',
    )

    report = format_weekly_report(analytics, "Watch <volatility>.\nRebalance slowly.", warnings=["Data is partial"])

    assert "<b>PFM Weekly Report</b>" in report.text
    assert "Net worth: <b>$12,345.67</b>" in report.text
    assert "<b>PnL (Weekly)</b>: ↑ $123.45 (1.23%)" in report.text
    assert "<b>Top Holdings</b>" in report.text
    assert "• BTC: $7,000.00 (56.70%)" in report.text
    assert "<b>Yield (Blend)</b>" in report.text
    assert "• USDC: $10.00 (2.50%)" in report.text
    assert "<b>AI Commentary</b>" in report.text
    assert "Watch &lt;volatility&gt;.<br>Rebalance slowly." in report.text
    assert "<b>Warnings</b>" in report.text
    assert "• Data is partial" in report.text


def test_format_weekly_report_handles_missing_data_branches():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(100),
        allocation_by_asset="[]",
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
        pnl='{"weekly":{"absolute_change":"-5","percentage_change":"-1.0"}}',
        yield_metrics="[]",
    )

    report = format_weekly_report(analytics, "No major changes.")

    assert "<b>PnL (Weekly)</b>: ↓ $-5.00 (-1.00%)" in report.text
    assert "• No holdings data available." in report.text
    assert "• No Blend yield data available." in report.text


def test_format_weekly_report_tolerates_invalid_numeric_values():
    analytics = AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal(100),
        allocation_by_asset='[{"asset":"BTC","usd_value":"not-a-number","percentage":"bad"}]',
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
        pnl='{"weekly":{"absolute_change":"oops","percentage_change":"nan-ish"}}',
        yield_metrics='[{"source":"blend","asset":"USDC","yield_amount":"oops","yield_percentage":"bad"}]',
    )

    report = format_weekly_report(analytics, "Still works.")
    assert "<b>PnL (Weekly)</b>: → $0.00 (0.00%)" in report.text
    assert "• BTC: $0.00 (0.00%)" in report.text
    assert "• USDC: $0.00 (0.00%)" in report.text
