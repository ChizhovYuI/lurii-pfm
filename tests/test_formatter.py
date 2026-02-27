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
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000","percentage":"56.7","asset_type":"crypto"}]',
        allocation_by_source="[]",
        allocation_by_category="[]",
        currency_exposure="[]",
        risk_metrics="{}",
        pnl='{"weekly":{"absolute_change":"123.45","percentage_change":"1.23"},'
        '"monthly":{"absolute_change":"456.78","percentage_change":"4.56"}}',
        weekly_pnl_by_asset='[{"asset":"BTC","absolute_change":"80","percentage_change":"1.16"}]',
    )

    report = format_weekly_report(analytics, "Watch <volatility>.\nRebalance slowly.", warnings=["Data is partial"])

    assert "<b>PFM Weekly Report</b>" in report.text
    assert "Net worth: <b>$12,345.67</b>" in report.text
    assert "<b>PnL (Weekly)</b>: ↑ $123.45 (1.23%)" in report.text
    assert "<b>PnL (Monthly)</b>: ↑ $456.78 (4.56%)" in report.text
    assert "<b>All Holdings</b> (🪙 crypto | 💵 fiat | 📈 stocks | 🏦 defi | 📦 other)" in report.text
    assert "🪙 BTC: $7,000.00 (56.70%) | 7d PnL: $80.00 (1.16%)" in report.text
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
        pnl='{"weekly":{"absolute_change":"-5","percentage_change":"-1.0"},'
        '"monthly":{"absolute_change":"-20","percentage_change":"-3.0"}}',
        weekly_pnl_by_asset="[]",
    )

    report = format_weekly_report(analytics, "No major changes.")

    assert "<b>PnL (Weekly)</b>: ↓ $-5.00 (-1.00%)" in report.text
    assert "<b>PnL (Monthly)</b>: ↓ $-20.00 (-3.00%)" in report.text
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
        pnl='{"weekly":{"absolute_change":"oops","percentage_change":"nan-ish"},'
        '"monthly":{"absolute_change":"oops","percentage_change":"nan-ish"}}',
        weekly_pnl_by_asset='[{"asset":"BTC","absolute_change":"oops","percentage_change":"bad"}]',
    )

    report = format_weekly_report(analytics, "Still works.")
    assert "<b>PnL (Weekly)</b>: → $0.00 (0.00%)" in report.text
    assert "<b>PnL (Monthly)</b>: → $0.00 (0.00%)" in report.text
    assert "• BTC: $0.00 (0.00%)" not in report.text
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
        pnl='{"weekly":{"absolute_change":"0","percentage_change":"0"}}',
        weekly_pnl_by_asset="[]",
    )

    report = format_weekly_report(analytics, "All holdings visible.")
    assert "📈 A9: $9.00 (1.00%)" not in report.text
    assert "📈 A10: $10.00 (1.00%) | 7d PnL: $0.00 (0.00%)" in report.text
    assert "📈 A11: $11.00 (1.00%) | 7d PnL: $0.00 (0.00%)" in report.text
