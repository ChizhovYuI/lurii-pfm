"""Integration-style tests for reporting flow."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pydantic import SecretStr

from pfm.cli import _report_async, _run_pipeline_async
from pfm.db.models import CollectorResult, Snapshot, init_db
from pfm.db.repository import Repository
from pfm.db.telegram_store import TelegramStore


async def test_report_async_loads_cache_formats_and_sends(tmp_path):
    db_path = tmp_path / "report.db"
    await init_db(db_path)
    await TelegramStore(db_path).set(bot_token="token", chat_id="chat")
    async with Repository(db_path) as repo:
        snapshot_date = date(2024, 1, 15)
        await repo.save_snapshot(
            Snapshot(
                date=snapshot_date,
                source="wise",
                asset="USD",
                amount=Decimal("100.0"),
                usd_value=Decimal("100.0"),
            )
        )
        await repo.save_analytics_metric(snapshot_date, "net_worth", '{"usd":"100.0"}')
        await repo.save_analytics_metric(snapshot_date, "allocation_by_asset", "[]")
        await repo.save_analytics_metric(snapshot_date, "allocation_by_source", "[]")
        await repo.save_analytics_metric(snapshot_date, "allocation_by_category", "[]")
        await repo.save_analytics_metric(snapshot_date, "currency_exposure", "[]")
        await repo.save_analytics_metric(snapshot_date, "risk_metrics", "{}")
        await repo.save_analytics_metric(
            snapshot_date,
            "pnl",
            '{"weekly":{"absolute_change":"0","percentage_change":"0"}}',
        )
        await repo.save_analytics_metric(snapshot_date, "weekly_pnl_by_asset", "[]")

    settings = SimpleNamespace(
        database_path=db_path,
        gemini_api_key=SecretStr("gemini-key"),
        telegram_chat_id="chat",
        telegram_bot_token=SecretStr("token"),
    )
    mock_send = AsyncMock(return_value=True)
    with (
        patch("pfm.cli.get_settings", return_value=settings),
        patch("pfm.reporting.send_report", mock_send),
    ):
        ok = await _report_async()

    assert ok is True
    sent_report = mock_send.await_args.args[0]
    assert "<b>PFM Weekly Report</b>" in sent_report.text
    assert "<b>AI Commentary</b>" in (sent_report.ai_summary_text or "")


async def test_run_pipeline_async_sends_error_alert_on_collect_failure():
    collect_result = CollectorResult(
        source="wise",
        snapshots_count=0,
        transactions_count=0,
        errors=["timeout"],
        duration_seconds=0.1,
    )

    mock_alert = AsyncMock(return_value=True)
    with (
        patch("pfm.cli._collect_async", AsyncMock(return_value=[collect_result])),
        patch("pfm.cli._analyze_async", AsyncMock()),
        patch("pfm.cli._report_async", AsyncMock(return_value=True)),
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.reporting.send_error_alert", mock_alert),
    ):
        ok = await _run_pipeline_async()

    assert ok is True
    mock_alert.assert_awaited_once_with(["wise: timeout"])
