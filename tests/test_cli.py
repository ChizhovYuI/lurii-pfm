"""Tests for CLI source management commands."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from pfm.ai import CommentaryResult
from pfm.ai.base import CommentarySection
from pfm.cli import cli
from pfm.collectors.base import BaseCollector
from pfm.db.ai_store import AIProviderStore
from pfm.db.gemini_store import GeminiStore
from pfm.db.models import CollectorResult, Snapshot, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore
from pfm.db.telegram_store import TelegramStore

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def db_path(tmp_path: Path):
    """Create a temp DB and patch settings to use it."""
    path = tmp_path / "test.db"
    asyncio.run(init_db(path))
    return path


@pytest.fixture
def _patched_settings(db_path):
    """Patch get_settings to use temp DB path."""
    with patch("pfm.cli.get_settings") as mock_settings:
        settings = mock_settings.return_value
        settings.database_path = db_path
        settings.coingecko_api_key = ""
        yield


@pytest.fixture
def store(db_path):
    return SourceStore(db_path)


# ── Help ──────────────────────────────────────────────────────────────


def test_cli_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "source" in result.output
    assert "collect" in result.output


def test_source_help(runner):
    result = runner.invoke(cli, ["source", "--help"])
    assert result.exit_code == 0
    assert "add" in result.output
    assert "list" in result.output
    assert "show" in result.output
    assert "delete" in result.output
    assert "enable" in result.output
    assert "disable" in result.output


# ── source list ───────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_source_list_empty(runner):
    result = runner.invoke(cli, ["source", "list"])
    assert result.exit_code == 0
    assert "No sources configured" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_list_with_sources(runner, store):
    asyncio.run(
        store.add(
            "okx-main",
            "okx",
            {
                "api_key": "k",
                "api_secret": "s",
                "passphrase": "p",
            },
        )
    )
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    result = runner.invoke(cli, ["source", "list"])
    assert result.exit_code == 0
    assert "okx-main" in result.output
    assert "wise-main" in result.output
    assert "yes" in result.output  # enabled


# ── source add ────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_wizard(runner, store):
    # Simulate wizard: pick type 14 (wise), name "wise-main", token "my-token"
    # Type list is sorted, so wise is at position 14
    input_text = "14\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "added successfully" in result.output

    source = asyncio.run(store.get("wise-main"))
    assert source.type == "wise"


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_duplicate(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    input_text = "14\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 1
    assert "already exists" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_with_defaults(runner, store):
    # Blend has optional soroban_rpc_url with a default.
    # Sorted types: binance=1, binance_th=2, bitget_wallet=3, blend=4,
    # bybit=5, ibkr=6, kbank=7, lobstr=8, mexc=9, mexc_earn=10, okx=11, rabby=12, revolut=13, wise=14, yo=15.
    # blend is index 4: address, contract_id, rpc_url (has default)
    input_text = "4\nblend-main\nGABC123\nCABC456\n\n"  # empty = accept default
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "added successfully" in result.output


# ── source show ───────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_source_show(runner, store):
    asyncio.run(
        store.add(
            "okx-main",
            "okx",
            {
                "api_key": "abcdefghijk",
                "api_secret": "secretvalue123",
                "passphrase": "mypass",
            },
        )
    )
    result = runner.invoke(cli, ["source", "show", "okx-main"])
    assert result.exit_code == 0
    assert "okx-main" in result.output
    assert "okx" in result.output
    # Secrets should be masked
    assert "abc...ijk" in result.output
    assert "sec...123" in result.output
    # Short secrets get fully masked
    assert "***" in result.output  # "mypass" is <= 8 chars


@pytest.mark.usefixtures("_patched_settings")
def test_source_show_not_found(runner):
    result = runner.invoke(cli, ["source", "show", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


# ── source delete ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_source_delete_confirmed(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    result = runner.invoke(cli, ["source", "delete", "wise-main"], input="y\n")
    assert result.exit_code == 0
    assert "deleted" in result.output

    sources = asyncio.run(store.list_all())
    assert len(sources) == 0


@pytest.mark.usefixtures("_patched_settings")
def test_source_delete_cancelled(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    result = runner.invoke(cli, ["source", "delete", "wise-main"], input="n\n")
    assert result.exit_code == 0
    assert "Cancelled" in result.output

    sources = asyncio.run(store.list_all())
    assert len(sources) == 1


@pytest.mark.usefixtures("_patched_settings")
def test_source_delete_not_found(runner):
    result = runner.invoke(cli, ["source", "delete", "nonexistent"], input="y\n")
    assert result.exit_code == 1
    assert "not found" in result.output


# ── source enable / disable ───────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_source_enable_disable(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    result = runner.invoke(cli, ["source", "disable", "wise-main"])
    assert result.exit_code == 0
    assert "disabled" in result.output

    source = asyncio.run(store.get("wise-main"))
    assert source.enabled is False

    result = runner.invoke(cli, ["source", "enable", "wise-main"])
    assert result.exit_code == 0
    assert "enabled" in result.output

    source = asyncio.run(store.get("wise-main"))
    assert source.enabled is True


@pytest.mark.usefixtures("_patched_settings")
def test_source_enable_not_found(runner):
    result = runner.invoke(cli, ["source", "enable", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_disable_not_found(runner):
    result = runner.invoke(cli, ["source", "disable", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


# ── mask helper ───────────────────────────────────────────────────────


def test_mask_short():
    from pfm.cli import _mask

    assert _mask("short") == "***"
    assert _mask("12345678") == "***"


def test_mask_long():
    from pfm.cli import _mask

    assert _mask("abcdefghijk") == "abc...ijk"
    assert _mask("123456789") == "123...789"


# ── collect command ───────────────────────────────────────────────────


def _make_mock_collector(  # noqa: PLR0913
    source_name,
    snaps=1,
    usd_total=Decimal(0),
    txns=0,
    errors=None,
    statement_date: date | None = None,
):
    """Create a mock collector class that returns a CollectorResult."""
    result = CollectorResult(
        source=source_name,
        snapshots_count=snaps,
        snapshots_usd_total=usd_total,
        transactions_count=txns,
        errors=errors or [],
        duration_seconds=0.1,
    )

    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_instance.collect = AsyncMock(return_value=result)
    mock_instance.last_statement_date = statement_date
    mock_cls.return_value = mock_instance
    return mock_cls


@pytest.mark.usefixtures("_patched_settings")
def test_collect_no_sources(runner):
    result = runner.invoke(cli, ["collect"])
    assert result.exit_code == 0
    assert "No enabled sources" in result.output


@pytest.fixture
def _mock_pricing_repo():
    """Mock PricingService and Repository for collect tests."""
    mock_pricing = MagicMock()
    mock_pricing.close = AsyncMock()
    mock_pricing.today.return_value = date(2026, 2, 27)

    mock_repo = MagicMock()
    mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
    mock_repo.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("pfm.pricing.PricingService", return_value=mock_pricing),
        patch("pfm.db.repository.Repository", return_value=mock_repo),
    ):
        yield


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_single_source(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = _make_mock_collector("wise")
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "Collecting: wise-main" in result.output
    assert "Collection complete" in result.output
    mock_cls.assert_called_once()


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_all_enabled(runner, store):
    asyncio.run(
        store.add(
            "okx-main",
            "okx",
            {"api_key": "k", "api_secret": "s", "passphrase": "p"},
        )
    )
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_okx = _make_mock_collector("okx", usd_total=Decimal("450.0"))
    mock_wise = _make_mock_collector("wise", usd_total=Decimal("100.0"))
    registry = {"okx": mock_okx, "wise": mock_wise}
    with patch("pfm.cli.COLLECTOR_REGISTRY", registry):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0
    assert "Collecting: okx-main" in result.output
    assert "Collecting: wise-main" in result.output
    assert "TOTAL" in result.output
    assert "550.00" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_collect_keeps_snapshots_for_two_sources_of_same_type(runner, store, db_path):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t-main"}))
    asyncio.run(store.add("wise-alt", "wise", {"api_token": "t-alt"}))

    class _FakeWiseCollector(BaseCollector):
        source_name = "wise"

        def __init__(self, pricing, *, api_token):
            super().__init__(pricing)
            self._api_token = api_token

        async def fetch_balances(self) -> list[Snapshot]:
            return [
                Snapshot(
                    date=self._pricing.today(),
                    source=self.source_name,
                    asset="USD",
                    amount=Decimal(1),
                    usd_value=Decimal(1),
                )
            ]

        async def fetch_transactions(self, since: date | None = None):
            return []

    mock_pricing = MagicMock()
    mock_pricing.today.return_value = date(2026, 2, 27)
    mock_pricing.close = AsyncMock()

    with (
        patch("pfm.server.client.is_daemon_reachable", return_value=False),
        patch("pfm.pricing.PricingService", return_value=mock_pricing),
        patch("pfm.cli.COLLECTOR_REGISTRY", {"wise": _FakeWiseCollector}),
    ):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0

    async def _load_sources() -> set[tuple[str, str]]:
        async with Repository(db_path) as repo:
            snaps = await repo.get_snapshots_by_date(date(2026, 2, 27))
        return {(s.source, s.source_name) for s in snaps}

    assert asyncio.run(_load_sources()) == {("wise", "wise-alt"), ("wise", "wise-main")}


@pytest.mark.usefixtures("_patched_settings")
def test_collect_skips_disabled(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    asyncio.run(store.update("wise-main", enabled=False))

    result = runner.invoke(cli, ["collect"])
    assert result.exit_code == 0
    assert "No enabled sources" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_collect_source_not_found(runner):
    result = runner.invoke(cli, ["collect", "--source", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_disabled_source_by_name(runner, store):
    """Running a disabled source by name should warn but still run."""
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    asyncio.run(store.update("wise-main", enabled=False))

    mock_cls = _make_mock_collector("wise")
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "disabled" in result.output
    assert "Collecting: wise-main" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_unknown_collector_type(runner, store):
    """Source in DB with no registered collector should be skipped."""
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    with patch("pfm.cli.COLLECTOR_REGISTRY", {}):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0
    assert "Skipping" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_with_errors(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = _make_mock_collector("wise", errors=["Connection timeout"])
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "Connection timeout" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_with_country_access_error_pretty_output(runner, store):
    asyncio.run(store.add("okx-main", "okx", {"api_key": "k", "api_secret": "s", "passphrase": "p"}))

    error = (
        "Failed to fetch balances from okx: "
        "service access appears restricted from your current network or region. try a vpn and retry."
    )
    mock_cls = _make_mock_collector("okx", errors=[error])
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"okx": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "okx-main"])

    assert result.exit_code == 0
    assert "okx: cannot fetch balances because access looks geo-restricted." in result.output
    assert "Hint: connect a VPN (or run from a supported country) and retry." in result.output
    assert "service access appears restricted from your current network or region" not in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_handles_unexpected_collector_exception(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_instance.collect = AsyncMock(side_effect=RuntimeError("boom"))
    mock_cls.return_value = mock_instance

    with patch("pfm.cli.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "Unhandled collector error: boom" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_kbank_logs_statement_date_without_stale_hint_when_yesterday(runner, store):
    asyncio.run(
        store.add(
            "kbank-main",
            "kbank",
            {"gmail_address": "a@b.com", "gmail_app_password": "pass", "pdf_password": "01011990"},
        )
    )

    mock_cls = _make_mock_collector("kbank", statement_date=date(2026, 2, 26))
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-26" in result.output
    assert "Statement is older than yesterday" not in result.output
    assert "Request a new statement from K PLUS and send it to your email" not in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_kbank_logs_statement_date_without_stale_hint_when_fresh(runner, store):
    asyncio.run(
        store.add(
            "kbank-main",
            "kbank",
            {"gmail_address": "a@b.com", "gmail_app_password": "pass", "pdf_password": "01011990"},
        )
    )

    mock_cls = _make_mock_collector("kbank", statement_date=date(2026, 2, 27))
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-27" in result.output
    assert "Request a new statement from K PLUS and send it to your email" not in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_kbank_logs_stale_hint_when_older_than_yesterday(runner, store):
    asyncio.run(
        store.add(
            "kbank-main",
            "kbank",
            {"gmail_address": "a@b.com", "gmail_app_password": "pass", "pdf_password": "01011990"},
        )
    )

    mock_cls = _make_mock_collector("kbank", statement_date=date(2026, 2, 25))
    with patch("pfm.cli.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-25" in result.output
    assert "Statement is older than yesterday (2026-02-26)." in result.output
    assert "Request a new statement from K PLUS and send it to your email" in result.output


# ── pipeline stubs ────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_analyze_no_snapshots(runner):
    result = runner.invoke(cli, ["analyze"])
    assert result.exit_code == 0
    assert "No snapshots found" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_analyze_computes_and_displays_metrics(runner, db_path):
    async def _seed_data() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshots(
                [
                    Snapshot(
                        date=date(2024, 1, 14),
                        source="wise",
                        asset="USD",
                        amount=Decimal("100.0"),
                        usd_value=Decimal("100.0"),
                    ),
                    Snapshot(
                        date=date(2024, 1, 15),
                        source="wise",
                        asset="USD",
                        amount=Decimal("120.0"),
                        usd_value=Decimal("120.0"),
                    ),
                    Snapshot(
                        date=date(2024, 1, 15),
                        source="okx",
                        asset="BTC",
                        amount=Decimal("0.01"),
                        usd_value=Decimal("450.0"),
                    ),
                ]
            )

    asyncio.run(_seed_data())

    result = runner.invoke(cli, ["analyze"])
    assert result.exit_code == 0
    assert "Analytics date: 2024-01-15" in result.output
    assert "Net worth (USD): 570.00" in result.output
    assert "Analytics computed (on-the-fly, no caching)." in result.output

    # Verify nothing was cached
    async def _load_metrics() -> dict[str, str]:
        async with Repository(db_path) as repo:
            return await repo.get_analytics_metrics_by_date(date(2024, 1, 15))

    metrics = asyncio.run(_load_metrics())
    assert metrics == {}


@pytest.mark.usefixtures("_patched_settings")
def test_report_no_snapshots(runner):
    result = runner.invoke(cli, ["report"])
    assert result.exit_code == 0
    assert "Telegram is not configured. Skipping report send." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_report_telegram_not_configured_with_snapshots(runner, db_path):
    async def _seed_snapshot() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshot(
                Snapshot(
                    date=date(2024, 1, 15),
                    source="wise",
                    asset="USD",
                    amount=Decimal("100.0"),
                    usd_value=Decimal("100.0"),
                )
            )

    asyncio.run(_seed_snapshot())

    result = runner.invoke(cli, ["report"])
    assert result.exit_code == 0
    assert "Telegram is not configured. Skipping report send." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_report_success(runner, db_path):
    async def _seed() -> None:
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
            await repo.save_analytics_metric(snapshot_date, "ai_commentary", '{"text":"All good."}')

    asyncio.run(_seed())

    with (
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.reporting.send_report", AsyncMock(return_value=True)),
    ):
        result = runner.invoke(cli, ["report"])

    assert result.exit_code == 0
    assert "Using cached AI commentary." in result.output
    assert "Report sent to Telegram." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_report_uses_cached_ai_commentary(runner, db_path):
    async def _seed() -> None:
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
            await repo.save_analytics_metric(
                snapshot_date,
                "ai_commentary",
                '{"text":"Cached comment","model":"gemini-2.5-flash"}',
            )

    asyncio.run(_seed())

    with (
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.reporting.send_report", AsyncMock(return_value=True)),
    ):
        result = runner.invoke(cli, ["report"])

    assert result.exit_code == 0
    assert "Using cached AI commentary." in result.output
    assert "AI commentary model: gemini-2.5-flash" in result.output
    assert "Report sent to Telegram." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_report_without_cached_ai_commentary_uses_fallback_text(runner, db_path):
    async def _seed() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshot(
                Snapshot(
                    date=date(2024, 1, 15),
                    source="wise",
                    asset="USD",
                    amount=Decimal("100.0"),
                    usd_value=Decimal("100.0"),
                )
            )

    asyncio.run(_seed())

    with (
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.reporting.send_report", AsyncMock(return_value=True)),
    ):
        result = runner.invoke(cli, ["report"])

    assert result.exit_code == 0
    assert "No cached AI commentary for this analysis date. Using fallback text." in result.output
    assert "Report sent to Telegram." in result.output
    assert "Report sent to Telegram." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_comment_command_generates_prints_and_caches(runner, db_path):
    async def _seed() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshot(
                Snapshot(
                    date=date(2024, 1, 15),
                    source="wise",
                    asset="USD",
                    amount=Decimal("100.0"),
                    usd_value=Decimal("100.0"),
                )
            )

    asyncio.run(_seed())

    with patch(
        "pfm.ai.generate_commentary_with_model",
        AsyncMock(return_value=CommentaryResult(text="AI says hold steady.", model="gemini-2.5-flash")),
    ):
        result = runner.invoke(cli, ["comment"])

    assert result.exit_code == 0
    assert "AI commentary date: 2024-01-15" in result.output
    assert "AI model: gemini-2.5-flash" in result.output
    assert "AI says hold steady." in result.output
    assert "AI commentary cached." in result.output

    async def _load_metrics() -> dict[str, str]:
        async with Repository(db_path) as repo:
            return await repo.get_analytics_metrics_by_date(date(2024, 1, 15))

    metrics = asyncio.run(_load_metrics())
    assert json.loads(metrics["ai_commentary"]) == {
        "text": "AI says hold steady.",
        "model": "gemini-2.5-flash",
    }


@pytest.mark.usefixtures("_patched_settings")
def test_comment_command_stores_sections(runner, db_path):
    """When commentary has sections, they are included in the cached metric."""

    async def _seed() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshot(
                Snapshot(
                    date=date(2024, 1, 15),
                    source="wise",
                    asset="USD",
                    amount=Decimal("100.0"),
                    usd_value=Decimal("100.0"),
                )
            )

    asyncio.run(_seed())

    sections = (
        CommentarySection(title="Market", description="BTC up."),
        CommentarySection(title="Risk", description="Low."),
    )
    with patch(
        "pfm.ai.generate_commentary_with_model",
        AsyncMock(
            return_value=CommentaryResult(
                text="Market\nBTC up.\n\nRisk\nLow.",
                model="llama3.1:8b",
                sections=sections,
            )
        ),
    ):
        result = runner.invoke(cli, ["comment"])

    assert result.exit_code == 0

    async def _load_metrics() -> dict[str, str]:
        async with Repository(db_path) as repo:
            return await repo.get_analytics_metrics_by_date(date(2024, 1, 15))

    metrics = asyncio.run(_load_metrics())
    cached = json.loads(metrics["ai_commentary"])
    assert cached["model"] == "llama3.1:8b"
    assert cached["sections"] == [
        {"title": "Market", "description": "BTC up."},
        {"title": "Risk", "description": "Low."},
    ]


@pytest.mark.usefixtures("_patched_settings")
def test_report_handles_internal_exception(runner, db_path):
    async def _seed() -> None:
        async with Repository(db_path) as repo:
            await repo.save_snapshot(
                Snapshot(
                    date=date(2024, 1, 15),
                    source="wise",
                    asset="USD",
                    amount=Decimal("100.0"),
                    usd_value=Decimal("100.0"),
                )
            )

    asyncio.run(_seed())

    with (
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.ai.generate_commentary", AsyncMock(return_value="All good.")),
        patch("pfm.reporting.format_weekly_report", side_effect=RuntimeError("format failed")),
    ):
        result = runner.invoke(cli, ["report"])

    assert result.exit_code == 1
    assert "Failed to generate/send report: format failed" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_run_pipeline_success(runner):
    with (
        patch("pfm.cli._collect_async", AsyncMock(return_value=[])),
        patch("pfm.cli._analyze_async", AsyncMock()),
        patch("pfm.cli._report_async", AsyncMock(return_value=True)),
    ):
        result = runner.invoke(cli, ["run"])

    assert result.exit_code == 0
    assert "Pipeline finished successfully." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_run_pipeline_sends_error_alert(runner):
    collect_result = CollectorResult(
        source="wise",
        snapshots_count=1,
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
        result = runner.invoke(cli, ["run"])

    assert result.exit_code == 0
    assert "Collection completed with 1 error(s)." in result.output
    mock_alert.assert_awaited_once_with(["wise: timeout"])


@pytest.mark.usefixtures("_patched_settings")
def test_run_pipeline_report_failure(runner):
    with (
        patch("pfm.cli._collect_async", AsyncMock(return_value=[])),
        patch("pfm.cli._analyze_async", AsyncMock()),
        patch("pfm.cli._report_async", AsyncMock(return_value=False)),
    ):
        result = runner.invoke(cli, ["run"])

    assert result.exit_code == 1
    assert "Pipeline finished with errors." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_run_pipeline_analyze_exception_triggers_alert(runner):
    mock_alert = AsyncMock(return_value=True)
    with (
        patch("pfm.cli._collect_async", AsyncMock(return_value=[])),
        patch("pfm.cli._analyze_async", AsyncMock(side_effect=RuntimeError("analyze boom"))),
        patch("pfm.cli._report_async", AsyncMock(return_value=True)),
        patch("pfm.reporting.is_telegram_configured", AsyncMock(return_value=True)),
        patch("pfm.reporting.send_error_alert", mock_alert),
    ):
        result = runner.invoke(cli, ["run"])

    assert result.exit_code == 1
    assert "Analyze failed: analyze boom" in result.output
    assert "Pipeline finished with errors." in result.output
    sent_errors = mock_alert.await_args.args[0]
    assert any("analyze stage failed: analyze boom" in err for err in sent_errors)


@pytest.mark.usefixtures("_patched_settings")
def test_gemini_set_show_clear(runner, db_path):
    set_result = runner.invoke(
        cli,
        ["gemini", "set", "--api-key", "gemini-secret-123"],
    )
    assert set_result.exit_code == 0
    assert "Gemini API key saved." in set_result.output

    show_result = runner.invoke(cli, ["gemini", "show"])
    assert show_result.exit_code == 0
    assert "Gemini configuration:" in show_result.output
    assert "gem...123" in show_result.output

    clear_result = runner.invoke(cli, ["gemini", "clear"], input="y\n")
    assert clear_result.exit_code == 0
    assert "Gemini API key removed." in clear_result.output

    async def _load() -> object:
        return await GeminiStore(db_path).get()

    assert asyncio.run(_load()) is None


@pytest.mark.usefixtures("_patched_settings")
def test_gemini_show_empty(runner):
    result = runner.invoke(cli, ["gemini", "show"])
    assert result.exit_code == 0
    assert "Gemini is not configured. Run 'pfm gemini set'." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_telegram_set_show_clear(runner, db_path):
    set_result = runner.invoke(
        cli,
        ["telegram", "set", "--bot-token", "token-12345", "--chat-id", "chat-42"],
    )
    assert set_result.exit_code == 0
    assert "Telegram credentials saved." in set_result.output

    show_result = runner.invoke(cli, ["telegram", "show"])
    assert show_result.exit_code == 0
    assert "Telegram configuration:" in show_result.output
    assert "tok...345" in show_result.output
    assert "chat-42" in show_result.output

    clear_result = runner.invoke(cli, ["telegram", "clear"], input="y\n")
    assert clear_result.exit_code == 0
    assert "Telegram credentials removed." in clear_result.output

    async def _load() -> object:
        return await TelegramStore(db_path).get()

    assert asyncio.run(_load()) is None


@pytest.mark.usefixtures("_patched_settings")
def test_telegram_show_empty(runner):
    result = runner.invoke(cli, ["telegram", "show"])
    assert result.exit_code == 0
    assert "Telegram is not configured. Run 'pfm telegram set'." in result.output


# ── AI provider commands ─────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_show_clear(runner, db_path):
    set_result = runner.invoke(
        cli,
        ["ai", "set", "--provider", "gemini", "--api-key", "gemini-secret-123"],
    )
    assert set_result.exit_code == 0
    assert "AI provider set to: gemini" in set_result.output

    show_result = runner.invoke(cli, ["ai", "show"])
    assert show_result.exit_code == 0
    assert "AI configuration:" in show_result.output
    assert "Provider: gemini" in show_result.output
    assert "gem...123" in show_result.output

    clear_result = runner.invoke(cli, ["ai", "clear"], input="y\n")
    assert clear_result.exit_code == 0
    assert "AI provider deactivated." in clear_result.output

    async def _load():
        return await AIProviderStore(db_path).get_active()

    assert asyncio.run(_load()) is None


@pytest.mark.usefixtures("_patched_settings")
def test_ai_show_empty(runner):
    result = runner.invoke(cli, ["ai", "show"])
    assert result.exit_code == 0
    assert "AI provider is not configured. Run 'pfm ai set'." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_clear_cancelled(runner):
    result = runner.invoke(cli, ["ai", "clear"], input="n\n")
    assert result.exit_code == 0
    assert "Cancelled." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_clear_when_empty(runner):
    result = runner.invoke(cli, ["ai", "clear"], input="y\n")
    assert result.exit_code == 0
    assert "No AI provider was active." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_ollama_no_api_key(runner):
    result = runner.invoke(cli, ["ai", "set", "--provider", "ollama", "--model", "llama3.1:8b"])
    assert result.exit_code == 0
    assert "AI provider set to: ollama" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_ollama_picks_model_interactively(runner):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "models": [
            {
                "name": "llama3.1:8b",
                "size": 4_920_753_328,
                "details": {"parameter_size": "8.0B", "quantization_level": "Q4_K_M"},
            },
            {
                "name": "mistral:7b",
                "size": 3_800_000_000,
                "details": {"parameter_size": "7.2B", "quantization_level": "Q4_0"},
            },
        ]
    }
    mock_response.raise_for_status = MagicMock()
    with patch("pfm.cli.httpx.get", return_value=mock_response):
        result = runner.invoke(cli, ["ai", "set", "--provider", "ollama"], input="1\n")

    assert result.exit_code == 0
    assert "Available Ollama models:" in result.output
    assert "llama3.1:8b" in result.output
    assert "4.6 GB RAM" in result.output
    assert "AI provider set to: ollama" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_ollama_connect_error_exits(runner):
    with patch("pfm.cli.httpx.get", side_effect=__import__("httpx").ConnectError("refused")):
        result = runner.invoke(cli, ["ai", "set", "--provider", "ollama"])

    assert result.exit_code == 1
    assert "brew install ollama" in result.output
    assert "ollama serve" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_with_model_and_base_url(runner):
    result = runner.invoke(
        cli,
        [
            "ai",
            "set",
            "--provider",
            "ollama",
            "--model",
            "llama3.1:70b",
            "--base-url",
            "http://gpu-server:11434",
        ],
    )
    assert result.exit_code == 0
    assert "AI provider set to: ollama" in result.output
    assert "Model: llama3.1:70b" in result.output
    assert "Base URL: http://gpu-server:11434" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_openrouter_picks_model_interactively(runner):
    # input: api key + model choice "1"
    result = runner.invoke(
        cli,
        ["ai", "set", "--provider", "openrouter"],
        input="sk-or-test-key\n1\n",
    )
    assert result.exit_code == 0
    assert "Available OpenRouter models:" in result.output
    assert "free" in result.output
    assert "qwen/qwen3-235b-a22b-thinking-2507" in result.output
    assert "AI provider set to: openrouter" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_providers_lists_all(runner):
    result = runner.invoke(cli, ["ai", "providers"])
    assert result.exit_code == 0
    assert "gemini" in result.output
    assert "ollama" in result.output
    assert "openrouter" in result.output
    assert "grok" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_set_prompts_for_key_when_required(runner, db_path):
    """Providers like gemini should prompt for API key if not given."""
    result = runner.invoke(
        cli,
        ["ai", "set", "--provider", "gemini"],
        input="my-api-key-for-gemini\n",
    )
    assert result.exit_code == 0
    assert "AI provider set to: gemini" in result.output

    async def _load():
        return await AIProviderStore(db_path).get_active()

    config = asyncio.run(_load())
    assert config is not None
    assert config.api_key == "my-api-key-for-gemini"


# ── AI multi-provider commands ───────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_ai_list_empty(runner):
    result = runner.invoke(cli, ["ai", "list"])
    assert result.exit_code == 0
    assert "No AI providers configured" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_list_with_providers(runner):
    runner.invoke(cli, ["ai", "set", "--provider", "gemini", "--api-key", "gk"])
    runner.invoke(cli, ["ai", "set", "--provider", "ollama", "--model", "llama3.1:8b"])

    result = runner.invoke(cli, ["ai", "list"])
    assert result.exit_code == 0
    assert "ollama (active)" in result.output
    assert "gemini" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_activate(runner):
    runner.invoke(cli, ["ai", "set", "--provider", "gemini", "--api-key", "gk"])
    runner.invoke(cli, ["ai", "set", "--provider", "ollama", "--model", "llama3.1:8b"])

    result = runner.invoke(cli, ["ai", "activate", "gemini"])
    assert result.exit_code == 0
    assert "Activated AI provider: gemini" in result.output

    show = runner.invoke(cli, ["ai", "show"])
    assert "Provider: gemini" in show.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_activate_unconfigured(runner):
    result = runner.invoke(cli, ["ai", "activate", "nonexistent"])
    assert result.exit_code == 1
    assert "not configured" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_deactivate(runner):
    runner.invoke(cli, ["ai", "set", "--provider", "gemini", "--api-key", "gk"])

    result = runner.invoke(cli, ["ai", "deactivate"])
    assert result.exit_code == 0
    assert "AI provider deactivated." in result.output

    show = runner.invoke(cli, ["ai", "show"])
    assert "AI provider is not configured" in show.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_deactivate_when_none(runner):
    result = runner.invoke(cli, ["ai", "deactivate"])
    assert result.exit_code == 0
    assert "No AI provider was active." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_remove(runner):
    runner.invoke(cli, ["ai", "set", "--provider", "gemini", "--api-key", "gk"])

    result = runner.invoke(cli, ["ai", "remove", "gemini"], input="y\n")
    assert result.exit_code == 0
    assert "removed" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_remove_cancelled(runner):
    runner.invoke(cli, ["ai", "set", "--provider", "gemini", "--api-key", "gk"])

    result = runner.invoke(cli, ["ai", "remove", "gemini"], input="n\n")
    assert result.exit_code == 0
    assert "Cancelled." in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_ai_remove_nonexistent(runner):
    result = runner.invoke(cli, ["ai", "remove", "nonexistent"], input="y\n")
    assert result.exit_code == 0
    assert "not found" in result.output
