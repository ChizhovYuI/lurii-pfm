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

from pfm.cli import cli
from pfm.db.models import CollectorResult, Snapshot, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore

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
    # Simulate wizard: pick type 9 (wise), name "wise-main", token "my-token"
    # Type list is sorted, so wise is at position 9
    input_text = "9\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "added successfully" in result.output

    source = asyncio.run(store.get("wise-main"))
    assert source.type == "wise"


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_duplicate(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    input_text = "9\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 1
    assert "already exists" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_with_defaults(runner, store):
    # Blend has optional soroban_rpc_url with a default.
    # Sorted types: binance=1, binance_th=2, blend=3, bybit=4, ibkr=5, kbank=6, lobstr=7, okx=8, wise=9
    # blend is index 3: address, contract_id, rpc_url (has default)
    input_text = "3\nblend-main\nGABC123\nCABC456\n\n"  # empty = accept default
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


def _make_mock_collector(source_name, snaps=1, txns=0, errors=None):
    """Create a mock collector class that returns a CollectorResult."""
    result = CollectorResult(
        source=source_name,
        snapshots_count=snaps,
        transactions_count=txns,
        errors=errors or [],
        duration_seconds=0.1,
    )

    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_instance.collect = AsyncMock(return_value=result)
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

    mock_okx = _make_mock_collector("okx")
    mock_wise = _make_mock_collector("wise")
    registry = {"okx": mock_okx, "wise": mock_wise}
    with patch("pfm.cli.COLLECTOR_REGISTRY", registry):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0
    assert "Collecting: okx-main" in result.output
    assert "Collecting: wise-main" in result.output
    assert "TOTAL" in result.output


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


# ── pipeline stubs ────────────────────────────────────────────────────


@pytest.mark.usefixtures("_patched_settings")
def test_analyze_no_snapshots(runner):
    result = runner.invoke(cli, ["analyze"])
    assert result.exit_code == 0
    assert "No snapshots found" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_analyze_computes_and_caches_metrics(runner, db_path):
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
    assert "Cached analytics metrics" in result.output

    async def _load_metrics() -> dict[str, str]:
        async with Repository(db_path) as repo:
            return await repo.get_analytics_metrics_by_date(date(2024, 1, 15))

    metrics = asyncio.run(_load_metrics())
    assert set(metrics) == {
        "allocation_by_asset",
        "allocation_by_category",
        "allocation_by_source",
        "currency_exposure",
        "net_worth",
        "pnl",
        "risk_metrics",
        "yield",
    }
    assert json.loads(metrics["net_worth"]) == {"usd": "570.0"}
    assert json.loads(metrics["yield"]) == []
    pnl = json.loads(metrics["pnl"])
    assert pnl["daily"]["start_date"] == "2024-01-14"
    assert pnl["daily"]["end_date"] == "2024-01-15"


def test_report_stub(runner):
    result = runner.invoke(cli, ["report"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_run_stub(runner):
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output
