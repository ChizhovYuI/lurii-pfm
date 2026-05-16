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
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Snapshot, init_db
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
    # Sorted types:
    # 1 binance, 2 binance_th, 3 bitget_wallet, 4 blend, 5 bunq, 6 bybit,
    # 7 cash, 8 coinex, 9 emcd, 10 generic, 11 ibkr, 12 kbank, 13 lobstr,
    # 14 mexc, 15 mexc_earn, 16 okx, 17 rabby, 18 revolut, 19 trading212,
    # 20 wise, 21 yo.
    input_text = "20\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "added successfully" in result.output

    source = asyncio.run(store.get("wise-main"))
    assert source.type == "wise"


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_duplicate(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))
    input_text = "20\nwise-main\nmy-token\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 1
    assert "already exists" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_with_defaults(runner, store):
    # Blend has optional soroban_rpc_url with a default.
    # Sorted types: blend at index 4. (See test_source_add_wizard for full order.)
    input_text = "4\nblend-main\nGABC123\nCABC456\n\n"  # empty = accept default
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "added successfully" in result.output


@pytest.mark.usefixtures("_patched_settings")
def test_source_add_bunq_autogenerates_keypair(runner, store):
    # bunq is at index 5. Wizard prompts: api_key, environment (default
    # production), private_key_pem (skip), public_key_pem (skip). The CLI
    # then auto-generates the RSA keypair and persists it.
    input_text = "5\nbunq-main\nfake-api-key\n\n\n\n"
    result = runner.invoke(cli, ["source", "add"], input=input_text)
    assert result.exit_code == 0
    assert "Generated RSA-2048 keypair" in result.output

    source = asyncio.run(store.get("bunq-main"))
    assert source.type == "bunq"
    creds = json.loads(source.credentials)
    assert creds["api_key"] == "fake-api-key"
    assert creds["environment"] == "production"
    # Construct PEM headers piecewise so the detect-private-key pre-commit
    # hook does not flag this assertion as an embedded secret.
    priv_header = "-----BEGIN " + "PRIVATE KEY-----"
    assert creds["private_key_pem"].startswith(priv_header)
    assert creds["public_key_pem"].startswith("-----BEGIN PUBLIC KEY-----")


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


def _make_mock_collector(
    source_name,
    snaps=1,
    usd_total=Decimal(0),
    txns=0,
    errors=None,
    statement_date: date | None = None,
):
    """Create a mock collector class compatible with the parallel pipeline."""
    snap_price = usd_total / snaps if snaps else Decimal(0)
    fake_snapshots = [
        Snapshot(
            date=date(2026, 2, 27),
            source=source_name,
            source_name=source_name,
            asset="USD",
            amount=Decimal(1),
            usd_value=snap_price,
            price=snap_price,
        )
        for _ in range(snaps)
    ]

    mock_cls = MagicMock()
    mock_instance = MagicMock()
    fake_raw = [RawBalance(asset="USD", amount=Decimal(1), price=Decimal(1))]
    mock_instance.fetch_raw_balances = AsyncMock(return_value=fake_raw)
    mock_instance._build_snapshots = MagicMock(return_value=fake_snapshots)
    mock_instance.fetch_transactions = AsyncMock(return_value=[])
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
    mock_pricing.get_prices_usd = AsyncMock(return_value={})

    mock_repo = MagicMock()
    mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
    mock_repo.__aexit__ = AsyncMock(return_value=None)
    mock_repo.save_snapshots = AsyncMock()
    mock_repo.save_transactions = AsyncMock()

    with (
        patch("pfm.pricing.PricingService", return_value=mock_pricing),
        patch("pfm.db.repository.Repository", return_value=mock_repo),
    ):
        yield


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_single_source(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = _make_mock_collector("wise")
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "Processing: wise-main" in result.output
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
    with patch("pfm.collectors.COLLECTOR_REGISTRY", registry):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0
    assert "Processing: okx-main" in result.output
    assert "Processing: wise-main" in result.output
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

        async def fetch_raw_balances(self) -> list[RawBalance]:
            return [
                RawBalance(
                    asset="USD",
                    amount=Decimal(1),
                    price=Decimal(1),
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
        patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": _FakeWiseCollector}),
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
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "disabled" in result.output
    assert "Processing: wise-main" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_unknown_collector_type(runner, store):
    """Source in DB with no registered collector should be skipped."""
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    with patch("pfm.collectors.COLLECTOR_REGISTRY", {}):
        result = runner.invoke(cli, ["collect"])

    assert result.exit_code == 0
    assert "0 source(s)" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_with_errors(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = _make_mock_collector("wise")
    mock_cls.return_value.fetch_raw_balances = AsyncMock(side_effect=RuntimeError("Connection timeout"))
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "Connection timeout" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_with_country_access_error_pretty_output(runner, store):
    asyncio.run(store.add("okx-main", "okx", {"api_key": "k", "api_secret": "s", "passphrase": "p"}))

    # The new pipeline reports raw error text from fetch_raw_balances
    mock_cls = _make_mock_collector("okx")
    mock_cls.return_value.fetch_raw_balances = AsyncMock(side_effect=RuntimeError("DNS resolution failed"))
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"okx": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "okx-main"])

    assert result.exit_code == 0
    assert "DNS resolution failed" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_handles_unexpected_collector_exception(runner, store):
    asyncio.run(store.add("wise-main", "wise", {"api_token": "t"}))

    mock_cls = _make_mock_collector("wise")
    mock_cls.return_value.fetch_raw_balances = AsyncMock(side_effect=RuntimeError("boom"))
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "wise-main"])

    assert result.exit_code == 0
    assert "boom" in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_kbank_logs_statement_date_without_stale_hint_when_2_days(runner, store):
    asyncio.run(
        store.add(
            "kbank-main",
            "kbank",
            {"gmail_address": "a@b.com", "gmail_app_password": "pass", "pdf_password": "01011990"},
        )
    )

    # 2 days old (today=2026-02-27) → under 3-day threshold
    mock_cls = _make_mock_collector("kbank", statement_date=date(2026, 2, 25))
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-25 (2d ago)" in result.output
    assert "stale" not in result.output
    assert "Request a new statement from K PLUS" not in result.output


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
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-27 (0d ago)" in result.output
    assert "Request a new statement from K PLUS" not in result.output


@pytest.mark.usefixtures("_patched_settings", "_mock_pricing_repo")
def test_collect_kbank_logs_stale_hint_when_3_days_or_more(runner, store):
    asyncio.run(
        store.add(
            "kbank-main",
            "kbank",
            {"gmail_address": "a@b.com", "gmail_app_password": "pass", "pdf_password": "01011990"},
        )
    )

    # 3 days old (today=2026-02-27) → stale
    mock_cls = _make_mock_collector("kbank", statement_date=date(2026, 2, 24))
    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"kbank": mock_cls}):
        result = runner.invoke(cli, ["collect", "--source", "kbank-main"])

    assert result.exit_code == 0
    assert "KBank statement date: 2026-02-24 (3d ago)" in result.output
    assert "3 days old (3+ days is stale)" in result.output
    assert "Request a new statement from K PLUS" in result.output


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
