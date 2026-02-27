"""Tests for CLI source management commands."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pfm.cli import cli
from pfm.db.models import init_db
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


# ── pipeline stubs ────────────────────────────────────────────────────


def test_collect_stub(runner):
    result = runner.invoke(cli, ["collect"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_analyze_stub(runner):
    result = runner.invoke(cli, ["analyze"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_report_stub(runner):
    result = runner.invoke(cli, ["report"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output


def test_run_stub(runner):
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output
