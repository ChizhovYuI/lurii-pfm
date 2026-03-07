"""Tests for source store CRUD and source type validation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from pfm.db.models import Source, init_db
from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    InvalidSourceTypeError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.source_types import SOURCE_TYPES, validate_credentials


@pytest.fixture
async def store(tmp_path: Path) -> SourceStore:
    db_path = tmp_path / "test.db"
    await init_db(db_path)
    return SourceStore(db_path)


# ── Source types / credential validation ──────────────────────────────


def test_all_source_types_defined():
    assert len(SOURCE_TYPES) == 17
    expected = {
        "okx",
        "binance",
        "binance_th",
        "bybit",
        "mexc",
        "mexc_earn",
        "bitget_wallet",
        "lobstr",
        "blend",
        "wise",
        "kbank",
        "ibkr",
        "rabby",
        "revolut",
        "trading212",
        "emcd",
        "yo",
    }
    assert set(SOURCE_TYPES.keys()) == expected


def test_validate_credentials_unknown_type():
    errors = validate_credentials("unknown", {})
    assert len(errors) == 1
    assert "Unknown source type" in errors[0]


def test_validate_credentials_missing_required():
    errors = validate_credentials("okx", {"api_key": "k"})
    assert len(errors) == 2  # api_secret, passphrase


def test_validate_credentials_all_present():
    errors = validate_credentials(
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    assert errors == []


def test_validate_credentials_optional_fields():
    """Blend has optional soroban_rpc_url — should pass without it."""
    errors = validate_credentials(
        "blend",
        {
            "stellar_address": "GABC",
            "pool_contract_id": "CABC",
        },
    )
    assert errors == []


def test_validate_credentials_kbank():
    errors = validate_credentials(
        "kbank",
        {
            "gmail_address": "a@b.com",
            "gmail_app_password": "pass",
            "pdf_password": "01011990",
        },
    )
    assert errors == []


# ── Source store CRUD ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_add_and_get(store: SourceStore):
    creds = {"api_key": "k", "api_secret": "s", "passphrase": "p"}
    source = await store.add("okx-main", "okx", creds)

    assert source.name == "okx-main"
    assert source.type == "okx"
    assert json.loads(source.credentials) == creds
    assert source.enabled is True
    assert source.id is not None

    fetched = await store.get("okx-main")
    assert fetched.name == source.name
    assert json.loads(fetched.credentials) == creds


@pytest.mark.asyncio
async def test_add_duplicate_raises(store: SourceStore):
    creds = {"api_key": "k", "api_secret": "s", "passphrase": "p"}
    await store.add("okx-main", "okx", creds)
    with pytest.raises(DuplicateSourceError, match="already exists"):
        await store.add("okx-main", "okx", creds)


@pytest.mark.asyncio
async def test_add_invalid_type_raises(store: SourceStore):
    with pytest.raises(InvalidSourceTypeError, match="Unknown source type"):
        await store.add("foo", "unknown_exchange", {})


@pytest.mark.asyncio
async def test_add_missing_credentials_raises(store: SourceStore):
    with pytest.raises(InvalidCredentialsError, match="Missing required field"):
        await store.add("okx-main", "okx", {"api_key": "k"})


@pytest.mark.asyncio
async def test_get_not_found_raises(store: SourceStore):
    with pytest.raises(SourceNotFoundError, match="not found"):
        await store.get("nonexistent")


@pytest.mark.asyncio
async def test_list_all(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    await store.add("wise-main", "wise", {"api_token": "t"})

    sources = await store.list_all()
    assert len(sources) == 2
    assert sources[0].name == "okx-main"
    assert sources[1].name == "wise-main"


@pytest.mark.asyncio
async def test_list_enabled(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    await store.add("wise-main", "wise", {"api_token": "t"})
    await store.update("wise-main", enabled=False)

    enabled = await store.list_enabled()
    assert len(enabled) == 1
    assert enabled[0].name == "okx-main"


@pytest.mark.asyncio
async def test_delete(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    result = await store.delete("okx-main")
    assert result is True

    sources = await store.list_all()
    assert len(sources) == 0


@pytest.mark.asyncio
async def test_delete_not_found_raises(store: SourceStore):
    with pytest.raises(SourceNotFoundError, match="not found"):
        await store.delete("nonexistent")


@pytest.mark.asyncio
async def test_update_credentials(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    new_creds = {"api_key": "k2", "api_secret": "s2", "passphrase": "p2"}
    updated = await store.update("okx-main", credentials=new_creds)
    assert json.loads(updated.credentials) == new_creds


@pytest.mark.asyncio
async def test_update_enabled(store: SourceStore):
    await store.add("wise-main", "wise", {"api_token": "t"})

    disabled = await store.update("wise-main", enabled=False)
    assert disabled.enabled is False

    enabled = await store.update("wise-main", enabled=True)
    assert enabled.enabled is True


@pytest.mark.asyncio
async def test_update_partial_credentials_merges(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    updated = await store.update("okx-main", credentials={"api_key": "k2"})
    creds = json.loads(updated.credentials)
    assert creds == {"api_key": "k2", "api_secret": "s", "passphrase": "p"}


@pytest.mark.asyncio
async def test_update_invalid_credentials_raises(store: SourceStore):
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k",
            "api_secret": "s",
            "passphrase": "p",
        },
    )
    with pytest.raises(InvalidCredentialsError):
        await store.update("okx-main", credentials={"api_key": ""})


@pytest.mark.asyncio
async def test_update_not_found_raises(store: SourceStore):
    with pytest.raises(SourceNotFoundError):
        await store.update("nonexistent", enabled=False)


@pytest.mark.asyncio
async def test_update_no_changes(store: SourceStore):
    await store.add("wise-main", "wise", {"api_token": "t"})
    source = await store.update("wise-main")
    assert source.name == "wise-main"


@pytest.mark.asyncio
async def test_multiple_instances_same_type(store: SourceStore):
    """Multiple named instances of the same source type."""
    await store.add(
        "okx-main",
        "okx",
        {
            "api_key": "k1",
            "api_secret": "s1",
            "passphrase": "p1",
        },
    )
    await store.add(
        "okx-trading",
        "okx",
        {
            "api_key": "k2",
            "api_secret": "s2",
            "passphrase": "p2",
        },
    )

    sources = await store.list_all()
    assert len(sources) == 2
    assert {s.name for s in sources} == {"okx-main", "okx-trading"}


@pytest.mark.asyncio
async def test_source_model_frozen():
    """Source dataclass is frozen."""
    source = Source(name="test", type="okx", credentials="{}")
    with pytest.raises(AttributeError):
        source.name = "other"  # type: ignore[misc]
