"""Tests for manual cash balance REST endpoints."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from pfm.db.models import Snapshot, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app
from pfm.server.state import get_pricing


def _today_utc() -> date:
    return datetime.now(tz=UTC).date()


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_get_cash_manual_requires_cash_source(client):
    resp = await client.get("/api/v1/cash/manual")
    assert resp.status == 404
    data = await resp.json()
    assert data["error"] == "Cash source not found"


async def test_put_cash_manual_requires_cash_source(client):
    resp = await client.put(
        "/api/v1/cash/manual",
        json={"selected_currencies": ["USD"], "balances": {"USD": "100"}},
    )
    assert resp.status == 404
    data = await resp.json()
    assert data["error"] == "Cash source not found"


async def test_put_cash_manual_saves_today_balances_and_updates_source(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD"})

    pricing = get_pricing(client.app)
    pricing._set_cache("EUR", Decimal("1.1"))

    resp = await client.put(
        "/api/v1/cash/manual",
        json={
            "selected_currencies": ["USD", "EUR"],
            "balances": {"USD": "100", "EUR": "50"},
        },
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["updated"] is True
    assert data["source_name"] == "cash"
    assert data["selected_currencies"] == ["USD", "EUR"]
    assert data["balances"]["USD"]["amount"] == "100"
    assert data["balances"]["EUR"]["usd_value"] == "55"

    updated_source = await store.get("cash")
    updated_credentials = json.loads(updated_source.credentials)
    assert updated_credentials["fiat_currencies"] == "USD,EUR"

    async with Repository(db_path) as repo:
        snapshots = await repo.get_snapshots_by_date(_today_utc())
    cash_rows = [row for row in snapshots if row.source == "cash" and row.source_name == "cash"]
    assert {(row.asset, row.amount, row.usd_value) for row in cash_rows} == {
        ("USD", Decimal(100), Decimal(100)),
        ("EUR", Decimal(50), Decimal(55)),
    }


async def test_get_cash_manual_returns_latest_resolved_balances(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD,EUR"})

    snapshot_date = date(2026, 3, 2)
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=snapshot_date,
                    source="cash",
                    source_name="cash",
                    asset="USD",
                    amount=Decimal(120),
                    usd_value=Decimal(120),
                    price=Decimal(1),
                ),
                Snapshot(
                    date=snapshot_date,
                    source="cash",
                    source_name="cash",
                    asset="EUR",
                    amount=Decimal(50),
                    usd_value=Decimal(55),
                    price=Decimal("1.1"),
                ),
            ]
        )

    resp = await client.get("/api/v1/cash/manual")
    assert resp.status == 200
    data = await resp.json()
    assert data["source_name"] == "cash"
    assert data["selected_currencies"] == ["USD", "EUR"]
    assert data["latest_snapshot_date"] == snapshot_date.isoformat()
    assert data["balances"]["USD"]["amount"] == "120"
    assert data["balances"]["EUR"]["price"] == "1.1"


async def test_put_cash_manual_stores_explicit_zero_and_prevents_old_carry_forward(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD"})

    yesterday = _today_utc() - timedelta(days=1)
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=yesterday,
                    source="cash",
                    source_name="cash",
                    asset="USD",
                    amount=Decimal(250),
                    usd_value=Decimal(250),
                    price=Decimal(1),
                )
            ]
        )

    resp = await client.put(
        "/api/v1/cash/manual",
        json={
            "selected_currencies": ["USD"],
            "balances": {"USD": "0"},
        },
    )
    assert resp.status == 200

    async with Repository(db_path) as repo:
        resolved = await repo.get_snapshots_resolved(_today_utc())
    cash_rows = [row for row in resolved if row.source == "cash" and row.source_name == "cash"]
    assert len(cash_rows) == 1
    assert cash_rows[0].date == _today_utc()
    assert cash_rows[0].amount == Decimal(0)
    assert cash_rows[0].usd_value == Decimal(0)


async def test_put_cash_manual_deselect_clears_old_currency_balance(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD,EUR"})

    pricing = get_pricing(client.app)
    pricing._set_cache("EUR", Decimal("1.1"))

    yesterday = _today_utc() - timedelta(days=1)
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=yesterday,
                    source="cash",
                    source_name="cash",
                    asset="USD",
                    amount=Decimal(100),
                    usd_value=Decimal(100),
                    price=Decimal(1),
                ),
                Snapshot(
                    date=yesterday,
                    source="cash",
                    source_name="cash",
                    asset="EUR",
                    amount=Decimal(25),
                    usd_value=Decimal("27.5"),
                    price=Decimal("1.1"),
                ),
            ]
        )

    resp = await client.put(
        "/api/v1/cash/manual",
        json={
            "selected_currencies": ["USD"],
            "balances": {"USD": "40"},
        },
    )
    assert resp.status == 200

    updated_source = await store.get("cash")
    updated_credentials = json.loads(updated_source.credentials)
    assert updated_credentials["fiat_currencies"] == "USD"

    async with Repository(db_path) as repo:
        resolved = await repo.get_snapshots_resolved(_today_utc())
    cash_rows = {row.asset: row for row in resolved if row.source == "cash" and row.source_name == "cash"}
    assert cash_rows["USD"].amount == Decimal(40)
    assert cash_rows["EUR"].date == _today_utc()
    assert cash_rows["EUR"].amount == Decimal(0)
    assert cash_rows["EUR"].usd_value == Decimal(0)


async def test_put_cash_manual_rejects_invalid_currency(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD"})

    resp = await client.put(
        "/api/v1/cash/manual",
        json={
            "selected_currencies": ["USD", "ABC"],
            "balances": {"USD": "100", "ABC": "50"},
        },
    )
    assert resp.status == 400
    data = await resp.json()
    assert "selected_currencies" in data["error"]


async def test_put_cash_manual_rejects_negative_amount(client, db_path):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD"})

    resp = await client.put(
        "/api/v1/cash/manual",
        json={
            "selected_currencies": ["USD"],
            "balances": {"USD": "-1"},
        },
    )
    assert resp.status == 400
    data = await resp.json()
    assert "non-negative" in data["error"]
