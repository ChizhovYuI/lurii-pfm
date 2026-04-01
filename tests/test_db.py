"""Tests for database models and repository."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import aiosqlite
import pytest

from pfm.db.migrations import runner as migration_runner
from pfm.db.models import Price, Snapshot, Transaction, TransactionType, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceNotFoundError


@pytest.fixture
async def repo(tmp_path):
    db_path = tmp_path / "test.db"
    async with Repository(db_path) as r:
        yield r


async def test_init_db(tmp_path):
    db_path = tmp_path / "init_test.db"
    await init_db(db_path)
    assert db_path.exists()
    async with aiosqlite.connect(str(db_path)) as db:
        version_row = await (await db.execute("SELECT version_num FROM alembic_version")).fetchone()
    assert version_row is not None
    assert version_row[0] == "g7h8i9j0k1l2"


def test_runner_uses_package_relative_migration_path(tmp_path):
    db_path = tmp_path / "config_test.db"
    engine = migration_runner._create_engine(db_path)
    try:
        with engine.connect() as connection:
            config = migration_runner._make_config(db_path, connection)
    finally:
        engine.dispose()

    assert config.config_file_name is None
    assert Path(config.get_main_option("script_location")) == Path(migration_runner.__file__).resolve().parent


async def test_init_db_migrates_legacy_transactions_schema(tmp_path):
    db_path = tmp_path / "legacy.db"
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                counterparty_asset TEXT NOT NULL DEFAULT '',
                counterparty_amount TEXT NOT NULL DEFAULT '0',
                tx_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                asset TEXT NOT NULL,
                currency TEXT NOT NULL,
                price TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'coingecko',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE analytics_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                credentials TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        await db.executemany(
            (
                "INSERT INTO transactions "
                "(date, source, tx_type, asset, amount, usd_value, tx_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            [
                ("2024-01-10", "wise", "deposit", "EUR", "100", "110", "dup"),
                ("2024-01-11", "wise", "deposit", "EUR", "100", "110", "dup"),
            ],
        )
        await db.commit()

    await init_db(db_path)

    async with aiosqlite.connect(str(db_path)) as db:
        cursor = await db.execute("PRAGMA table_info(transactions)")
        columns = {row[1] for row in await cursor.fetchall()}
        assert "source_name" in columns
        assert "trade_side" in columns

        index_row = await (
            await db.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_transactions_source_name_tx_id_unique'"
            )
        ).fetchone()
        # Migration drops all transactions (stale data incompatible with new type rules).
        cursor = await db.execute("SELECT COUNT(*) FROM transactions")
        count = (await cursor.fetchone())[0]

    assert count == 0
    assert index_row is not None
    assert "source_name != ''" in str(index_row[0])


async def test_init_db_keeps_existing_transaction_source_name_rows_without_backfill(tmp_path):
    db_path = tmp_path / "legacy_projected_collision.db"
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                counterparty_asset TEXT NOT NULL DEFAULT '',
                counterparty_amount TEXT NOT NULL DEFAULT '0',
                tx_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                source_name TEXT NOT NULL DEFAULT '',
                trade_side TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                credentials TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                asset TEXT NOT NULL,
                currency TEXT NOT NULL,
                price TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'coingecko',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE analytics_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        await db.execute("CREATE INDEX idx_transactions_source_name_date ON transactions(source_name, date)")
        await db.execute(
            "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
            ("lobstr-main", "lobstr", "{}", 1),
        )
        await db.executemany(
            (
                "INSERT INTO transactions "
                "(date, source, source_name, tx_type, asset, amount, usd_value, tx_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            [
                ("2024-01-10", "lobstr", "lobstr", "deposit", "XLM", "10", "10", "dup"),
                ("2024-01-10", "lobstr", "lobstr-main", "deposit", "XLM", "10", "10", "dup"),
            ],
        )
        await db.commit()

    await init_db(db_path)

    async with aiosqlite.connect(str(db_path)) as db:
        index_row = await (
            await db.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_transactions_source_name_tx_id_unique'"
            )
        ).fetchone()
        # Migration drops all transactions (stale data incompatible with new type rules).
        cursor = await db.execute("SELECT COUNT(*) FROM transactions")
        count = (await cursor.fetchone())[0]

    assert count == 0
    assert index_row is not None
    assert "source_name != ''" in str(index_row[0])


async def test_init_db_deduplicates_kbank_rows_with_empty_tx_id(tmp_path):
    db_path = tmp_path / "kbank-empty-txid.db"
    await init_db(db_path)

    duplicate_payload = '{"description":"QRyment","balance":"45457.78"}'
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executemany(
            (
                "INSERT INTO transactions "
                "(date, source, source_name, tx_type, asset, amount, usd_value, tx_id, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            [
                ("2026-03-10", "kbank", "kbank-main", "deposit", "THB", "45000.00", "0", "", duplicate_payload),
                ("2026-03-10", "kbank", "kbank-main", "deposit", "THB", "45000.00", "0", "", duplicate_payload),
                ("2026-03-10", "kbank", "kbank-main", "deposit", "THB", "45000.00", "0", "", duplicate_payload),
                ("2026-03-10", "wise", "wise-main", "deposit", "THB", "45000.00", "0", "", duplicate_payload),
                ("2026-03-10", "wise", "wise-main", "deposit", "THB", "45000.00", "0", "", duplicate_payload),
            ],
        )
        await db.execute("DELETE FROM alembic_version")
        await db.execute("INSERT INTO alembic_version (version_num) VALUES (?)", ("e7b9c1d4a5f0",))
        await db.commit()

    await init_db(db_path)

    async with aiosqlite.connect(str(db_path)) as db:
        version_row = await (await db.execute("SELECT version_num FROM alembic_version")).fetchone()
        cursor = await db.execute(
            "SELECT source, source_name, COUNT(*) "
            "FROM transactions "
            "WHERE tx_id = '' "
            "GROUP BY source, source_name "
            "ORDER BY source, source_name"
        )
        rows = await cursor.fetchall()

    assert version_row is not None
    assert version_row[0] == "g7h8i9j0k1l2"
    # Migration drops all transactions (stale data incompatible with new type rules).
    assert rows == []


async def test_save_and_get_snapshot(repo):
    snapshot = Snapshot(
        date=date(2024, 1, 15),
        source="test",
        asset="BTC",
        amount=Decimal("1.5"),
        usd_value=Decimal("67500.00"),
    )
    await repo.save_snapshot(snapshot)
    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "test"
    assert results[0].source_name == "test"
    assert results[0].asset == "BTC"
    assert results[0].amount == Decimal("1.5")
    assert results[0].usd_value == Decimal("67500.00")


async def test_save_snapshots_batch(repo):
    snapshots = [
        Snapshot(date=date(2024, 1, 15), source="test", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
        Snapshot(date=date(2024, 1, 15), source="test", asset="ETH", amount=Decimal(10), usd_value=Decimal(25000)),
    ]
    await repo.save_snapshots(snapshots)
    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 2


async def test_save_snapshots_replaces_same_source_and_date(repo):
    first_batch = [
        Snapshot(date=date(2024, 1, 15), source="wise", asset="GBP", amount=Decimal(100), usd_value=Decimal(125)),
        Snapshot(date=date(2024, 1, 15), source="wise", asset="EUR", amount=Decimal(50), usd_value=Decimal(55)),
    ]
    second_batch = [
        Snapshot(date=date(2024, 1, 15), source="wise", asset="GBP", amount=Decimal(120), usd_value=Decimal(150)),
    ]

    await repo.save_snapshots(first_batch)
    await repo.save_snapshots(second_batch)

    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "wise"
    assert results[0].source_name == "wise"
    assert results[0].asset == "GBP"
    assert results[0].amount == Decimal(120)
    assert results[0].usd_value == Decimal(150)


async def test_get_latest_snapshots_empty(repo):
    results = await repo.get_latest_snapshots()
    assert results == []


async def test_get_latest_snapshots(repo):
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 14), source="s1", asset="BTC", amount=Decimal(1), usd_value=Decimal(1))
    )
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 15), source="s1", asset="ETH", amount=Decimal(2), usd_value=Decimal(2))
    )
    results = await repo.get_latest_snapshots()
    assert len(results) == 1
    assert results[0].date == date(2024, 1, 15)


async def test_get_latest_snapshots_resolves_per_source(repo):
    """Stale sources (e.g., KBank) are included by resolving per-source latest date."""
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28))
    )
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 15), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000))
    )
    results = await repo.get_latest_snapshots()
    assert len(results) == 2
    sources = {r.source for r in results}
    assert sources == {"kbank", "okx"}


async def test_save_snapshots_keeps_same_type_with_different_source_names(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(
                date=target_date,
                source="blend",
                source_name="blend-main",
                asset="USDC",
                amount=Decimal(100),
                usd_value=Decimal(100),
            )
        ]
    )
    await repo.save_snapshots(
        [
            Snapshot(
                date=target_date,
                source="blend",
                source_name="blend-alt",
                asset="USDC",
                amount=Decimal(200),
                usd_value=Decimal(200),
            )
        ]
    )

    results = await repo.get_snapshots_by_date(target_date)
    assert len(results) == 2
    assert {(r.source, r.source_name, r.amount) for r in results} == {
        ("blend", "blend-main", Decimal(100)),
        ("blend", "blend-alt", Decimal(200)),
    }


async def test_get_snapshots_resolved_resolves_per_source_name(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(
                date=target_date,
                source="wise",
                source_name="wise-main",
                asset="USD",
                amount=Decimal(100),
                usd_value=Decimal(100),
            ),
            Snapshot(
                date=date(2024, 1, 14),
                source="wise",
                source_name="wise-alt",
                asset="USD",
                amount=Decimal(50),
                usd_value=Decimal(50),
            ),
        ]
    )

    results = await repo.get_snapshots_resolved(target_date)
    assert {(r.source, r.source_name, r.date) for r in results} == {
        ("wise", "wise-main", target_date),
        ("wise", "wise-alt", date(2024, 1, 14)),
    }


async def test_get_snapshots_resolved(repo):
    """get_snapshots_resolved returns latest per source up to target date."""
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 5), source="kbank", asset="THB", amount=Decimal(900), usd_value=Decimal(25)),
            Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28)),
            Snapshot(date=date(2024, 1, 12), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
            Snapshot(date=date(2024, 1, 12), source="okx", asset="ETH", amount=Decimal(5), usd_value=Decimal(12000)),
        ]
    )
    results = await repo.get_snapshots_resolved(date(2024, 1, 12))
    assert len(results) == 3  # kbank(Jan 10) + okx BTC(Jan 12) + okx ETH(Jan 12)
    kbank = [r for r in results if r.source == "kbank"]
    assert len(kbank) == 1
    assert kbank[0].date == date(2024, 1, 10)
    assert kbank[0].amount == Decimal(1000)
    okx = [r for r in results if r.source == "okx"]
    assert len(okx) == 2


async def test_get_snapshots_resolved_ignores_future(repo):
    """Snapshots after target date are excluded."""
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28)),
            Snapshot(date=date(2024, 1, 20), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
        ]
    )
    results = await repo.get_snapshots_resolved(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "kbank"


async def test_save_and_get_transaction(repo):
    tx = Transaction(
        date=date(2024, 1, 15),
        source="test",
        source_name="test-main",
        tx_type=TransactionType.TRADE,
        asset="BTC",
        amount=Decimal("0.5"),
        usd_value=Decimal(22500),
        trade_side="buy",
    )
    await repo.save_transaction(tx)
    results = await repo.get_transactions(source="test")
    assert len(results) == 1
    assert results[0].tx_type == TransactionType.TRADE
    assert results[0].source_name == "test-main"
    assert results[0].trade_side == "buy"


async def test_get_transactions_with_filters(repo):
    txs = [
        Transaction(
            date=date(2024, 1, 10),
            source="a",
            source_name="a-main",
            tx_type=TransactionType.DEPOSIT,
            asset="BTC",
            amount=Decimal(1),
            usd_value=Decimal(1),
        ),
        Transaction(
            date=date(2024, 1, 20),
            source="b",
            source_name="b-main",
            tx_type=TransactionType.WITHDRAWAL,
            asset="ETH",
            amount=Decimal(2),
            usd_value=Decimal(2),
        ),
    ]
    await repo.save_transactions(txs)

    # Filter by source
    results = await repo.get_transactions(source="a")
    assert len(results) == 1
    assert results[0].source == "a"

    # Filter by date range
    results = await repo.get_transactions(start=date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "b"


async def test_get_transactions_filters_by_source_name(repo):
    txs = [
        Transaction(
            date=date(2024, 1, 10),
            source="trading212",
            source_name="t212-main",
            tx_type=TransactionType.DEPOSIT,
            asset="EUR",
            amount=Decimal(10),
            usd_value=Decimal(11),
            tx_id="tx-a",
        ),
        Transaction(
            date=date(2024, 1, 11),
            source="trading212",
            source_name="t212-alt",
            tx_type=TransactionType.DEPOSIT,
            asset="EUR",
            amount=Decimal(20),
            usd_value=Decimal(22),
            tx_id="tx-b",
        ),
    ]
    await repo.save_transactions(txs)

    results = await repo.get_transactions(source_name="t212-main")
    assert len(results) == 1
    assert results[0].source_name == "t212-main"


async def test_save_transactions_ignores_duplicate_tx_id_per_source_name(repo):
    tx = Transaction(
        date=date(2024, 1, 10),
        source="trading212",
        source_name="t212-main",
        tx_type=TransactionType.DEPOSIT,
        asset="EUR",
        amount=Decimal(10),
        usd_value=Decimal(11),
        tx_id="dup-id",
    )
    await repo.save_transactions([tx, tx])
    results = await repo.get_transactions(source_name="t212-main")
    assert len(results) == 1


async def test_duplicate_tx_id_allowed_for_different_source_names(repo):
    txs = [
        Transaction(
            date=date(2024, 1, 10),
            source="trading212",
            source_name="t212-main",
            tx_type=TransactionType.DEPOSIT,
            asset="EUR",
            amount=Decimal(10),
            usd_value=Decimal(11),
            tx_id="shared-id",
        ),
        Transaction(
            date=date(2024, 1, 10),
            source="trading212",
            source_name="t212-alt",
            tx_type=TransactionType.DEPOSIT,
            asset="EUR",
            amount=Decimal(10),
            usd_value=Decimal(11),
            tx_id="shared-id",
        ),
    ]
    await repo.save_transactions(txs)
    results = await repo.get_transactions(source="trading212")
    assert len(results) == 2


async def test_get_latest_transaction_date_by_source_name(repo):
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 10),
                source="trading212",
                source_name="t212-main",
                tx_type=TransactionType.DEPOSIT,
                asset="EUR",
                amount=Decimal(10),
                usd_value=Decimal(11),
                tx_id="tx-1",
            ),
            Transaction(
                date=date(2024, 1, 20),
                source="trading212",
                source_name="t212-main",
                tx_type=TransactionType.DEPOSIT,
                asset="EUR",
                amount=Decimal(20),
                usd_value=Decimal(22),
                tx_id="tx-2",
            ),
        ]
    )

    latest = await repo.get_latest_transaction_date("t212-main")
    assert latest == date(2024, 1, 20)


async def test_save_and_get_price(repo):
    price = Price(date=date(2024, 1, 15), asset="BTC", currency="USD", price=Decimal(45000))
    await repo.save_price(price)
    result = await repo.get_price("BTC", "USD", date(2024, 1, 15))
    assert result is not None
    assert result.price == Decimal(45000)


async def test_get_price_not_found(repo):
    result = await repo.get_price("BTC", "USD", date(2024, 1, 15))
    assert result is None


async def test_save_and_get_analytics_metric(repo):
    metric_date = date(2024, 1, 15)
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"123.45"}')
    await repo.save_analytics_metric(metric_date, "allocation", '{"top":"BTC"}')

    metrics = await repo.get_analytics_metrics_by_date(metric_date)
    assert metrics["net_worth"] == '{"usd":"123.45"}'
    assert metrics["allocation"] == '{"top":"BTC"}'


async def test_save_analytics_metric_replaces_existing(repo):
    metric_date = date(2024, 1, 15)
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"100"}')
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"200"}')

    metrics = await repo.get_analytics_metrics_by_date(metric_date)
    assert metrics["net_worth"] == '{"usd":"200"}'


async def test_delete_source_cascade_removes_owned_state_and_counts(repo):
    await repo._db.execute(
        "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
        ("wise-main", "wise", "{}", 1),
    )
    await repo._db.commit()

    await repo.save_snapshots(
        [
            Snapshot(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-main",
                asset="USD",
                amount=Decimal(100),
                usd_value=Decimal(100),
            ),
            Snapshot(
                date=date(2024, 1, 11),
                source="wise",
                source_name="wise-main",
                asset="EUR",
                amount=Decimal(50),
                usd_value=Decimal(55),
            ),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-main",
                tx_type=TransactionType.DEPOSIT,
                asset="USD",
                amount=Decimal(10),
                usd_value=Decimal(10),
                tx_id="wise-1",
            ),
            Transaction(
                date=date(2024, 1, 11),
                source="wise",
                source_name="wise-main",
                tx_type=TransactionType.DEPOSIT,
                asset="EUR",
                amount=Decimal(20),
                usd_value=Decimal(22),
                tx_id="wise-2",
            ),
        ]
    )
    await repo.save_analytics_metric(date(2024, 1, 10), "ai_commentary", '{"text":"hello"}')
    await repo.save_analytics_metric(date(2024, 1, 10), "weekly_pnl", '{"usd":"10"}')
    await repo.save_analytics_metric(date(2024, 1, 11), "ai_commentary", '{"text":"bye"}')
    await repo.save_analytics_metric(date(2024, 1, 20), "ai_commentary", '{"text":"keep"}')
    await repo._db.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?)",
        ("apy_rules:wise-main", json.dumps([{"id": "r1"}, {"id": "r2"}])),
    )
    await repo._db.commit()

    result = await repo.delete_source_cascade("wise-main")

    assert result.name == "wise-main"
    assert result.snapshots == 2
    assert result.transactions == 2
    assert result.analytics_metrics == 3
    assert result.apy_rules == 2
    assert (
        await repo.get_snapshots_by_source_name_and_date_range("wise-main", date(2024, 1, 1), date(2024, 1, 31)) == []
    )
    assert await repo.get_transactions(source_name="wise-main") == []
    assert await repo.get_analytics_metrics_by_date(date(2024, 1, 10)) == {}
    assert await repo.get_analytics_metrics_by_date(date(2024, 1, 11)) == {}
    assert await repo.get_analytics_metrics_by_date(date(2024, 1, 20)) == {"ai_commentary": '{"text":"keep"}'}

    source_row = await (await repo._db.execute("SELECT name FROM sources WHERE name = ?", ("wise-main",))).fetchone()
    apy_row = await (
        await repo._db.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            ("apy_rules:wise-main",),
        )
    ).fetchone()
    assert source_row is None
    assert apy_row is None


async def test_delete_source_cascade_keeps_unrelated_source_state(repo):
    await repo._db.executemany(
        "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
        [
            ("wise-main", "wise", "{}", 1),
            ("wise-alt", "wise", "{}", 1),
        ],
    )
    await repo._db.commit()

    await repo.save_snapshots(
        [
            Snapshot(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-main",
                asset="USD",
                amount=Decimal(100),
                usd_value=Decimal(100),
            ),
            Snapshot(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-alt",
                asset="GBP",
                amount=Decimal(200),
                usd_value=Decimal(250),
            ),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-main",
                tx_type=TransactionType.DEPOSIT,
                asset="USD",
                amount=Decimal(10),
                usd_value=Decimal(10),
                tx_id="main-1",
            ),
            Transaction(
                date=date(2024, 1, 10),
                source="wise",
                source_name="wise-alt",
                tx_type=TransactionType.DEPOSIT,
                asset="GBP",
                amount=Decimal(20),
                usd_value=Decimal(25),
                tx_id="alt-1",
            ),
        ]
    )
    await repo._db.executemany(
        "INSERT INTO app_settings (key, value) VALUES (?, ?)",
        [
            ("apy_rules:wise-main", json.dumps([{"id": "r1"}])),
            ("apy_rules:wise-alt", json.dumps([{"id": "r2"}])),
        ],
    )
    await repo._db.commit()

    await repo.delete_source_cascade("wise-main")

    remaining_snaps = await repo.get_snapshots_by_source_name_and_date_range(
        "wise-alt",
        date(2024, 1, 1),
        date(2024, 1, 31),
    )
    remaining_txs = await repo.get_transactions(source_name="wise-alt")
    remaining_source = await (
        await repo._db.execute("SELECT name FROM sources WHERE name = ?", ("wise-alt",))
    ).fetchone()
    remaining_rules = await (
        await repo._db.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            ("apy_rules:wise-alt",),
        )
    ).fetchone()

    assert len(remaining_snaps) == 1
    assert remaining_snaps[0].asset == "GBP"
    assert len(remaining_txs) == 1
    assert remaining_txs[0].tx_id == "alt-1"
    assert remaining_source is not None
    assert remaining_rules is not None


async def test_delete_source_cascade_malformed_apy_rules_counted_as_zero(repo):
    await repo._db.execute(
        "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
        ("wise-main", "wise", "{}", 1),
    )
    await repo._db.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?)",
        ("apy_rules:wise-main", "{not-json"),
    )
    await repo._db.commit()

    result = await repo.delete_source_cascade("wise-main")

    assert result.apy_rules == 0
    apy_row = await (
        await repo._db.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            ("apy_rules:wise-main",),
        )
    ).fetchone()
    assert apy_row is None


async def test_delete_source_cascade_not_found(repo):
    with pytest.raises(SourceNotFoundError):
        await repo.delete_source_cascade("missing-source")


async def test_init_db_migrates_legacy_snapshots_with_source_name(tmp_path):
    db_path = tmp_path / "legacy.db"
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                credentials TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                counterparty_asset TEXT NOT NULL DEFAULT '',
                counterparty_amount TEXT NOT NULL DEFAULT '0',
                tx_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                asset TEXT NOT NULL,
                currency TEXT NOT NULL,
                price TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'coingecko',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE analytics_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        await db.execute(
            "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
            ("wise-main", "wise", "{}", 1),
        )
        await db.execute(
            "INSERT INTO snapshots (date, source, asset, amount, usd_value) VALUES (?, ?, ?, ?, ?)",
            ("2024-01-15", "wise", "USD", "100", "100"),
        )
        await db.commit()

    await init_db(db_path)

    async with Repository(db_path) as migrated_repo:
        rows = await migrated_repo.get_snapshots_by_date(date(2024, 1, 15))
    async with aiosqlite.connect(db_path) as db:
        stored_row = await (await db.execute("SELECT source_name FROM snapshots")).fetchone()
    assert len(rows) == 1
    assert rows[0].source == "wise"
    assert rows[0].source_name == "wise"
    assert stored_row == ("",)
