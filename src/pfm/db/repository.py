"""Async data access layer wrapping aiosqlite."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, NoReturn, Self

import aiosqlite

from pfm.db.models import Price, Snapshot, SourceDeleteResult, Transaction, TransactionType, init_db
from pfm.db.source_store import SourceNotFoundError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType


def _raise_source_not_found(source_name: str) -> NoReturn:
    msg = f"Source {source_name!r} not found"
    raise SourceNotFoundError(msg)


class Repository:
    """Async repository for all database operations."""

    def __init__(self, db_path: Path, key_hex: str | None = None) -> None:
        self._db_path = db_path
        self._key_hex = key_hex
        self._conn: aiosqlite.Connection | None = None
        self._source_id_cache: dict[str, int] = {}

    async def __aenter__(self) -> Self:
        from pfm.db.encryption import connect_db

        await init_db(self._db_path, key_hex=self._key_hex)
        conn = connect_db(self._db_path, key_hex=self._key_hex)
        await conn.__aenter__()
        conn.row_factory = aiosqlite.Row
        # ADR-030 Stage 3: enforce FK integrity (off by default in SQLite).
        await conn.execute("PRAGMA foreign_keys = ON")
        self._conn = conn
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def connection(self) -> aiosqlite.Connection:
        """Return the underlying database connection for use by store classes."""
        return self._db

    @property
    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            msg = "Repository not opened. Use 'async with Repository(path) as repo:'"
            raise RuntimeError(msg)
        return self._conn

    async def _resolve_source_id(self, source_name: str) -> int | None:
        """Resolve sources.id by sources.name. Cached per Repository instance."""
        if not source_name:
            return None
        cached = self._source_id_cache.get(source_name)
        if cached is not None:
            return cached
        cursor = await self._db.execute(
            "SELECT id FROM sources WHERE name = ? LIMIT 1",
            (source_name,),
        )
        row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        sid = int(row[0])
        self._source_id_cache[source_name] = sid
        return sid

    async def _ensure_source(self, source_type: str, source_name: str) -> int:
        """Resolve or auto-create a ``sources`` row for the given (type, name).

        Stage 3 (ADR-030) tightens ``source_id NOT NULL`` on data tables. Most
        production paths flow through ``SourceStore`` first so the row exists,
        but collectors/CLI/tests sometimes save data before the source is
        configured. Auto-creating an unconfigured (empty credentials, enabled)
        row keeps the FK invariant satisfied without forcing every caller to
        run ``SourceStore.add()`` first.
        """
        name = source_name or source_type
        if not name:
            msg = "Cannot ensure source: both source and source_name are empty"
            raise ValueError(msg)
        sid = await self._resolve_source_id(name)
        if sid is not None:
            return sid
        cursor = await self._db.execute(
            "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
            (name, source_type or name, "{}", 1),
        )
        await self._db.commit()
        sid = int(cursor.lastrowid or 0)
        self._source_id_cache[name] = sid
        return sid

    async def list_sources_with_counts(self) -> list[dict[str, object]]:
        """Return all configured sources with tx/snap counts via FK join.

        Uses ``source_id`` (Stage 1 backfill); rows whose ``source_id`` is
        still NULL (no matching ``sources`` row) are not counted. Surface
        for the categorization-curator skill survey pass.
        """
        cursor = await self._db.execute(
            "SELECT s.id, s.name, s.type, s.enabled,"
            "  (SELECT COUNT(*) FROM transactions WHERE source_id = s.id) AS tx_count,"
            "  (SELECT COUNT(*) FROM snapshots WHERE source_id = s.id) AS snap_count"
            " FROM sources s"
            " ORDER BY s.id",
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": int(row[0]),
                "name": str(row[1]),
                "type": str(row[2]),
                "enabled": bool(row[3]),
                "tx_count": int(row[4] or 0),
                "snap_count": int(row[5] or 0),
            }
            for row in rows
        ]

    async def rename_source(self, old_name: str, new_name: str) -> None:
        """Rename a configured source. Stage 3: single UPDATE on ``sources``.

        Data tables hydrate ``source_name`` via FK JOIN at read time, so the
        rename takes effect immediately without touching ``transactions`` or
        ``snapshots``.
        """
        if not old_name or not new_name or old_name == new_name:
            return
        await self._db.execute(
            "UPDATE sources SET name = ? WHERE name = ?",
            (new_name, old_name),
        )
        self._source_id_cache.pop(old_name, None)
        await self._db.commit()

    # ── Snapshots ─────────────────────────────────────────────────────

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save a single balance snapshot."""
        await self.save_snapshots([snapshot])

    async def save_snapshots(self, snapshots: list[Snapshot]) -> None:
        """Save snapshots atomically, replacing same ``(date, source_id)`` rows.

        Stage 3 (ADR-030): replace key is the FK ``source_id`` rather than
        the dropped ``source_name`` column. Snapshots whose ``source_id``
        cannot be resolved by the configured source name fall back to the
        type-only resolver in ``_resolve_source_id`` — when neither lands
        on a sources row the insert raises (FK enforcement).
        """
        if not snapshots:
            return

        rows: list[tuple[str, str, int, str, str, str, str, str, str]] = []
        resolved_pairs: set[tuple[str, int]] = set()
        for s in snapshots:
            sid = (
                s.source_id
                if s.source_id is not None
                else await self._ensure_source(s.source, s.source_name or s.source)
            )
            rows.append(
                (
                    str(s.date),
                    s.source,
                    sid,
                    s.asset,
                    str(s.amount),
                    str(s.usd_value),
                    str(s.price),
                    str(s.apy),
                    s.raw_json,
                )
            )
            resolved_pairs.add((str(s.date), sid))

        for snapshot_date, sid in resolved_pairs:
            await self._db.execute(
                "DELETE FROM snapshots WHERE date = ? AND source_id = ?",
                (snapshot_date, sid),
            )

        await self._db.executemany(
            "INSERT INTO snapshots "
            "(date, source, source_id, asset, amount, usd_value, price, apy, raw_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        await self._db.commit()

    async def get_snapshots_by_date(self, d: date) -> list[Snapshot]:
        """Get all snapshots for a specific date."""
        cursor = await self._db.execute(
            "SELECT s.*, src.name AS canonical_source_name FROM snapshots s"
            " LEFT JOIN sources src ON s.source_id = src.id"
            " WHERE s.date = ?"
            " ORDER BY s.source, canonical_source_name, s.asset",
            (str(d),),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def get_latest_snapshots(self) -> list[Snapshot]:
        """Get snapshots resolving each source to its most recent date."""
        cursor = await self._db.execute("SELECT MAX(date) FROM snapshots")
        row = await cursor.fetchone()
        if not row or not row[0]:
            return []
        return await self.get_snapshots_resolved(date.fromisoformat(row[0]))

    async def get_earliest_snapshot_date(self) -> date | None:
        """Return the earliest snapshot date present in the database."""
        cursor = await self._db.execute("SELECT MIN(date) FROM snapshots")
        row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return date.fromisoformat(str(row[0]))

    async def get_snapshots_resolved(self, target_date: date) -> list[Snapshot]:
        """Get the most recent snapshots per source_id where date <= target_date.

        Sources with older data (e.g. KBank monthly statements) are still
        included when other sources have fresher snapshots.
        """
        cursor = await self._db.execute(
            "SELECT s.*, src.name AS canonical_source_name FROM snapshots s"
            " LEFT JOIN sources src ON s.source_id = src.id"
            " INNER JOIN ("
            "   SELECT source_id, MAX(date) AS max_date"
            "   FROM snapshots WHERE date <= ?"
            "   GROUP BY source_id"
            " ) latest ON s.source_id = latest.source_id AND s.date = latest.max_date"
            " ORDER BY s.source, canonical_source_name, s.asset",
            (str(target_date),),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def get_snapshots_for_range(self, start: date, end: date) -> list[Snapshot]:
        """Get all snapshots between two dates (inclusive)."""
        cursor = await self._db.execute(
            "SELECT s.*, src.name AS canonical_source_name FROM snapshots s"
            " LEFT JOIN sources src ON s.source_id = src.id"
            " WHERE s.date >= ? AND s.date <= ?"
            " ORDER BY s.date, s.source, canonical_source_name, s.asset",
            (str(start), str(end)),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    @staticmethod
    def _row_to_snapshot(row: aiosqlite.Row) -> Snapshot:
        columns = row.keys()
        canonical = row["canonical_source_name"] if "canonical_source_name" in columns else None
        source_name = canonical or row["source"]
        source_id_raw = row["source_id"] if "source_id" in columns else None
        return Snapshot(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            source=row["source"],
            source_name=source_name,
            source_id=int(source_id_raw) if source_id_raw is not None else None,
            asset=row["asset"],
            amount=Decimal(row["amount"]),
            usd_value=Decimal(row["usd_value"]),
            price=Decimal(row["price"]),
            apy=Decimal(row["apy"]),
            raw_json=row["raw_json"],
        )

    async def get_snapshots_by_source_name_and_date_range(
        self, source_name: str, start: date, end: date
    ) -> list[Snapshot]:
        """Get snapshots for a specific source_name between two dates (inclusive)."""
        cursor = await self._db.execute(
            "SELECT s.*, src.name AS canonical_source_name FROM snapshots s"
            " INNER JOIN sources src ON s.source_id = src.id"
            " WHERE src.name = ? AND s.date >= ? AND s.date <= ?"
            " ORDER BY s.date, s.asset",
            (source_name, str(start), str(end)),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def update_snapshot_apy(self, snapshot_id: int, new_apy: Decimal) -> None:
        """Update the APY of a single snapshot by ID."""
        await self._db.execute(
            "UPDATE snapshots SET apy = ? WHERE id = ?",
            (str(new_apy), snapshot_id),
        )
        await self._db.commit()

    async def delete_snapshots_by_source_names(self, source_names: list[str]) -> int:
        """Delete all snapshots for the given source names (FK-resolved)."""
        if not source_names:
            return 0
        placeholders = ",".join("?" for _ in source_names)
        # safe — placeholders count derived from input length, no string interp.
        cursor = await self._db.execute(
            f"DELETE FROM snapshots WHERE source_id IN"  # noqa: S608
            f" (SELECT id FROM sources WHERE name IN ({placeholders}))",
            source_names,
        )
        await self._db.commit()
        return cursor.rowcount

    async def delete_source_cascade(self, source_name: str) -> SourceDeleteResult:
        """Delete a source and all source-owned state in one transaction.

        Stage 2 (ADR-030): switched to FK-based deletes — tx/snap purge by
        ``source_id`` rather than the denormalized ``source_name`` column.
        Legacy rows whose Stage 1 backfill could not link (``source_id IS
        NULL``) are not removed by this cascade — surface as orphans via
        a future cleanup tool if needed.
        """
        source_row = await (await self._db.execute("SELECT id FROM sources WHERE name = ?", (source_name,))).fetchone()
        if source_row is None or source_row[0] is None:
            _raise_source_not_found(source_name)
        source_id = int(source_row[0])

        apy_rules_key = f"apy_rules:{source_name}"

        await self._db.execute("BEGIN")
        try:
            snapshot_rows = await (
                await self._db.execute("SELECT DISTINCT date FROM snapshots WHERE source_id = ?", (source_id,))
            ).fetchall()
            tx_rows = await (
                await self._db.execute("SELECT DISTINCT date FROM transactions WHERE source_id = ?", (source_id,))
            ).fetchall()
            affected_dates = sorted({str(row[0]) for row in [*snapshot_rows, *tx_rows] if row[0]})

            apy_rule_row = await (
                await self._db.execute("SELECT value FROM app_settings WHERE key = ?", (apy_rules_key,))
            ).fetchone()
            apy_rules_count = 0
            if apy_rule_row is not None:
                try:
                    parsed_rules = json.loads(str(apy_rule_row[0]))
                    if isinstance(parsed_rules, list):
                        apy_rules_count = len(parsed_rules)
                except json.JSONDecodeError:
                    apy_rules_count = 0

            snapshot_cursor = await self._db.execute(
                "DELETE FROM snapshots WHERE source_id = ?",
                (source_id,),
            )
            transaction_cursor = await self._db.execute(
                "DELETE FROM transactions WHERE source_id = ?",
                (source_id,),
            )

            analytics_count = 0
            if affected_dates:
                placeholders = ",".join("?" for _ in affected_dates)
                analytics_cursor = await self._db.execute(
                    f"DELETE FROM analytics_cache WHERE date IN ({placeholders})",  # noqa: S608
                    affected_dates,
                )
                analytics_count = analytics_cursor.rowcount

            await self._db.execute("DELETE FROM app_settings WHERE key = ?", (apy_rules_key,))
            earn_overrides_key = f"earn_overrides:{source_name}"
            await self._db.execute("DELETE FROM app_settings WHERE key = ?", (earn_overrides_key,))

            source_cursor = await self._db.execute(
                "DELETE FROM sources WHERE id = ?",
                (source_id,),
            )
            if source_cursor.rowcount == 0:
                _raise_source_not_found(source_name)

            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

        self._source_id_cache.pop(source_name, None)
        return SourceDeleteResult(
            name=source_name,
            snapshots=snapshot_cursor.rowcount,
            transactions=transaction_cursor.rowcount,
            analytics_metrics=analytics_count,
            apy_rules=apy_rules_count,
        )

    # ── Transactions ──────────────────────────────────────────────────

    _TX_INSERT_SQL = (
        "INSERT OR IGNORE INTO transactions "
        "(date, source, source_id, tx_type, asset, amount, usd_value, "
        "counterparty_asset, counterparty_amount, trade_side, tx_id, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    @staticmethod
    def _tx_to_row(tx: Transaction, source_id: int) -> tuple[object, ...]:
        return (
            str(tx.date),
            tx.source,
            source_id,
            tx.tx_type.value,
            tx.asset,
            str(tx.amount),
            str(tx.usd_value),
            tx.counterparty_asset,
            str(tx.counterparty_amount),
            tx.trade_side,
            tx.tx_id,
            tx.raw_json,
        )

    async def _resolve_required_source_id(self, tx: Transaction) -> int:
        if tx.source_id is not None:
            return tx.source_id
        return await self._ensure_source(tx.source, tx.source_name or tx.source)

    async def save_transaction(self, tx: Transaction) -> None:
        """Save a single transaction."""
        sid = await self._resolve_required_source_id(tx)
        await self._db.execute(self._TX_INSERT_SQL, self._tx_to_row(tx, sid))
        await self._db.commit()

    async def save_transactions(self, txs: list[Transaction]) -> None:
        """Save multiple transactions atomically."""
        rows: list[tuple[object, ...]] = []
        for tx in txs:
            sid = await self._resolve_required_source_id(tx)
            rows.append(self._tx_to_row(tx, sid))
        await self._db.executemany(self._TX_INSERT_SQL, rows)
        await self._db.commit()

    async def get_transactions(
        self,
        source: str | None = None,
        source_name: str | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> list[Transaction]:
        """Get transactions with optional filters.

        ``source`` matches the cached ``transactions.source`` (type) column.
        ``source_name`` matches ``sources.name`` via FK JOIN.
        """
        query = (
            "SELECT t.*, s.name AS canonical_source_name FROM transactions t"
            " LEFT JOIN sources s ON t.source_id = s.id"
            " WHERE 1=1"
        )
        params: list[str] = []

        if source is not None:
            query += " AND t.source = ?"
            params.append(source)
        if source_name is not None:
            query += " AND s.name = ?"
            params.append(source_name)
        if start is not None:
            query += " AND t.date >= ?"
            params.append(str(start))
        if end is not None:
            query += " AND t.date <= ?"
            params.append(str(end))

        query += " ORDER BY t.date DESC, t.id DESC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self.row_to_transaction(row) for row in rows]

    async def get_latest_transaction_date(self, source_name: str) -> date | None:
        """Return the latest transaction date for a configured source (FK-resolved)."""
        cursor = await self._db.execute(
            "SELECT MAX(t.date) FROM transactions t INNER JOIN sources s ON t.source_id = s.id WHERE s.name = ?",
            (source_name,),
        )
        row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return date.fromisoformat(str(row[0]))

    @staticmethod
    def row_to_transaction(row: aiosqlite.Row) -> Transaction:
        columns = row.keys()
        source_id_raw = row["source_id"] if "source_id" in columns else None
        canonical = row["canonical_source_name"] if "canonical_source_name" in columns else None
        resolved_source_name = canonical or row["source"]
        return Transaction(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            source=row["source"],
            source_name=resolved_source_name,
            source_id=int(source_id_raw) if source_id_raw is not None else None,
            tx_type=TransactionType(row["tx_type"]),
            asset=row["asset"],
            amount=Decimal(row["amount"]),
            usd_value=Decimal(row["usd_value"]),
            counterparty_asset=row["counterparty_asset"],
            counterparty_amount=Decimal(row["counterparty_amount"]),
            tx_id=row["tx_id"],
            raw_json=row["raw_json"],
            trade_side=row["trade_side"] if "trade_side" in columns else "",
        )

    async def update_transaction_types(self, updates: list[tuple[int, TransactionType]]) -> None:
        """Batch update tx_type for resolved transactions."""
        if not updates:
            return
        await self._db.executemany(
            "UPDATE transactions SET tx_type = ? WHERE id = ?",
            [(tx_type.value, tx_id) for tx_id, tx_type in updates],
        )
        await self._db.commit()

    # ── Prices ────────────────────────────────────────────────────────

    async def save_price(self, price: Price) -> None:
        """Save a price entry."""
        await self._db.execute(
            "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, ?, ?, ?)",
            (str(price.date), price.asset, price.currency, str(price.price), price.source),
        )
        await self._db.commit()

    async def save_prices(self, prices: list[Price]) -> None:
        """Save multiple prices atomically."""
        await self._db.executemany(
            "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, ?, ?, ?)",
            [(str(p.date), p.asset, p.currency, str(p.price), p.source) for p in prices],
        )
        await self._db.commit()

    async def get_prices_by_date(self, d: date) -> list[Price]:
        """Get all cached prices for a specific date."""
        cursor = await self._db.execute(
            "SELECT * FROM prices WHERE date = ?",
            (str(d),),
        )
        rows = await cursor.fetchall()
        return [self._row_to_price(row) for row in rows]

    async def get_price(self, asset: str, currency: str, d: date) -> Price | None:
        """Get a specific price if cached."""
        cursor = await self._db.execute(
            "SELECT * FROM prices WHERE asset = ? AND currency = ? AND date = ? ORDER BY created_at DESC LIMIT 1",
            (asset, currency, str(d)),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return self._row_to_price(row)

    @staticmethod
    def _row_to_price(row: aiosqlite.Row) -> Price:
        return Price(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            asset=row["asset"],
            currency=row["currency"],
            price=Decimal(row["price"]),
            source=row["source"],
        )

    # ── Analytics Cache ───────────────────────────────────────────────

    async def save_analytics_metric(self, metric_date: date, metric_name: str, metric_json: str) -> None:
        """Upsert one analytics metric JSON blob for a date."""
        await self._db.execute(
            "DELETE FROM analytics_cache WHERE date = ? AND metric_name = ?",
            (str(metric_date), metric_name),
        )
        await self._db.execute(
            "INSERT INTO analytics_cache (date, metric_name, metric_json) VALUES (?, ?, ?)",
            (str(metric_date), metric_name, metric_json),
        )
        await self._db.commit()

    async def get_analytics_metrics_by_date(self, metric_date: date) -> dict[str, str]:
        """Get cached analytics metrics for a date."""
        cursor = await self._db.execute(
            "SELECT metric_name, metric_json FROM analytics_cache WHERE date = ? ORDER BY metric_name",
            (str(metric_date),),
        )
        rows = await cursor.fetchall()
        return {str(row[0]): str(row[1]) for row in rows}
