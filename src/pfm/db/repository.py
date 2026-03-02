"""Async data access layer wrapping aiosqlite."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Self

import aiosqlite

from pfm.db.models import Price, Snapshot, Transaction, TransactionType, init_db

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType


class Repository:
    """Async repository for all database operations."""

    def __init__(self, db_path: Path, key_hex: str | None = None) -> None:
        self._db_path = db_path
        self._key_hex = key_hex
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> Self:
        from pfm.db.encryption import connect_db

        await init_db(self._db_path, key_hex=self._key_hex)
        conn = connect_db(self._db_path, key_hex=self._key_hex)
        await conn.__aenter__()
        conn.row_factory = aiosqlite.Row
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
    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            msg = "Repository not opened. Use 'async with Repository(path) as repo:'"
            raise RuntimeError(msg)
        return self._conn

    # ── Snapshots ─────────────────────────────────────────────────────

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save a single balance snapshot."""
        await self.save_snapshots([snapshot])

    async def save_snapshots(self, snapshots: list[Snapshot]) -> None:
        """Save multiple snapshots atomically, replacing same source/source_name/date rows."""
        if not snapshots:
            return

        normalized: list[Snapshot] = []
        for snap in snapshots:
            source_name = snap.source_name or snap.source
            normalized.append(snap if snap.source_name == source_name else replace(snap, source_name=source_name))

        source_dates = {(str(s.date), s.source, s.source_name) for s in normalized}
        for snapshot_date, source, source_name in source_dates:
            await self._db.execute(
                "DELETE FROM snapshots WHERE date = ? AND source = ? AND source_name = ?",
                (snapshot_date, source, source_name),
            )

        await self._db.executemany(
            "INSERT INTO snapshots (date, source, source_name, asset, amount, usd_value, price, apy, raw_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    str(s.date),
                    s.source,
                    s.source_name,
                    s.asset,
                    str(s.amount),
                    str(s.usd_value),
                    str(s.price),
                    str(s.apy),
                    s.raw_json,
                )
                for s in normalized
            ],
        )
        await self._db.commit()

    async def get_snapshots_by_date(self, d: date) -> list[Snapshot]:
        """Get all snapshots for a specific date."""
        cursor = await self._db.execute(
            "SELECT * FROM snapshots WHERE date = ? ORDER BY source, source_name, asset",
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

    async def get_snapshots_resolved(self, target_date: date) -> list[Snapshot]:
        """Get the most recent snapshots per source where date <= target_date.

        For each source, finds MAX(date) where date <= target_date,
        then returns all snapshot rows for those source+date combos.
        This ensures sources with older data (e.g., KBank monthly statements)
        are included even when other sources have fresher snapshots.
        """
        cursor = await self._db.execute(
            "SELECT s.* FROM snapshots s"
            " INNER JOIN ("
            "   SELECT source, source_name, MAX(date) AS max_date"
            "   FROM snapshots WHERE date <= ?"
            "   GROUP BY source, source_name"
            " ) latest ON s.source = latest.source AND s.source_name = latest.source_name AND s.date = latest.max_date"
            " ORDER BY s.source, s.source_name, s.asset",
            (str(target_date),),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def get_snapshots_for_range(self, start: date, end: date) -> list[Snapshot]:
        """Get all snapshots between two dates (inclusive)."""
        cursor = await self._db.execute(
            "SELECT * FROM snapshots WHERE date >= ? AND date <= ? ORDER BY date, source, source_name, asset",
            (str(start), str(end)),
        )
        rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    @staticmethod
    def _row_to_snapshot(row: aiosqlite.Row) -> Snapshot:
        columns = row.keys()
        source_name = row["source_name"] if "source_name" in columns else row["source"]
        if not source_name:
            source_name = row["source"]
        return Snapshot(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            source=row["source"],
            source_name=source_name,
            asset=row["asset"],
            amount=Decimal(row["amount"]),
            usd_value=Decimal(row["usd_value"]),
            price=Decimal(row["price"]),
            apy=Decimal(row["apy"]),
            raw_json=row["raw_json"],
        )

    async def delete_snapshots_by_source_names(self, source_names: list[str]) -> int:
        """Delete all snapshots for the given source_names. Returns count deleted."""
        if not source_names:
            return 0
        placeholders = ",".join("?" for _ in source_names)
        cursor = await self._db.execute(
            f"DELETE FROM snapshots WHERE source_name IN ({placeholders})",  # noqa: S608
            source_names,
        )
        await self._db.commit()
        return cursor.rowcount

    # ── Transactions ──────────────────────────────────────────────────

    _TX_INSERT_SQL = (
        "INSERT INTO transactions "
        "(date, source, tx_type, asset, amount, usd_value, "
        "counterparty_asset, counterparty_amount, tx_id, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    @staticmethod
    def _tx_to_row(tx: Transaction) -> tuple[str, ...]:
        return (
            str(tx.date),
            tx.source,
            tx.tx_type.value,
            tx.asset,
            str(tx.amount),
            str(tx.usd_value),
            tx.counterparty_asset,
            str(tx.counterparty_amount),
            tx.tx_id,
            tx.raw_json,
        )

    async def save_transaction(self, tx: Transaction) -> None:
        """Save a single transaction."""
        await self._db.execute(self._TX_INSERT_SQL, self._tx_to_row(tx))
        await self._db.commit()

    async def save_transactions(self, txs: list[Transaction]) -> None:
        """Save multiple transactions atomically."""
        await self._db.executemany(
            self._TX_INSERT_SQL,
            [self._tx_to_row(tx) for tx in txs],
        )
        await self._db.commit()

    async def get_transactions(
        self,
        source: str | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> list[Transaction]:
        """Get transactions with optional filters."""
        query = "SELECT * FROM transactions WHERE 1=1"
        params: list[str] = []

        if source is not None:
            query += " AND source = ?"
            params.append(source)
        if start is not None:
            query += " AND date >= ?"
            params.append(str(start))
        if end is not None:
            query += " AND date <= ?"
            params.append(str(end))

        query += " ORDER BY date DESC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self._row_to_transaction(row) for row in rows]

    @staticmethod
    def _row_to_transaction(row: aiosqlite.Row) -> Transaction:
        return Transaction(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            source=row["source"],
            tx_type=TransactionType(row["tx_type"]),
            asset=row["asset"],
            amount=Decimal(row["amount"]),
            usd_value=Decimal(row["usd_value"]),
            counterparty_asset=row["counterparty_asset"],
            counterparty_amount=Decimal(row["counterparty_amount"]),
            tx_id=row["tx_id"],
            raw_json=row["raw_json"],
        )

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
