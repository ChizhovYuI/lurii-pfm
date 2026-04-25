"""Async data access layer wrapping aiosqlite."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Self

import aiosqlite

from pfm.db.models import Price, Snapshot, SourceDeleteResult, Transaction, TransactionType, init_db
from pfm.db.source_store import SourceNotFoundError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType


def _raise_source_not_found(source_name: str) -> None:
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

    async def rename_source(self, old_name: str, new_name: str) -> None:
        """Rename a configured source and refresh denormalized text columns.

        Stage 1: source_id FK is invariant under rename, but transactions /
        snapshots still carry source_name as a denormalized cache, so we
        update both. Stage 3 will drop source_name and this becomes a single
        UPDATE on sources.
        """
        if not old_name or not new_name or old_name == new_name:
            return
        await self._db.execute(
            "UPDATE sources SET name = ? WHERE name = ?",
            (new_name, old_name),
        )
        await self._db.execute(
            "UPDATE transactions SET source_name = ? WHERE source_name = ?",
            (new_name, old_name),
        )
        await self._db.execute(
            "UPDATE snapshots SET source_name = ? WHERE source_name = ?",
            (new_name, old_name),
        )
        self._source_id_cache.pop(old_name, None)
        await self._db.commit()

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

        rows: list[tuple[str, str, str, int | None, str, str, str, str, str, str]] = []
        for s in normalized:
            sid = s.source_id if s.source_id is not None else await self._resolve_source_id(s.source_name)
            rows.append(
                (
                    str(s.date),
                    s.source,
                    s.source_name,
                    sid,
                    s.asset,
                    str(s.amount),
                    str(s.usd_value),
                    str(s.price),
                    str(s.apy),
                    s.raw_json,
                )
            )

        source_dates = {(str(s.date), s.source, s.source_name) for s in normalized}
        for snapshot_date, source, source_name in source_dates:
            await self._db.execute(
                "DELETE FROM snapshots WHERE date = ? AND source = ? AND source_name = ?",
                (snapshot_date, source, source_name),
            )

        await self._db.executemany(
            "INSERT INTO snapshots "
            "(date, source, source_name, source_id, asset, amount, usd_value, price, apy, raw_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
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

    async def get_earliest_snapshot_date(self) -> date | None:
        """Return the earliest snapshot date present in the database."""
        cursor = await self._db.execute("SELECT MIN(date) FROM snapshots")
        row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return date.fromisoformat(str(row[0]))

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
            "SELECT * FROM snapshots WHERE source_name = ? AND date >= ? AND date <= ? ORDER BY date, asset",
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

    async def delete_source_cascade(self, source_name: str) -> SourceDeleteResult:
        """Delete a source and all source-owned state in one transaction."""
        row = await (await self._db.execute("SELECT name FROM sources WHERE name = ?", (source_name,))).fetchone()
        if row is None:
            _raise_source_not_found(source_name)

        apy_rules_key = f"apy_rules:{source_name}"

        await self._db.execute("BEGIN")
        try:
            snapshot_rows = await (
                await self._db.execute("SELECT DISTINCT date FROM snapshots WHERE source_name = ?", (source_name,))
            ).fetchall()
            tx_rows = await (
                await self._db.execute("SELECT DISTINCT date FROM transactions WHERE source_name = ?", (source_name,))
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
                "DELETE FROM snapshots WHERE source_name = ?",
                (source_name,),
            )
            transaction_cursor = await self._db.execute(
                "DELETE FROM transactions WHERE source_name = ?",
                (source_name,),
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
                "DELETE FROM sources WHERE name = ?",
                (source_name,),
            )
            if source_cursor.rowcount == 0:
                _raise_source_not_found(source_name)

            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

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
        "(date, source, source_name, source_id, tx_type, asset, amount, usd_value, "
        "counterparty_asset, counterparty_amount, trade_side, tx_id, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    @staticmethod
    def _tx_to_row(tx: Transaction, source_id: int | None) -> tuple[object, ...]:
        return (
            str(tx.date),
            tx.source,
            tx.source_name or tx.source,
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

    async def save_transaction(self, tx: Transaction) -> None:
        """Save a single transaction."""
        normalized = tx if tx.source_name else replace(tx, source_name=tx.source)
        sid = (
            normalized.source_id
            if normalized.source_id is not None
            else await self._resolve_source_id(normalized.source_name)
        )
        await self._db.execute(self._TX_INSERT_SQL, self._tx_to_row(normalized, sid))
        await self._db.commit()

    async def save_transactions(self, txs: list[Transaction]) -> None:
        """Save multiple transactions atomically."""
        normalized = [tx if tx.source_name else replace(tx, source_name=tx.source) for tx in txs]
        rows: list[tuple[object, ...]] = []
        for tx in normalized:
            sid = tx.source_id if tx.source_id is not None else await self._resolve_source_id(tx.source_name)
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
        """Get transactions with optional filters."""
        query = "SELECT * FROM transactions WHERE 1=1"
        params: list[str] = []

        if source is not None:
            query += " AND source = ?"
            params.append(source)
        if source_name is not None:
            query += " AND source_name = ?"
            params.append(source_name)
        if start is not None:
            query += " AND date >= ?"
            params.append(str(start))
        if end is not None:
            query += " AND date <= ?"
            params.append(str(end))

        query += " ORDER BY date DESC, id DESC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self.row_to_transaction(row) for row in rows]

    async def get_latest_transaction_date(self, source_name: str) -> date | None:
        """Return the latest transaction date for a specific configured source."""
        cursor = await self._db.execute(
            "SELECT MAX(date) FROM transactions WHERE source_name = ?",
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
        return Transaction(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            source=row["source"],
            source_name=row["source_name"] if "source_name" in columns and row["source_name"] else row["source"],
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
