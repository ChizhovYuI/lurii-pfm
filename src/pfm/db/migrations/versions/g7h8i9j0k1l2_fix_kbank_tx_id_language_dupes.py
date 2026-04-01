"""Fix KBank tx_id to be language-independent and remove duplicates.

The old _build_tx_id used the description field which changes with PDF
language (English vs Thai).  The new formula uses date + time + amount +
balance — all language-independent.  This migration recalculates every
KBank tx_id and removes duplicate rows created by the language mismatch,
keeping the row that has user edits (manual category or type override).

Revision ID: g7h8i9j0k1l2
Revises: f6g7h8i9j0k1
Create Date: 2026-04-01 12:00:00.000000

"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "g7h8i9j0k1l2"
down_revision: Final[str | None] = "f6g7h8i9j0k1"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def _new_tx_id(tx_date: str, tx_time: str, amount: str, balance: str) -> str:
    """Compute the new language-independent tx_id."""
    amt = Decimal(amount).normalize()
    bal_str = format(Decimal(balance).normalize(), "f") if balance else ""
    canonical = "|".join(
        [
            tx_date,
            tx_time,
            "THB",
            format(amt, "f"),
            bal_str,
        ]
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"kbank:{digest}"


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "transactions" not in tables:
        return

    # 1. Read all kbank transactions with their metadata.
    rows = bind.execute(
        sa.text(
            "SELECT t.id, t.date, t.amount, t.raw_json, t.tx_id,"
            " m.category_source, m.type_override"
            " FROM transactions t"
            " LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            " WHERE t.source_name = 'kbank-main'"
            " ORDER BY t.id"
        )
    ).fetchall()

    if not rows:
        return

    # 2. Compute new tx_id for each row and group by it.
    groups: dict[str, list[tuple[int, bool]]] = {}
    new_ids: dict[int, str] = {}

    for row in rows:
        row_id, tx_date, amount, raw_json, _old_tx_id, cat_source, type_ovr = row
        parsed = json.loads(raw_json) if raw_json else {}
        tx_time = parsed.get("time", "")
        balance = parsed.get("balance", "")

        new_id = _new_tx_id(tx_date, tx_time, amount, balance)
        new_ids[row_id] = new_id

        has_user_edit = cat_source == "manual" or bool(type_ovr)
        groups.setdefault(new_id, []).append((row_id, has_user_edit))

    # 3. For each group, pick the row to keep and collect the rest for deletion.
    ids_to_delete: list[int] = []
    ids_to_update: dict[int, str] = {}

    for new_id, members in groups.items():
        # Prefer the row with user edits; tie-break by lowest id (oldest).
        members.sort(key=lambda m: (not m[1], m[0]))
        keep_id = members[0][0]
        ids_to_update[keep_id] = new_id
        for row_id, _ in members[1:]:
            ids_to_delete.append(row_id)

    # 4. Delete duplicates (metadata first, then transactions).
    if ids_to_delete:
        ph = ",".join(str(i) for i in ids_to_delete)
        bind.execute(
            sa.text(
                f"DELETE FROM transaction_metadata WHERE transaction_id IN ({ph})"  # noqa: S608
            )
        )
        bind.execute(
            sa.text(
                f"DELETE FROM transactions WHERE id IN ({ph})"  # noqa: S608
            )
        )

    # 5. Update tx_id on kept rows.
    for row_id, new_id in ids_to_update.items():
        bind.execute(
            sa.text("UPDATE transactions SET tx_id = :new_id WHERE id = :row_id"),
            {"new_id": new_id, "row_id": row_id},
        )


def downgrade() -> None:
    """Downgrade schema.

    Irreversible data-cleanup migration (deleted duplicate rows cannot be
    restored and old tx_id values are not preserved).
    """
