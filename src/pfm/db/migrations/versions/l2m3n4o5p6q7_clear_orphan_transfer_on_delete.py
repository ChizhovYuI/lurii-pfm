"""Clear the orphaned transfer overlay when a transfer's partner is deleted.

When one side of an internal-transfer pair is deleted, the ``transfer_pair_id``
foreign key fires ``ON DELETE SET NULL`` on the surviving partner but leaves it
flagged ``is_internal_transfer = 1`` with a now-NULL pair — a one-sided
("orphan") link that ``categorization_runner`` then skips forever and the
summary keeps counting as ``transfer_unpaired``.

This installs a ``BEFORE DELETE`` trigger on ``transactions`` that clears the
partner's full transfer overlay before the row is removed. It runs *before* the
FK SET-NULL, so it can still find the partner via ``transfer_pair_id = OLD.id``.
This stops the asymmetry at the write path; ``repair_transfer_pairs`` remains a
recovery tool for rows already broken before this migration.
"""

from __future__ import annotations

from typing import Final

from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "l2m3n4o5p6q7"
down_revision: Final[str | None] = "k1l2m3n4o5p6"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


# Plain string literals (no interpolation) so there is no SQL-injection surface;
# the trigger name is fixed.
_CREATE_TRIGGER: Final[str] = """
CREATE TRIGGER IF NOT EXISTS trg_clear_orphan_transfer_before_tx_delete
BEFORE DELETE ON transactions
FOR EACH ROW
BEGIN
    UPDATE transaction_metadata
    SET is_internal_transfer = 0,
        transfer_pair_id = NULL,
        transfer_detected_by = NULL,
        type_override = NULL,
        category = NULL,
        category_source = 'auto',
        category_confidence = NULL,
        updated_at = datetime('now')
    WHERE transfer_pair_id = OLD.id;
END;
"""

_DROP_TRIGGER: Final[str] = "DROP TRIGGER IF EXISTS trg_clear_orphan_transfer_before_tx_delete"


def upgrade() -> None:
    """Install the orphan-transfer cleanup trigger."""
    op.execute(_CREATE_TRIGGER)


def downgrade() -> None:
    """Drop the orphan-transfer cleanup trigger."""
    op.execute(_DROP_TRIGGER)
