"""One-time database path migration from project-local to App Support."""

from __future__ import annotations

import logging
import shutil
from typing import TYPE_CHECKING

from pfm.config import get_settings
from pfm.server.daemon import get_db_path

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


def migrate_db_if_needed() -> Path:
    """If old data/pfm.db exists and new path doesn't, copy to App Support.

    Returns the resolved database path to use.
    """
    new_path = get_db_path()
    if new_path.exists():
        return new_path

    settings = get_settings()
    old_path = settings.database_path
    if old_path.exists():
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(old_path, new_path)
        logger.info("Migrated database from %s to %s", old_path, new_path)

    return new_path
