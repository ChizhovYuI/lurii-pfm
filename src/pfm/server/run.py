"""Server entry point — blocking run, used by launchd."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.daemon import DEFAULT_PORT, remove_pid_file, write_pid_file

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


def run_server(
    port: int = DEFAULT_PORT,
    db_path: Path | None = None,
) -> None:
    """Create the app, bind to 127.0.0.1, write PID, block until stopped."""
    from pfm.server.app import create_app
    from pfm.server.migrate_db import migrate_db_if_needed

    if db_path is None:
        db_path = migrate_db_if_needed()

    app = create_app(db_path)

    write_pid_file()
    try:
        logger.info("Starting server on 127.0.0.1:%d (db=%s)", port, db_path)
        web.run_app(app, host="127.0.0.1", port=port, print=None)
    finally:
        remove_pid_file()
