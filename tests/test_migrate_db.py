"""Tests for database migration from project-local to App Support."""

from __future__ import annotations

from unittest.mock import patch

from pfm.config import Settings
from pfm.server.migrate_db import migrate_db_if_needed


class TestMigrateDbIfNeeded:
    def test_new_path_exists(self, tmp_path):
        """If new path already exists, return it as-is."""
        new_db = tmp_path / "new" / "lurii.db"
        new_db.parent.mkdir(parents=True)
        new_db.write_text("existing")

        with patch("pfm.server.migrate_db.get_db_path", return_value=new_db):
            result = migrate_db_if_needed()
            assert result == new_db

    def test_old_path_migrated(self, tmp_path):
        """If old path exists and new doesn't, copy to new location."""
        old_db = tmp_path / "old" / "pfm.db"
        old_db.parent.mkdir(parents=True)
        old_db.write_text("old data")

        new_db = tmp_path / "new" / "lurii.db"

        settings = Settings(database_path=old_db)
        with (
            patch("pfm.server.migrate_db.get_db_path", return_value=new_db),
            patch("pfm.server.migrate_db.get_settings", return_value=settings),
        ):
            result = migrate_db_if_needed()
            assert result == new_db
            assert new_db.exists()
            assert new_db.read_text() == "old data"

    def test_neither_exists(self, tmp_path):
        """If neither path exists, return new path (will be created on first use)."""
        old_db = tmp_path / "nonexistent" / "pfm.db"
        new_db = tmp_path / "new" / "lurii.db"

        settings = Settings(database_path=old_db)
        with (
            patch("pfm.server.migrate_db.get_db_path", return_value=new_db),
            patch("pfm.server.migrate_db.get_settings", return_value=settings),
        ):
            result = migrate_db_if_needed()
            assert result == new_db
            assert not new_db.exists()  # Not created, just returns the path
