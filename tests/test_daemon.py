"""Tests for daemon lifecycle management."""

from __future__ import annotations

import os
from unittest.mock import patch

from pfm.server.daemon import (
    DEFAULT_PORT,
    generate_plist,
    get_app_support_dir,
    get_db_path,
    get_log_path,
    get_pid_path,
    get_plist_path,
    is_daemon_running,
    remove_pid_file,
    write_pid_file,
)


class TestPaths:
    def test_app_support_dir(self):
        path = get_app_support_dir()
        assert "Lurii Finance" in str(path)
        assert "Library/Application Support" in str(path)

    def test_pid_path(self):
        assert get_pid_path().name == "daemon.pid"

    def test_log_path(self):
        assert get_log_path().name == "daemon.log"

    def test_db_path(self):
        assert get_db_path().name == "lurii.db"

    def test_plist_path(self):
        path = get_plist_path()
        assert "LaunchAgents" in str(path)
        assert path.suffix == ".plist"


class TestPidFile:
    def test_write_and_remove(self, tmp_path):
        pid_path = tmp_path / "test.pid"
        with patch("pfm.server.daemon.get_pid_path", return_value=pid_path):
            write_pid_file()
            assert pid_path.exists()
            assert pid_path.read_text().strip() == str(os.getpid())

            remove_pid_file()
            assert not pid_path.exists()

    def test_remove_nonexistent(self, tmp_path):
        pid_path = tmp_path / "nonexistent.pid"
        with patch("pfm.server.daemon.get_pid_path", return_value=pid_path):
            remove_pid_file()  # Should not raise


class TestIsDaemonRunning:
    def test_no_pid_file(self, tmp_path):
        pid_path = tmp_path / "nonexistent.pid"
        with patch("pfm.server.daemon.get_pid_path", return_value=pid_path):
            running, pid = is_daemon_running()
            assert running is False
            assert pid is None

    def test_stale_pid_file(self, tmp_path):
        pid_path = tmp_path / "stale.pid"
        pid_path.write_text("999999999")  # Very unlikely to be a real PID
        with patch("pfm.server.daemon.get_pid_path", return_value=pid_path):
            running, pid = is_daemon_running()
            assert running is False
            assert pid is None
            assert not pid_path.exists()  # Should be cleaned up

    def test_current_process_pid(self, tmp_path):
        pid_path = tmp_path / "current.pid"
        pid_path.write_text(str(os.getpid()))
        with patch("pfm.server.daemon.get_pid_path", return_value=pid_path):
            running, pid = is_daemon_running()
            assert running is True
            assert pid == os.getpid()


class TestGeneratePlist:
    def test_contains_required_fields(self):
        plist = generate_plist(19274)
        assert "finance.lurii.pfm" in plist
        assert "19274" in plist
        assert "pfm" in plist
        assert "server" in plist
        assert "KeepAlive" in plist
        assert "RunAtLoad" in plist


class TestDefaultPort:
    def test_default_port_value(self):
        assert DEFAULT_PORT == 19274
