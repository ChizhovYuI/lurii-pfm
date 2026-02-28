"""Tests for the CLI thin-client module."""

from __future__ import annotations

from unittest.mock import patch

import httpx

from pfm.server.client import get_base_url, is_daemon_reachable


class TestIsDaemonReachable:
    def test_unreachable(self):
        """Daemon not running → should return False."""
        # Use an unlikely port
        assert is_daemon_reachable(port=1) is False

    def test_reachable(self):
        """Mock a successful health check."""
        mock_resp = httpx.Response(200, json={"status": "ok"})
        with patch("pfm.server.client.httpx.get", return_value=mock_resp):
            assert is_daemon_reachable() is True

    def test_non_200(self):
        """Non-200 response → not reachable."""
        mock_resp = httpx.Response(500)
        with patch("pfm.server.client.httpx.get", return_value=mock_resp):
            assert is_daemon_reachable() is False


class TestGetBaseUrl:
    def test_default(self):
        assert get_base_url() == "http://127.0.0.1:19274"

    def test_custom_port(self):
        assert get_base_url(port=8080) == "http://127.0.0.1:8080"
