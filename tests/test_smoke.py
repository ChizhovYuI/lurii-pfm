"""Smoke tests to verify project imports correctly."""

from pfm import __version__


def test_version():
    assert __version__ == "0.10.0"
