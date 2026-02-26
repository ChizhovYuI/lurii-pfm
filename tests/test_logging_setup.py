"""Tests for structured logging with secret redaction."""

import logging

from pfm.logging import _SecretRedactingFilter, setup_logging


def test_setup_logging_configures_root_logger():
    setup_logging("DEBUG")
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1


def test_setup_logging_info_level():
    setup_logging("INFO")
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_secret_redaction_filter():
    f = _SecretRedactingFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="token=%s",
        args=None,
        exc_info=None,
    )
    # Pre-format the message with the secret
    secret = "A" * 40
    record.msg = f"token={secret}"
    f.filter(record)
    assert "[REDACTED]" in record.msg
    assert secret not in record.msg


def test_secret_redaction_filter_non_secret():
    f = _SecretRedactingFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="normal message hello",
        args=None,
        exc_info=None,
    )
    f.filter(record)
    assert "hello" in record.msg
    assert "[REDACTED]" not in record.msg


def test_secret_redaction_filter_non_string_msg():
    f = _SecretRedactingFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg=12345,
        args=None,
        exc_info=None,  # type: ignore[arg-type]
    )
    result = f.filter(record)
    assert result is True  # should pass through without error
