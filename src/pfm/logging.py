"""Structured logging setup with secret redaction."""

from __future__ import annotations

import logging
import re

_SECRET_PATTERN = re.compile(
    r"([A-Za-z0-9+/]{32,}|sk-[a-zA-Z0-9]{20,}|xox[bpoas]-[a-zA-Z0-9-]+)",
)


class _SecretRedactingFilter(logging.Filter):
    """Redact potential secrets from log messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _SECRET_PATTERN.sub("[REDACTED]", record.msg)
        return True


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging with secret redaction."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ),
    )
    handler.addFilter(_SecretRedactingFilter())

    root = logging.getLogger()
    preserved_handlers = [
        existing
        for existing in root.handlers
        if type(existing).__module__.startswith("_pytest.logging") or type(existing).__name__ == "LogCaptureHandler"
    ]
    root.handlers.clear()
    for existing in preserved_handlers:
        root.addHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())
