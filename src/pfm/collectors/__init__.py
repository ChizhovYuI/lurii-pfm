"""Data collectors for all financial sources."""

from __future__ import annotations

from pfm.collectors.base import BaseCollector

# Registry: source_name -> collector class
# Populated as collectors are implemented
COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {}


def register_collector(cls: type[BaseCollector]) -> type[BaseCollector]:
    """Decorator to register a collector class."""
    COLLECTOR_REGISTRY[cls.source_name] = cls
    return cls


__all__ = [
    "COLLECTOR_REGISTRY",
    "BaseCollector",
    "register_collector",
]
