"""Data collectors for all financial sources."""

from __future__ import annotations

from pfm.collectors.base import BaseCollector

# Registry: source_name -> collector class
# Populated as collectors are imported below
COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {}


def register_collector(cls: type[BaseCollector]) -> type[BaseCollector]:
    """Decorator to register a collector class."""
    COLLECTOR_REGISTRY[cls.source_name] = cls
    return cls


# Import all collector modules to populate the registry.
# Each module uses @register_collector on its class.
from pfm.collectors import (  # noqa: E402
    binance,
    binance_th,
    bitget_wallet,
    blend,
    bybit,
    ibkr,
    kbank,
    lobstr,
    okx,
    rabby,
    revolut,
    wise,
    yo,
)

__all__ = [
    "COLLECTOR_REGISTRY",
    "BaseCollector",
    "binance",
    "binance_th",
    "bitget_wallet",
    "blend",
    "bybit",
    "ibkr",
    "kbank",
    "lobstr",
    "okx",
    "rabby",
    "register_collector",
    "revolut",
    "wise",
    "yo",
]
