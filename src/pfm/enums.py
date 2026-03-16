"""Shared enumerations for sources, operators, and source groups."""

from __future__ import annotations

from enum import StrEnum


class RuleOperator(StrEnum):
    """Operators available for rule matching."""

    EQ = "eq"
    CONTAINS = "contains"


class SourceName(StrEnum):
    """Canonical source identifiers."""

    OKX = "okx"
    BINANCE = "binance"
    BINANCE_TH = "binance_th"
    BYBIT = "bybit"
    COINEX = "coinex"
    MEXC = "mexc"
    LOBSTR = "lobstr"
    RABBY = "rabby"
    BLEND = "blend"
    YO = "yo"
    BITGET_WALLET = "bitget_wallet"
    WISE = "wise"
    KBANK = "kbank"
    REVOLUT = "revolut"
    IBKR = "ibkr"
    TRADING212 = "trading212"
    CASH = "cash"


class SourceGroup(StrEnum):
    """High-level source classification."""

    CRYPTO = "crypto"
    DEFI = "defi"
    BROKER = "broker"
    BANK = "bank"


SOURCE_GROUP_MAP: dict[SourceName, SourceGroup] = {
    SourceName.OKX: SourceGroup.CRYPTO,
    SourceName.BINANCE: SourceGroup.CRYPTO,
    SourceName.BINANCE_TH: SourceGroup.CRYPTO,
    SourceName.BYBIT: SourceGroup.CRYPTO,
    SourceName.COINEX: SourceGroup.CRYPTO,
    SourceName.MEXC: SourceGroup.CRYPTO,
    SourceName.LOBSTR: SourceGroup.CRYPTO,
    SourceName.RABBY: SourceGroup.CRYPTO,
    SourceName.BLEND: SourceGroup.DEFI,
    SourceName.YO: SourceGroup.DEFI,
    SourceName.BITGET_WALLET: SourceGroup.DEFI,
    SourceName.WISE: SourceGroup.BANK,
    SourceName.KBANK: SourceGroup.BANK,
    SourceName.REVOLUT: SourceGroup.BANK,
    SourceName.IBKR: SourceGroup.BROKER,
    SourceName.TRADING212: SourceGroup.BROKER,
    SourceName.CASH: SourceGroup.BANK,
}


def source_group(name: str) -> SourceGroup:
    """Resolve a source name to its group. Falls back to CRYPTO."""
    try:
        return SOURCE_GROUP_MAP[SourceName(name.lower())]
    except (ValueError, KeyError):
        return SourceGroup.CRYPTO
