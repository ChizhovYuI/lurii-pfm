"""Reusable JSON serializers for dataclasses and domain objects."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pfm.db.models import CollectorResult, Snapshot, Source

# ── Asset classification constants ──────────────────────────────────────

_FIAT_ASSETS: frozenset[str] = frozenset(
    {
        "USD",
        "THB",
        "GBP",
        "EUR",
        "JPY",
        "CHF",
        "CAD",
        "AUD",
        "NZD",
        "SGD",
        "HKD",
    },
)
_CRYPTO_SOURCES: frozenset[str] = frozenset(
    {"okx", "binance", "binance_th", "bybit", "mexc", "mexc_earn", "lobstr", "rabby"}
)
_FIAT_SOURCES: frozenset[str] = frozenset({"wise", "kbank"})
_STOCK_SOURCES: frozenset[str] = frozenset({"ibkr"})
_DEFI_SOURCES: frozenset[str] = frozenset({"blend", "yo", "bitget_wallet"})


# ── Decimal formatting ─────────────────────────────────────────────────


def _str_decimal(value: Decimal) -> str:
    """Convert a Decimal to string preserving full precision (no rounding)."""
    return format(value.normalize(), "f")


# ── Masking ─────────────────────────────────────────────────────────────


def mask_secret(value: str) -> str:
    """Mask a secret value, showing first 3 and last 3 chars."""
    if len(value) <= 8:  # noqa: PLR2004
        return "***"
    return f"{value[:3]}...{value[-3:]}"


# ── Domain object serializers ───────────────────────────────────────────


def source_to_dict(source: Source, *, mask_secrets: bool = True) -> dict[str, Any]:
    """Convert a Source dataclass to a JSON-safe dict."""
    creds: dict[str, str] = json.loads(source.credentials)
    if mask_secrets:
        creds = {k: mask_secret(v) for k, v in creds.items()}
    return {
        "name": source.name,
        "type": source.type,
        "credentials": creds,
        "enabled": source.enabled,
    }


def snapshot_to_dict(snapshot: Snapshot) -> dict[str, Any]:
    """Convert a Snapshot dataclass to a JSON-safe dict."""
    source_name = snapshot.source_name or snapshot.source
    return {
        "date": snapshot.date.isoformat(),
        "source": snapshot.source,
        "source_name": source_name,
        "asset": snapshot.asset,
        "amount": _str_decimal(snapshot.amount),
        "usd_value": _str_decimal(snapshot.usd_value),
        "price": _str_decimal(snapshot.price),
        "apy": _str_decimal(snapshot.apy),
    }


def collector_result_to_dict(result: CollectorResult) -> dict[str, Any]:
    """Convert a CollectorResult to a JSON-safe dict."""
    return {
        "source": result.source,
        "snapshots_count": result.snapshots_count,
        "snapshots_usd_total": _str_decimal(result.snapshots_usd_total),
        "transactions_count": result.transactions_count,
        "errors": result.errors,
        "duration_seconds": result.duration_seconds,
    }


def analytics_to_dict(metrics: dict[str, str]) -> dict[str, Any]:
    """Parse cached JSON analytics metrics into a single dict."""
    result: dict[str, Any] = {}
    for key, raw_json in metrics.items():
        try:
            result[key] = json.loads(raw_json)
        except json.JSONDecodeError:
            result[key] = raw_json
    return result


# ── Asset type classification ───────────────────────────────────────────


def asset_type_for_snapshot(source: str, asset: str) -> str:
    """Classify an asset by its source and ticker."""
    source_lower = source.lower()
    asset_upper = asset.upper()
    if source_lower in _DEFI_SOURCES:
        return "defi"
    if source_lower in _FIAT_SOURCES:
        return "fiat"
    if source_lower in _STOCK_SOURCES:
        return "fiat" if asset_upper in _FIAT_ASSETS else "stocks"
    if asset_upper in _FIAT_ASSETS:
        return "fiat"
    if source_lower in _CRYPTO_SOURCES:
        return "crypto"
    return "other"


def build_asset_type_map(snapshots: list[Snapshot]) -> dict[str, str]:
    """Build a mapping from asset ticker to dominant asset type."""
    by_asset: dict[str, dict[str, Decimal]] = {}
    for snap in snapshots:
        asset = snap.asset.upper()
        asset_types = by_asset.setdefault(asset, {})
        a_type = asset_type_for_snapshot(snap.source, snap.asset)
        asset_types[a_type] = asset_types.get(a_type, Decimal(0)) + snap.usd_value

    resolved: dict[str, str] = {}
    for asset, scored_types in by_asset.items():
        resolved[asset] = max(scored_types.items(), key=lambda item: item[1])[0]
    return resolved


# ── Analytics cache parsers ─────────────────────────────────────────────


def parse_net_worth_usd(raw_json: str) -> Decimal:
    """Extract net worth USD value from cached metric JSON."""
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return Decimal(0)

    if not isinstance(parsed, dict):
        return Decimal(0)
    value = parsed.get("usd", "0")
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)


def parse_cached_ai_commentary(raw_json: str | None) -> str | None:
    """Parse cached AI commentary metric text, if present."""
    if raw_json is None:
        return None
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        text = raw_json.strip()
        return text or None
    if isinstance(parsed, str):
        text = parsed.strip()
        return text or None
    if isinstance(parsed, dict):
        text_value = parsed.get("text")
        if isinstance(text_value, str):
            value = text_value.strip()
            return value or None
    return None


def parse_cached_ai_commentary_model(raw_json: str | None) -> str | None:
    """Parse cached AI commentary metric model name, if present."""
    if raw_json is None:
        return None
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        model_value = parsed.get("model")
        if isinstance(model_value, str):
            value = model_value.strip()
            return value or None
    return None


# ── JSON default handler ────────────────────────────────────────────────


def decimal_default(obj: object) -> str:
    """JSON default handler for Decimal and date objects."""
    if isinstance(obj, Decimal):
        return _str_decimal(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)
