"""Reusable JSON serializers for dataclasses and domain objects."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pfm.analytics.pnl import AssetPnl, PnlResult
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
_CRYPTO_SOURCES: frozenset[str] = frozenset({"okx", "binance", "binance_th", "bybit", "lobstr"})
_FIAT_SOURCES: frozenset[str] = frozenset({"wise", "kbank"})
_STOCK_SOURCES: frozenset[str] = frozenset({"ibkr"})
_DEFI_SOURCES: frozenset[str] = frozenset({"blend"})


# ── Decimal formatting ─────────────────────────────────────────────────

_TWO_PLACES = Decimal("0.01")
_EIGHT_PLACES = Decimal("0.00000001")


def fmt_usd(value: Decimal) -> str:
    """Round a USD value to 2 decimal places."""
    return str(value.quantize(_TWO_PLACES))


def fmt_pct(value: Decimal) -> str:
    """Round a percentage to 2 decimal places."""
    return str(value.quantize(_TWO_PLACES))


def fmt_amount(value: Decimal) -> str:
    """Format an asset amount — up to 8 decimal places, trailing zeros stripped."""
    return format(value.quantize(_EIGHT_PLACES).normalize(), "f")


def fmt_price(value: Decimal) -> str:
    """Format a unit price — 2dp for values ≥1, up to 8dp for small values."""
    if value >= 1:
        return str(value.quantize(_TWO_PLACES))
    return format(value.quantize(_EIGHT_PLACES).normalize(), "f")


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
    return {
        "date": snapshot.date.isoformat(),
        "source": snapshot.source,
        "asset": snapshot.asset,
        "amount": fmt_amount(snapshot.amount),
        "usd_value": fmt_usd(snapshot.usd_value),
    }


def collector_result_to_dict(result: CollectorResult) -> dict[str, Any]:
    """Convert a CollectorResult to a JSON-safe dict."""
    return {
        "source": result.source,
        "snapshots_count": result.snapshots_count,
        "snapshots_usd_total": fmt_usd(result.snapshots_usd_total),
        "transactions_count": result.transactions_count,
        "errors": result.errors,
        "duration_seconds": result.duration_seconds,
    }


def pnl_result_to_dict(result: PnlResult) -> dict[str, object]:
    """Serialize PnL dataclass to a JSON-safe dict."""

    def _asset_pnl(row: AssetPnl) -> dict[str, object]:
        return {
            "asset": row.asset,
            "start_value": fmt_usd(row.start_value),
            "end_value": fmt_usd(row.end_value),
            "absolute_change": fmt_usd(row.absolute_change),
            "percentage_change": fmt_pct(row.percentage_change),
            "cost_basis_value": fmt_usd(row.cost_basis_value) if row.cost_basis_value is not None else None,
        }

    return {
        "start_date": result.start_date.isoformat() if result.start_date else None,
        "end_date": result.end_date.isoformat() if result.end_date else None,
        "start_value": fmt_usd(result.start_value),
        "end_value": fmt_usd(result.end_value),
        "absolute_change": fmt_usd(result.absolute_change),
        "percentage_change": fmt_pct(result.percentage_change),
        "by_asset": [_asset_pnl(row) for row in result.by_asset],
        "top_gainers": [_asset_pnl(row) for row in result.top_gainers],
        "top_losers": [_asset_pnl(row) for row in result.top_losers],
        "notes": list(result.notes),
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
        return fmt_usd(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)
