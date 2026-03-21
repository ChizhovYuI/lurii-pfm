"""Transaction REST endpoints with category metadata."""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.db.models import TransactionType, TypeRule, effective_type
from pfm.server.serializers import _str_decimal
from pfm.server.state import get_repo

if TYPE_CHECKING:
    from pfm.analytics.transaction_grouper import TransactionGroup
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.models import CategoryRule, Transaction, TransactionMetadata

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()

_VALID_TYPES = frozenset(t.value for t in TransactionType if t != TransactionType.UNKNOWN)
_VALID_OPERATORS = frozenset({"eq", "contains"})


def _parse_int_param(request: web.Request, name: str = "id") -> int | web.Response:
    """Parse an integer path parameter. Returns int or a 400 Response on failure."""
    try:
        return int(request.match_info[name])
    except (ValueError, KeyError):
        return web.json_response({"error": f"{name} must be an integer"}, status=400)


def _parse_int_query(request: web.Request, name: str, default: int) -> int:
    """Parse an integer query parameter with a fallback default."""
    try:
        return int(request.query.get(name, str(default)))
    except ValueError:
        return default


# ── Helpers ────────────────────────────────────────────────────────────


def _extract_description(tx: Transaction) -> str:
    """Extract description from raw_json if present."""
    if not tx.raw_json:
        return ""
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            return str(parsed.get("description", ""))
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


def _extract_counterparty(tx: Transaction) -> str:
    """Infer counterparty asset from raw_json (OKX instId, IBKR currency, etc.)."""
    if not tx.raw_json:
        return ""
    try:
        parsed = json.loads(tx.raw_json)
        if not isinstance(parsed, dict):
            return ""
        # Instrument pair (e.g. OKX instId "BTC-USD").
        inst_id = parsed.get("instId") or parsed.get("market") or ""
        if isinstance(inst_id, str) and "-" in inst_id:
            parts = inst_id.split("-", maxsplit=1)
            other = parts[1] if parts[0].upper() == tx.asset.upper() else parts[0]
            return other.upper()
        # Settlement currency (e.g. IBKR currency "USD" for stock trades).
        currency = parsed.get("currency")
        if isinstance(currency, str) and currency.upper() != tx.asset.upper():
            return currency.upper()
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


_TIME_KEYS_DIRECT = ("time", "ts", "fillTime", "transactionTime")
_TIME_KEYS_DATETIME = (
    "timestamp",
    "blockTimestamp",
    "time_at",
    "createdAt",
    "dateTime",
    "paidAt",
    "bookingDateTime",
    "created_at",
    "applyTime",
    "completeTime",
)


_LocalDT = tuple[str | None, str]  # (local_date or None, local_time HH:MM)


def _epoch_to_local(epoch: float) -> _LocalDT:
    """Convert epoch seconds to (YYYY-MM-DD, HH:MM) in the system local timezone."""
    from datetime import UTC, datetime

    dt = datetime.fromtimestamp(epoch, tz=UTC).astimezone()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")


def _parse_time_value(val: object) -> _LocalDT | None:  # noqa: PLR0911
    """Parse a single timestamp value into (local_date, HH:MM)."""
    if isinstance(val, int | float):
        epoch = val / 1000 if val > 1e12 else val  # noqa: PLR2004
        return _epoch_to_local(epoch)
    if not isinstance(val, str) or len(val) < 5:  # noqa: PLR2004
        return None
    # Numeric string (epoch ms or seconds, e.g. OKX "ts": "1773935980602")
    if val.isdigit():
        epoch_num = int(val)
        epoch = epoch_num / 1000 if epoch_num > 1e12 else epoch_num  # noqa: PLR2004
        return _epoch_to_local(epoch)
    # ISO datetime with timezone: "2026-03-02T12:58:35.000Z" or "+07:00"
    if "T" in val:
        return _parse_iso_to_local(val)
    # Space-separated: "2026-02-25 17:47:41"
    if " " in val and len(val) >= 16:  # noqa: PLR2004
        return _parse_spaced_to_local(val)
    # IBKR Flex format: "YYYYMMDD;HHMMSS" (already local)
    if ";" in val and len(val) >= 15:  # noqa: PLR2004
        parts = val.split(";", maxsplit=1)
        if len(parts[1]) >= 4:  # noqa: PLR2004
            return None, f"{parts[1][:2]}:{parts[1][2:4]}"
    # Direct HH:MM (already local, e.g. KBank) — no date shift
    if val[2] == ":":
        return None, val[:5]
    return None


def _parse_iso_to_local(val: str) -> _LocalDT | None:
    """Parse ISO datetime and convert to local (date, HH:MM)."""
    from datetime import datetime

    try:
        dt = datetime.fromisoformat(val)
        local = dt.astimezone()
        return local.strftime("%Y-%m-%d"), local.strftime("%H:%M")
    except (ValueError, OSError):
        pass
    # Fallback: extract HH:MM directly, no date shift
    hhmm = val.split("T", maxsplit=1)[1][:5]
    if len(hhmm) == 5 and hhmm[2] == ":":  # noqa: PLR2004
        return None, hhmm
    return None


def _parse_spaced_to_local(val: str) -> _LocalDT | None:
    """Parse space-separated datetime and convert to local (date, HH:MM)."""
    from datetime import datetime

    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is not None:
            local = dt.astimezone()
            return local.strftime("%Y-%m-%d"), local.strftime("%H:%M")
        # No timezone — return parsed date and time as-is (assumed local)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")
    except (ValueError, OSError):
        pass
    # Fallback
    hhmm = val.split(" ", maxsplit=1)[1][:5]
    if len(hhmm) == 5 and hhmm[2] == ":":  # noqa: PLR2004
        return None, hhmm
    return None


def _extract_datetime(tx: Transaction) -> tuple[str | None, str | None]:
    """Extract local (date, time) from raw_json. Date is non-None only when it differs from tx.date."""
    if not tx.raw_json:
        return None, None
    try:
        parsed = json.loads(tx.raw_json)
        if not isinstance(parsed, dict):
            return None, None
        result = _find_time_in_raw(parsed, tx.date.isoformat())
        if result:
            return result
    except (json.JSONDecodeError, TypeError, OSError, OverflowError, ValueError):
        pass
    return None, None


def _find_time_in_raw(parsed: dict[str, object], tx_date_iso: str) -> tuple[str | None, str] | None:
    """Search raw_json dict for a parseable timestamp, return (shifted_date, HH:MM) or None."""
    for key in _TIME_KEYS_DIRECT + _TIME_KEYS_DATETIME:
        result = _parse_time_value(parsed.get(key))
        if result:
            local_date, local_time = result
            shifted = local_date if local_date and local_date != tx_date_iso else None
            return shifted, local_time
    return None


async def _build_price_map(
    app: web.Application,
    items: list[tuple[Transaction, TransactionMetadata | None]],
) -> dict[str, Decimal]:
    """Build asset -> USD price map from the prices table (latest available date)."""
    from pfm.server.price_resolver import build_price_map

    repo = get_repo(app)
    dates = list({tx.date for tx, _ in items})
    return await build_price_map(repo, dates)


def _build_id_lookup(
    items: list[tuple[Transaction, TransactionMetadata | None]],
) -> dict[int, tuple[Transaction, TransactionMetadata | None]]:
    """Build a mapping from transaction ID to (Transaction, metadata) pair."""
    return {tx.id: (tx, meta) for tx, meta in items if tx.id is not None}


def _resolve_usd(tx: Transaction, prices: dict[str, Decimal]) -> Decimal:
    from pfm.server.price_resolver import resolve_usd

    return resolve_usd(tx, prices)


def _serialize_tx(
    tx: Transaction,
    meta: TransactionMetadata | None,
    prices: dict[str, Decimal] | None = None,
) -> dict[str, object]:
    etype = effective_type(tx, meta)
    local_date, local_time = _extract_datetime(tx)
    usd_value = _resolve_usd(tx, prices) if prices else tx.usd_value
    cp_asset = tx.counterparty_asset or _extract_counterparty(tx)
    result: dict[str, object] = {
        "id": tx.id,
        "date": local_date or tx.date.isoformat(),
        "time": local_time,
        "source": tx.source,
        "source_name": tx.source_name or tx.source,
        "tx_type": tx.tx_type.value,
        "effective_type": etype,
        "asset": tx.asset,
        "amount": _str_decimal(tx.amount),
        "usd_value": _str_decimal(usd_value),
        "counterparty_asset": cp_asset or None,
        "counterparty_amount": _str_decimal(tx.counterparty_amount),
        "tx_id": tx.tx_id,
        "trade_side": tx.trade_side,
        "description": _extract_description(tx),
    }
    if meta:
        result["metadata"] = {
            "category": meta.category,
            "category_source": meta.category_source,
            "category_confidence": meta.category_confidence,
            "type_override": meta.type_override,
            "is_internal_transfer": meta.is_internal_transfer,
            "transfer_pair_id": meta.transfer_pair_id,
            "transfer_detected_by": meta.transfer_detected_by,
            "reviewed": meta.reviewed,
            "notes": meta.notes,
        }
    else:
        result["metadata"] = None
    return result


def _serialize_grouped_tx(
    group: TransactionGroup,
    group_index: int,
    by_id: dict[int, tuple[Transaction, TransactionMetadata | None]] | None = None,
    prices: dict[str, Decimal] | None = None,
) -> dict[str, object]:
    """Serialize a transaction group as a single row with negative synthetic ID."""
    local_date, local_time = _group_datetime(group, by_id)
    category = _group_category(group, by_id)
    usd_value = _group_usd_value(group, by_id, prices)
    return {
        "id": -(group_index + 1),
        "date": local_date or group.display_date.isoformat(),
        "time": local_time,
        "source": group.from_source,
        "source_name": group.from_source,
        "tx_type": group.display_tx_type,
        "effective_type": group.display_tx_type,
        "asset": group.from_asset,
        "amount": _str_decimal(group.from_amount),
        "usd_value": _str_decimal(usd_value),
        "counterparty_asset": None,
        "counterparty_amount": None,
        "tx_id": None,
        "trade_side": None,
        "description": "",
        "metadata": {"category": category} if category else None,
        "group": {
            "type": group.group_type,
            "child_ids": group.child_ids,
            "child_count": len(group.child_ids),
            "from_source": group.from_source,
            "to_source": group.to_source,
            "from_source_type": group.from_source_type,
            "to_source_type": group.to_source_type,
            "from_asset": group.from_asset,
            "to_asset": group.to_asset,
            "from_amount": _str_decimal(group.from_amount),
            "to_amount": _str_decimal(group.to_amount),
        },
    }


def _group_datetime(
    group: TransactionGroup,
    by_id: dict[int, tuple[Transaction, TransactionMetadata | None]] | None,
) -> tuple[str | None, str | None]:
    """Return (local_date, earliest HH:MM) from child transactions."""
    if not by_id:
        return None, None
    times: list[tuple[str | None, str]] = []
    for cid in group.child_ids:
        pair = by_id.get(cid)
        if pair is None:
            continue
        local_date, local_time = _extract_datetime(pair[0])
        if local_time:
            times.append((local_date, local_time))
    if not times:
        return None, None
    return min(times, key=lambda t: t[1])


def _group_category(
    group: TransactionGroup,
    by_id: dict[int, tuple[Transaction, TransactionMetadata | None]] | None,
) -> str | None:
    """Return the most common category from child transaction metadata."""
    if not by_id:
        return None
    counts: dict[str, int] = {}
    for cid in group.child_ids:
        pair = by_id.get(cid)
        if pair is None:
            continue
        meta = pair[1]
        if meta and meta.category:
            counts[meta.category] = counts.get(meta.category, 0) + 1
    if not counts:
        return None
    return max(counts, key=lambda k: counts[k])


def _group_usd_value(
    group: TransactionGroup,
    by_id: dict[int, tuple[Transaction, TransactionMetadata | None]] | None,
    prices: dict[str, Decimal] | None,
) -> Decimal:
    """Return group USD value, estimating from prices if stored value is zero."""
    if group.display_usd_value:
        return group.display_usd_value
    if not by_id or not prices:
        return Decimal(0)
    # Sum USD for the from-asset side only (avoids double-counting trade pairs).
    total = Decimal(0)
    for cid in group.child_ids:
        pair = by_id.get(cid)
        if pair is None:
            continue
        tx = pair[0]
        if tx.asset.upper() == group.from_asset.upper():
            total += _resolve_usd(tx, prices)
    return total


def _serialize_category_rule(rule: CategoryRule) -> dict[str, object]:
    return {
        "id": rule.id,
        "type_match": rule.type_match,
        "type_operator": rule.type_operator,
        "field_name": rule.field_name or None,
        "field_operator": rule.field_operator or None,
        "field_value": rule.field_value or None,
        "source": rule.source,
        "result_category": rule.result_category,
        "priority": rule.priority,
        "builtin": rule.builtin,
        "deleted": rule.deleted,
    }


def _serialize_type_rule(rule: TypeRule) -> dict[str, object]:
    return {
        "id": rule.id,
        "source": rule.source,
        "field_name": rule.field_name or None,
        "field_operator": rule.field_operator or None,
        "field_value": rule.field_value or None,
        "result_type": rule.result_type,
        "priority": rule.priority,
        "builtin": rule.builtin,
        "deleted": rule.deleted,
    }


def _get_metadata_store(app: web.Application) -> MetadataStore:
    from pfm.db.metadata_store import MetadataStore

    repo = get_repo(app)
    return MetadataStore(repo.connection)


def _escape_like(value: str) -> str:
    """Escape SQL LIKE wildcard characters."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


_RAW_SKIP_KEYS = {"_links", "topics"}
_RAW_MAX_DEPTH = 2
_RAW_MAX_LIST = 3
_RAW_MAX_HEX = 66


def _is_long_hex(v: str) -> bool:
    return len(v) > _RAW_MAX_HEX and v.startswith("0x") and all(c in "0123456789abcdefABCDEF" for c in v[2:])


def _flatten_value(obj: object, prefix: str, depth: int, out: dict[str, str]) -> None:
    """Recursively flatten a value into dot-notation keys."""
    if isinstance(obj, dict):
        _flatten_dict(obj, prefix, depth, out)
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:_RAW_MAX_LIST]):
            _flatten_value(item, f"{prefix}.{i}", depth + 1, out)
        if len(obj) > _RAW_MAX_LIST:
            out[f"{prefix}._count"] = str(len(obj))
    elif obj is not None:
        s = str(obj)
        if not _is_long_hex(s):
            out[prefix] = s


def _flatten_dict(d: dict[str, object], prefix: str, depth: int, out: dict[str, str]) -> None:
    """Flatten a dict into dot-notation keys, skipping noise fields."""
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if k in _RAW_SKIP_KEYS:
            continue
        if depth >= _RAW_MAX_DEPTH:
            s = str(v)
            if not _is_long_hex(s):
                out[key] = s
        else:
            _flatten_value(v, key, depth + 1, out)


def _parse_raw_fields(tx: Transaction) -> dict[str, str]:
    """Parse raw_json into a flat dict for display and rule authoring."""
    if not tx.raw_json:
        return {}
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            result: dict[str, str] = {}
            _flatten_dict(parsed, "", 0, result)
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    return {}


# ══════════════════════════════════════════════════════════════════════
# IMPORTANT: Static GET routes MUST be registered before /{id} to avoid
# aiohttp's FIFO matching from shadowing them.
# ══════════════════════════════════════════════════════════════════════


# ── Transaction list (static path — before /{id}) ──────────────────────


def _filter_by_local_date(
    items: list[tuple[Transaction, TransactionMetadata | None]],
    start_iso: str,
    end_iso: str,
) -> list[tuple[Transaction, TransactionMetadata | None]]:
    """Keep only transactions whose local-timezone date falls within [start, end]."""
    result: list[tuple[Transaction, TransactionMetadata | None]] = []
    for tx, meta in items:
        local_date, _ = _extract_datetime(tx)
        effective = local_date or tx.date.isoformat()
        if start_iso <= effective <= end_iso:
            result.append((tx, meta))
    return result


@routes.get("/api/v1/transactions")
async def list_transactions(request: web.Request) -> web.Response:
    """Fetch transactions for a calendar month (local timezone).

    Use ``month=YYYY-MM`` to select a month. Omit for the current month.
    Response includes ``next_month`` cursor for older pages.
    """
    from calendar import monthrange
    from datetime import UTC, date, datetime, timedelta

    store = _get_metadata_store(request.app)
    source_name = request.query.get("source_name")
    tx_type = request.query.get("tx_type")
    category = request.query.get("category")
    search = request.query.get("search")
    grouped = request.query.get("grouped", "true").lower() != "false"
    month_param = request.query.get("month")

    today = datetime.now(tz=UTC).astimezone().date()
    if month_param:
        try:
            parts = month_param.split("-")
            year, month = int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            return web.json_response({"error": "month must be YYYY-MM"}, status=400)
    else:
        year, month = today.year, today.month

    window_start = date(year, month, 1)
    _, last_day = monthrange(year, month)
    window_end = date(year, month, last_day)
    window_end = min(window_end, today)

    # Fetch with ±1 day buffer for timezone-shifted transactions.
    items, _ = await store.get_transactions_paginated(
        source_name=source_name,
        tx_type=tx_type,
        category=category,
        start=window_start - timedelta(days=1),
        end=window_end + timedelta(days=1),
        search=search,
        limit=10000,
        offset=0,
    )

    items = _filter_by_local_date(items, window_start.isoformat(), window_end.isoformat())
    total = len(items)

    # Cursor for the previous month.
    prev_month_end = window_start - timedelta(days=1)
    _, older_total = await store.get_transactions_paginated(
        source_name=source_name,
        tx_type=tx_type,
        category=category,
        start=None,
        end=prev_month_end,
        search=search,
        limit=1,
        offset=0,
    )
    next_month = f"{prev_month_end.year}-{prev_month_end.month:02d}" if older_total > 0 else None

    # Fetch transfer counterparts that may fall outside the month.
    if grouped:
        page_ids = {tx.id for tx, _ in items if tx.id is not None}
        for _, meta in items:
            if meta and meta.is_internal_transfer and meta.transfer_pair_id and meta.transfer_pair_id not in page_ids:
                pair = await store.get_transaction_by_id(meta.transfer_pair_id)
                if pair:
                    items.append(pair)

    prices = await _build_price_map(request.app, items)
    extra: dict[str, object] = {
        "month": f"{year}-{month:02d}",
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "next_month": next_month,
    }

    if not grouped:
        return web.json_response(
            {
                "items": [_serialize_tx(tx, meta, prices) for tx, meta in items],
                "total": total,
                **extra,
            }
        )

    return _grouped_response(items, total, prices, extra)


def _grouped_response(
    items: list[tuple[Transaction, TransactionMetadata | None]],
    total: int,
    prices: dict[str, Decimal],
    extra: dict[str, object],
) -> web.Response:
    """Build grouped transaction response (counterparts must be pre-fetched by caller)."""
    from pfm.analytics.transaction_grouper import group_transactions

    grouping = group_transactions(items)
    by_id = _build_id_lookup(items)

    serialized: list[dict[str, object]] = []
    for i, group in enumerate(grouping.groups):
        serialized.append(_serialize_grouped_tx(group, i, by_id, prices))
    for tx, meta in grouping.ungrouped:
        row = _serialize_tx(tx, meta, prices)
        row["group"] = None
        serialized.append(row)

    serialized.sort(key=lambda r: (str(r.get("date", "")), r.get("id", 0)), reverse=True)

    return web.json_response(
        {
            "items": serialized,
            "total": total,
            "total_ungrouped": grouping.total_ungrouped,
            **extra,
        }
    )


# ── Static GET routes (must appear before /{id}) ──────────────────────


@routes.get("/api/v1/transactions/review-queue")
async def review_queue(request: web.Request) -> web.Response:
    """Get unreviewed/low-confidence transactions."""
    store = _get_metadata_store(request.app)
    limit = min(_parse_int_query(request, "limit", 50), 200)
    offset = _parse_int_query(request, "offset", 0)
    items = await store.get_review_queue(limit=limit, offset=offset)
    return web.json_response({"items": [_serialize_tx(tx, meta) for tx, meta in items]})


@routes.get("/api/v1/transactions/categories")
async def list_categories(request: web.Request) -> web.Response:
    """List all transaction categories."""
    store = _get_metadata_store(request.app)
    tx_type = request.query.get("tx_type")
    categories = await store.get_categories(tx_type=tx_type)
    return web.json_response(
        [
            {
                "id": cat.id,
                "tx_type": cat.tx_type,
                "category": cat.category,
                "display_name": cat.display_name,
                "sort_order": cat.sort_order,
            }
            for cat in categories
        ]
    )


@routes.get("/api/v1/transactions/analytics/summary")
async def analytics_summary(request: web.Request) -> web.Response:
    """Spending/income by category for a period."""
    from pfm.analytics.transaction_analytics import compute_analytics_summary

    repo = get_repo(request.app)
    store = _get_metadata_store(request.app)

    start_str = request.query.get("start")
    end_str = request.query.get("end")

    from datetime import UTC, date, datetime

    end = date.fromisoformat(end_str) if end_str else datetime.now(tz=UTC).date()
    start = date.fromisoformat(start_str) if start_str else date(2020, 1, 1)

    summary = await compute_analytics_summary(repo, store, start, end)
    return web.json_response(summary)


@routes.get("/api/v1/transactions/analytics/trends")
async def analytics_trends(request: web.Request) -> web.Response:
    """Monthly category trends."""
    from pfm.analytics.transaction_analytics import compute_monthly_trends

    repo = get_repo(request.app)
    store = _get_metadata_store(request.app)

    months = _parse_int_query(request, "months", 6)
    trends = await compute_monthly_trends(repo, store, months)
    return web.json_response(trends)


# ── Transaction detail (dynamic /{id} — AFTER static routes) ──────────


@routes.get("/api/v1/transactions/{id}")
async def get_transaction(request: web.Request) -> web.Response:
    """Get a single transaction with metadata, matched rule, raw fields, and available options."""
    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    store = _get_metadata_store(request.app)
    result = await store.get_transaction_by_id(tx_id)
    if result is None:
        return web.json_response({"error": "Transaction not found"}, status=404)
    tx, meta = result

    prices = await _build_price_map(request.app, [(tx, meta)])
    data = _serialize_tx(tx, meta, prices)
    data["rawFields"] = _parse_raw_fields(tx)

    # Find the matched category rule (if any).
    from pfm.analytics.categorizer import _match_category_rule

    etype = effective_type(tx, meta)
    rules = await store.get_category_rules()
    matched_rule = None
    for rule in rules:
        if _match_category_rule(etype, tx, rule):
            matched_rule = rule
            break
    data["matchedRule"] = _serialize_category_rule(matched_rule) if matched_rule else None

    # Available categories for this effective type.
    categories = await store.get_categories(tx_type=etype)
    data["availableCategories"] = [
        {"category": c.category, "display_name": c.display_name, "tx_type": c.tx_type} for c in categories
    ]

    # Matched type rule.
    from pfm.analytics.type_resolver import match_type_rule

    type_rules = await store.get_type_rules()
    matched_type_rule = None
    for tr in type_rules:
        if match_type_rule(tx, tr):
            matched_type_rule = tr
            break
    data["matchedTypeRule"] = _serialize_type_rule(matched_type_rule) if matched_type_rule else None

    # Available types for manual override.
    data["availableTypes"] = [t.value for t in TransactionType]

    return web.json_response(data)


_OUTFLOW_EFFECTIVE_TYPES = frozenset({"withdrawal", "transfer"})


def _score_transfer_candidates(  # noqa: PLR0913
    tx: Transaction,
    meta: TransactionMetadata | None,
    tx_id: int,
    tx_source: str,
    source_filter: str | None,
    nearby: list[Transaction],
    meta_map: dict[int, TransactionMetadata],
    prices: dict[str, Decimal],
) -> list[tuple[Decimal, Transaction]]:
    """Find transfer counterparts sorted by USD amount difference (ascending)."""
    is_outflow = effective_type(tx, meta) in _OUTFLOW_EFFECTIVE_TYPES
    tx_usd = _resolve_usd(tx, prices)
    result: list[tuple[Decimal, Transaction]] = []

    for candidate in nearby:
        if candidate.id is None or candidate.id == tx_id:
            continue
        c_meta = meta_map.get(candidate.id)
        if c_meta and c_meta.is_internal_transfer and c_meta.transfer_pair_id:
            continue
        c_source = candidate.source_name or candidate.source
        if c_source == tx_source:
            continue
        if source_filter and c_source != source_filter:
            continue
        c_is_outflow = effective_type(candidate, c_meta) in _OUTFLOW_EFFECTIVE_TYPES
        if c_is_outflow == is_outflow:
            continue
        usd_diff = abs(tx_usd - _resolve_usd(candidate, prices))
        result.append((usd_diff, candidate))

    return result


@routes.get("/api/v1/transactions/{id}/transfer-candidates")
async def transfer_candidates(request: web.Request) -> web.Response:
    """Find transactions that could be the other side of a transfer."""
    from datetime import timedelta

    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    store = _get_metadata_store(request.app)
    result = await store.get_transaction_by_id(tx_id)
    if result is None:
        return web.json_response({"error": "Transaction not found"}, status=404)
    tx, meta = result

    source_filter = request.query.get("source")
    tx_source = tx.source_name or tx.source

    # Fetch nearby transactions (±3 days).
    repo = get_repo(request.app)
    nearby = await repo.get_transactions(
        start=tx.date - timedelta(days=3),
        end=tx.date + timedelta(days=3),
    )
    nearby_ids = [t.id for t in nearby if t.id is not None]
    meta_map = await store.get_metadata_batch(nearby_ids)

    prices = await _build_price_map(request.app, [(tx, None)])
    scored = _score_transfer_candidates(tx, meta, tx_id, tx_source, source_filter, nearby, meta_map, prices)
    scored.sort(key=lambda p: p[0])  # ascending — closest amount first

    # Collect unique source names for the source picker.
    sources: list[str] = []
    seen_sources: set[str] = set()
    for _, c in scored:
        s = c.source_name or c.source
        if s not in seen_sources:
            seen_sources.add(s)
            sources.append(s)
    candidates_list = [
        {
            "id": c.id,
            "date": c.date.isoformat(),
            "source": c.source,
            "source_name": c.source_name or c.source,
            "asset": c.asset,
            "amount": _str_decimal(c.amount),
            "usd_value": _str_decimal(_resolve_usd(c, prices)),
            "usd_diff": _str_decimal(diff),
        }
        for diff, c in scored[:20]
    ]

    return web.json_response({"sources": sources, "candidates": candidates_list})


# ── Metadata update ────────────────────────────────────────────────────


@routes.put("/api/v1/transactions/{id}/metadata")
async def update_metadata(request: web.Request) -> web.Response:
    """Set category, notes, or confirm a transaction."""
    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    body = await request.json()
    store = _get_metadata_store(request.app)
    existing = await store.get_metadata(tx_id)

    def _default(key: str, fallback: object) -> object:
        return body.get(key, getattr(existing, key, fallback) if existing else fallback)

    cat_source_default = "manual" if "category" in body else (existing.category_source if existing else "auto")
    meta = await store.upsert_metadata(
        tx_id,
        category=_default("category", None),  # type: ignore[arg-type]
        category_source=body.get("category_source", cat_source_default),
        category_confidence=_default("category_confidence", None),  # type: ignore[arg-type]
        type_override=_default("type_override", None),  # type: ignore[arg-type]
        is_internal_transfer=_default("is_internal_transfer", False),  # type: ignore[arg-type]  # noqa: FBT003
        transfer_pair_id=_default("transfer_pair_id", None),  # type: ignore[arg-type]
        transfer_detected_by=_default("transfer_detected_by", None),  # type: ignore[arg-type]
        reviewed=_default("reviewed", False),  # type: ignore[arg-type]  # noqa: FBT003
        notes=_default("notes", ""),  # type: ignore[arg-type]
    )
    return web.json_response(
        {
            "transaction_id": meta.transaction_id,
            "category": meta.category,
            "category_source": meta.category_source,
            "category_confidence": meta.category_confidence,
            "type_override": meta.type_override,
            "is_internal_transfer": meta.is_internal_transfer,
            "transfer_pair_id": meta.transfer_pair_id,
            "reviewed": meta.reviewed,
            "notes": meta.notes,
        }
    )


# ── Quick category change ──────────────────────────────────────────────


@routes.put("/api/v1/transactions/{id}/category")
async def set_category(request: web.Request) -> web.Response:
    """Quick 2-click category change. Records choice for learning."""
    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    body = await request.json()
    category = body.get("category")
    if not category:
        return web.json_response({"error": "category is required"}, status=400)

    store = _get_metadata_store(request.app)
    result = await store.get_transaction_by_id(tx_id)
    if result is None:
        return web.json_response({"error": "Transaction not found"}, status=404)
    tx, existing_meta = result

    etype = effective_type(tx, existing_meta)
    previous = existing_meta.category if existing_meta else None

    # Record choice for learning.
    raw_fields = _parse_raw_fields(tx)
    await store.record_category_choice(
        transaction_id=tx_id,
        source=(tx.source_name or tx.source).lower(),
        effective_type=etype,
        chosen_category=category,
        field_snapshot=json.dumps(raw_fields) if raw_fields else "",
        previous_category=previous or "",
    )

    # Upsert metadata.
    meta = await store.upsert_metadata(
        tx_id,
        category=category,
        category_source="manual",
        category_confidence=1.0,
        type_override=existing_meta.type_override if existing_meta else None,
        is_internal_transfer=existing_meta.is_internal_transfer if existing_meta else False,
        transfer_pair_id=existing_meta.transfer_pair_id if existing_meta else None,
        transfer_detected_by=existing_meta.transfer_detected_by if existing_meta else None,
        reviewed=existing_meta.reviewed if existing_meta else False,
        notes=existing_meta.notes if existing_meta else "",
    )
    return web.json_response({"category": meta.category, "category_source": "manual", "category_confidence": 1.0})


# ── Quick type override ────────────────────────────────────────────────


@routes.put("/api/v1/transactions/{id}/type")
async def set_type_override(request: web.Request) -> web.Response:
    """Override a transaction's type. Re-evaluates category afterward."""
    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    body = await request.json()
    new_type = body.get("type")
    if not new_type:
        return web.json_response({"error": "type is required"}, status=400)
    if new_type not in _VALID_TYPES:
        return web.json_response(
            {"error": f"type must be one of: {sorted(_VALID_TYPES)}"},
            status=400,
        )

    store = _get_metadata_store(request.app)
    result = await store.get_transaction_by_id(tx_id)
    if result is None:
        return web.json_response({"error": "Transaction not found"}, status=404)
    tx, existing_meta = result

    # Set type_override and clear category so it gets re-evaluated.
    meta = await store.upsert_metadata(
        tx_id,
        category=None,
        category_source="auto",
        category_confidence=None,
        type_override=new_type,
        is_internal_transfer=existing_meta.is_internal_transfer if existing_meta else False,
        transfer_pair_id=existing_meta.transfer_pair_id if existing_meta else None,
        transfer_detected_by=existing_meta.transfer_detected_by if existing_meta else None,
        reviewed=existing_meta.reviewed if existing_meta else False,
        notes=existing_meta.notes if existing_meta else "",
    )

    # Re-categorize with the new type.
    from pfm.analytics.categorizer import categorize_transaction

    rules = await store.get_category_rules()
    cat_result = categorize_transaction(tx, rules, meta)
    if cat_result and cat_result.source == "rule":
        meta = await store.upsert_metadata(
            tx_id,
            category=cat_result.category,
            category_source=cat_result.source,
            category_confidence=cat_result.confidence,
            type_override=new_type,
            is_internal_transfer=meta.is_internal_transfer,
            transfer_pair_id=meta.transfer_pair_id,
            transfer_detected_by=meta.transfer_detected_by,
            reviewed=meta.reviewed,
            notes=meta.notes,
        )

    return web.json_response({"type_override": new_type, "effective_type": new_type, "category": meta.category})


# ── Transfer linking ───────────────────────────────────────────────────


@routes.post("/api/v1/transactions/link-transfer")
async def link_transfer(request: web.Request) -> web.Response:
    """Link two transactions as an internal transfer pair."""
    body = await request.json()
    tx_id_a = body.get("tx_id_a")
    tx_id_b = body.get("tx_id_b")
    if not tx_id_a or not tx_id_b:
        return web.json_response({"error": "tx_id_a and tx_id_b are required"}, status=400)

    store = _get_metadata_store(request.app)
    await store.link_transfer(int(tx_id_a), int(tx_id_b))
    return web.json_response({"linked": True})


@routes.delete("/api/v1/transactions/{id}/link-transfer")
async def unlink_transfer(request: web.Request) -> web.Response:
    """Unlink a transaction from its transfer pair."""
    tx_id = _parse_int_param(request)
    if isinstance(tx_id, web.Response):
        return tx_id

    store = _get_metadata_store(request.app)
    await store.unlink_transfer(tx_id)
    return web.json_response({"unlinked": True})


# ── Categories ─────────────────────────────────────────────────────────


@routes.post("/api/v1/transactions/categories")
async def create_category(request: web.Request) -> web.Response:
    """Create a custom transaction category."""
    body = await request.json()
    required = ("tx_type", "category", "display_name")
    for field in required:
        if field not in body:
            return web.json_response({"error": f"{field} is required"}, status=400)

    store = _get_metadata_store(request.app)
    cat = await store.create_category(
        tx_type=body["tx_type"],
        category=body["category"],
        display_name=body["display_name"],
        sort_order=body.get("sort_order", 0),
    )
    return web.json_response(
        {
            "id": cat.id,
            "tx_type": cat.tx_type,
            "category": cat.category,
            "display_name": cat.display_name,
            "sort_order": cat.sort_order,
        },
        status=201,
    )


# ── Category rules CRUD ───────────────────────────────────────────────


@routes.get("/api/v1/category-rules")
async def list_category_rules(request: web.Request) -> web.Response:
    """List all category rules."""
    store = _get_metadata_store(request.app)
    source = request.query.get("source")
    include_deleted = request.query.get("include_deleted", "false").lower() == "true"
    rules = await store.get_category_rules(source=source, include_deleted=include_deleted)
    return web.json_response([_serialize_category_rule(r) for r in rules])


@routes.post("/api/v1/category-rules")
async def create_category_rule(request: web.Request) -> web.Response:
    """Create a compound category rule."""
    body = await request.json()
    if "type_match" not in body or "result_category" not in body:
        return web.json_response({"error": "type_match and result_category are required"}, status=400)

    # Validate operator if a field condition is provided.
    field_op = body.get("field_operator", "")
    if field_op and field_op not in _VALID_OPERATORS:
        return web.json_response(
            {"error": f"field_operator must be one of: {sorted(_VALID_OPERATORS)}"},
            status=400,
        )

    # Serialize array values to JSON.
    field_value = body.get("field_value", "")
    if isinstance(field_value, list):
        field_value = json.dumps(field_value)

    store = _get_metadata_store(request.app)
    rule = await store.create_category_rule(
        type_match=body["type_match"],
        result_category=body["result_category"],
        type_operator=body.get("type_operator", "eq"),
        field_name=body.get("field_name", ""),
        field_operator=field_op,
        field_value=field_value,
        source=body.get("source", "*"),
        priority=body.get("priority", 400),
    )

    # Apply new rule to uncategorized transactions.
    repo = get_repo(request.app)
    await _run_categorization(repo, store, force=True)

    return web.json_response(_serialize_category_rule(rule), status=201)


async def _run_categorization(repo: object, store: MetadataStore, *, force: bool = False) -> None:
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.repository import Repository

    if isinstance(repo, Repository):
        try:
            await run_categorization(repo, store, force=force)
        except Exception:
            logger.exception("Post-rule categorization failed")


@routes.delete("/api/v1/category-rules/{id}")
async def delete_category_rule(request: web.Request) -> web.Response:
    """Delete a category rule (soft-delete for builtins)."""
    rule_id = _parse_int_param(request)
    if isinstance(rule_id, web.Response):
        return rule_id

    store = _get_metadata_store(request.app)
    deleted = await store.delete_category_rule(rule_id)
    if not deleted:
        return web.json_response({"error": "Rule not found"}, status=404)
    return web.json_response({"deleted": True})


@routes.post("/api/v1/category-rules/preview")
async def preview_category_rule(request: web.Request) -> web.Response:
    """Dry-run a rule against all transactions to show affected ones."""
    body = await request.json()
    if "type_match" not in body or "result_category" not in body:
        return web.json_response({"error": "type_match and result_category are required"}, status=400)

    from pfm.analytics.categorizer import _match_category_rule
    from pfm.db.models import CategoryRule

    field_value = body.get("field_value", "")
    if isinstance(field_value, list):
        field_value = json.dumps(field_value)

    preview_rule = CategoryRule(
        type_match=body["type_match"],
        result_category=body["result_category"],
        type_operator=body.get("type_operator", "eq"),
        field_name=body.get("field_name", ""),
        field_operator=body.get("field_operator", ""),
        field_value=field_value,
        source=body.get("source", "*"),
    )

    repo = get_repo(request.app)
    store = _get_metadata_store(request.app)
    all_txs = await repo.get_transactions()
    tx_ids = [tx.id for tx in all_txs if tx.id is not None]
    meta_map = await store.get_metadata_batch(tx_ids)

    affected: list[dict[str, object]] = []
    for tx in all_txs:
        if tx.id is None:
            continue
        meta = meta_map.get(tx.id)
        etype = effective_type(tx, meta)
        if _match_category_rule(etype, tx, preview_rule):
            affected.append(
                {
                    "id": tx.id,
                    "date": tx.date.isoformat(),
                    "source": tx.source_name or tx.source,
                    "description": _extract_description(tx),
                    "current_category": meta.category if meta else None,
                    "new_category": body["result_category"],
                }
            )

    return web.json_response(
        {
            "affected_count": len(affected),
            "sample": affected[:50],
        }
    )


@routes.get("/api/v1/category-rules/suggestions")
async def category_rule_suggestions(request: web.Request) -> web.Response:
    """Analyze user choices and suggest new rules."""
    store = _get_metadata_store(request.app)
    min_evidence = _parse_int_query(request, "min_evidence", 2)
    suggestions = await store.get_category_suggestions(min_evidence=min_evidence)
    return web.json_response(suggestions)


@routes.post("/api/v1/category-rules/reset")
async def reset_category_rules(request: web.Request) -> web.Response:
    """Reset rules: soft-delete custom, restore builtins."""
    body = await request.json()
    source = body.get("source")
    store = _get_metadata_store(request.app)
    await store.reset_category_rules(source=source)
    return web.json_response({"reset": True, "source": source})


# ── Categorization trigger ─────────────────────────────────────────────


@routes.post("/api/v1/transactions/categorize")
async def run_categorize(request: web.Request) -> web.Response:
    """Trigger auto-categorization run."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore

    repo = get_repo(request.app)
    store = MetadataStore(repo.connection)

    force = request.query.get("force", "false").lower() == "true"
    summary = await run_categorization(repo, store, force=force)

    return web.json_response(summary)


# ── Type rules CRUD ────────────────────────────────────────────────


@routes.get("/api/v1/type-rules")
async def list_type_rules(request: web.Request) -> web.Response:
    """List all type rules."""
    store = _get_metadata_store(request.app)
    source = request.query.get("source")
    include_deleted = request.query.get("include_deleted", "false").lower() == "true"
    rules = await store.get_type_rules(source=source, include_deleted=include_deleted)
    return web.json_response([_serialize_type_rule(r) for r in rules])


@routes.post("/api/v1/type-rules")
async def create_type_rule(request: web.Request) -> web.Response:
    """Create a type rule."""
    body = await request.json()
    result_type = body.get("result_type")
    if not result_type:
        return web.json_response({"error": "result_type is required"}, status=400)
    if result_type not in _VALID_TYPES:
        return web.json_response(
            {"error": f"result_type must be one of: {sorted(_VALID_TYPES)}"},
            status=400,
        )

    field_op = body.get("field_operator", "eq")
    if field_op and field_op not in _VALID_OPERATORS:
        return web.json_response(
            {"error": f"field_operator must be one of: {sorted(_VALID_OPERATORS)}"},
            status=400,
        )

    field_value = body.get("field_value", "")
    if isinstance(field_value, list):
        field_value = json.dumps(field_value)

    store = _get_metadata_store(request.app)
    rule = await store.create_type_rule(
        result_type=result_type,
        source=body.get("source", "*"),
        field_name=body.get("field_name", ""),
        field_operator=field_op,
        field_value=field_value,
        priority=body.get("priority", 400),
    )

    # Apply new rule — re-run full categorization (types affect all transactions).
    repo = get_repo(request.app)
    await _run_categorization(repo, store, force=True)

    return web.json_response(_serialize_type_rule(rule), status=201)


@routes.delete("/api/v1/type-rules/{id}")
async def delete_type_rule(request: web.Request) -> web.Response:
    """Delete a type rule (soft-delete for builtins)."""
    rule_id = _parse_int_param(request)
    if isinstance(rule_id, web.Response):
        return rule_id

    store = _get_metadata_store(request.app)
    deleted = await store.delete_type_rule(rule_id)
    if not deleted:
        return web.json_response({"error": "Rule not found"}, status=404)
    return web.json_response({"deleted": True})


@routes.post("/api/v1/type-rules/preview")
async def preview_type_rule(request: web.Request) -> web.Response:
    """Dry-run a type rule against transactions."""
    body = await request.json()
    result_type = body.get("result_type")
    if not result_type:
        return web.json_response({"error": "result_type is required"}, status=400)

    from pfm.analytics.type_resolver import match_type_rule
    from pfm.db.models import TypeRule as TypeRuleModel

    field_value = body.get("field_value", "")
    if isinstance(field_value, list):
        field_value = json.dumps(field_value)

    preview_rule = TypeRuleModel(
        source=body.get("source", "*"),
        field_name=body.get("field_name", ""),
        field_operator=body.get("field_operator", "eq"),
        field_value=field_value,
        result_type=result_type,
    )

    repo = get_repo(request.app)
    all_txs = await repo.get_transactions()

    affected: list[dict[str, object]] = []
    for tx in all_txs:
        if tx.id is None:
            continue
        if match_type_rule(tx, preview_rule):
            affected.append(
                {
                    "id": tx.id,
                    "date": tx.date.isoformat(),
                    "source": tx.source_name or tx.source,
                    "current_type": tx.tx_type.value,
                    "new_type": result_type,
                }
            )

    return web.json_response(
        {
            "affected_count": len(affected),
            "sample": affected[:50],
        }
    )


@routes.post("/api/v1/type-rules/reset")
async def reset_type_rules(request: web.Request) -> web.Response:
    """Reset type rules: soft-delete custom, restore builtins."""
    body = await request.json()
    source = body.get("source")
    store = _get_metadata_store(request.app)
    await store.reset_type_rules(source=source)
    return web.json_response({"reset": True, "source": source})
