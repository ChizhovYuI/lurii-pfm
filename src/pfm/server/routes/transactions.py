"""Transaction REST endpoints with category metadata."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.db.models import TransactionType, effective_type
from pfm.server.serializers import _str_decimal
from pfm.server.state import get_repo

if TYPE_CHECKING:
    from pfm.analytics.transaction_grouper import TransactionGroup
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.models import CategoryRule, Transaction, TransactionMetadata

routes = web.RouteTableDef()

_VALID_TYPES = frozenset(t.value for t in TransactionType)
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


def _extract_time(tx: Transaction) -> str | None:
    """Extract time (HH:MM) from raw_json if present."""
    if not tx.raw_json:
        return None
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            t = parsed.get("time") or parsed.get("ts") or parsed.get("transactionTime")
            if t and isinstance(t, str) and len(t) >= 5 and t[2] == ":":  # noqa: PLR2004
                return str(t[:5])
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _serialize_tx(tx: Transaction, meta: TransactionMetadata | None) -> dict[str, object]:
    etype = effective_type(tx, meta)
    result: dict[str, object] = {
        "id": tx.id,
        "date": tx.date.isoformat(),
        "time": _extract_time(tx),
        "source": tx.source,
        "source_name": tx.source_name or tx.source,
        "tx_type": tx.tx_type.value,
        "effective_type": etype,
        "asset": tx.asset,
        "amount": _str_decimal(tx.amount),
        "usd_value": _str_decimal(tx.usd_value),
        "counterparty_asset": tx.counterparty_asset,
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


def _serialize_grouped_tx(group: TransactionGroup, group_index: int) -> dict[str, object]:
    """Serialize a transaction group as a single row with negative synthetic ID."""
    return {
        "id": -(group_index + 1),
        "date": group.display_date.isoformat(),
        "source": group.from_source,
        "source_name": group.from_source,
        "tx_type": group.display_tx_type,
        "effective_type": group.display_tx_type,
        "asset": group.from_asset,
        "amount": _str_decimal(group.from_amount),
        "usd_value": _str_decimal(group.display_usd_value),
        "counterparty_asset": None,
        "counterparty_amount": None,
        "tx_id": None,
        "trade_side": None,
        "description": "",
        "metadata": None,
        "group": {
            "type": group.group_type,
            "child_ids": group.child_ids,
            "child_count": len(group.child_ids),
            "from_source": group.from_source,
            "to_source": group.to_source,
            "from_asset": group.from_asset,
            "to_asset": group.to_asset,
            "from_amount": _str_decimal(group.from_amount),
            "to_amount": _str_decimal(group.to_amount),
        },
    }


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


@routes.get("/api/v1/transactions")
async def list_transactions(request: web.Request) -> web.Response:
    """Paginated transaction list with optional filters."""
    store = _get_metadata_store(request.app)

    source_name = request.query.get("source_name")
    tx_type = request.query.get("tx_type")
    category = request.query.get("category")
    start_str = request.query.get("start")
    end_str = request.query.get("end")
    search = request.query.get("search")
    grouped = request.query.get("grouped", "true").lower() != "false"
    limit = min(_parse_int_query(request, "limit", 50), 200)
    offset = _parse_int_query(request, "offset", 0)

    from datetime import date

    start = date.fromisoformat(start_str) if start_str else None
    end = date.fromisoformat(end_str) if end_str else None

    items, total = await store.get_transactions_paginated(
        source_name=source_name,
        tx_type=tx_type,
        category=category,
        start=start,
        end=end,
        search=search,
        limit=limit,
        offset=offset,
    )

    if not grouped:
        return web.json_response(
            {
                "items": [_serialize_tx(tx, meta) for tx, meta in items],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        )

    # Fetch transfer counterparts that may be outside the current page.
    counterpart_ids: list[int] = []
    page_ids = {tx.id for tx, _ in items if tx.id is not None}
    for _, meta in items:
        if meta and meta.is_internal_transfer and meta.transfer_pair_id and meta.transfer_pair_id not in page_ids:
            counterpart_ids.append(meta.transfer_pair_id)

    if counterpart_ids:
        for cid in counterpart_ids:
            result = await store.get_transaction_by_id(cid)
            if result:
                items.append(result)

    from pfm.analytics.transaction_grouper import group_transactions

    grouping = group_transactions(items)

    serialized: list[dict[str, object]] = []
    for i, group in enumerate(grouping.groups):
        row = _serialize_grouped_tx(group, i)
        serialized.append(row)

    for tx, meta in grouping.ungrouped:
        row = _serialize_tx(tx, meta)
        row["group"] = None
        serialized.append(row)

    # Sort by date descending (grouped rows use display_date).
    serialized.sort(key=lambda r: (str(r.get("date", "")), r.get("id", 0)), reverse=True)

    return web.json_response(
        {
            "items": serialized,
            "total": total,
            "total_ungrouped": grouping.total_ungrouped,
            "limit": limit,
            "offset": offset,
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

    from datetime import UTC, date, datetime, timedelta

    end = date.fromisoformat(end_str) if end_str else datetime.now(tz=UTC).date()
    start = date.fromisoformat(start_str) if start_str else end - timedelta(days=30)

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

    data = _serialize_tx(tx, meta)
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

    # Available types for manual override.
    data["availableTypes"] = [t.value for t in TransactionType]

    return web.json_response(data)


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
        priority=body.get("priority"),
    )
    return web.json_response(_serialize_category_rule(rule), status=201)


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
    import contextlib

    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore

    repo = get_repo(request.app)
    store = MetadataStore(repo.connection)

    ai_provider = await _try_get_ai_provider(request.app)

    force = request.query.get("force", "false").lower() == "true"
    summary = await run_categorization(repo, store, ai_provider=ai_provider, force=force)  # type: ignore[arg-type]

    if ai_provider:
        with contextlib.suppress(OSError):
            await ai_provider.close()  # type: ignore[attr-defined]

    return web.json_response(summary)


async def _try_get_ai_provider(app: web.Application) -> object:
    """Attempt to load the active AI provider. Returns None on failure."""
    import logging

    try:
        from pfm.db.ai_store import AIProviderStore

        ai_store = AIProviderStore(app["db_path"])
        active = await ai_store.get_active()
        if not active:
            return None

        from pfm.ai.providers.registry import PROVIDER_REGISTRY

        provider_cls = PROVIDER_REGISTRY.get(active.type)  # type: ignore[call-overload]
        if provider_cls:
            return provider_cls(
                api_key=active.api_key,
                model=active.model,
                base_url=active.base_url,
            )
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).debug("AI provider unavailable for categorization", exc_info=True)
    return None
