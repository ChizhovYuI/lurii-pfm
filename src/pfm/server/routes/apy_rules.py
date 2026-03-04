"""APY rules REST endpoints."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import web

from pfm.db.apy_rules_store import (
    ApyRule,
    ApyRuleNotFoundError,
    ApyRulesStore,
    ApyRuleValidationError,
    compute_effective_apy,
    rule_to_dict,
)
from pfm.db.source_store import SourceNotFoundError, SourceStore

if TYPE_CHECKING:
    from datetime import date
    from decimal import Decimal

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


async def _validate_source(request: web.Request) -> str:
    """Validate source exists and supports APY rules. Returns source name."""
    from pfm.source_types import APY_RULES_TYPES

    name: str = request.match_info["name"]
    store = SourceStore(request.app["db_path"])
    try:
        source = await store.get(name)
    except SourceNotFoundError as exc:
        raise web.HTTPNotFound(
            text=json.dumps({"error": f"Source {name!r} not found"}),
            content_type="application/json",
        ) from exc
    if source.type not in APY_RULES_TYPES:
        raise web.HTTPBadRequest(
            text=json.dumps({"error": f"APY rules are not supported for {source.type!r} sources"}),
            content_type="application/json",
        )
    return name


@routes.get("/api/v1/sources/{name}/apy-rules")
async def list_rules(request: web.Request) -> web.Response:
    """List all APY rules for a source."""
    name = await _validate_source(request)
    store = ApyRulesStore(request.app["db_path"])
    rules = await store.load_rules(name)
    return web.json_response([rule_to_dict(r) for r in rules])


@routes.post("/api/v1/sources/{name}/apy-rules")
async def add_rule(request: web.Request) -> web.Response:
    """Add a new APY rule and recalculate affected snapshots."""
    name = await _validate_source(request)
    body: dict[str, Any] = await request.json()
    store = ApyRulesStore(request.app["db_path"])
    try:
        rules = await store.add_rule(name, body)
    except ApyRuleValidationError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    await _recalculate_snapshots(request.app, name, rules)
    return web.json_response([rule_to_dict(r) for r in rules], status=201)


@routes.put("/api/v1/sources/{name}/apy-rules/{rule_id}")
async def update_rule(request: web.Request) -> web.Response:
    """Update an APY rule and recalculate affected snapshots."""
    name = await _validate_source(request)
    rule_id = request.match_info["rule_id"]
    body: dict[str, Any] = await request.json()
    store = ApyRulesStore(request.app["db_path"])

    old_rules = await store.load_rules(name)

    try:
        rules = await store.update_rule(name, rule_id, body)
    except ApyRuleNotFoundError:
        return web.json_response({"error": f"Rule {rule_id!r} not found"}, status=404)
    except ApyRuleValidationError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    await _recalculate_snapshots(request.app, name, rules, old_rules=old_rules)
    return web.json_response([rule_to_dict(r) for r in rules])


@routes.delete("/api/v1/sources/{name}/apy-rules/{rule_id}")
async def delete_rule(request: web.Request) -> web.Response:
    """Delete an APY rule and recalculate affected snapshots."""
    name = await _validate_source(request)
    rule_id = request.match_info["rule_id"]
    store = ApyRulesStore(request.app["db_path"])

    old_rules = await store.load_rules(name)

    try:
        rules = await store.delete_rule(name, rule_id)
    except ApyRuleNotFoundError:
        return web.json_response({"error": f"Rule {rule_id!r} not found"}, status=404)

    await _recalculate_snapshots(request.app, name, rules, old_rules=old_rules)
    return web.json_response([rule_to_dict(r) for r in rules])


def _affected_date_range(
    rules: list[ApyRule],
    old_rules: list[ApyRule] | None = None,
) -> tuple[date, date] | None:
    """Compute the union date range across all rules (current + old)."""
    all_rules = list(rules)
    if old_rules:
        all_rules.extend(old_rules)
    if not all_rules:
        return None
    start = min(r.started_at for r in all_rules)
    end = max(r.finished_at for r in all_rules)
    return (start, end)


async def _recalculate_snapshots(
    app: web.Application,
    source_name: str,
    rules: list[ApyRule],
    old_rules: list[ApyRule] | None = None,
) -> None:
    """Recalculate APY for affected snapshots after a rule change."""
    date_range = _affected_date_range(rules, old_rules)
    if date_range is None:
        return

    start, end = date_range
    repo = app["repo"]
    snapshots = await repo.get_snapshots_by_source_name_and_date_range(source_name, start, end)

    updated = 0
    for snap in snapshots:
        raw = _parse_raw_json(snap.raw_json)
        protocol_apy = _extract_protocol_apy(raw)
        if protocol_apy is None:
            continue

        new_apy = compute_effective_apy(
            protocol_apy,
            rules,
            _extract_protocol(raw),
            snap.asset.lower(),
            snap.amount,
            snap.date,
        )
        if new_apy != snap.apy and snap.id is not None:
            await repo.update_snapshot_apy(snap.id, new_apy)
            updated += 1

    if updated:
        logger.info("Recalculated APY for %d snapshots of %s", updated, source_name)


def _parse_raw_json(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _extract_protocol_apy(raw: dict[str, Any]) -> Decimal | None:
    """Extract protocol APY from raw_json -> apy -> value."""
    from decimal import Decimal

    apy_obj = raw.get("apy")
    if not isinstance(apy_obj, dict):
        return None
    value = apy_obj.get("value")
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _extract_protocol(raw: dict[str, Any]) -> str:
    """Infer protocol from raw_json (currently always 'aave' for bitget_wallet)."""
    market = raw.get("market")
    if isinstance(market, dict) and "aave" in str(market.get("name", "")).lower():
        return "aave"
    return "aave"
