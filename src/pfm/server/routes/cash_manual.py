"""Manual cash balance REST endpoints."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from aiohttp import web

from pfm.cash_manual import (
    SUPPORTED_FIAT_CURRENCIES,
    CashSourceAmbiguousError,
    CashSourceNotFoundError,
    CashValidationError,
    get_cash_balance_view,
    parse_selected_amounts,
    parse_selected_currencies,
    resolve_cash_source,
    snapshots_to_balance_dict,
    upsert_manual_cash,
)
from pfm.db.source_store import InvalidCredentialsError
from pfm.server.state import get_broadcaster, get_pricing, get_repo

routes = web.RouteTableDef()


@routes.get("/api/v1/cash/manual")
async def get_cash_manual(request: web.Request) -> web.Response:
    """Return cash source metadata and latest resolved balances."""
    repo = get_repo(request.app)
    today = datetime.now(tz=UTC).date()
    try:
        view = await get_cash_balance_view(
            repo=repo,
            db_path=request.app["db_path"],
            target_date=today,
        )
    except CashSourceNotFoundError as exc:
        return web.json_response({"error": str(exc)}, status=404)
    except CashSourceAmbiguousError as exc:
        return web.json_response({"error": str(exc), "matches": exc.names}, status=409)
    return web.json_response(view.to_dict())


@routes.put("/api/v1/cash/manual")
async def put_cash_manual(request: web.Request) -> web.Response:
    """Upsert today's manual cash balances for selected fiat currencies."""
    try:
        source = await resolve_cash_source(request.app["db_path"])
    except CashSourceNotFoundError as exc:
        return web.json_response({"error": str(exc)}, status=404)
    except CashSourceAmbiguousError as exc:
        return web.json_response({"error": str(exc), "matches": exc.names}, status=409)

    body, error = await _read_json_body(request)
    if error is not None:
        return error
    body = cast("dict[str, Any]", body)

    try:
        selected_currencies = parse_selected_currencies(body.get("selected_currencies"))
        amounts = parse_selected_amounts(body.get("balances", {}), selected_currencies)
    except CashValidationError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    repo = get_repo(request.app)
    pricing = get_pricing(request.app)
    today = datetime.now(tz=UTC).date()

    try:
        snapshots = await upsert_manual_cash(
            repo=repo,
            pricing=pricing,
            db_path=request.app["db_path"],
            source_name=source.name,
            selected_currencies=selected_currencies,
            amounts=amounts,
            today=today,
        )
    except InvalidCredentialsError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    await get_broadcaster(request.app).broadcast({"type": "snapshot_updated"})

    return web.json_response(
        {
            "updated": True,
            "date": today.isoformat(),
            "source_name": source.name,
            "selected_currencies": selected_currencies,
            "supported_currencies": list(SUPPORTED_FIAT_CURRENCIES),
            "latest_snapshot_date": today.isoformat(),
            "balances": snapshots_to_balance_dict(snapshots),
        }
    )


async def _read_json_body(request: web.Request) -> tuple[dict[str, Any] | None, web.Response | None]:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return None, web.json_response({"error": "Invalid JSON body"}, status=400)
    if not isinstance(body, dict):
        return None, web.json_response({"error": "JSON body must be an object"}, status=400)
    return body, None
