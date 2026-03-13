"""Extension snapshot ingest endpoint."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from aiohttp import web

from pfm.db.models import Snapshot, Source, make_sync_marker_snapshot
from pfm.db.source_store import SourceStore
from pfm.server.state import get_broadcaster, get_pricing, get_repo

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.post("/api/v1/ext/snapshot")
async def ingest_extension_snapshot(request: web.Request) -> web.Response:
    """Ingest snapshots posted by browser extensions.

    The source instance is resolved by:
    - query/body ``source_type`` (maps to Source.type)
    - query/body ``uid`` (maps to Source.credentials.uid)
    """
    try:
        body: dict[str, Any] = await request.json()
    except (json.JSONDecodeError, web.HTTPBadRequest):
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    source_type = _resolve_source_type(request, body)
    uid = _resolve_uid(request, body)
    if not source_type:
        return web.json_response({"error": "source_type is required"}, status=400)
    if not uid:
        return web.json_response({"error": "uid is required"}, status=400)

    store = SourceStore(request.app["db_path"])
    enabled_sources = await store.list_enabled()
    matched_sources = _match_sources(enabled_sources, source_type=source_type, uid=uid)

    if not matched_sources:
        return web.json_response(
            {"error": f"No enabled source found for type={source_type!r} uid={uid!r}"},
            status=404,
        )
    if len(matched_sources) > 1:
        return web.json_response(
            {
                "error": f"Multiple enabled sources match type={source_type!r} uid={uid!r}",
                "matches": [source.name for source in matched_sources],
            },
            status=409,
        )

    matched_source = matched_sources[0]
    assets = _resolve_assets(body)
    captured_date = _resolve_captured_date(body.get("capturedAt"))
    pricing: PricingService = get_pricing(request.app)
    snapshots = await _build_snapshots(
        assets,
        snapshot_date=captured_date,
        source_type=source_type,
        source_name=matched_source.name,
        pricing=pricing,
    )

    repo = get_repo(request.app)
    await repo.save_snapshots(snapshots)
    await get_broadcaster(request.app).broadcast({"type": "snapshot_updated"})

    return web.json_response(
        {
            "saved": len(snapshots),
            "source_type": source_type,
            "source_name": matched_source.name,
            "uid": uid,
            "date": captured_date.isoformat(),
        }
    )


def _resolve_source_type(request: web.Request, body: dict[str, Any]) -> str:
    source_type = str(request.query.get("source_type", "")).strip().lower()
    if source_type:
        return source_type

    source = body.get("source")
    if isinstance(source, dict):
        return str(source.get("type", "")).strip().lower()
    return ""


def _resolve_uid(request: web.Request, body: dict[str, Any]) -> str:
    uid = str(request.query.get("uid", "")).strip()
    if uid:
        return uid

    source = body.get("source")
    if isinstance(source, dict):
        return str(source.get("uid", "")).strip()
    return ""


def _match_sources(sources: list[Source], *, source_type: str, uid: str) -> list[Source]:
    matches: list[Source] = []
    for source in sources:
        if source.type != source_type:
            continue
        try:
            credentials = json.loads(source.credentials)
        except json.JSONDecodeError:
            continue
        if not isinstance(credentials, dict):
            continue
        source_uid = str(credentials.get("uid", credentials.get("email", ""))).strip()
        if source_uid == uid:
            matches.append(source)
    return matches


def _resolve_assets(body: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot = body.get("snapshot")
    if not isinstance(snapshot, dict):
        return []
    assets = snapshot.get("assets")
    if not isinstance(assets, list):
        return []
    return [asset for asset in assets if isinstance(asset, dict)]


def _resolve_captured_date(value: object) -> date:
    if isinstance(value, str) and value.strip():
        parsed = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(parsed).astimezone(UTC).date()
        except ValueError:
            pass
    return datetime.now(tz=UTC).date()


async def _build_snapshots(
    assets: list[dict[str, Any]],
    *,
    snapshot_date: date,
    source_type: str,
    source_name: str,
    pricing: PricingService,
) -> list[Snapshot]:
    snapshots: list[Snapshot] = []

    for asset in assets:
        symbol = str(asset.get("symbol", asset.get("asset", ""))).upper().strip()
        if not symbol:
            continue

        amount = _to_decimal(asset.get("amount", "0"))
        usd_value = _to_decimal(asset.get("usdValue", asset.get("usd_value", "0")))

        # Skip empty rows; mirrors collector behavior that ignores zero balances.
        if amount == 0 and usd_value == 0:
            continue

        # When the extension doesn't provide USD values, look up the price.
        if usd_value == 0 and amount > 0:
            try:
                price = await pricing.get_price_usd(symbol)
                usd_value = amount * price
            except Exception:  # noqa: BLE001
                logger.warning("ext_snapshot: cannot price %s, saving with usd_value=0", symbol)
                price = Decimal(0)
        else:
            price = usd_value / amount if amount != 0 else Decimal(0)

        quoted_apr = _to_decimal(
            asset.get(
                "effectiveAprPercent",
                asset.get(
                    "effective_apr_percent",
                    asset.get("quotedAprPercent", asset.get("quoted_apr_percent", "0")),
                ),
            )
        )
        apy = quoted_apr / Decimal(100) if quoted_apr > 1 else quoted_apr

        snapshots.append(
            Snapshot(
                date=snapshot_date,
                source=source_type,
                source_name=source_name,
                asset=symbol,
                amount=amount,
                usd_value=usd_value,
                price=price,
                apy=apy,
                raw_json=json.dumps(asset),
            )
        )

    if not snapshots:
        snapshots.append(
            make_sync_marker_snapshot(
                snapshot_date=snapshot_date,
                source=source_type,
                source_name=source_name,
            )
        )

    return snapshots


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)
