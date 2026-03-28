"""Manual cash balance REST endpoints."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from aiohttp import web

from pfm.db.models import Snapshot, Source, is_sync_marker_snapshot
from pfm.db.source_store import InvalidCredentialsError, SourceStore
from pfm.server.serializers import _str_decimal
from pfm.server.state import get_broadcaster, get_pricing, get_repo

if TYPE_CHECKING:
    from pfm.db.repository import Repository

routes = web.RouteTableDef()

_CASH_SOURCE_TYPE = "cash"
_FIAT_CURRENCIES_KEY = "fiat_currencies"
_SUPPORTED_FIAT_CURRENCIES: tuple[str, ...] = (
    "USD",
    "EUR",
    "GBP",
    "THB",
    "JPY",
    "CHF",
    "CAD",
    "AUD",
    "NZD",
    "SGD",
    "HKD",
    "AMD",
)
_SUPPORTED_FIAT_SET = set(_SUPPORTED_FIAT_CURRENCIES)


@routes.get("/api/v1/cash/manual")
async def get_cash_manual(request: web.Request) -> web.Response:
    """Return cash source metadata and latest resolved balances."""
    source, error = await _resolve_cash_source(request.app["db_path"])
    if error is not None:
        return error
    source = cast("Source", source)

    repo = get_repo(request.app)
    today = datetime.now(tz=UTC).date()
    selected_currencies = _selected_currencies_from_source(source)
    balances, latest_snapshot_date = await _load_resolved_cash_balances(
        repo,
        source_name=source.name,
        target_date=today,
    )
    return web.json_response(
        {
            "source_name": source.name,
            "selected_currencies": selected_currencies,
            "supported_currencies": list(_SUPPORTED_FIAT_CURRENCIES),
            "latest_snapshot_date": latest_snapshot_date.isoformat() if latest_snapshot_date is not None else None,
            "balances": balances,
        }
    )


@routes.put("/api/v1/cash/manual")
async def put_cash_manual(request: web.Request) -> web.Response:
    """Upsert today's manual cash balances for selected fiat currencies."""
    source, error = await _resolve_cash_source(request.app["db_path"])
    if error is not None:
        return error
    source = cast("Source", source)

    body, error = await _read_json_body(request)
    if error is not None:
        return error
    body = cast("dict[str, Any]", body)

    selected_currencies, error = _parse_selected_currencies(body)
    if error is not None:
        return error
    selected_currencies = cast("list[str]", selected_currencies)

    amounts, error = _parse_selected_amounts(body, selected_currencies)
    if error is not None:
        return error
    amounts = cast("dict[str, Decimal]", amounts)

    store = SourceStore(request.app["db_path"])
    try:
        await store.update(
            source.name,
            credentials={_FIAT_CURRENCIES_KEY: ",".join(selected_currencies)},
        )
    except InvalidCredentialsError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    repo = get_repo(request.app)
    pricing = get_pricing(request.app)
    today = datetime.now(tz=UTC).date()
    existing_balances, _ = await _load_resolved_cash_balances(
        repo,
        source_name=source.name,
        target_date=today,
    )
    currencies_to_clear = sorted(code for code in existing_balances if code not in selected_currencies)

    snapshots: list[Snapshot] = []
    for currency in [*selected_currencies, *currencies_to_clear]:
        amount = amounts.get(currency, Decimal(0))
        price = await pricing.get_price_usd(currency)
        snapshots.append(
            Snapshot(
                date=today,
                source=_CASH_SOURCE_TYPE,
                source_name=source.name,
                asset=currency,
                amount=amount,
                usd_value=amount * price,
                price=price,
                raw_json=json.dumps({"manual": True, "currency": currency}),
            )
        )

    await repo.save_snapshots(snapshots)
    await get_broadcaster(request.app).broadcast({"type": "snapshot_updated"})

    balances = {
        snap.asset: {
            "amount": _str_decimal(snap.amount),
            "usd_value": _str_decimal(snap.usd_value),
            "price": _str_decimal(snap.price),
        }
        for snap in snapshots
    }
    return web.json_response(
        {
            "updated": True,
            "date": today.isoformat(),
            "source_name": source.name,
            "selected_currencies": selected_currencies,
            "supported_currencies": list(_SUPPORTED_FIAT_CURRENCIES),
            "latest_snapshot_date": today.isoformat(),
            "balances": balances,
        }
    )


async def _resolve_cash_source(db_path: str) -> tuple[Source | None, web.Response | None]:
    store = SourceStore(db_path)
    cash_sources = [source for source in await store.list_all() if source.type == _CASH_SOURCE_TYPE]
    if not cash_sources:
        return None, web.json_response({"error": "Cash source not found"}, status=404)
    if len(cash_sources) > 1:
        return None, web.json_response(
            {"error": "Multiple cash sources are configured", "matches": [s.name for s in cash_sources]},
            status=409,
        )
    return cash_sources[0], None


def _bad_request(message: str) -> web.Response:
    return web.json_response({"error": message}, status=400)


async def _read_json_body(request: web.Request) -> tuple[dict[str, Any] | None, web.Response | None]:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return None, _bad_request("Invalid JSON body")
    if not isinstance(body, dict):
        return None, _bad_request("JSON body must be an object")
    return body, None


def _parse_selected_currencies(body: dict[str, Any]) -> tuple[list[str] | None, web.Response | None]:
    selected_raw = body.get("selected_currencies")
    if not isinstance(selected_raw, list):
        return None, _bad_request("selected_currencies must be a JSON array")

    selected_codes_raw = [str(raw).strip().upper() for raw in selected_raw if str(raw).strip()]
    unsupported_selected = sorted({code for code in selected_codes_raw if code not in _SUPPORTED_FIAT_SET})
    if unsupported_selected:
        return None, _bad_request(
            "selected_currencies contains unsupported currencies: " + ", ".join(unsupported_selected)
        )

    selected_currencies = _normalize_selected_currencies(selected_codes_raw)
    if not selected_currencies:
        return None, _bad_request("selected_currencies must include at least one supported currency")
    return selected_currencies, None


def _parse_selected_amounts(
    body: dict[str, Any],
    selected_currencies: list[str],
) -> tuple[dict[str, Decimal] | None, web.Response | None]:
    balances_raw = body.get("balances", {})
    if not isinstance(balances_raw, dict):
        return None, _bad_request("balances must be a JSON object")

    unsupported_balance_keys = sorted(
        {str(key).upper() for key in balances_raw if str(key).upper() not in _SUPPORTED_FIAT_SET}
    )
    if unsupported_balance_keys:
        return None, _bad_request("Unsupported balance currencies: " + ", ".join(unsupported_balance_keys))

    amounts: dict[str, Decimal] = {}
    for currency in selected_currencies:
        amount_raw = balances_raw.get(currency, balances_raw.get(currency.lower(), "0"))
        amount, parse_error = _parse_non_negative_decimal(amount_raw, currency)
        if parse_error is not None:
            return None, _bad_request(parse_error)
        amounts[currency] = amount
    return amounts, None


def _selected_currencies_from_source(source: Source) -> list[str]:
    try:
        credentials = json.loads(source.credentials)
    except json.JSONDecodeError:
        return ["USD"]
    raw = ""
    if isinstance(credentials, dict):
        raw = str(credentials.get(_FIAT_CURRENCIES_KEY, ""))
    normalized = _normalize_selected_currencies(raw.split(","))
    return normalized or ["USD"]


def _normalize_selected_currencies(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        code = str(raw).strip().upper()
        if not code or code not in _SUPPORTED_FIAT_SET or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _parse_non_negative_decimal(value: object, currency: str) -> tuple[Decimal, str | None]:
    try:
        amount = Decimal(str(value).strip())
    except ArithmeticError:
        return Decimal(0), f"Invalid amount for {currency!r}"
    if not amount.is_finite():
        return Decimal(0), f"Amount for {currency!r} must be finite"
    if amount < 0:
        return Decimal(0), f"Amount for {currency!r} must be non-negative"
    return amount, None


async def _load_resolved_cash_balances(
    repo: Repository,
    *,
    source_name: str,
    target_date: date,
) -> tuple[dict[str, dict[str, str]], date | None]:
    snapshots = await repo.get_snapshots_resolved(target_date)
    rows = [
        snap
        for snap in snapshots
        if snap.source == _CASH_SOURCE_TYPE and snap.source_name == source_name and not is_sync_marker_snapshot(snap)
    ]
    if not rows:
        return {}, None
    latest_date = max(snap.date for snap in rows)
    balances = {
        snap.asset: {
            "amount": _str_decimal(snap.amount),
            "usd_value": _str_decimal(snap.usd_value),
            "price": _str_decimal(snap.price),
        }
        for snap in rows
    }
    return balances, latest_date
