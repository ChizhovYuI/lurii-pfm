"""Manual cash balance domain logic shared by REST routes and MCP tools."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.db.models import Snapshot, is_sync_marker_snapshot
from pfm.db.source_store import SourceStore

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

    from pfm.db.models import Source
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

CASH_SOURCE_TYPE = "cash"
FIAT_CURRENCIES_KEY = "fiat_currencies"
SUPPORTED_FIAT_CURRENCIES: tuple[str, ...] = (
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
SUPPORTED_FIAT_SET = frozenset(SUPPORTED_FIAT_CURRENCIES)


class CashError(Exception):
    """Base error for cash operations."""


class CashSourceNotFoundError(CashError):
    """Raised when no cash source is configured."""


class CashSourceAmbiguousError(CashError):
    """Raised when multiple cash sources exist."""

    def __init__(self, names: list[str]) -> None:
        super().__init__("Multiple cash sources are configured")
        self.names = names


class CashValidationError(CashError):
    """Raised when input data is invalid."""


@dataclass(frozen=True, slots=True)
class CashBalanceView:
    """Resolved cash balance snapshot for a source."""

    source_name: str
    selected_currencies: list[str]
    supported_currencies: list[str]
    latest_snapshot_date: date | None
    balances: dict[str, dict[str, str]]

    def to_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "selected_currencies": self.selected_currencies,
            "supported_currencies": self.supported_currencies,
            "latest_snapshot_date": (
                self.latest_snapshot_date.isoformat() if self.latest_snapshot_date is not None else None
            ),
            "balances": self.balances,
        }


async def resolve_cash_source(db_path: str | Path) -> Source:
    """Return the single cash source. Raise if missing or ambiguous."""
    store = SourceStore(db_path)
    cash_sources = [s for s in await store.list_all() if s.type == CASH_SOURCE_TYPE]
    if not cash_sources:
        msg = "Cash source not found"
        raise CashSourceNotFoundError(msg)
    if len(cash_sources) > 1:
        raise CashSourceAmbiguousError([s.name for s in cash_sources])
    return cash_sources[0]


async def load_resolved_cash_balances(
    repo: Repository,
    *,
    source_name: str,
    target_date: date,
) -> tuple[dict[str, dict[str, str]], date | None]:
    """Return current resolved cash balances + latest snapshot date for a cash source."""
    snapshots = await repo.get_snapshots_resolved(target_date)
    rows = [
        snap
        for snap in snapshots
        if snap.source == CASH_SOURCE_TYPE and snap.source_name == source_name and not is_sync_marker_snapshot(snap)
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


async def get_cash_balance_view(
    *,
    repo: Repository,
    db_path: str | Path,
    target_date: date,
) -> CashBalanceView:
    """Resolve cash source and return CashBalanceView for ``target_date``."""
    source = await resolve_cash_source(db_path)
    balances, latest = await load_resolved_cash_balances(
        repo,
        source_name=source.name,
        target_date=target_date,
    )
    return CashBalanceView(
        source_name=source.name,
        selected_currencies=selected_currencies_from_source(source),
        supported_currencies=list(SUPPORTED_FIAT_CURRENCIES),
        latest_snapshot_date=latest,
        balances=balances,
    )


def selected_currencies_from_source(source: Source) -> list[str]:
    """Read normalized selected currencies from a cash source's credentials."""
    try:
        credentials = json.loads(source.credentials)
    except json.JSONDecodeError:
        return ["USD"]
    raw = ""
    if isinstance(credentials, dict):
        raw = str(credentials.get(FIAT_CURRENCIES_KEY, ""))
    normalized = normalize_selected_currencies(raw.split(","))
    return normalized or ["USD"]


def normalize_selected_currencies(values: list[str]) -> list[str]:
    """Upper-case, dedupe, drop unsupported."""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        code = str(raw).strip().upper()
        if not code or code not in SUPPORTED_FIAT_SET or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def parse_selected_currencies(raw: object) -> list[str]:
    """Validate and normalize a selected_currencies payload."""
    if not isinstance(raw, list):
        msg = "selected_currencies must be a JSON array"
        raise CashValidationError(msg)
    candidates = [str(v).strip().upper() for v in raw if str(v).strip()]
    unsupported = sorted({c for c in candidates if c not in SUPPORTED_FIAT_SET})
    if unsupported:
        msg = "selected_currencies contains unsupported currencies: " + ", ".join(unsupported)
        raise CashValidationError(msg)
    selected = normalize_selected_currencies(candidates)
    if not selected:
        msg = "selected_currencies must include at least one supported currency"
        raise CashValidationError(msg)
    return selected


def parse_selected_amounts(raw: object, selected_currencies: list[str]) -> dict[str, Decimal]:
    """Validate amounts mapping. Missing entries default to 0."""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        msg = "balances must be a JSON object"
        raise CashValidationError(msg)

    unsupported_keys = sorted({str(k).upper() for k in raw if str(k).upper() not in SUPPORTED_FIAT_SET})
    if unsupported_keys:
        msg = "Unsupported balance currencies: " + ", ".join(unsupported_keys)
        raise CashValidationError(msg)

    amounts: dict[str, Decimal] = {}
    for currency in selected_currencies:
        amount_raw = raw.get(currency, raw.get(currency.lower(), "0"))
        amounts[currency] = _parse_non_negative_decimal(amount_raw, currency)
    return amounts


def _parse_non_negative_decimal(value: object, currency: str) -> Decimal:
    try:
        amount = Decimal(str(value).strip())
    except ArithmeticError as exc:
        msg = f"Invalid amount for {currency!r}"
        raise CashValidationError(msg) from exc
    if not amount.is_finite():
        msg = f"Amount for {currency!r} must be finite"
        raise CashValidationError(msg)
    if amount < 0:
        msg = f"Amount for {currency!r} must be non-negative"
        raise CashValidationError(msg)
    return amount


async def upsert_manual_cash(  # noqa: PLR0913
    *,
    repo: Repository,
    pricing: PricingService,
    db_path: str | Path,
    source_name: str,
    selected_currencies: list[str],
    amounts: dict[str, Decimal],
    today: date,
) -> list[Snapshot]:
    """Persist today's manual cash snapshots and update source credentials.

    Snapshots are saved for ``selected_currencies`` plus zero rows for any
    currency previously held by the source but no longer selected. Returns
    the saved snapshots; caller broadcasts events as needed.
    """
    store = SourceStore(db_path)
    await store.update(
        source_name,
        credentials={FIAT_CURRENCIES_KEY: ",".join(selected_currencies)},
    )

    existing_balances, _ = await load_resolved_cash_balances(
        repo,
        source_name=source_name,
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
                source=CASH_SOURCE_TYPE,
                source_name=source_name,
                asset=currency,
                amount=amount,
                usd_value=amount * price,
                price=price,
                raw_json=json.dumps({"manual": True, "currency": currency}),
            )
        )
    await repo.save_snapshots(snapshots)
    return snapshots


def snapshots_to_balance_dict(snapshots: list[Snapshot]) -> dict[str, dict[str, str]]:
    """Format saved snapshots in the same shape used by GET responses."""
    return {
        snap.asset: {
            "amount": _str_decimal(snap.amount),
            "usd_value": _str_decimal(snap.usd_value),
            "price": _str_decimal(snap.price),
        }
        for snap in snapshots
    }


def _str_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")
