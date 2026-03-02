"""yo.xyz vault collector."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_YO_BASE_URL = "https://api.yo.xyz"
_RATE_LIMITER = RateLimiter(requests_per_minute=240.0)


@register_collector
class YoCollector(BaseCollector):
    """Collector for a single yo.xyz vault position."""

    source_name = "yo"

    def __init__(
        self,
        pricing: PricingService,
        *,
        network: str,
        vault_address: str,
        user_address: str,
    ) -> None:
        super().__init__(pricing)
        self._network = network.strip()
        self._vault_address = vault_address.strip()
        self._user_address = user_address.strip()
        self._client = httpx.AsyncClient(base_url=_YO_BASE_URL, timeout=30.0)

    @retry()
    async def _get(self, path: str, *, params: dict[str, str] | None = None) -> dict[str, Any]:
        await _RATE_LIMITER.acquire()
        resp = await self._client.get(path, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict):
            return payload
        return {}

    async def _get_vault(self) -> dict[str, Any]:
        payload = await self._get(
            f"/api/v1/vault/{self._network}/{self._vault_address}",
            params={"userAddress": self._user_address},
        )
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return {}

    async def _get_history(self, *, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self._get(
            f"/api/v1/history/user/{self._network}/{self._vault_address}/{self._user_address}",
            params={"limit": str(limit)},
        )
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch current vault position balances for a user."""
        today = self._pricing.today()
        vault = await self._get_vault()
        holdings = _extract_holdings(vault)

        if not holdings:
            logger.info("yo: no balances found for network=%s vault=%s", self._network, self._vault_address)
            return []

        snapshots: list[Snapshot] = []
        for holding in holdings:
            symbol = holding.symbol.upper()
            amount = holding.amount
            if not symbol or amount <= 0:
                continue

            price = holding.price_usd
            if price <= 0:
                try:
                    price = await self._pricing.get_price_usd(symbol)
                except ValueError:
                    logger.warning("yo: cannot price %s, skipping", symbol)
                    continue

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=symbol,
                    amount=amount,
                    usd_value=amount * price,
                    price=price,
                    raw_json=json.dumps(holding.raw),
                )
            )

        logger.info("yo: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch user vault history and map it to normalized transaction rows."""
        rows = await self._get_history(limit=100)
        txs: list[Transaction] = []
        for row in rows:
            tx = _parse_history_row(row)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            txs.append(tx)

        logger.info("yo: parsed %d transactions", len(txs))
        return txs


class _Holding:
    def __init__(self, symbol: str, amount: Decimal, price_usd: Decimal, raw: dict[str, Any]) -> None:
        self.symbol = symbol
        self.amount = amount
        self.price_usd = price_usd
        self.raw = raw


def _extract_holdings(vault: dict[str, Any]) -> list[_Holding]:
    asset = _as_dict(vault.get("asset"))
    share_asset = _as_dict(vault.get("shareAsset"))
    stats = _as_dict(vault.get("stats"))

    base_symbol = str(asset.get("symbol", "")).upper()
    share_symbol = str(share_asset.get("symbol", "")).upper()

    rows: list[_Holding] = []
    seen: set[str] = set()

    # Preferred direct user balances if present.
    input_balance = _read_amount(_as_dict(vault.get("inputTokenBalance")))
    if base_symbol and input_balance > 0:
        rows.append(
            _Holding(
                base_symbol,
                input_balance,
                Decimal(0),
                {"inputTokenBalance": vault.get("inputTokenBalance")},
            )
        )
        seen.add(base_symbol)

    output_rows = vault.get("outputTokenBalances")
    if isinstance(output_rows, list):
        for entry in output_rows:
            if not isinstance(entry, dict):
                continue
            token = _as_dict(entry.get("token"))
            symbol = str(entry.get("symbol") or token.get("symbol") or "").upper()
            amount = _read_amount(entry)
            if not symbol or amount <= 0:
                continue
            if symbol in seen:
                continue
            price = _to_decimal(entry.get("priceUsd") or entry.get("price"))
            rows.append(_Holding(symbol, amount, price, entry))
            seen.add(symbol)

    share_balance = _read_amount(_as_dict(vault.get("shareBalance")))
    if share_symbol and share_balance > 0 and share_symbol not in seen:
        rows.append(
            _Holding(
                share_symbol,
                share_balance,
                Decimal(0),  # resolved later with share_price/base asset fallback
                {"shareBalance": vault.get("shareBalance"), "sharePrice": stats.get("sharePrice")},
            )
        )
        seen.add(share_symbol)

    # Fallback: derive a position from history if user-specific balance keys are missing.
    if not rows and base_symbol:
        implied_balance = _read_amount(_as_dict(vault.get("balance")))
        if implied_balance > 0:
            rows.append(_Holding(base_symbol, implied_balance, Decimal(0), {"balance": vault.get("balance")}))

    return rows


def _parse_history_row(row: dict[str, Any]) -> Transaction | None:
    history_type = str(row.get("type", "")).lower()
    timestamp = row.get("timestamp")
    tx_date = _parse_timestamp(timestamp)
    tx_hash = str(row.get("transactionHash", ""))

    assets = row.get("assets")
    shares = row.get("shares")
    asset_symbol, asset_amount = _first_symbol_amount(assets)
    share_symbol, share_amount = _first_symbol_amount(shares)

    if "deposit" in history_type:
        symbol = share_symbol or asset_symbol
        amount = share_amount if share_amount > 0 else asset_amount
        tx_type = TransactionType.DEPOSIT
    elif "redeem" in history_type or "withdraw" in history_type:
        symbol = asset_symbol or share_symbol
        amount = asset_amount if asset_amount > 0 else share_amount
        tx_type = TransactionType.WITHDRAWAL
    elif "claim" in history_type:
        symbol = asset_symbol or share_symbol
        amount = asset_amount if asset_amount > 0 else share_amount
        tx_type = TransactionType.YIELD
    else:
        symbol = asset_symbol or share_symbol
        amount = asset_amount if asset_amount > 0 else share_amount
        tx_type = TransactionType.TRANSFER

    if not symbol or amount <= 0:
        return None

    return Transaction(
        date=tx_date,
        source="yo",
        tx_type=tx_type,
        asset=symbol,
        amount=amount,
        usd_value=Decimal(0),
        tx_id=tx_hash,
        raw_json=json.dumps(row),
    )


def _first_symbol_amount(value: object) -> tuple[str, Decimal]:
    if not isinstance(value, list):
        return "", Decimal(0)
    for row in value:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        amount = _read_amount(row)
        if symbol and amount > 0:
            return symbol, amount
    return "", Decimal(0)


def _parse_timestamp(value: object) -> date:
    ts = _to_decimal(value)
    if ts <= 0:
        return datetime.now(tz=UTC).date()
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC).date()
    except (OverflowError, OSError, ValueError):
        return datetime.now(tz=UTC).date()


def _read_amount(value: dict[str, Any]) -> Decimal:
    if not value:
        return Decimal(0)
    for key in ("formatted", "amount", "value", "raw"):
        if key in value:
            amount = _to_decimal(value.get(key))
            if amount != 0:
                return amount
    return Decimal(0)


def _as_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)
