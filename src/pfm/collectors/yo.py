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
from pfm.db.models import RawBalance, Transaction, TransactionType

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
        payload = await self._get(f"/api/v1/vault/{self._network}/{self._vault_address}")
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

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch current vault position balances for a user."""
        vault = await self._get_vault()
        history_rows = await self._get_history(limit=1000)
        derived_holding = _derive_share_holding_from_history(vault, history_rows)
        holdings = [derived_holding] if derived_holding is not None else []
        apy = _extract_vault_apy(vault)

        if not holdings:
            logger.info(
                "yo: no balances found for network=%s vault=%s (saving zero snapshot)",
                self._network,
                self._vault_address,
            )
            zero_symbol = _pick_zero_asset_symbol(vault)
            if not zero_symbol:
                return []
            return [
                RawBalance(
                    asset=zero_symbol,
                    amount=Decimal(0),
                    apy=apy,
                    price=Decimal(0),
                    raw_json=json.dumps(
                        {
                            "derivedFrom": "history",
                            "state": "empty_position",
                            "network": self._network,
                            "vaultAddress": self._vault_address,
                        }
                    ),
                )
            ]

        raw: list[RawBalance] = []
        for holding in holdings:
            symbol = holding.symbol.upper()
            amount = holding.amount
            if not symbol or amount <= 0:
                continue

            price = holding.price_usd if holding.price_usd > 0 else None

            raw.append(
                RawBalance(
                    asset=symbol,
                    amount=amount,
                    apy=apy,
                    price=price,
                    raw_json=json.dumps(holding.raw),
                )
            )

        logger.info("yo: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch user vault history and map it to normalized transaction rows."""
        vault = await self._get_vault()
        asset_symbol = str(_as_dict(vault.get("asset")).get("symbol", "")).upper()
        share_symbol = str(_as_dict(vault.get("shareAsset")).get("symbol", "")).upper()
        rows = await self._get_history(limit=100)
        txs: list[Transaction] = []
        for row in rows:
            tx = _parse_history_row(
                row,
                fallback_asset_symbol=asset_symbol,
                fallback_share_symbol=share_symbol,
            )
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


def _derive_share_holding_from_history(vault: dict[str, Any], rows: list[dict[str, Any]]) -> _Holding | None:
    share_asset = _as_dict(vault.get("shareAsset"))
    share_symbol = str(share_asset.get("symbol", "")).upper()
    if not share_symbol:
        return None

    stats = _as_dict(vault.get("stats"))
    share_price = _read_amount(_as_dict(stats.get("sharePrice")))

    share_balance = Decimal(0)
    for row in rows:
        history_type = str(row.get("type", "")).lower()
        _, shares_amount = _first_symbol_amount(row.get("shares"), share_symbol)
        if shares_amount <= 0:
            continue
        if "deposit" in history_type:
            share_balance += shares_amount
        elif "redeem" in history_type or "withdraw" in history_type:
            share_balance -= shares_amount

    if share_balance <= 0:
        return None

    return _Holding(
        share_symbol,
        share_balance,
        share_price,
        {"derivedFrom": "history", "sharePrice": stats.get("sharePrice")},
    )


def _parse_history_row(
    row: dict[str, Any],
    *,
    fallback_asset_symbol: str = "",
    fallback_share_symbol: str = "",
) -> Transaction | None:
    history_type = str(row.get("type", "")).lower()
    timestamp = row.get("timestamp")
    tx_date = _parse_timestamp(timestamp)
    tx_hash = str(row.get("transactionHash", ""))

    assets = row.get("assets")
    shares = row.get("shares")
    asset_symbol, asset_amount = _first_symbol_amount(assets, fallback_asset_symbol)
    share_symbol, share_amount = _first_symbol_amount(shares, fallback_share_symbol)

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


def _first_symbol_amount(value: object, fallback_symbol: str = "") -> tuple[str, Decimal]:
    fallback = fallback_symbol.upper()
    if isinstance(value, dict):
        symbol = str(value.get("symbol", "")).upper() or fallback
        amount = _read_amount(value)
        if symbol and amount > 0:
            return symbol, amount
        return "", Decimal(0)
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


def _extract_vault_apy(vault: dict[str, Any]) -> Decimal:
    stats = _as_dict(vault.get("stats"))
    yield_obj = _as_dict(stats.get("yield"))
    # Prefer smoother period first; API values are percentages.
    for key in ("7d", "30d", "1d"):
        value = _to_decimal(yield_obj.get(key))
        if value == 0:
            continue
        return value / Decimal(100) if value > 1 else value
    return Decimal(0)


def _pick_zero_asset_symbol(vault: dict[str, Any]) -> str:
    share_asset = _as_dict(vault.get("shareAsset"))
    share_symbol = str(share_asset.get("symbol", "")).upper()
    if share_symbol:
        return share_symbol
    asset = _as_dict(vault.get("asset"))
    return str(asset.get("symbol", "")).upper()
