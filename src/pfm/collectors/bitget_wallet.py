"""Bitget Wallet Stablecoin Earn Plus collector via Aave V3 Base on-chain data."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._math import apr_to_apy
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_RATE_LIMITER = RateLimiter(requests_per_minute=180.0)

# Base chain + Aave V3 Base defaults (official Aave address book).
_DEFAULT_RPC_URL = "https://base-rpc.publicnode.com"
_DEFAULT_ASSET_SYMBOL = "USDC"
_DEFAULT_UNDERLYING_TOKEN_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # noqa: S105
_DEFAULT_A_TOKEN_ADDRESS = "0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB"  # noqa: S105
_DEFAULT_DATA_PROVIDER_ADDRESS = "0x0F43731EB8d45A581f4a36DD74F5f358bc90C73A"
_DEFAULT_TOKEN_DECIMALS = "6"  # noqa: S105

_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ZERO_ADDRESS_TOPIC = "0x" + ("0" * 64)

_BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)
_GET_RESERVE_DATA_SELECTOR = "0x35ea6a75"  # getReserveData(address)

_RAY = Decimal(10) ** 27
_RECENT_LOOKBACK_BLOCKS = 200_000
_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_RESERVE_DATA_MIN_WORDS = 6
_MAX_TOKEN_DECIMALS = 36
_MAX_LOG_BLOCK_RANGE = 50_000


@register_collector
class BitgetWalletCollector(BaseCollector):
    """Collector for Bitget Wallet Stablecoin Earn Plus (Aave V3 Base-backed)."""

    source_name = "bitget_wallet"

    def __init__(  # noqa: PLR0913
        self,
        pricing: PricingService,
        *,
        wallet_address: str,
        rpc_url: str = _DEFAULT_RPC_URL,
        asset_symbol: str = _DEFAULT_ASSET_SYMBOL,
        underlying_token_address: str = _DEFAULT_UNDERLYING_TOKEN_ADDRESS,
        a_token_address: str = _DEFAULT_A_TOKEN_ADDRESS,
        pool_data_provider_address: str = _DEFAULT_DATA_PROVIDER_ADDRESS,
        token_decimals: str = _DEFAULT_TOKEN_DECIMALS,
        bonus_apy: str = "0",
        base_apy_override: str = "",
        lookback_blocks: str = str(_RECENT_LOOKBACK_BLOCKS),
    ) -> None:
        super().__init__(pricing)
        self._wallet_address = _normalize_address(wallet_address, field_name="wallet_address")
        self._rpc_url = rpc_url.strip() or _DEFAULT_RPC_URL
        self._asset_symbol = (asset_symbol.strip() or _DEFAULT_ASSET_SYMBOL).upper()
        self._underlying_token_address = _normalize_address(
            underlying_token_address,
            field_name="underlying_token_address",
        )
        self._a_token_address = _normalize_address(a_token_address, field_name="a_token_address")
        self._pool_data_provider_address = _normalize_address(
            pool_data_provider_address,
            field_name="pool_data_provider_address",
        )
        self._token_decimals = _parse_decimals(token_decimals)
        self._bonus_apy = _parse_rate(bonus_apy)
        self._base_apy_override = _parse_optional_rate(base_apy_override)
        self._lookback_blocks = _parse_non_negative_int(lookback_blocks, field_name="lookback_blocks")
        self._client = httpx.AsyncClient(timeout=30.0)

    @retry()
    async def _rpc(self, method: str, params: list[object]) -> object:
        """Make a JSON-RPC request to the configured Base endpoint."""
        await _RATE_LIMITER.acquire()
        resp = await self._client.post(
            self._rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            msg = f"Base RPC returned non-object payload for {method}"
            raise TypeError(msg)

        error = payload.get("error")
        if error is not None:
            msg = f"Base RPC error on {method}: {error}"
            raise ValueError(msg)

        if "result" not in payload:
            msg = f"Base RPC missing result for {method}"
            raise ValueError(msg)
        return payload["result"]

    async def _eth_call_hex(self, *, to: str, data: str) -> str:
        result = await self._rpc("eth_call", [{"to": to, "data": data}, "latest"])
        if not isinstance(result, str) or not result.startswith("0x"):
            msg = "Invalid eth_call result payload"
            raise ValueError(msg)
        return result

    async def _fetch_position_amount(self) -> Decimal:
        call_data = _encode_address_arg_call(_BALANCE_OF_SELECTOR, self._wallet_address)
        raw = int(await self._eth_call_hex(to=self._a_token_address, data=call_data), 16)
        if raw <= 0:
            return Decimal(0)
        return Decimal(raw) / (Decimal(10) ** self._token_decimals)

    async def _fetch_base_apy(self) -> Decimal:
        if self._base_apy_override is not None:
            return self._base_apy_override

        call_data = _encode_address_arg_call(_GET_RESERVE_DATA_SELECTOR, self._underlying_token_address)
        result_hex = await self._eth_call_hex(to=self._pool_data_provider_address, data=call_data)
        words = _decode_uint256_words(result_hex)
        if len(words) < _RESERVE_DATA_MIN_WORDS:
            msg = "Unexpected Aave reserve data payload"
            raise ValueError(msg)

        # Aave liquidityRate is annualized APR in ray units.
        apr = Decimal(words[5]) / _RAY
        return apr_to_apy(apr)

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch current supplied USDC amount and APY for Stablecoin Earn Plus."""
        amount = await self._fetch_position_amount()
        if amount <= 0:
            logger.info("bitget_wallet: no Aave position for wallet=%s", self._wallet_address)
            return []

        try:
            base_apy = await self._fetch_base_apy()
        except (ArithmeticError, TypeError, ValueError) as exc:
            logger.warning("bitget_wallet: failed to fetch base APY from Aave (%s), falling back to 0", exc)
            base_apy = Decimal(0)

        total_apy = base_apy + self._bonus_apy

        price = await self._pricing.get_price_usd(self._asset_symbol)
        usd_value = amount * price
        today = self._pricing.today()

        raw_payload = {
            "wallet_address": self._wallet_address,
            "rpc_url": self._rpc_url,
            "underlying_token_address": self._underlying_token_address,
            "a_token_address": self._a_token_address,
            "pool_data_provider_address": self._pool_data_provider_address,
            "base_apy": str(base_apy),
            "bonus_apy": str(self._bonus_apy),
            "apy_total": str(total_apy),
        }

        return [
            Snapshot(
                date=today,
                source=self.source_name,
                asset=self._asset_symbol,
                amount=amount,
                usd_value=usd_value,
                price=price,
                apy=total_apy,
                raw_json=json.dumps(raw_payload),
            )
        ]

    async def _latest_block(self) -> int:
        result = await self._rpc("eth_blockNumber", [])
        if not isinstance(result, str) or not result.startswith("0x"):
            msg = "Invalid eth_blockNumber result payload"
            raise ValueError(msg)
        return int(result, 16)

    async def _fetch_transfer_logs(
        self,
        *,
        from_topic: str | None,
        to_topic: str | None,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        topics: list[str | None] = [_TRANSFER_TOPIC, from_topic, to_topic]
        out: list[dict[str, Any]] = []
        start_block = from_block
        while start_block <= to_block:
            end_block = min(start_block + _MAX_LOG_BLOCK_RANGE - 1, to_block)
            payload = {
                "fromBlock": hex(start_block),
                "toBlock": hex(end_block),
                "address": self._a_token_address,
                "topics": topics,
            }
            result = await self._rpc("eth_getLogs", [payload])
            if isinstance(result, list):
                out.extend(row for row in result if isinstance(row, dict))
            start_block = end_block + 1
        return out

    async def _block_date(self, block_number_hex: str, cache: dict[str, date]) -> date:
        cached = cache.get(block_number_hex)
        if cached is not None:
            return cached

        result = await self._rpc("eth_getBlockByNumber", [block_number_hex, False])
        if not isinstance(result, dict):
            return datetime.now(tz=UTC).date()
        ts_hex = result.get("timestamp")
        if not isinstance(ts_hex, str) or not ts_hex.startswith("0x"):
            return datetime.now(tz=UTC).date()

        tx_date = datetime.fromtimestamp(int(ts_hex, 16), tz=UTC).date()
        cache[block_number_hex] = tx_date
        return tx_date

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent Aave supply/withdraw events from aToken Transfer logs."""
        latest_block = await self._latest_block()
        from_block = max(latest_block - self._lookback_blocks, 0)
        wallet_topic = _address_topic(self._wallet_address)

        minted_logs = await self._fetch_transfer_logs(
            from_topic=_ZERO_ADDRESS_TOPIC,
            to_topic=wallet_topic,
            from_block=from_block,
            to_block=latest_block,
        )
        burned_logs = await self._fetch_transfer_logs(
            from_topic=wallet_topic,
            to_topic=_ZERO_ADDRESS_TOPIC,
            from_block=from_block,
            to_block=latest_block,
        )

        block_date_cache: dict[str, date] = {}
        transactions: list[Transaction] = []
        for log_row, tx_type in (
            *((row, TransactionType.DEPOSIT) for row in minted_logs),
            *((row, TransactionType.WITHDRAWAL) for row in burned_logs),
        ):
            data_hex = log_row.get("data")
            if not isinstance(data_hex, str) or not data_hex.startswith("0x"):
                continue
            amount_raw = int(data_hex, 16)
            if amount_raw <= 0:
                continue
            amount = Decimal(amount_raw) / (Decimal(10) ** self._token_decimals)
            if amount <= 0:
                continue

            block_number_hex = str(log_row.get("blockNumber", "0x0"))
            tx_date = await self._block_date(block_number_hex, block_date_cache)
            if since and tx_date < since:
                continue

            tx_hash = str(log_row.get("transactionHash", ""))
            transactions.append(
                Transaction(
                    date=tx_date,
                    source=self.source_name,
                    tx_type=tx_type,
                    asset=self._asset_symbol,
                    amount=amount,
                    usd_value=Decimal(0),
                    tx_id=tx_hash,
                    raw_json=json.dumps(log_row),
                )
            )

        transactions.sort(key=lambda tx: (tx.date, tx.tx_id))
        logger.info("bitget_wallet: parsed %d transactions", len(transactions))
        return transactions


def _normalize_address(value: str, *, field_name: str) -> str:
    text = value.strip()
    if not _ADDRESS_RE.fullmatch(text):
        msg = f"{field_name} must be a 0x-prefixed 40-hex address"
        raise ValueError(msg)
    return "0x" + text[2:].lower()


def _parse_non_negative_int(value: str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} must be an integer"
        raise ValueError(msg) from exc
    if parsed < 0:
        msg = f"{field_name} must be >= 0"
        raise ValueError(msg)
    return parsed


def _parse_decimals(value: str) -> int:
    parsed = _parse_non_negative_int(value, field_name="token_decimals")
    if parsed > _MAX_TOKEN_DECIMALS:
        msg = f"token_decimals must be <= {_MAX_TOKEN_DECIMALS}"
        raise ValueError(msg)
    return parsed


def _parse_rate(value: str) -> Decimal:
    text = value.strip()
    if not text:
        return Decimal(0)
    try:
        parsed = Decimal(text)
    except ArithmeticError as exc:
        msg = "Rate fields must be decimal numbers"
        raise ValueError(msg) from exc
    if parsed < 0:
        msg = "Rate fields must be >= 0"
        raise ValueError(msg)
    # Accept both decimal (0.188) and percent (18.8) user inputs.
    return parsed / Decimal(100) if parsed > 1 else parsed


def _parse_optional_rate(value: str) -> Decimal | None:
    text = value.strip()
    if not text:
        return None
    return _parse_rate(text)


def _encode_address_arg_call(selector: str, address: str) -> str:
    return selector + ("0" * 24) + address[2:].lower()


def _decode_uint256_words(hex_data: str) -> list[int]:
    body = hex_data[2:]
    if len(body) % 64 != 0:
        return []
    out: list[int] = []
    for i in range(0, len(body), 64):
        chunk = body[i : i + 64]
        try:
            out.append(int(chunk, 16))
        except ValueError:
            return []
    return out


def _address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + address[2:].lower()
