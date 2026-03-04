"""Bitget Wallet collector: Aave V3 (Base) + native SOL staking."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
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

_RATE_LIMITER = RateLimiter(requests_per_minute=180.0)
_AAVE_GRAPHQL_URL = "https://api.v3.aave.com/graphql"
_BASE_RPC_URL = "https://mainnet.base.org"
_BASE_CHAIN_ID = 8453
_GRAPHQL_PAGE_SIZE = "FIFTY"
_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

# Solana staking constants
_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
_STAKE_PROGRAM_ID = "Stake11111111111111111111111111111111111111"
_SOL_DECIMALS = 9
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_STAKEWIZ_API_URL = "https://api.stakewiz.com/validator"

# aToken contract addresses on Base (Aave V3)
_ATOKEN_MAP: dict[str, str] = {
    "USDC": "0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB",
    "USDT": "0x8619d80FB0141ba7F184CbF22fd724116943bA1C",
}
_ATOKEN_DECIMALS: dict[str, int] = {"USDC": 6, "USDT": 6}
_BALANCE_OF_SELECTOR = "0x70a08231"

_MARKETS_QUERY = """
query Markets($request: MarketsRequest!) {
  value: markets(request: $request) {
    address
    name
    chain {
      chainId
      name
    }
  }
}
"""

_USER_SUPPLIES_QUERY = """
query UserSupplies($request: UserSuppliesRequest!) {
  value: userSupplies(request: $request) {
    market {
      address
      name
      chain {
        chainId
        name
      }
    }
    currency {
      symbol
      address
      decimals
      chainId
    }
    balance {
      amount {
        value
      }
      usd
    }
    apy {
      value
      formatted
    }
  }
}
"""

_USER_TX_HISTORY_QUERY = """
query UserTransactionHistory($request: UserTransactionHistoryRequest!) {
  value: userTransactionHistory(request: $request) {
    items {
      __typename
      ... on UserSupplyTransaction {
        txHash
        timestamp
        amount {
          amount {
            value
          }
          usd
        }
        reserve {
          underlyingToken {
            symbol
          }
        }
      }
      ... on UserWithdrawTransaction {
        txHash
        timestamp
        amount {
          amount {
            value
          }
          usd
        }
        reserve {
          underlyingToken {
            symbol
          }
        }
      }
    }
    pageInfo {
      next
    }
  }
}
"""


@register_collector
class BitgetWalletCollector(BaseCollector):
    """Collector for Bitget Wallet positions indexed by Aave API."""

    source_name = "bitget_wallet"

    def __init__(
        self,
        pricing: PricingService,
        *,
        wallet_address: str,
        solana_address: str = "",
    ) -> None:
        super().__init__(pricing)
        self._wallet_address = _normalize_address(wallet_address, field_name="wallet_address")
        self._solana_address = _normalize_solana_address(solana_address) if solana_address else ""
        self._graph_ql_url = _AAVE_GRAPHQL_URL
        self._client = httpx.AsyncClient(timeout=30.0)

    @retry()
    async def _graphql(self, query: str, variables: dict[str, object]) -> object:
        """Make a GraphQL request to Aave API."""
        await _RATE_LIMITER.acquire()
        resp = await self._client.post(
            self._graph_ql_url,
            json={"query": query, "variables": variables},
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            msg = "Aave GraphQL returned non-object payload"
            raise TypeError(msg)

        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            msg = f"Aave GraphQL returned errors: {errors}"
            raise ValueError(msg)

        data = payload.get("data")
        if not isinstance(data, dict) or "value" not in data:
            msg = "Aave GraphQL payload missing data.value"
            raise ValueError(msg)
        return data["value"]

    async def _fetch_base_markets(self) -> list[dict[str, str]]:
        value = await self._graphql(_MARKETS_QUERY, {"request": {"chainIds": [_BASE_CHAIN_ID]}})
        if not isinstance(value, list):
            msg = "Aave markets payload is invalid"
            raise TypeError(msg)

        markets: list[dict[str, str]] = []
        for row in value:
            if not isinstance(row, dict):
                continue
            address = _coerce_address(row.get("address"))
            if not address:
                continue
            chain = row.get("chain")
            chain_id = chain.get("chainId") if isinstance(chain, dict) else None
            if chain_id != _BASE_CHAIN_ID:
                continue
            name = str(row.get("name") or "AaveV3Base")
            markets.append({"address": address, "name": name})

        if not markets:
            msg = "No Aave markets found on Base"
            raise ValueError(msg)
        return markets

    async def _fetch_onchain_balance(self, asset: str) -> Decimal | None:
        """Query aToken balanceOf on Base via JSON-RPC eth_call."""
        atoken = _ATOKEN_MAP.get(asset)
        if not atoken:
            return None
        decimals = _ATOKEN_DECIMALS.get(asset, 6)
        # balanceOf(address) — pad wallet address to 32 bytes
        data = _BALANCE_OF_SELECTOR + self._wallet_address[2:].lower().zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": atoken, "data": data}, "latest"],
        }
        resp = await self._client.post(_BASE_RPC_URL, json=payload)
        resp.raise_for_status()
        result = resp.json().get("result")
        if not result or result == "0x":
            return None
        raw = int(result, 16)
        return Decimal(raw) / Decimal(10**decimals)

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch Aave supply balances and SOL staking positions."""
        snapshots: list[Snapshot] = []
        snapshots.extend(await self._fetch_aave_balances())
        snapshots.extend(await self._fetch_sol_staking())
        if not snapshots:
            logger.info("bitget_wallet: no positions for wallet=%s", self._wallet_address)
        return snapshots

    async def _fetch_aave_balances(self) -> list[Snapshot]:
        """Fetch current wallet supply balances from Aave Base market."""
        markets = await self._fetch_base_markets()
        value = await self._graphql(
            _USER_SUPPLIES_QUERY,
            {
                "request": {
                    "markets": [{"address": m["address"], "chainId": _BASE_CHAIN_ID} for m in markets],
                    "user": self._wallet_address,
                }
            },
        )
        if not isinstance(value, list):
            msg = "Aave userSupplies payload is invalid"
            raise TypeError(msg)

        snapshots: list[Snapshot] = []
        today = self._pricing.today()
        for row in value:
            if not isinstance(row, dict):
                continue

            graphql_amount = _decimal_or_zero(_get_path(row, "balance", "amount", "value"))
            if graphql_amount <= 0:
                continue
            apy = _decimal_or_zero(_get_path(row, "apy", "value"))

            currency = row.get("currency")
            asset = str(currency.get("symbol") or "UNKNOWN") if isinstance(currency, dict) else "UNKNOWN"

            # Prefer on-chain aToken balance (includes real-time accrued interest)
            onchain_amount = await self._fetch_onchain_balance(asset)
            amount = onchain_amount if onchain_amount and onchain_amount > 0 else graphql_amount

            price = await self._pricing.get_price_usd(asset)
            usd_value = amount * price

            raw_payload: dict[str, Any] = {
                "wallet_address": self._wallet_address,
                "market": row.get("market"),
                "currency": currency,
                "balance": row.get("balance"),
                "apy": row.get("apy"),
                "onchain_amount": str(onchain_amount) if onchain_amount else None,
            }

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=asset,
                    amount=amount,
                    usd_value=usd_value,
                    price=price,
                    apy=apy,
                    raw_json=json.dumps(raw_payload),
                )
            )

        if not snapshots:
            logger.info("bitget_wallet: no Aave supply positions for wallet=%s", self._wallet_address)
        return snapshots

    async def _fetch_sol_staking(self) -> list[Snapshot]:
        """Fetch native SOL staking positions via Solana RPC."""
        if not self._solana_address:
            return []

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                _STAKE_PROGRAM_ID,
                {
                    "encoding": "jsonParsed",
                    "filters": [
                        {"memcmp": {"offset": 44, "bytes": self._solana_address}},
                    ],
                },
            ],
        }
        resp = await self._client.post(_SOLANA_RPC_URL, json=payload)
        resp.raise_for_status()
        result = resp.json().get("result")
        if not isinstance(result, list) or not result:
            logger.info("bitget_wallet: no SOL stake accounts for %s", self._solana_address)
            return []

        total_lamports = 0
        voter: str | None = None
        stake_accounts: list[dict[str, Any]] = []
        for entry in result:
            if not isinstance(entry, dict):
                continue
            account = entry.get("account")
            if not isinstance(account, dict):
                continue
            lamports = account.get("lamports", 0)
            if not isinstance(lamports, int) or lamports <= 0:
                continue
            total_lamports += lamports
            if not voter:
                extracted = str(_get_path(account, "data", "parsed", "info", "stake", "delegation", "voter") or "")
                if extracted:
                    voter = extracted
            stake_accounts.append(
                {
                    "pubkey": entry.get("pubkey"),
                    "lamports": lamports,
                }
            )

        if total_lamports == 0:
            return []

        amount = Decimal(total_lamports) / Decimal(10**_SOL_DECIMALS)
        price = await self._pricing.get_price_usd("SOL")
        usd_value = amount * price
        apy = await self._fetch_validator_apy(voter) if voter else Decimal(0)
        today = self._pricing.today()

        raw_payload: dict[str, Any] = {
            "solana_address": self._solana_address,
            "stake_accounts": stake_accounts,
            "total_lamports": total_lamports,
            "voter": voter,
        }

        return [
            Snapshot(
                date=today,
                source=self.source_name,
                asset="SOL",
                amount=amount,
                usd_value=usd_value,
                price=price,
                apy=apy,
                raw_json=json.dumps(raw_payload),
            )
        ]

    async def _fetch_validator_apy(self, voter: str) -> Decimal:
        """Fetch validator APY estimate from Stakewiz API."""
        try:
            resp = await self._client.get(f"{_STAKEWIZ_API_URL}/{voter}", timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("apy_estimate") is not None:
                # Stakewiz returns percentage (e.g. 6.13), convert to decimal (0.0613)
                return Decimal(str(data["apy_estimate"])) / Decimal(100)
        except Exception as exc:  # noqa: BLE001
            logger.warning("bitget_wallet: failed to fetch validator APY for voter=%s: %s", voter, exc)
        return Decimal(0)

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch supply/withdraw history from Aave transaction API."""
        markets = await self._fetch_base_markets()
        transactions: list[Transaction] = []
        for market in markets:
            transactions.extend(await self._fetch_market_transactions(market_address=market["address"], since=since))

        transactions.sort(key=lambda tx: (tx.date, tx.tx_id))
        logger.info("bitget_wallet: parsed %d transactions", len(transactions))
        return transactions

    async def _fetch_market_transactions(self, *, market_address: str, since: date | None) -> list[Transaction]:
        out: list[Transaction] = []
        cursor: str | None = None
        stop = False
        while not stop:
            value = await self._fetch_tx_page(market_address=market_address, cursor=cursor)
            items = value.get("items")
            page_info = value.get("pageInfo")
            if not isinstance(items, list):
                break

            for row in items:
                tx, stop = _parse_tx_row(row, since=since, source=self.source_name)
                if tx is not None:
                    out.append(tx)

            cursor = page_info.get("next") if isinstance(page_info, dict) else None
            if not cursor:
                break
        return out

    async def _fetch_tx_page(self, *, market_address: str, cursor: str | None) -> dict[str, Any]:
        request: dict[str, object] = {
            "market": market_address,
            "chainId": _BASE_CHAIN_ID,
            "user": self._wallet_address,
            "filter": ["SUPPLY", "WITHDRAW"],
            "orderBy": {"date": "DESC"},
            "pageSize": _GRAPHQL_PAGE_SIZE,
        }
        if cursor:
            request["cursor"] = cursor
        value = await self._graphql(_USER_TX_HISTORY_QUERY, {"request": request})
        if not isinstance(value, dict):
            msg = "Aave userTransactionHistory payload is invalid"
            raise TypeError(msg)
        return value


def _normalize_address(value: str, *, field_name: str) -> str:
    text = value.strip()
    if not _ADDRESS_RE.fullmatch(text):
        msg = f"{field_name} must be a 0x-prefixed 40-hex address"
        raise ValueError(msg)
    return "0x" + text[2:].lower()


def _normalize_solana_address(value: str) -> str:
    text = value.strip()
    if not _SOLANA_ADDRESS_RE.fullmatch(text):
        msg = "solana_address must be a base58 Solana public key (32-44 chars)"
        raise ValueError(msg)
    return text


def _coerce_address(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not _ADDRESS_RE.fullmatch(text):
        return ""
    return "0x" + text[2:].lower()


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except ArithmeticError:
        return None


def _decimal_or_zero(value: object) -> Decimal:
    parsed = _to_decimal(value)
    return parsed if parsed is not None else Decimal(0)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _get_path(obj: object, *path: str) -> object:
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _parse_tx_row(
    row: object,
    *,
    since: date | None,
    source: str,
) -> tuple[Transaction | None, bool]:
    if not isinstance(row, dict):
        return (None, False)

    typename = row.get("__typename")
    if typename == "UserSupplyTransaction":
        tx_type = TransactionType.DEPOSIT
    elif typename == "UserWithdrawTransaction":
        tx_type = TransactionType.WITHDRAWAL
    else:
        return (None, False)

    timestamp = row.get("timestamp")
    tx_dt = _parse_datetime(timestamp)
    if tx_dt is None:
        return (None, False)
    tx_date = tx_dt.date()
    if since and tx_date < since:
        return (None, True)

    amount = _decimal_or_zero(_get_path(row, "amount", "amount", "value"))
    if amount <= 0:
        return (None, False)
    usd_value = _decimal_or_zero(_get_path(row, "amount", "usd"))
    asset = str(_get_path(row, "reserve", "underlyingToken", "symbol") or "UNKNOWN")
    tx_hash = str(row.get("txHash") or "")

    tx = Transaction(
        date=tx_date,
        source=source,
        tx_type=tx_type,
        asset=asset,
        amount=amount,
        usd_value=usd_value,
        tx_id=tx_hash,
        raw_json=json.dumps(row),
    )
    return (tx, False)
