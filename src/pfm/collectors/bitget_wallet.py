"""Bitget Wallet collector backed by Aave V3 API data on Base."""

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
_BASE_CHAIN_ID = 8453
_GRAPHQL_PAGE_SIZE = "FIFTY"
_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

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
        **legacy_settings: str,
    ) -> None:
        super().__init__(pricing)
        self._wallet_address = _normalize_address(wallet_address, field_name="wallet_address")
        self._graph_ql_url = _AAVE_GRAPHQL_URL
        self._client = httpx.AsyncClient(timeout=30.0)
        if legacy_settings:
            logger.info(
                "bitget_wallet: ignoring deprecated settings keys: %s",
                ", ".join(sorted(legacy_settings)),
            )

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

    async def fetch_balances(self) -> list[Snapshot]:
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

            amount = _decimal_or_zero(_get_path(row, "balance", "amount", "value"))
            if amount <= 0:
                continue
            apy = _decimal_or_zero(_get_path(row, "apy", "value"))
            usd_value_raw = _to_decimal(_get_path(row, "balance", "usd"))
            usd_value = usd_value_raw if usd_value_raw is not None else Decimal(0)
            price = (usd_value / amount) if amount > 0 and usd_value > 0 else Decimal(0)

            currency = row.get("currency")
            asset = str(currency.get("symbol") or "UNKNOWN") if isinstance(currency, dict) else "UNKNOWN"
            raw_payload: dict[str, Any] = {
                "wallet_address": self._wallet_address,
                "market": row.get("market"),
                "currency": currency,
                "balance": row.get("balance"),
                "apy": row.get("apy"),
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
            return []
        return snapshots

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
