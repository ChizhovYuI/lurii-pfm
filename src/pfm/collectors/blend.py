"""Blend collector — reads DeFi lending positions via Stellar Soroban RPC."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import retry
from pfm.collectors.base import BaseCollector

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Snapshot, Transaction
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)


@register_collector
class BlendCollector(BaseCollector):
    """Collector for Blend Protocol positions on Stellar via Soroban RPC.

    Reads fixed pool lending positions by simulating a contract call
    to get_positions(user_address) on the Blend pool contract.
    """

    source_name = "blend"

    def __init__(
        self,
        pricing: PricingService,
        *,
        stellar_address: str,
        pool_contract_id: str,
        soroban_rpc_url: str,
    ) -> None:
        super().__init__(pricing)
        self._address = stellar_address
        self._pool_contract_id = pool_contract_id
        self._rpc_url = soroban_rpc_url
        self._client = httpx.AsyncClient(timeout=30.0)

    @retry()
    async def _simulate_get_positions(self) -> dict[str, Any]:
        """Simulate a Soroban contract call to get_positions."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "simulateTransaction",
            "params": {
                "transaction": self._build_simulation_xdr(),
            },
        }

        resp = await self._client.post(self._rpc_url, json=payload)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def _build_simulation_xdr(self) -> str:
        """Build the transaction XDR for simulating get_positions.

        Placeholder — full implementation requires stellar-sdk to construct
        the proper Soroban invocation envelope.
        """
        logger.warning(
            "Blend: stellar-sdk not yet integrated. Full Soroban contract simulation requires stellar-sdk dependency."
        )
        return ""

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch Blend lending positions."""
        today = self._pricing.today()

        if not self._pool_contract_id:
            logger.warning("Blend: pool contract ID not configured, skipping")
            return []

        try:
            result = await self._simulate_get_positions()
            return self._parse_positions(result, today)
        except (httpx.HTTPStatusError, ValueError) as exc:
            logger.warning("Blend: failed to fetch positions: %s", exc)
            return []

    def _parse_positions(self, rpc_result: dict[str, Any], _today: date) -> list[Snapshot]:
        """Parse Soroban RPC simulation result into Snapshot objects."""
        snapshots: list[Snapshot] = []

        error = rpc_result.get("error")
        if error:
            logger.warning("Blend: Soroban simulation error: %s", error)
            return []

        result_data = rpc_result.get("result", {})
        results = result_data.get("results", [])

        if not results:
            logger.info("Blend: no position data returned")
            return []

        for result_entry in results:
            xdr_val = result_entry.get("xdr", "")
            if xdr_val:
                logger.debug("Blend: got XDR result, parsing deferred until stellar-sdk integration")

        logger.info("Blend: parsed %d positions", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:  # noqa: ARG002
        """Blend transactions tracked via balance diffs between snapshots."""
        return []
