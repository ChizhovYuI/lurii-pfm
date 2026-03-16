"""Blend collector — reads DeFi lending positions via Stellar Soroban RPC."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
from stellar_sdk import Address, Network, SorobanServer, TransactionBuilder, scval
from stellar_sdk import xdr as stellar_xdr
from stellar_sdk.exceptions import SdkError

from pfm.collectors import register_collector
from pfm.collectors._math import apr_to_apy
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance
from pfm.enums import SourceName

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Transaction
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_SCALAR_7 = 10**7
_SCALAR_12 = 10**12
_U_95 = Decimal("0.95")
# Known Stellar asset contract → ticker mapping (mainnet).
# Native XLM uses a Stellar Asset Contract (SAC) wrapper.
_KNOWN_ASSETS: dict[str, str] = {
    "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA": "XLM",
    "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75": "USDC",
    "CDTKPWPLOURQA2SGTKTUQOWRCBZEORB4BWBOMJ3D3ZTQQSGE5F6JBQLV": "EURC",
}


@register_collector
class BlendCollector(BaseCollector):
    """Collector for Blend Protocol positions on Stellar via Soroban RPC.

    Reads lending positions by simulating ``get_positions(address)`` on the
    Blend pool contract, then converts bToken balances to underlying assets
    using each reserve's ``b_rate``.
    """

    source_name = SourceName.BLEND

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

    # ── Soroban helpers ────────────────────────────────────────────────

    def _simulate(
        self,
        function_name: str,
        parameters: list[Any],
    ) -> Any:  # noqa: ANN401
        """Build, simulate a Soroban contract call and return the native result."""
        server = SorobanServer(self._rpc_url)
        source = server.load_account(self._address)
        tx = (
            TransactionBuilder(source, Network.PUBLIC_NETWORK_PASSPHRASE, base_fee=100)
            .set_timeout(30)
            .append_invoke_contract_function_op(
                contract_id=self._pool_contract_id,
                function_name=function_name,
                parameters=parameters,
            )
            .build()
        )
        sim = server.simulate_transaction(tx)
        if not sim.results or not sim.results[0].xdr:
            return None
        val = stellar_xdr.SCVal.from_xdr(sim.results[0].xdr)
        return scval.to_native(val)

    def _get_positions(self) -> dict[str, Any]:
        """Call get_positions(address) on the pool contract."""
        result = self._simulate("get_positions", [scval.to_address(self._address)])
        if not isinstance(result, dict):
            return {"collateral": {}, "supply": {}, "liabilities": {}}
        return result

    def _get_reserve_list(self) -> list[Address]:
        """Call get_reserve_list() → list of asset Address objects."""
        result = self._simulate("get_reserve_list", [])
        if not isinstance(result, list):
            return []
        return result

    def _get_reserve(self, asset_addr: Address) -> dict[str, Any] | None:
        """Call get_reserve(asset) → reserve config + data (includes b_rate)."""
        result = self._simulate("get_reserve", [asset_addr.to_xdr_sc_val()])
        if not isinstance(result, dict):
            return None
        return result

    def _resolve_ticker(self, asset_addr: Address) -> str:
        """Resolve asset contract address to ticker symbol."""
        contract_id = asset_addr.address
        if contract_id in _KNOWN_ASSETS:
            return _KNOWN_ASSETS[contract_id]
        # Fall back to calling the token's symbol() function
        try:
            server = SorobanServer(self._rpc_url)
            source = server.load_account(self._address)
            tx = (
                TransactionBuilder(source, Network.PUBLIC_NETWORK_PASSPHRASE, base_fee=100)
                .set_timeout(30)
                .append_invoke_contract_function_op(
                    contract_id=contract_id,
                    function_name="symbol",
                    parameters=[],
                )
                .build()
            )
            sim = server.simulate_transaction(tx)
            if sim.results and sim.results[0].xdr:
                val = stellar_xdr.SCVal.from_xdr(sim.results[0].xdr)
                symbol = str(scval.to_native(val)).upper()
                if symbol == "NATIVE":
                    return "XLM"
                return symbol
        except (httpx.HTTPStatusError, SdkError, ValueError, KeyError):
            logger.warning("Blend: could not resolve ticker for %s", contract_id[:12])
        return "UNKNOWN"

    def _get_pool_config(self) -> dict[str, Any]:
        """Call get_config() on the pool contract."""
        result = self._simulate("get_config", [])
        if not isinstance(result, dict):
            return {}
        return result

    @staticmethod
    def _compute_supply_apy(reserve: dict[str, Any], backstop_rate: Decimal) -> Decimal:
        """Compute supply APY from on-chain reserve data.

        Uses Blend's 3-segment piecewise borrow interest rate curve,
        then derives supply APR and converts to APY (weekly compounding).
        """
        data = reserve.get("data", {})
        config = reserve.get("config", {})

        b_supply = int(data.get("b_supply", 0))
        d_supply = int(data.get("d_supply", 0))
        b_rate = int(data.get("b_rate", 0))
        d_rate = int(data.get("d_rate", 0))
        ir_mod = int(data.get("ir_mod", _SCALAR_7))

        r_base = int(config.get("r_base", 0))
        r_one = int(config.get("r_one", 0))
        r_two = int(config.get("r_two", 0))
        r_three = int(config.get("r_three", 0))
        util_target_raw = int(config.get("util", 0))

        # Compute utilization
        total_supply = Decimal(b_supply) * Decimal(b_rate) / Decimal(_SCALAR_12)
        total_borrow = Decimal(d_supply) * Decimal(d_rate) / Decimal(_SCALAR_12)
        if total_supply == 0:
            return Decimal(0)
        utilization = total_borrow / total_supply

        # 3-segment piecewise borrow curve
        u_target = Decimal(util_target_raw) / Decimal(_SCALAR_7)
        u = utilization

        if u_target > 0 and u <= u_target:
            base_ir = (u / u_target) * Decimal(r_one) + Decimal(r_base)
        elif u <= _U_95:
            denom = _U_95 - u_target
            if denom > 0:
                base_ir = ((u - u_target) / denom) * Decimal(r_two) + Decimal(r_one) + Decimal(r_base)
            else:
                base_ir = Decimal(r_one) + Decimal(r_base)
        else:
            base_ir = (
                ((u - _U_95) / Decimal("0.05")) * Decimal(r_three) + Decimal(r_two) + Decimal(r_one) + Decimal(r_base)
            )

        # Apply reactive interest rate modifier
        cur_ir = base_ir * Decimal(ir_mod) / Decimal(_SCALAR_7)
        borrow_apr = cur_ir / Decimal(_SCALAR_7)

        # Supply APR = borrow_apr * (1 - backstop_rate) * utilization
        supply_apr = borrow_apr * (1 - backstop_rate) * utilization

        # Weekly compounding per Blend SDK convention
        return apr_to_apy(supply_apr, periods=52)

    # ── Public interface ───────────────────────────────────────────────

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch Blend lending positions (supply + collateral) with APY."""
        if not self._pool_contract_id:
            logger.warning("Blend: pool contract ID not configured, skipping")
            return []

        try:
            positions = self._get_positions()
            reserve_list = self._get_reserve_list()
        except (httpx.HTTPStatusError, SdkError, ValueError, KeyError) as exc:
            logger.warning("Blend: failed to fetch positions: %s", exc)
            return []

        # Merge supply + collateral bTokens per reserve index
        totals: dict[int, int] = {}
        for pos_type in ("collateral", "supply"):
            for idx, b_tokens in positions.get(pos_type, {}).items():
                idx_int = int(idx)
                totals[idx_int] = totals.get(idx_int, 0) + int(b_tokens)

        if not totals:
            logger.info("Blend: no active positions")
            return []

        # Fetch pool config for backstop rate
        try:
            pool_config = self._get_pool_config()
            backstop_rate = Decimal(int(pool_config.get("bstop_rate", 0))) / Decimal(_SCALAR_7)
        except (httpx.HTTPStatusError, SdkError, ValueError, KeyError):
            backstop_rate = Decimal("0.20")

        raw: list[RawBalance] = []

        for idx, b_tokens in totals.items():
            if idx >= len(reserve_list):
                continue
            asset_addr = reserve_list[idx]
            reserve = self._get_reserve(asset_addr)
            if reserve is None:
                continue

            b_rate = int(reserve["data"]["b_rate"])
            scalar = int(reserve["scalar"])
            underlying_raw = b_tokens * b_rate // _SCALAR_12
            amount = Decimal(underlying_raw) / Decimal(scalar)

            ticker = self._resolve_ticker(asset_addr)
            apy = self._compute_supply_apy(reserve, backstop_rate)

            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=amount,
                    apy=apy,
                )
            )

        logger.info("Blend: found %d positions", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:  # noqa: ARG002
        """Blend transactions tracked via balance diffs between snapshots."""
        return []
