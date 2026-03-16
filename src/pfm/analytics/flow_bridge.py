"""Derived transaction summaries that explain fiat balance changes for AI reports."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.analytics.portfolio import is_fiat_asset
from pfm.db.models import TransactionType

if TYPE_CHECKING:
    from pfm.db.models import Snapshot, Transaction

_ZERO = Decimal(0)


def build_capital_flows_summary(txs: list[Transaction]) -> list[dict[str, object]]:
    """Summarize external flows, income, and fees for the AI prompt."""
    result: list[dict[str, object]] = []
    for tx in sorted(txs, key=lambda item: (item.date, item.usd_value), reverse=True):
        kind = _capital_flow_kind(tx.tx_type)
        if kind is None:
            continue
        result.append(
            {
                "date": tx.date.isoformat(),
                "source": tx.source_name or tx.source,
                "kind": kind,
                "tx_type": tx.tx_type.value,
                "asset": tx.asset,
                "amount": _str_decimal(tx.amount),
                "usd_value": _str_decimal(tx.usd_value),
            }
        )
    return result


def build_internal_conversions_summary(txs: list[Transaction]) -> list[dict[str, object]]:
    """Summarize asset-to-asset conversions implied by trade transactions."""
    result: list[dict[str, object]] = []
    for tx in sorted(txs, key=lambda item: (item.date, item.usd_value), reverse=True):
        if tx.tx_type != TransactionType.TRADE:
            continue
        if not tx.counterparty_asset or tx.counterparty_amount <= _ZERO:
            continue

        trade_side = tx.trade_side.lower()
        if trade_side == "sell":
            from_asset = tx.asset
            from_amount = tx.amount
            to_asset = tx.counterparty_asset
            to_amount = tx.counterparty_amount
        else:
            from_asset = tx.counterparty_asset
            from_amount = tx.counterparty_amount
            to_asset = tx.asset
            to_amount = tx.amount

        result.append(
            {
                "date": tx.date.isoformat(),
                "source": tx.source_name or tx.source,
                "from_asset": from_asset,
                "from_amount": _str_decimal(from_amount),
                "to_asset": to_asset,
                "to_amount": _str_decimal(to_amount),
                "usd_value": _str_decimal(tx.usd_value),
                "trade_side": trade_side or "buy",
            }
        )
    return result


def build_currency_flow_bridge(
    current_snaps: list[Snapshot],
    previous_snaps: list[Snapshot],
    txs: list[Transaction],
) -> list[dict[str, object]]:
    """Explain fiat balance changes using recent flows and trade cash legs."""
    current_amounts, current_usd = _sum_fiat_snapshots(current_snaps)
    previous_amounts, previous_usd = _sum_fiat_snapshots(previous_snaps)

    tx_by_currency: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "external_inflows": _ZERO,
            "external_outflows": _ZERO,
            "income": _ZERO,
            "trade_spend": _ZERO,
            "trade_proceeds": _ZERO,
        }
    )
    counterparties: dict[str, dict[tuple[str, str], Decimal]] = defaultdict(lambda: defaultdict(lambda: _ZERO))

    for tx in txs:
        if tx.tx_type == TransactionType.DEPOSIT and is_fiat_asset(tx.asset):
            tx_by_currency[tx.asset.upper()]["external_inflows"] += tx.amount
            continue
        if tx.tx_type == TransactionType.WITHDRAWAL and is_fiat_asset(tx.asset):
            tx_by_currency[tx.asset.upper()]["external_outflows"] += tx.amount
            continue
        if tx.tx_type == TransactionType.YIELD and is_fiat_asset(tx.asset):
            tx_by_currency[tx.asset.upper()]["income"] += tx.amount
            continue
        if tx.tx_type != TransactionType.TRADE or not tx.counterparty_asset or tx.counterparty_amount <= _ZERO:
            continue

        trade_side = tx.trade_side.lower()
        if trade_side == "sell" and is_fiat_asset(tx.counterparty_asset):
            currency = tx.counterparty_asset.upper()
            tx_by_currency[currency]["trade_proceeds"] += tx.counterparty_amount
            counterparties[currency][("sold", tx.asset)] += tx.amount
        elif trade_side != "sell" and is_fiat_asset(tx.counterparty_asset):
            currency = tx.counterparty_asset.upper()
            tx_by_currency[currency]["trade_spend"] += tx.counterparty_amount
            counterparties[currency][("bought", tx.asset)] += tx.amount

    rows: list[dict[str, object]] = []
    currencies = sorted(set(current_amounts) | set(previous_amounts) | set(tx_by_currency))
    for currency in currencies:
        previous_amount = previous_amounts.get(currency, _ZERO)
        current_amount = current_amounts.get(currency, _ZERO)
        previous_usd_value = previous_usd.get(currency, _ZERO)
        current_usd_value = current_usd.get(currency, _ZERO)
        delta_amount = current_amount - previous_amount
        delta_usd_value = current_usd_value - previous_usd_value

        explained = tx_by_currency[currency]
        residual = (
            delta_amount
            - explained["external_inflows"]
            + explained["external_outflows"]
            - explained["income"]
            + explained["trade_spend"]
            - explained["trade_proceeds"]
        )

        if (
            delta_amount == _ZERO
            and explained["external_inflows"] == _ZERO
            and explained["external_outflows"] == _ZERO
            and explained["income"] == _ZERO
            and explained["trade_spend"] == _ZERO
            and explained["trade_proceeds"] == _ZERO
        ):
            continue

        likely_counterparties = [
            {
                "asset": asset,
                "amount": _str_decimal(amount),
                "direction": direction,
            }
            for (direction, asset), amount in sorted(
                counterparties[currency].items(),
                key=lambda item: item[1],
                reverse=True,
            )[:3]
        ]

        rows.append(
            {
                "currency": currency,
                "previous_amount": _str_decimal(previous_amount),
                "current_amount": _str_decimal(current_amount),
                "delta_amount": _str_decimal(delta_amount),
                "previous_usd_value": _str_decimal(previous_usd_value),
                "current_usd_value": _str_decimal(current_usd_value),
                "delta_usd_value": _str_decimal(delta_usd_value),
                "explained_by_external_inflows": _str_decimal(explained["external_inflows"]),
                "explained_by_external_outflows": _str_decimal(explained["external_outflows"]),
                "explained_by_income": _str_decimal(explained["income"]),
                "explained_by_trade_spend": _str_decimal(explained["trade_spend"]),
                "explained_by_trade_proceeds": _str_decimal(explained["trade_proceeds"]),
                "residual_unexplained": _str_decimal(residual),
                "likely_counterparties": likely_counterparties,
            }
        )

    rows.sort(key=lambda row: abs(Decimal(str(row.get("delta_usd_value", "0")))), reverse=True)
    return rows


def _capital_flow_kind(tx_type: TransactionType) -> str | None:
    if tx_type == TransactionType.DEPOSIT:
        return "external_inflow"
    if tx_type == TransactionType.WITHDRAWAL:
        return "external_outflow"
    if tx_type == TransactionType.YIELD:
        return "income"
    if tx_type == TransactionType.FEE:
        return "fee"
    if tx_type == TransactionType.TRANSFER:
        return "internal_transfer"
    return None


def _sum_fiat_snapshots(snaps: list[Snapshot]) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    by_amount: dict[str, Decimal] = defaultdict(lambda: _ZERO)
    by_usd: dict[str, Decimal] = defaultdict(lambda: _ZERO)
    for snap in snaps:
        asset = snap.asset.upper()
        if not is_fiat_asset(asset):
            continue
        by_amount[asset] += snap.amount
        by_usd[asset] += snap.usd_value
    return dict(by_amount), dict(by_usd)


def _str_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")
