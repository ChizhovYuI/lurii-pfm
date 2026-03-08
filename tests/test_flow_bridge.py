from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.flow_bridge import (
    build_capital_flows_summary,
    build_currency_flow_bridge,
    build_internal_conversions_summary,
)
from pfm.db.models import Snapshot, Transaction, TransactionType


def _snapshot(*, when: date, asset: str, amount: str, usd_value: str) -> Snapshot:
    return Snapshot(
        date=when,
        source="ibkr",
        source_name="ibkr-main",
        asset=asset,
        amount=Decimal(amount),
        usd_value=Decimal(usd_value),
    )


def _trade_buy_gbp_vwra() -> Transaction:
    return Transaction(
        date=date(2026, 3, 8),
        source="ibkr",
        source_name="ibkr-main",
        tx_type=TransactionType.TRADE,
        asset="VWRA",
        amount=Decimal("37.2"),
        usd_value=Decimal(6400),
        counterparty_asset="GBP",
        counterparty_amount=Decimal(5000),
        trade_side="buy",
    )


def test_currency_flow_bridge_explains_fiat_drop_with_trade_spend():
    rows = build_currency_flow_bridge(
        current_snaps=[
            _snapshot(when=date(2026, 3, 8), asset="GBP", amount="0", usd_value="0"),
            _snapshot(when=date(2026, 3, 8), asset="VWRA", amount="37.2", usd_value="6400"),
        ],
        previous_snaps=[_snapshot(when=date(2026, 3, 7), asset="GBP", amount="5000", usd_value="6400")],
        txs=[_trade_buy_gbp_vwra()],
    )

    assert rows[0]["currency"] == "GBP"
    assert rows[0]["delta_amount"] == "-5000"
    assert rows[0]["explained_by_trade_spend"] == "5000"
    assert rows[0]["residual_unexplained"] == "0"
    assert rows[0]["likely_counterparties"] == [{"asset": "VWRA", "amount": "37.2", "direction": "bought"}]


def test_currency_flow_bridge_explains_fiat_increase_with_trade_proceeds():
    tx = Transaction(
        date=date(2026, 3, 8),
        source="ibkr",
        source_name="ibkr-main",
        tx_type=TransactionType.TRADE,
        asset="VWRA",
        amount=Decimal(10),
        usd_value=Decimal(1500),
        counterparty_asset="GBP",
        counterparty_amount=Decimal(1200),
        trade_side="sell",
    )

    rows = build_currency_flow_bridge(
        current_snaps=[_snapshot(when=date(2026, 3, 8), asset="GBP", amount="1200", usd_value="1500")],
        previous_snaps=[_snapshot(when=date(2026, 3, 7), asset="GBP", amount="0", usd_value="0")],
        txs=[tx],
    )

    assert rows[0]["currency"] == "GBP"
    assert rows[0]["explained_by_trade_proceeds"] == "1200"
    assert rows[0]["residual_unexplained"] == "0"
    assert rows[0]["likely_counterparties"] == [{"asset": "VWRA", "amount": "10", "direction": "sold"}]


def test_currency_flow_bridge_keeps_unexplained_residual_without_trade_history():
    rows = build_currency_flow_bridge(
        current_snaps=[_snapshot(when=date(2026, 3, 8), asset="GBP", amount="3000", usd_value="3800")],
        previous_snaps=[_snapshot(when=date(2026, 3, 7), asset="GBP", amount="5000", usd_value="6400")],
        txs=[],
    )

    assert rows[0]["currency"] == "GBP"
    assert rows[0]["delta_amount"] == "-2000"
    assert rows[0]["residual_unexplained"] == "-2000"


def test_internal_conversions_summary_maps_buy_trade_correctly():
    rows = build_internal_conversions_summary([_trade_buy_gbp_vwra()])

    assert rows == [
        {
            "date": "2026-03-08",
            "source": "ibkr-main",
            "from_asset": "GBP",
            "from_amount": "5000",
            "to_asset": "VWRA",
            "to_amount": "37.2",
            "usd_value": "6400",
            "trade_side": "buy",
        }
    ]


def test_capital_flows_summary_classifies_income_and_external_flows():
    txs = [
        Transaction(
            date=date(2026, 3, 8),
            source="wise",
            source_name="wise-main",
            tx_type=TransactionType.DEPOSIT,
            asset="GBP",
            amount=Decimal(500),
            usd_value=Decimal(640),
        ),
        Transaction(
            date=date(2026, 3, 7),
            source="ibkr",
            source_name="ibkr-main",
            tx_type=TransactionType.DIVIDEND,
            asset="USD",
            amount=Decimal(12),
            usd_value=Decimal(12),
        ),
        Transaction(
            date=date(2026, 3, 6),
            source="wise",
            source_name="wise-main",
            tx_type=TransactionType.WITHDRAWAL,
            asset="GBP",
            amount=Decimal(100),
            usd_value=Decimal(128),
        ),
    ]

    rows = build_capital_flows_summary(txs)

    assert rows[0]["kind"] == "external_inflow"
    assert rows[1]["kind"] == "income"
    assert rows[2]["kind"] == "external_outflow"
