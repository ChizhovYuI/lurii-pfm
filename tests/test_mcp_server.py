"""Tests for the MCP server tools."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from pfm.mcp_server import (
    _dec,
    _dec2,
    _json,
    _parse_date,
    _pct,
    _today,
)


class TestHelpers:
    def test_dec_formats_decimal(self):
        assert _dec(Decimal("123.4500")) == "123.45"

    def test_dec2_rounds_to_2dp(self):
        assert _dec2(Decimal("123.456")) == "123.46"

    def test_pct_formats_percentage(self):
        assert _pct(Decimal("56.789")) == "56.79%"

    def test_parse_date_with_value(self):
        assert _parse_date("2024-01-15") == date(2024, 1, 15)

    def test_parse_date_default_today(self):
        result = _parse_date(None)
        assert result == _today()

    def test_json_serializes_decimal_and_date(self):
        result = _json({"amount": Decimal("100.5"), "date": date(2024, 1, 1)})
        parsed = json.loads(result)
        assert parsed["amount"] == "100.5"
        assert parsed["date"] == "2024-01-01"


class TestGetSources:
    @pytest.mark.asyncio
    async def test_returns_sources_list(self):
        from pfm.db.models import Source
        from pfm.mcp_server import get_sources

        mock_sources = [
            Source(name="okx", type="okx", credentials="{}", enabled=True),
            Source(name="wise", type="wise", credentials="{}", enabled=False),
        ]

        mock_store = AsyncMock()
        mock_store.list_all.return_value = mock_sources

        mock_ctx = AsyncMock()
        mock_ctx.request_context.lifespan_context.db_path = "/tmp/test.db"

        with patch("pfm.db.source_store.SourceStore", return_value=mock_store):
            result = await get_sources(mock_ctx)
            parsed = json.loads(result)
            assert len(parsed["sources"]) == 2
            assert parsed["sources"][0]["name"] == "okx"
            assert parsed["sources"][0]["enabled"] is True
            assert parsed["sources"][1]["name"] == "wise"
            assert parsed["sources"][1]["enabled"] is False


class TestGetTransactions:
    @pytest.mark.asyncio
    async def test_returns_transactions(self):
        from pfm.db.models import Transaction, TransactionType
        from pfm.mcp_server import get_transactions

        mock_txs = [
            Transaction(
                date=date(2024, 1, 15),
                source="wise",
                source_name="wise-main",
                tx_type=TransactionType.WITHDRAWAL,
                asset="GBP",
                amount=Decimal(5000),
                usd_value=Decimal(6300),
                counterparty_asset="",
                counterparty_amount=Decimal(0),
                tx_id="tx1",
                raw_json="",
                trade_side="",
            ),
        ]

        mock_ctx = AsyncMock()
        mock_repo = AsyncMock()
        mock_repo.get_transactions.return_value = mock_txs
        mock_ctx.request_context.lifespan_context.repo = mock_repo

        result = await get_transactions(mock_ctx, source="wise", limit=10)
        parsed = json.loads(result)
        assert parsed["count"] == 1
        tx = parsed["transactions"][0]
        assert tx["source"] == "wise"
        assert tx["source_name"] == "wise-main"
        assert tx["type"] == "withdrawal"
        assert tx["asset"] == "GBP"
        assert tx["amount"] == "5000.00"
        assert tx["trade_side"] is None
        mock_repo.get_transactions.assert_awaited_once_with(source="wise", source_name=None, start=None, end=None)


class TestGetPnl:
    @pytest.mark.asyncio
    async def test_returns_pnl(self):
        from pfm.analytics.pnl import AssetPnl, PnlResult
        from pfm.mcp_server import get_pnl

        mock_pnl = PnlResult(
            start_date=date(2024, 1, 8),
            end_date=date(2024, 1, 15),
            start_value=Decimal(9500),
            end_value=Decimal(10000),
            absolute_change=Decimal(500),
            percentage_change=Decimal("5.26"),
            top_gainers=[
                AssetPnl(
                    asset="BTC",
                    start_value=Decimal(6000),
                    end_value=Decimal(7000),
                    absolute_change=Decimal(1000),
                    percentage_change=Decimal("16.67"),
                ),
            ],
            top_losers=[],
        )

        mock_ctx = AsyncMock()
        mock_ctx.request_context.lifespan_context.repo = AsyncMock()

        with patch("pfm.analytics.pnl.compute_pnl", new_callable=AsyncMock, return_value=mock_pnl):
            result = await get_pnl(mock_ctx, period="weekly", date_str="2024-01-15")
            parsed = json.loads(result)
            assert parsed["period"] == "weekly"
            assert parsed["absolute_change_usd"] == "500.00"
            assert parsed["percentage_change"] == "5.26%"
            assert len(parsed["top_gainers"]) == 1
            assert parsed["top_gainers"][0]["asset"] == "BTC"


class TestEntryPoint:
    def test_main_calls_mcp_run(self):
        from pfm.mcp_server import main

        with patch("pfm.mcp_server.mcp") as mock_mcp:
            main()
            mock_mcp.run.assert_called_once_with(transport="stdio")
