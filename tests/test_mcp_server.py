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
    _memory_payload,
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


class TestAIReportMemory:
    @pytest.mark.asyncio
    async def test_get_ai_report_memory_tool_returns_current_memory(self):
        from pfm.mcp_server import get_ai_report_memory

        mock_ctx = AsyncMock()
        mock_ctx.request_context.lifespan_context.db_path = "/tmp/test.db"

        with patch("pfm.mcp_server.AIReportMemoryStore") as mock_store_cls:
            mock_store = AsyncMock()
            mock_store.get.return_value = "## Location & Expenses\nLiving in Thailand."
            mock_store_cls.return_value = mock_store

            result = await get_ai_report_memory(mock_ctx)

        parsed = json.loads(result)
        assert parsed == {
            "memory": "## Location & Expenses\nLiving in Thailand.",
            "length": 42,
            "normalized": True,
            "max_chars": 4000,
        }

    @pytest.mark.asyncio
    async def test_set_ai_report_memory_tool_persists_normalized_memory(self):
        from pfm.mcp_server import set_ai_report_memory

        mock_ctx = AsyncMock()
        mock_ctx.request_context.lifespan_context.db_path = "/tmp/test.db"

        with (
            patch("pfm.mcp_server.normalize_ai_report_memory", return_value="## Investment Profile\nGoal: FIRE."),
            patch("pfm.mcp_server.AIReportMemoryStore") as mock_store_cls,
        ):
            mock_store = AsyncMock()
            mock_store_cls.return_value = mock_store

            result = await set_ai_report_memory(mock_ctx, "  ## Investment Profile\r\nGoal: FIRE.\r\n")

        parsed = json.loads(result)
        mock_store.set.assert_awaited_once_with("## Investment Profile\nGoal: FIRE.")
        assert parsed["updated"] is True
        assert parsed["memory"] == "## Investment Profile\nGoal: FIRE."
        assert parsed["normalized"] is True

    @pytest.mark.asyncio
    async def test_clear_ai_report_memory_tool_clears_value(self):
        from pfm.mcp_server import clear_ai_report_memory

        mock_ctx = AsyncMock()
        mock_ctx.request_context.lifespan_context.db_path = "/tmp/test.db"

        with patch("pfm.mcp_server.AIReportMemoryStore") as mock_store_cls:
            mock_store = AsyncMock()
            mock_store_cls.return_value = mock_store

            result = await clear_ai_report_memory(mock_ctx)

        parsed = json.loads(result)
        mock_store.set.assert_awaited_once_with("")
        assert parsed["updated"] is True
        assert parsed["cleared"] is True
        assert parsed["memory"] == ""
        assert parsed["length"] == 0

    @pytest.mark.asyncio
    async def test_resource_ai_report_memory_returns_current_memory(self):
        from pfm.mcp_server import resource_ai_report_memory

        with (
            patch("pfm.server.daemon.get_db_path", return_value="/tmp/test.db"),
            patch("pfm.mcp_server.AIReportMemoryStore") as mock_store_cls,
        ):
            mock_store = AsyncMock()
            mock_store.get.return_value = "## Location & Expenses\nLiving in Thailand."
            mock_store_cls.return_value = mock_store

            result = await resource_ai_report_memory()

        parsed = json.loads(result)
        assert parsed["memory"] == "## Location & Expenses\nLiving in Thailand."
        assert parsed["length"] == 42


class TestWeeklyReportPromptPack:
    @pytest.mark.asyncio
    async def test_resource_weekly_report_prompt_returns_prompt_pack(self):
        from pfm.mcp_server import resource_weekly_report_prompt

        pack = {
            "kind": "weekly_report_prompt_pack",
            "prompt_version": 2,
            "workflow": "section_by_section",
            "includes_memory": True,
            "sections": [{"title": "Market Context"}],
        }

        mock_repo = AsyncMock()
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_repo
        cm.__aexit__.return_value = None

        with (
            patch("pfm.server.daemon.get_db_path", return_value="/tmp/test.db"),
            patch("pfm.db.repository.Repository", return_value=cm),
            patch("pfm.ai.build_weekly_report_prompt_pack", new=AsyncMock(return_value=pack)),
        ):
            result = await resource_weekly_report_prompt()

        parsed = json.loads(result)
        assert parsed["kind"] == "weekly_report_prompt_pack"
        assert parsed["prompt_version"] == 2
        assert parsed["workflow"] == "section_by_section"
        assert parsed["includes_memory"] is True
        assert len(parsed["sections"]) == 1

    @pytest.mark.asyncio
    async def test_resource_weekly_report_prompt_handles_no_snapshots(self):
        from pfm.mcp_server import resource_weekly_report_prompt

        pack = {
            "kind": "weekly_report_prompt_pack",
            "prompt_version": 2,
            "as_of_date": "2026-03-08",
            "error": "No snapshots available",
        }

        mock_repo = AsyncMock()
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_repo
        cm.__aexit__.return_value = None

        with (
            patch("pfm.server.daemon.get_db_path", return_value="/tmp/test.db"),
            patch("pfm.db.repository.Repository", return_value=cm),
            patch("pfm.ai.build_weekly_report_prompt_pack", new=AsyncMock(return_value=pack)),
        ):
            result = await resource_weekly_report_prompt()

        parsed = json.loads(result)
        assert parsed["error"] == "No snapshots available"


class TestLegacyPrompts:
    @pytest.mark.asyncio
    async def test_investment_review_uses_prompt_pack_builder(self):
        from pfm.mcp_server import investment_review

        pack = {
            "system_prompt": "system prompt",
            "analytics_context": "analytics block",
            "investor_memory": "## Profile\nGoal: FIRE.",
            "sections": [
                {"title": "Market Context"},
                {"title": "Portfolio Health Assessment"},
            ],
        }

        mock_repo = AsyncMock()
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_repo
        cm.__aexit__.return_value = None

        with (
            patch("pfm.server.daemon.get_db_path", return_value="/tmp/test.db"),
            patch("pfm.db.repository.Repository", return_value=cm),
            patch("pfm.ai.build_weekly_report_prompt_pack", new=AsyncMock(return_value=pack)),
        ):
            result = await investment_review("risk")

        assert "Use the weekly report prompt pack below as the authoritative contract." in result
        assert "system prompt" in result
        assert "analytics block" in result
        assert "Please focus on: risk" in result

    @pytest.mark.asyncio
    async def test_weekly_check_in_mentions_conversions_when_present(self):
        from pfm.analytics.pnl import PnlResult
        from pfm.db.models import Transaction, TransactionType
        from pfm.mcp_server import weekly_check_in

        txs = [
            Transaction(
                date=date(2024, 1, 15),
                source="ibkr",
                source_name="ibkr-main",
                tx_type=TransactionType.TRADE,
                asset="VWRA",
                amount=Decimal(10),
                usd_value=Decimal(1000),
                counterparty_asset="GBP",
                counterparty_amount=Decimal(800),
                trade_side="buy",
            )
        ]

        pnl = PnlResult(
            start_date=date(2024, 1, 8),
            end_date=date(2024, 1, 15),
            start_value=Decimal(9000),
            end_value=Decimal(10000),
            absolute_change=Decimal(1000),
            percentage_change=Decimal("11.11"),
            top_gainers=[],
            top_losers=[],
        )

        mock_repo = AsyncMock()
        mock_repo.get_transactions.return_value = txs
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_repo
        cm.__aexit__.return_value = None

        with (
            patch("pfm.server.daemon.get_db_path", return_value="/tmp/test.db"),
            patch("pfm.db.repository.Repository", return_value=cm),
            patch("pfm.analytics.compute_net_worth", new=AsyncMock(return_value=Decimal(10000))),
            patch("pfm.analytics.pnl.compute_pnl", new=AsyncMock(return_value=pnl)),
        ):
            result = await weekly_check_in()

        assert "Recent internal conversions / redeployments:" in result
        assert "GBP 800 -> VWRA 10" in result


class TestEntryPoint:
    def test_main_calls_mcp_run(self):
        from pfm.mcp_server import main

        with patch("pfm.mcp_server.mcp") as mock_mcp:
            main()
            mock_mcp.run.assert_called_once_with(transport="stdio")


def test_memory_payload_shape():
    payload = _memory_payload("## Profile\nGoal: FIRE.")
    assert payload == {
        "memory": "## Profile\nGoal: FIRE.",
        "length": 22,
        "normalized": True,
        "max_chars": 4000,
    }
