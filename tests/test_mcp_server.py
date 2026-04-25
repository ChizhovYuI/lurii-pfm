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


# ── ADR-028 categorization tools ─────────────────────────────────────────


def _make_tx(
    *,
    source_name: str = "kbank",
    tx_type=None,
    tx_id: str = "",
    raw_json: str = "",
    asset: str = "USD",
    d: date = date(2026, 3, 1),
):
    from pfm.db.models import Transaction, TransactionType

    return Transaction(
        date=d,
        source=source_name,
        source_name=source_name,
        tx_type=tx_type or TransactionType.SPEND,
        asset=asset,
        amount=Decimal(10),
        usd_value=Decimal(10),
        tx_id=tx_id,
        raw_json=raw_json,
    )


def _make_ctx(repo, store, db_path):
    from unittest.mock import MagicMock

    from pfm.mcp_server import AppContext

    ctx = MagicMock()
    ctx.request_context.lifespan_context = AppContext(repo=repo, db_path=db_path, metadata_store=store)
    return ctx


class TestCategorizationTools:
    @pytest.mark.asyncio
    async def test_list_category_rules_includes_new_rule(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_category_rules

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            rule = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )
            parsed = json.loads(await list_category_rules(ctx))
            ids = [r["id"] for r in parsed["rules"]]
            assert rule.id in ids
            row = next(r for r in parsed["rules"] if r["id"] == rule.id)
            assert row["result_category"] == "fx"
            assert row["field_value"] == "FX"

    @pytest.mark.asyncio
    async def test_list_type_rules_returns_data(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_type_rules

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            rule = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
            )
            parsed = json.loads(await list_type_rules(ctx))
            assert any(r["id"] == rule.id and r["result_type"] == "spend" for r in parsed["rules"])

    @pytest.mark.asyncio
    async def test_list_categories_filters_by_type(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_categories

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(await list_categories(ctx, tx_type="spend"))
            assert parsed["count"] >= 1
            assert all(c["tx_type"] == "spend" for c in parsed["categories"])

    @pytest.mark.asyncio
    async def test_categorization_summary_counts_per_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import TransactionType
        from pfm.db.repository import Repository
        from pfm.mcp_server import categorization_summary

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [
                    _make_tx(source_name="kbank", tx_type=TransactionType.UNKNOWN, tx_id="k1"),
                    _make_tx(source_name="kbank", tx_id="k2"),
                ]
            )
            parsed = json.loads(await categorization_summary(ctx, source="kbank"))
            assert len(parsed["sources"]) == 1
            row = parsed["sources"][0]
            assert row["source_name"] == "kbank"
            assert row["total"] == 2
            assert row["unknown_type"] == 1
            assert row["no_category"] == 2

    @pytest.mark.asyncio
    async def test_list_uncategorized_transactions_default_keys_only(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_uncategorized_transactions

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [_make_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 100", "kind": "purchase"}))]
            )
            parsed = json.loads(await list_uncategorized_transactions(ctx, missing_category=True))
            assert parsed["total"] == 1
            item = parsed["items"][0]
            assert item["tx_id"] == "s1"
            assert item["id"] is not None
            assert sorted(item["raw_keys"]) == ["description", "kind"]
            assert "raw_sample" not in item

    @pytest.mark.asyncio
    async def test_list_uncategorized_transactions_with_raw_sample(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_uncategorized_transactions

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [_make_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 100", "kind": "purchase"}))]
            )
            parsed = json.loads(
                await list_uncategorized_transactions(
                    ctx,
                    missing_category=True,
                    include_raw_sample=True,
                ),
            )
            item = parsed["items"][0]
            assert item["raw_sample"]["description"] == "FX 100"
            assert item["raw_sample"]["kind"] == "purchase"

    @pytest.mark.asyncio
    async def test_get_transaction_detail_returns_full_data(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_transaction_detail

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions([_make_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1"}))])
            txs = await repo.get_transactions()
            tid = txs[0].id
            assert tid is not None
            rule = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )

            parsed = json.loads(await get_transaction_detail(ctx, transaction_id=tid))
            assert parsed["transaction"]["tx_id"] == "s1"
            assert parsed["raw_json"] == {"description": "FX 1"}
            assert parsed["winning_rule_id"] == rule.id
            assert parsed["winning_category_rule"]["id"] == rule.id
            assert parsed["winning_category_rule"]["result_category"] == "fx"
            assert parsed["winning_type_rule"] is None

    @pytest.mark.asyncio
    async def test_get_transaction_detail_surfaces_winning_type_rule(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import TransactionType
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_transaction_detail

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [
                    _make_tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    ),
                ],
            )
            txs = await repo.get_transactions()
            tid = txs[0].id
            assert tid is not None
            type_rule = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
            )

            parsed = json.loads(await get_transaction_detail(ctx, transaction_id=tid))
            assert parsed["winning_type_rule"]["id"] == type_rule.id
            assert parsed["winning_type_rule"]["result_type"] == "spend"

    @pytest.mark.asyncio
    async def test_get_transaction_detail_not_found(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_transaction_detail

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(await get_transaction_detail(ctx, transaction_id=99999))
            assert parsed["error"] == "not found"
            assert parsed["transaction_id"] == 99999

    @pytest.mark.asyncio
    async def test_create_category_rule_happy_path(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import create_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(
                await create_category_rule(
                    ctx,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="contains",
                    field_value="FX",
                )
            )
            assert parsed["rule"]["result_category"] == "fx"
            assert parsed["rule"]["builtin"] is False
            saved = await store.get_category_rules()
            assert any(r.id == parsed["rule"]["id"] for r in saved)

    @pytest.mark.asyncio
    async def test_create_category_rule_invalid_regex_raises(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import create_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            with pytest.raises(ValueError, match="invalid regex"):
                await create_category_rule(
                    ctx,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="regex",
                    field_value="(",
                )

    @pytest.mark.asyncio
    async def test_delete_category_rule_returns_status(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import delete_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            rule = await store.create_category_rule("spend", "fx")
            assert rule.id is not None
            ok = json.loads(await delete_category_rule(ctx, rule_id=rule.id))
            assert ok == {"deleted": True, "rule_id": rule.id}
            missing = json.loads(await delete_category_rule(ctx, rule_id=999999))
            assert missing == {"deleted": False, "rule_id": 999999}

    @pytest.mark.asyncio
    async def test_create_and_delete_type_rule_smoke(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import create_type_rule, delete_type_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            created = json.loads(
                await create_type_rule(
                    ctx,
                    result_type="spend",
                    field_name="kind",
                    field_operator="eq",
                    field_value="purchase",
                )
            )
            rid = created["rule"]["id"]
            deleted = json.loads(await delete_type_rule(ctx, rule_id=rid))
            assert deleted == {"deleted": True, "rule_id": rid}

    @pytest.mark.asyncio
    async def test_set_transaction_category_writes_metadata_and_records_choice(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_transaction_category

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions([_make_tx(tx_id="s1", raw_json='{"description":"X"}')])
            tid = (await repo.get_transactions())[0].id
            assert tid is not None
            parsed = json.loads(await set_transaction_category(ctx, transaction_id=tid, category="dining"))
            assert parsed["metadata"]["category"] == "dining"
            assert parsed["metadata"]["category_source"] == "manual"

            cursor = await repo.connection.execute(
                "SELECT chosen_category, previous_category FROM user_category_choices WHERE transaction_id = ?",
                (tid,),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "dining"

    @pytest.mark.asyncio
    async def test_link_and_unlink_transfer_round_trip(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import link_transfer, unlink_transfer

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions([_make_tx(tx_id="a"), _make_tx(tx_id="b")])
            txs = await repo.get_transactions()
            a_id, b_id = txs[0].id, txs[1].id
            assert a_id is not None
            assert b_id is not None

            linked = json.loads(await link_transfer(ctx, tx_id_a=a_id, tx_id_b=b_id))
            assert linked["ok"] is True
            meta_a = await store.get_metadata(a_id)
            assert meta_a is not None
            assert meta_a.is_internal_transfer is True
            assert meta_a.transfer_pair_id == b_id

            unlinked = json.loads(await unlink_transfer(ctx, transaction_id=a_id))
            assert unlinked["ok"] is True
            meta_a_after = await store.get_metadata(a_id)
            assert meta_a_after is not None
            assert meta_a_after.is_internal_transfer is False

    @pytest.mark.asyncio
    async def test_dry_run_category_rule_wires(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import dry_run_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions([_make_tx(tx_id="s1", raw_json='{"description":"FX 1"}')])
            parsed = json.loads(
                await dry_run_category_rule(
                    ctx,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="contains",
                    field_value="FX",
                )
            )
            assert parsed["matched"] == 1
            assert parsed["changed"][0]["tx_id"] == "s1"

    @pytest.mark.asyncio
    async def test_dry_run_type_rule_wires(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import TransactionType
        from pfm.db.repository import Repository
        from pfm.mcp_server import dry_run_type_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [_make_tx(tx_id="u1", tx_type=TransactionType.UNKNOWN, raw_json='{"kind":"purchase"}')]
            )
            parsed = json.loads(
                await dry_run_type_rule(
                    ctx,
                    result_type="spend",
                    field_name="kind",
                    field_operator="eq",
                    field_value="purchase",
                )
            )
            assert parsed["matched"] == 1
            assert parsed["changed"][0]["proposed_type"] == "spend"

    @pytest.mark.asyncio
    async def test_apply_categorization_returns_counts(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import apply_categorization

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(await apply_categorization(ctx))
            assert "total" in parsed
            assert "type_resolved" in parsed
            assert "transfers" in parsed
            assert "categorized" in parsed
