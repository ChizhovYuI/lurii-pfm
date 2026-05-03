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


class TestListSourcesTool:
    @pytest.mark.asyncio
    async def test_list_sources_returns_counts(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import Snapshot, Transaction, TransactionType
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_sources

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            await repo._db.execute(
                "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
                ("wise-main", "wise", "{}", 1),
            )
            await repo._db.commit()

            await repo.save_snapshots(
                [
                    Snapshot(
                        date=date(2026, 4, 1),
                        source="wise",
                        source_name="wise-main",
                        asset="USD",
                        amount=Decimal(100),
                        usd_value=Decimal(100),
                    ),
                ]
            )
            await repo.save_transactions(
                [
                    Transaction(
                        date=date(2026, 4, 1),
                        source="wise",
                        source_name="wise-main",
                        tx_type=TransactionType.DEPOSIT,
                        asset="USD",
                        amount=Decimal(50),
                        usd_value=Decimal(50),
                        tx_id="ls-tx-1",
                    ),
                ]
            )

            parsed = json.loads(await list_sources(ctx))
            assert parsed["count"] == 1
            entry = parsed["sources"][0]
            assert entry["name"] == "wise-main"
            assert entry["type"] == "wise"
            assert entry["enabled"] is True
            assert entry["tx_count"] == 1
            assert entry["snap_count"] == 1


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


def _make_ctx(repo, store, db_path, pricing=None):
    from unittest.mock import MagicMock

    from pfm.mcp_server import AppContext
    from pfm.pricing.coingecko import PricingService

    ctx = MagicMock()
    ctx.request_context.lifespan_context = AppContext(
        repo=repo,
        db_path=db_path,
        metadata_store=store,
        pricing=pricing or PricingService(cache_db_path=None),
    )
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
    async def test_categorization_tools_surface_source_id(self, tmp_path):
        """ADR-030 Stage 2: source_id surfaces on summary/list/detail tools."""
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import (
            categorization_summary,
            get_transaction_detail,
            list_uncategorized_transactions,
        )

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo._db.execute(
                "INSERT INTO sources (name, type, credentials, enabled) VALUES (?, ?, ?, ?)",
                ("kbank-main", "kbank", "{}", 1),
            )
            await repo._db.commit()

            await repo.save_transactions(
                [_make_tx(source_name="kbank-main", tx_id="src-id-1", raw_json=json.dumps({"k": "v"}))]
            )
            txs = await repo.get_transactions()
            tid = txs[0].id
            assert tid is not None
            kbank_id = txs[0].source_id
            assert kbank_id is not None

            summary = json.loads(await categorization_summary(ctx, source="kbank-main"))
            assert summary["sources"][0]["source_id"] == kbank_id

            uncat = json.loads(await list_uncategorized_transactions(ctx, source="kbank-main", missing_category=True))
            assert uncat["items"][0]["source_id"] == kbank_id

            detail = json.loads(await get_transaction_detail(ctx, transaction_id=tid))
            assert detail["transaction"]["source_id"] == kbank_id

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
    async def test_create_category_rule_invalid_regex_returns_envelope(self, tmp_path):
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
                    field_operator="regex",
                    field_value="(",
                ),
            )
            assert parsed["error"] == "validation"
            assert "invalid regex" in parsed["message"]
            assert "rule" not in parsed

    @pytest.mark.asyncio
    async def test_validate_rule_args_accepts_valid_regex(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import validate_rule_args

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(
                await validate_rule_args(ctx, field_operator="regex", field_value=r"^FX\b"),
            )
            assert parsed == {"valid": True}

    @pytest.mark.asyncio
    async def test_validate_rule_args_rejects_invalid_regex(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import validate_rule_args

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(
                await validate_rule_args(ctx, field_operator="regex", field_value="("),
            )
            assert parsed["valid"] is False
            assert parsed["error"] == "validation"
            assert "invalid regex" in parsed["message"]

    @pytest.mark.asyncio
    async def test_validate_rule_args_no_op_for_non_regex(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import validate_rule_args

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(
                await validate_rule_args(ctx, field_operator="contains", field_value="("),
            )
            assert parsed == {"valid": True}

    @pytest.mark.asyncio
    async def test_dry_run_category_rule_invalid_regex_returns_envelope(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import dry_run_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(
                await dry_run_category_rule(
                    ctx,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="regex",
                    field_value="(",
                ),
            )
            assert parsed["error"] == "validation"
            assert "invalid regex" in parsed["message"]

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
    async def test_bulk_delete_category_rules(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import bulk_delete_category_rules

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            r1 = await store.create_category_rule("spend", "fx")
            r2 = await store.create_category_rule("spend", "groceries")
            assert r1.id is not None
            assert r2.id is not None
            parsed = json.loads(
                await bulk_delete_category_rules(ctx, rule_ids=[r1.id, r2.id, 999999]),
            )
            assert parsed["deleted"] == [r1.id, r2.id]
            assert parsed["not_found"] == [999999]
            remaining_ids = {r.id for r in await store.get_category_rules()}
            assert r1.id not in remaining_ids
            assert r2.id not in remaining_ids

    @pytest.mark.asyncio
    async def test_audit_category_rules_flags_dead_and_shadowed(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import audit_category_rules

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [_make_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 100"}))],
            )
            # Live: matches and wins.
            live = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=100,
            )
            # Shadowed: matches but loses to live (lower priority value wins).
            shadowed = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=200,
            )
            # Dead: pattern doesn't match any tx.
            dead = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="ZZZNEVER",
            )

            parsed = json.loads(await audit_category_rules(ctx))
            ids = {r["id"]: r for r in parsed["rules"]}
            assert ids[live.id]["matched_count"] == 1
            assert ids[live.id]["winning_count"] == 1
            assert ids[shadowed.id]["matched_count"] == 1
            assert ids[shadowed.id]["winning_count"] == 0
            assert ids[dead.id]["matched_count"] == 0
            assert ids[dead.id]["winning_count"] == 0
            assert dead.id in parsed["dead"]
            assert shadowed.id in parsed["shadowed_dead"]
            assert live.id not in parsed["dead"]
            assert live.id not in parsed["shadowed_dead"]

    @pytest.mark.asyncio
    async def test_audit_type_rules_basic(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import TransactionType
        from pfm.db.repository import Repository
        from pfm.mcp_server import audit_type_rules

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
            live = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
            )
            dead = await store.create_type_rule(
                "income",
                field_name="kind",
                field_operator="eq",
                field_value="never_value",
            )

            parsed = json.loads(await audit_type_rules(ctx))
            ids = {r["id"]: r for r in parsed["rules"]}
            assert ids[live.id]["matched_count"] == 1
            assert ids[live.id]["winning_count"] == 1
            assert ids[dead.id]["matched_count"] == 0
            assert dead.id in parsed["dead"]

    @pytest.mark.asyncio
    async def test_bulk_delete_type_rules(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import bulk_delete_type_rules

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            r1 = await store.create_type_rule("spend", field_name="kind", field_value="purchase")
            r2 = await store.create_type_rule("income", field_name="kind", field_value="salary")
            assert r1.id is not None
            assert r2.id is not None
            parsed = json.loads(
                await bulk_delete_type_rules(ctx, rule_ids=[r1.id, 999999, r2.id]),
            )
            assert parsed["deleted"] == [r1.id, r2.id]
            assert parsed["not_found"] == [999999]

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
    async def test_dry_run_category_rule_summary_only(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import dry_run_category_rule

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            await repo.save_transactions(
                [_make_tx(tx_id=f"s{i}", raw_json='{"description":"FX 1"}') for i in range(8)],
            )
            parsed = json.loads(
                await dry_run_category_rule(
                    ctx,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="contains",
                    field_value="FX",
                    summary_only=True,
                ),
            )
            assert parsed["matched"] == 8
            assert parsed["changed"]["count"] == 8
            assert len(parsed["changed"]["sample"]) == 5
            assert isinstance(parsed["unchanged"], dict)
            assert isinstance(parsed["shadowed_by_higher"], dict)
            assert isinstance(parsed["overlapping_rules"], list)
            assert isinstance(parsed["raw_field_samples"], list)

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


class TestCashTools:
    @pytest.mark.asyncio
    async def test_get_cash_balance_returns_error_when_no_cash_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_cash_balance

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            parsed = json.loads(await get_cash_balance(ctx))
            assert parsed == {"error": "Cash source not found"}

    @pytest.mark.asyncio
    async def test_set_cash_balance_creates_today_snapshots(self, tmp_path):
        from datetime import UTC, datetime

        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import get_cash_balance, set_cash_balance
        from pfm.pricing.coingecko import PricingService

        db_path = tmp_path / "x.db"
        pricing = PricingService(cache_db_path=None)
        pricing.set_test_price("EUR", Decimal("1.1"))

        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("cash", "cash", {"fiat_currencies": "USD"})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path, pricing=pricing)

            parsed = json.loads(
                await set_cash_balance(
                    ctx,
                    balances={"USD": "100", "EUR": "50"},
                    selected_currencies=["USD", "EUR"],
                )
            )
            assert parsed["updated"] is True
            assert parsed["selected_currencies"] == ["USD", "EUR"]
            assert parsed["balances"]["USD"]["amount"] == "100"
            assert parsed["balances"]["EUR"]["usd_value"] == "55"

            today = datetime.now(tz=UTC).date()
            snapshots = await repo.get_snapshots_by_date(today)
            cash_rows = {s.asset for s in snapshots if s.source == "cash"}
            assert cash_rows == {"USD", "EUR"}

            view = json.loads(await get_cash_balance(ctx))
            assert view["source_name"] == "cash"
            assert set(view["selected_currencies"]) == {"USD", "EUR"}
            assert view["balances"]["EUR"]["price"] == "1.1"

    @pytest.mark.asyncio
    async def test_set_cash_balance_rejects_unsupported_currency(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import set_cash_balance

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("cash", "cash", {"fiat_currencies": "USD"})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(
                await set_cash_balance(
                    ctx,
                    balances={"ABC": "10"},
                    selected_currencies=["ABC"],
                )
            )
            assert "selected_currencies" in parsed["error"]

    @pytest.mark.asyncio
    async def test_list_supported_fiat_currencies(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import list_supported_fiat_currencies

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await list_supported_fiat_currencies(ctx))
            assert "USD" in parsed["supported_currencies"]
            assert "EUR" in parsed["supported_currencies"]


class TestAddManualSnapshot:
    @pytest.mark.asyncio
    async def test_saves_snapshot_with_explicit_usd_value(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_snapshot

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "lobstr-main",
                "lobstr",
                {"stellar_address": "G" + "A" * 55},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            parsed = json.loads(
                await add_manual_snapshot(
                    ctx,
                    source_name="lobstr-main",
                    asset="xlm",
                    amount="100",
                    usd_value="50",
                    apy_percent="4.25",
                    snapshot_date="2026-04-15",
                )
            )
            assert parsed["saved"] == 1
            assert parsed["asset"] == "XLM"
            assert parsed["amount"] == "100"
            assert parsed["usd_value"] == "50"
            assert parsed["apy"] == "0.0425"

    @pytest.mark.asyncio
    async def test_apy_percent_below_one_is_still_percent(self, tmp_path):
        """Regression: '0.8' must be treated as 0.8% (not 80%)."""
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_snapshot

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "lobstr-main",
                "lobstr",
                {"stellar_address": "G" + "A" * 55},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            parsed = json.loads(
                await add_manual_snapshot(
                    ctx,
                    source_name="lobstr-main",
                    asset="XLM",
                    amount="100",
                    usd_value="50",
                    apy_percent="0.8",
                )
            )
            assert parsed["apy"] == "0.008"

    @pytest.mark.asyncio
    async def test_apy_percent_none_stores_zero(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_snapshot

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "lobstr-main",
                "lobstr",
                {"stellar_address": "G" + "A" * 55},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            parsed = json.loads(
                await add_manual_snapshot(
                    ctx,
                    source_name="lobstr-main",
                    asset="XLM",
                    amount="100",
                    usd_value="50",
                )
            )
            assert parsed["apy"] == "0"

    @pytest.mark.asyncio
    async def test_rejects_unknown_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_manual_snapshot

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await add_manual_snapshot(
                    ctx,
                    source_name="missing",
                    asset="BTC",
                    amount="1",
                    usd_value="60000",
                )
            )
            assert "not found" in parsed["error"]

    @pytest.mark.asyncio
    async def test_rejects_negative_amount(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_snapshot

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "lobstr-main",
                "lobstr",
                {"stellar_address": "G" + "A" * 55},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(
                await add_manual_snapshot(
                    ctx,
                    source_name="lobstr-main",
                    asset="XLM",
                    amount="-1",
                    usd_value="0",
                )
            )
            assert "non-negative" in parsed["error"]


class TestEarnOverridesTools:
    @pytest.mark.asyncio
    async def test_set_and_list_and_delete(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import delete_earn_overrides, list_earn_overrides, set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            empty = json.loads(await list_earn_overrides(ctx, source_name="okx-main"))
            assert empty["overrides"] == []

            saved = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[{"category": "savings", "coin": "USDT", "apr": "0.05"}],
                )
            )
            assert saved["overrides"][0]["coin"] == "USDT"

            after = json.loads(await list_earn_overrides(ctx, source_name="okx-main"))
            assert after["overrides"][0]["category"] == "savings"

            cleared = json.loads(await delete_earn_overrides(ctx, source_name="okx-main"))
            assert cleared["overrides"] == []

    @pytest.mark.asyncio
    async def test_set_rejects_missing_required_fields(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[{"coin": "USDT"}],
                )
            )
            assert "category" in parsed["error"]

    @pytest.mark.asyncio
    async def test_set_rejects_invalid_apr(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[{"category": "savings", "coin": "USDT", "apr": "not-a-number"}],
                )
            )
            assert "invalid apr" in parsed["error"]

    @pytest.mark.asyncio
    async def test_set_rejects_negative_apr(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[{"category": "savings", "coin": "USDT", "apr": "-0.01"}],
                )
            )
            assert "non-negative" in parsed["error"]

    @pytest.mark.asyncio
    async def test_set_rejects_invalid_settlement_at(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[{"category": "savings", "coin": "USDT", "settlement_at": "tomorrow"}],
                )
            )
            assert "ISO date" in parsed["error"]

    @pytest.mark.asyncio
    async def test_set_accepts_valid_apr_and_settlement(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import set_earn_overrides

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await set_earn_overrides(
                    ctx,
                    source_name="okx-main",
                    overrides=[
                        {
                            "category": "savings",
                            "coin": "USDT",
                            "apr": "0.05",
                            "settlement_at": "2026-04-15",
                        }
                    ],
                )
            )
            assert parsed["overrides"][0]["apr"] == "0.05"


class TestApyRulesTools:
    @pytest.mark.asyncio
    async def test_create_list_update_delete_apy_rule(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import (
            create_apy_rule,
            delete_apy_rule,
            list_apy_rules,
            update_apy_rule,
        )

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "wallet",
                "bitget_wallet",
                {"wallet_address": "0x" + "a" * 40},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            empty = json.loads(await list_apy_rules(ctx, source_name="wallet"))
            assert empty["rules"] == []

            created = json.loads(
                await create_apy_rule(
                    ctx,
                    source_name="wallet",
                    rule={
                        "protocol": "aave",
                        "coin": "usdc",
                        "type": "base",
                        "limits": [{"from_amount": "0", "to_amount": "10000", "apy": "0.05"}],
                        "started_at": "2026-01-01",
                        "finished_at": "2026-12-31",
                    },
                )
            )
            assert len(created["rules"]) == 1
            rule_id = created["rules"][0]["id"]

            updated = json.loads(
                await update_apy_rule(
                    ctx,
                    source_name="wallet",
                    rule_id=rule_id,
                    rule={
                        "protocol": "aave",
                        "coin": "usdt",
                        "type": "bonus",
                        "limits": [{"from_amount": "0", "to_amount": None, "apy": "0.01"}],
                        "started_at": "2026-01-01",
                        "finished_at": "2026-06-30",
                    },
                )
            )
            assert updated["rules"][0]["coin"] == "usdt"

            deleted = json.loads(await delete_apy_rule(ctx, source_name="wallet", rule_id=rule_id))
            assert deleted["rules"] == []

    @pytest.mark.asyncio
    async def test_rejects_unsupported_source_type(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import list_apy_rules

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("cash", "cash", {"fiat_currencies": "USD"})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(await list_apy_rules(ctx, source_name="cash"))
            assert "not supported" in parsed["error"]

    @pytest.mark.asyncio
    async def test_delete_apy_rule_rejects_unsupported_source_type(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import delete_apy_rule

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("cash", "cash", {"fiat_currencies": "USD"})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(await delete_apy_rule(ctx, source_name="cash", rule_id="x"))
            assert "not supported" in parsed["error"]


class TestBestEffortBroadcast:
    @pytest.mark.asyncio
    async def test_skips_when_daemon_unreachable(self):
        from pfm.mcp_server import _best_effort_broadcast

        with patch("pfm.server.client.is_daemon_reachable", return_value=False):
            await _best_effort_broadcast("snapshot_updated")

    @pytest.mark.asyncio
    async def test_swallows_httpx_errors(self):
        import httpx

        from pfm.mcp_server import _best_effort_broadcast

        class _BoomClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, *_args, **_kwargs):
                raise httpx.ConnectError("nope")

        with (
            patch("pfm.server.client.is_daemon_reachable", return_value=True),
            patch("httpx.AsyncClient", _BoomClient),
        ):
            await _best_effort_broadcast("snapshot_updated")

    @pytest.mark.asyncio
    async def test_posts_event_when_daemon_reachable(self):
        from pfm.mcp_server import _best_effort_broadcast

        captured: dict[str, object] = {}

        class _OkClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, path, *, json):
                captured["path"] = path
                captured["json"] = json

                class _Resp:
                    status_code = 200

                return _Resp()

        with (
            patch("pfm.server.client.is_daemon_reachable", return_value=True),
            patch("httpx.AsyncClient", _OkClient),
        ):
            await _best_effort_broadcast("snapshot_updated")

        assert captured["path"] == "/api/v1/internal/broadcast"
        assert captured["json"] == {"type": "snapshot_updated"}


class TestCollectTools:
    @pytest.mark.asyncio
    async def test_get_collect_status_daemon_unreachable(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_collect_status

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await get_collect_status(ctx))
            assert parsed == {"daemon": "unreachable"}

    @pytest.mark.asyncio
    async def test_trigger_collect_daemon_unreachable(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import trigger_collect

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await trigger_collect(ctx))
            assert "not reachable" in parsed["error"]

    @pytest.mark.asyncio
    async def test_trigger_collect_wraps_httpx_error(self, tmp_path):
        import httpx

        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import trigger_collect

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            class _BoomClient:
                def __init__(self, *_args, **_kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return False

                async def post(self, *_args, **_kwargs):
                    raise httpx.ConnectError("boom")

            with (
                patch("pfm.server.client.is_daemon_reachable", return_value=True),
                patch("httpx.AsyncClient", _BoomClient),
            ):
                parsed = json.loads(await trigger_collect(ctx))
            assert "Daemon request failed" in parsed["error"]
            assert "boom" in parsed["error"]

    @pytest.mark.asyncio
    async def test_get_collect_status_wraps_httpx_error(self, tmp_path):
        import httpx

        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import get_collect_status

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            class _BoomClient:
                def __init__(self, *_args, **_kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return False

                async def get(self, *_args, **_kwargs):
                    raise httpx.ReadTimeout("slow")

            with (
                patch("pfm.server.client.is_daemon_reachable", return_value=True),
                patch("httpx.AsyncClient", _BoomClient),
            ):
                parsed = json.loads(await get_collect_status(ctx))
            assert "Daemon request failed" in parsed["error"]


class TestGetSourceSchema:
    @pytest.mark.asyncio
    async def test_returns_all_types_when_no_filter(self):
        from pfm.mcp_server import get_source_schema

        ctx = AsyncMock()
        parsed = json.loads(await get_source_schema(ctx))
        assert "wise" in parsed
        assert "okx" in parsed
        wise_fields = {f["name"] for f in parsed["wise"]["fields"]}
        assert "api_token" in wise_fields

    @pytest.mark.asyncio
    async def test_returns_single_type(self):
        from pfm.mcp_server import get_source_schema

        ctx = AsyncMock()
        parsed = json.loads(await get_source_schema(ctx, source_type="bitget_wallet"))
        assert list(parsed.keys()) == ["bitget_wallet"]
        rules = parsed["bitget_wallet"]["supported_apy_rules"]
        assert any(r["protocol"] == "aave" for r in rules)

    @pytest.mark.asyncio
    async def test_rejects_unknown_type(self):
        from pfm.mcp_server import get_source_schema

        ctx = AsyncMock()
        parsed = json.loads(await get_source_schema(ctx, source_type="not_a_thing"))
        assert "Unknown source type" in parsed["error"]
        assert "wise" in parsed["valid_types"]


class TestAddSource:
    @pytest.mark.asyncio
    async def test_adds_and_triggers_collect(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            with (
                patch("pfm.server.client.is_daemon_reachable", return_value=False),
            ):
                parsed = json.loads(
                    await add_source(
                        ctx,
                        name="wise-main",
                        source_type="wise",
                        credentials={"api_token": "t"},
                    )
                )

            assert parsed["added"] is True
            assert parsed["source"]["name"] == "wise-main"
            assert parsed["source"]["type"] == "wise"
            assert parsed["source"]["enabled"] is True
            assert parsed["auto_refresh"]["collect"] == "skipped"

    @pytest.mark.asyncio
    async def test_rejects_unknown_type(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await add_source(ctx, name="x", source_type="totally_made_up", credentials={}))
        assert "Unknown source type" in parsed["error"]
        assert "valid_types" in parsed

    @pytest.mark.asyncio
    async def test_rejects_missing_required_credentials(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await add_source(ctx, name="wise-main", source_type="wise", credentials={}))
        assert "Missing required field" in parsed["error"]

    @pytest.mark.asyncio
    async def test_rejects_duplicate_name(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                first = json.loads(
                    await add_source(
                        ctx,
                        name="wise-main",
                        source_type="wise",
                        credentials={"api_token": "t"},
                    )
                )
                assert first["added"] is True
                second = json.loads(
                    await add_source(
                        ctx,
                        name="wise-main",
                        source_type="wise",
                        credentials={"api_token": "t2"},
                    )
                )
        assert "already exists" in second["error"]

    @pytest.mark.asyncio
    async def test_rejects_empty_name(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await add_source(ctx, name="   ", source_type="wise", credentials={"api_token": "t"}))
        assert "must not be empty" in parsed["error"]


class TestUpdateSource:
    @pytest.mark.asyncio
    async def test_renames_and_updates_credentials(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import update_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "old"})

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(
                    await update_source(
                        ctx,
                        name="wise-main",
                        new_name="wise-eu",
                        credentials={"api_token": "new"},
                    )
                )

            assert parsed["updated"] is True
            assert parsed["source"]["name"] == "wise-eu"

            renamed = await SourceStore(db_path).get("wise-eu")
            assert json.loads(renamed.credentials)["api_token"] == "new"

    @pytest.mark.asyncio
    async def test_rejects_when_no_fields_set(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import update_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await update_source(ctx, name="wise-main"))
        assert "at least one of" in parsed["error"]

    @pytest.mark.asyncio
    async def test_rejects_rename_collision(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import update_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "t"})
            await SourceStore(db_path).add("wise-eu", "wise", {"api_token": "t"})

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await update_source(ctx, name="wise-main", new_name="wise-eu"))
        assert "already exists" in parsed["error"]

    @pytest.mark.asyncio
    async def test_rejects_unknown_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import update_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await update_source(ctx, name="ghost", enabled=False))
        assert "not found" in parsed["error"]

    @pytest.mark.asyncio
    async def test_toggles_enabled(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import update_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "t"})

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await update_source(ctx, name="wise-main", enabled=False))
        assert parsed["source"]["enabled"] is False


class TestDeleteSource:
    @pytest.mark.asyncio
    async def test_refuses_without_cascade_when_data_present(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import Snapshot
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import delete_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "t"})
            await repo.save_snapshots(
                [
                    Snapshot(
                        date=date(2026, 4, 1),
                        source="wise",
                        source_name="wise-main",
                        asset="USD",
                        amount=Decimal(100),
                        usd_value=Decimal(100),
                    ),
                ]
            )

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await delete_source(ctx, name="wise-main"))

            assert "cascade=true" in parsed["error"]
            assert parsed["snap_count"] == 1
            assert (await SourceStore(db_path).get("wise-main")) is not None

    @pytest.mark.asyncio
    async def test_cascade_deletes_data(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.models import Snapshot
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceNotFoundError, SourceStore
        from pfm.mcp_server import delete_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "t"})
            await repo.save_snapshots(
                [
                    Snapshot(
                        date=date(2026, 4, 1),
                        source="wise",
                        source_name="wise-main",
                        asset="USD",
                        amount=Decimal(100),
                        usd_value=Decimal(100),
                    ),
                ]
            )

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await delete_source(ctx, name="wise-main", cascade=True))

            assert parsed["deleted"] is True
            assert parsed["removed"]["snapshots"] == 1

            with pytest.raises(SourceNotFoundError):
                await SourceStore(db_path).get("wise-main")

    @pytest.mark.asyncio
    async def test_deletes_empty_source_without_cascade(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import delete_source

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            await SourceStore(db_path).add("wise-main", "wise", {"api_token": "t"})

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(await delete_source(ctx, name="wise-main"))

            assert parsed["deleted"] is True
            assert parsed["removed"]["snapshots"] == 0

    @pytest.mark.asyncio
    async def test_rejects_unknown_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import delete_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(await delete_source(ctx, name="ghost"))
        assert "not found" in parsed["error"]


class TestBestEffortCollect:
    @pytest.mark.asyncio
    async def test_skipped_when_daemon_unreachable(self):
        from pfm.mcp_server import _best_effort_collect

        with patch("pfm.server.client.is_daemon_reachable", return_value=False):
            result = await _best_effort_collect("wise-main")
        assert result == {"collect": "skipped", "reason": "daemon unreachable"}

    @pytest.mark.asyncio
    async def test_started_on_success(self):
        from pfm.mcp_server import _best_effort_collect

        captured: dict[str, object] = {}

        class _OkClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, path, *, json):
                captured["path"] = path
                captured["json"] = json

                class _Resp:
                    status_code = 202

                    def raise_for_status(self):
                        return None

                return _Resp()

        with (
            patch("pfm.server.client.is_daemon_reachable", return_value=True),
            patch("httpx.AsyncClient", _OkClient),
        ):
            result = await _best_effort_collect("wise-main")

        assert result == {"collect": "started", "source": "wise-main"}
        assert captured["path"] == "/api/v1/collect"
        assert captured["json"] == {"source": "wise-main"}

    @pytest.mark.asyncio
    async def test_skipped_on_409(self):
        from pfm.mcp_server import _best_effort_collect

        class _ConflictClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, *_args, **_kwargs):
                class _Resp:
                    status_code = 409

                return _Resp()

        with (
            patch("pfm.server.client.is_daemon_reachable", return_value=True),
            patch("httpx.AsyncClient", _ConflictClient),
        ):
            result = await _best_effort_collect("wise-main")

        assert result["collect"] == "skipped"
        reason = result["reason"]
        assert isinstance(reason, str)
        assert "another collection" in reason


class TestGenericSource:
    @pytest.mark.asyncio
    async def test_add_generic_source_with_group_hint(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                parsed = json.loads(
                    await add_source(
                        ctx,
                        name="generic-vault",
                        source_type="generic",
                        credentials={"label": "Defi vault", "group_hint": "defi"},
                    )
                )

            assert parsed["added"] is True
            assert parsed["source"]["type"] == "generic"

    @pytest.mark.asyncio
    async def test_generic_rejects_invalid_group_hint(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await add_source(
                    ctx,
                    name="generic-x",
                    source_type="generic",
                    credentials={"group_hint": "garbage"},
                )
            )
        assert "Invalid group_hint" in parsed["error"]

    @pytest.mark.asyncio
    async def test_multiple_generic_sources_allowed(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_source

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")

            with patch("pfm.server.client.is_daemon_reachable", return_value=False):
                first = json.loads(
                    await add_source(
                        ctx,
                        name="generic-a",
                        source_type="generic",
                        credentials={"label": "A"},
                    )
                )
                second = json.loads(
                    await add_source(
                        ctx,
                        name="generic-b",
                        source_type="generic",
                        credentials={"label": "B"},
                    )
                )
        assert first["added"] is True
        assert second["added"] is True


class TestAddManualTransaction:
    @pytest.mark.asyncio
    async def test_saves_deposit_with_explicit_usd_value(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_transaction

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add(
                "generic-misc",
                "generic",
                {"label": "Misc"},
            )
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)

            parsed = json.loads(
                await add_manual_transaction(
                    ctx,
                    source_name="generic-misc",
                    tx_type="deposit",
                    asset="USD",
                    amount="100",
                    usd_value="100",
                    tx_date="2026-04-15",
                )
            )
            assert parsed["saved"] == 1
            assert parsed["tx_type"] == "deposit"
            assert parsed["asset"] == "USD"
            assert parsed["amount"] == "100"
            assert parsed["usd_value"] == "100"

            txs = await repo.get_transactions(source_name="generic-misc")
            assert len(txs) == 1
            assert txs[0].tx_type.value == "deposit"

    @pytest.mark.asyncio
    async def test_saves_trade_with_counterparty(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_transaction

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("generic-misc", "generic", {})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(
                await add_manual_transaction(
                    ctx,
                    source_name="generic-misc",
                    tx_type="trade",
                    asset="BTC",
                    amount="0.5",
                    usd_value="32500",
                    counterparty_asset="usd",
                    counterparty_amount="-32500",
                )
            )
            assert parsed["saved"] == 1
            assert parsed["counterparty_asset"] == "USD"
            assert parsed["counterparty_amount"] == "-32500"

    @pytest.mark.asyncio
    async def test_rejects_unknown_tx_type(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_transaction

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("generic-misc", "generic", {})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(
                await add_manual_transaction(
                    ctx,
                    source_name="generic-misc",
                    tx_type="totally_made_up",
                    asset="USD",
                    amount="100",
                    usd_value="100",
                )
            )
        assert "Invalid tx_type" in parsed["error"]

    @pytest.mark.asyncio
    async def test_rejects_unknown_source(self, tmp_path):
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.mcp_server import add_manual_transaction

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, tmp_path / "x.db")
            parsed = json.loads(
                await add_manual_transaction(
                    ctx,
                    source_name="missing",
                    tx_type="deposit",
                    asset="USD",
                    amount="100",
                    usd_value="100",
                )
            )
        assert "not found" in parsed["error"]

    @pytest.mark.asyncio
    async def test_skips_pricing_when_usd_value_provided(self, tmp_path):
        """Asset like REALESTATE not on CoinGecko — usd_value bypasses price lookup."""
        from pfm.db.metadata_store import MetadataStore
        from pfm.db.repository import Repository
        from pfm.db.source_store import SourceStore
        from pfm.mcp_server import add_manual_transaction

        db_path = tmp_path / "x.db"
        async with Repository(db_path) as repo:
            await SourceStore(db_path).add("generic-house", "generic", {})
            store = MetadataStore(repo.connection)
            ctx = _make_ctx(repo, store, db_path)
            parsed = json.loads(
                await add_manual_transaction(
                    ctx,
                    source_name="generic-house",
                    tx_type="deposit",
                    asset="REALESTATE",
                    amount="1",
                    usd_value="450000",
                )
            )
            assert parsed["saved"] == 1
            assert parsed["asset"] == "REALESTATE"
            assert parsed["usd_value"] == "450000"
