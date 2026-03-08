"""Tests for weekly report prompt pack strategy selection."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pfm.ai.prompts import AnalyticsSummary
from pfm.ai.weekly_report_prompt_pack import build_weekly_report_prompt_pack


def _sample_analytics() -> AnalyticsSummary:
    return AnalyticsSummary(
        as_of_date=date(2024, 1, 15),
        net_worth_usd=Decimal("12345.67"),
        allocation_by_asset='[{"asset":"BTC","usd_value":"7000","asset_type":"crypto","percentage":"56.7"}]',
        allocation_by_source='[{"source":"okx","usd_value":"7000","percentage":"56.7"}]',
        allocation_by_category='[{"category":"crypto","usd_value":"7000","percentage":"56.7"}]',
        currency_exposure='[{"currency":"USD","usd_value":"5000","percentage":"40"}]',
        risk_metrics='{"concentration_percentage":"56.7"}',
        capital_flows="[]",
        internal_conversions="[]",
        currency_flow_bridge="[]",
    )


@pytest.mark.asyncio
async def test_build_weekly_report_prompt_pack_uses_single_shot_json_for_deepseek_chat():
    repo = AsyncMock()
    repo.get_latest_snapshots.return_value = [SimpleNamespace(date=date(2024, 1, 15))]
    build_summary = AsyncMock(return_value=_sample_analytics())

    with (
        patch("pfm.ai.weekly_report_prompt_pack.build_analytics_summary", new=build_summary),
        patch("pfm.ai.weekly_report_prompt_pack.AIReportMemoryStore") as memory_store_cls,
        patch("pfm.ai.weekly_report_prompt_pack.AIProviderStore") as provider_store_cls,
    ):
        memory_store = AsyncMock()
        memory_store.get.return_value = "## Profile\nGoal: FIRE."
        memory_store_cls.return_value = memory_store

        provider_store = AsyncMock()
        provider_store.get_active.return_value = SimpleNamespace(type="deepseek", model="deepseek-chat")
        provider_store_cls.return_value = provider_store

        pack = await build_weekly_report_prompt_pack(repo, Path("/tmp/test.db"), date(2024, 1, 15))

    assert pack["workflow"] == "single_shot_json"
    assert pack["system_prompt"]
    assert pack["investor_memory"] == "## Profile\nGoal: FIRE."
    assert pack["sections"] == ()


@pytest.mark.asyncio
async def test_build_weekly_report_prompt_pack_keeps_section_pipeline_for_other_models():
    repo = AsyncMock()
    repo.get_latest_snapshots.return_value = [SimpleNamespace(date=date(2024, 1, 15))]
    build_summary = AsyncMock(return_value=_sample_analytics())

    with (
        patch("pfm.ai.weekly_report_prompt_pack.build_analytics_summary", new=build_summary),
        patch("pfm.ai.weekly_report_prompt_pack.AIReportMemoryStore") as memory_store_cls,
        patch("pfm.ai.weekly_report_prompt_pack.AIProviderStore") as provider_store_cls,
    ):
        memory_store = AsyncMock()
        memory_store.get.return_value = ""
        memory_store_cls.return_value = memory_store

        provider_store = AsyncMock()
        provider_store.get_active.return_value = SimpleNamespace(type="deepseek", model="deepseek-reasoner")
        provider_store_cls.return_value = provider_store

        pack = await build_weekly_report_prompt_pack(repo, Path("/tmp/test.db"), date(2024, 1, 15))

    assert pack["workflow"] == "section_by_section"
    assert len(pack["sections"]) == 5
