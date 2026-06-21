"""Tests for the parallel collection pipeline error formatting."""

from __future__ import annotations

import socket
from datetime import date
from typing import TYPE_CHECKING

import httpx

from pfm.collectors._retry import COUNTRY_ACCESS_HINT
from pfm.collectors.base import BaseCollector
from pfm.collectors.pipeline import run_parallel_pipeline
from pfm.db.models import Source

if TYPE_CHECKING:
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService


class _DnsBlockedCollector(BaseCollector):
    source_name = "okx"
    records_empty_sync_marker = False

    async def fetch_raw_balances(self):
        exc = httpx.ConnectError("connect error")
        exc.__cause__ = socket.gaierror(8, "nodename nor servname provided, or not known")
        raise exc

    async def fetch_transactions(self, since=None):  # pragma: no cover - skipped on DNS error
        _ = since
        msg = "should not be called when DNS is blocked"
        raise AssertionError(msg)


async def test_pipeline_reports_country_access_hint_on_dns_failure(repo: Repository, pricing: PricingService) -> None:
    collector = _DnsBlockedCollector(pricing)
    src = Source(name="okx-main", type="okx", credentials="{}")

    results = await run_parallel_pipeline([(src, collector)], pricing, repo)

    assert len(results) == 1
    result = results[0]
    assert result.source == "okx-main"
    assert result.snapshots_count == 0
    assert result.transactions_count == 0
    assert len(result.errors) == 1
    error = result.errors[0]
    assert COUNTRY_ACCESS_HINT in error
    assert "okx-main" in error
    assert "balances" in error


async def test_pipeline_categorization_does_not_value_transactions(repo: Repository) -> None:
    """Decoupling guard: the pipeline categorizes but never runs USD valuation.

    Valuation is a separate background job (server) / explicit CLI step. A
    regression that re-adds it inline would value this row here.
    """
    from decimal import Decimal

    from pfm.collectors.pipeline import _run_categorization
    from pfm.db.models import Transaction, TransactionType

    await repo.save_transactions(
        [
            Transaction(
                date=date(2026, 4, 1),
                source="okx",
                source_name="okx-main",
                tx_type=TransactionType.DEPOSIT,
                asset="BTC",
                amount=Decimal("0.5"),
                usd_value=Decimal(0),
                tx_id="cat1",
            )
        ]
    )

    await _run_categorization(repo)

    rows = await repo.get_transactions()
    assert rows[0].usd_value == Decimal(0)  # categorization left valuation untouched
