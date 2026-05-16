"""Tests for the parallel collection pipeline error formatting."""

from __future__ import annotations

import socket
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
