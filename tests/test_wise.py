"""Integration tests for the Wise collector."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from pfm.collectors.wise import WiseCollector


def _mock_response(payload: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


async def test_wise_collect_persists_snapshots(repo, pricing):
    pricing._set_cache("GBP", Decimal("1.25"))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]

    collector = WiseCollector(pricing, api_token="token")

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/v1/profiles":
            return _mock_response([{"id": 123, "type": "personal"}])
        if path == "/v4/profiles/123/balances":
            return _mock_response(
                [
                    {"id": 1, "amount": {"value": 1000, "currency": "GBP"}},
                    {"amount": {"value": 0, "currency": "EUR"}},
                ]
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 1
    assert result.transactions_count == 0
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    assert len(snapshots) == 1
