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


async def test_wise_collect_persists_snapshots_and_transactions(repo, pricing):
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
        if "balance-statements" in path:
            return _mock_response(
                {
                    "transactions": [
                        {
                            "amount": {"value": 100},
                            "date": "2024-01-15T00:00:00Z",
                            "type": "CREDIT",
                            "referenceNumber": "ref-1",
                        },
                        {
                            "amount": {"value": -50},
                            "date": "2024-01-14T00:00:00Z",
                            "type": "DEBIT",
                            "referenceNumber": "ref-2",
                        },
                        {
                            "amount": {"value": 0},
                            "date": "2024-01-14T00:00:00Z",
                            "type": "CREDIT",
                            "referenceNumber": "ref-3",
                        },
                    ]
                }
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 1
    assert result.transactions_count == 2
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    transactions = await repo.get_transactions(source="wise")
    assert len(snapshots) == 1
    assert len(transactions) == 2
