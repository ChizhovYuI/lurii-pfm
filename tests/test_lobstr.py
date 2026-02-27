"""Integration tests for the Lobstr collector."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from pfm.collectors.lobstr import LobstrCollector


def _mock_response(payload: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


async def test_lobstr_collect_persists_snapshots_and_transactions(repo, pricing):
    pricing._set_cache("XLM", Decimal("0.10"))
    pricing._set_cache("USDC", Decimal(1))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]

    collector = LobstrCollector(pricing, stellar_address="GABC123")
    account_resp = _mock_response(
        {
            "balances": [
                {"balance": "100.0", "asset_type": "native"},
                {"balance": "500.0", "asset_type": "credit_alphanum4", "asset_code": "USDC"},
                {"balance": "0.0", "asset_type": "credit_alphanum4", "asset_code": "BTC"},
            ]
        }
    )
    payments_resp = _mock_response(
        {
            "_embedded": {
                "records": [
                    {
                        "type": "payment",
                        "created_at": "2024-01-15T10:00:00Z",
                        "to": "GABC123",
                        "asset_type": "native",
                        "amount": "10",
                        "transaction_hash": "tx-1",
                    },
                    {
                        "type": "manage_offer",
                        "created_at": "2024-01-15T09:00:00Z",
                    },
                ]
            }
        }
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path.endswith("/payments"):
            return payments_resp
        return account_resp

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 2
    assert result.transactions_count == 1
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    transactions = await repo.get_transactions(source="lobstr")
    assert len(snapshots) == 2
    assert len(transactions) == 1
