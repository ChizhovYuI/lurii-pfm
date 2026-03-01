"""Tests for the Revolut collector (GoCardless Bank Account Data API)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from pfm.collectors.revolut import RevolutCollector


def _mock_response(payload: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


def _make_collector(pricing: object) -> RevolutCollector:
    collector = RevolutCollector(
        pricing,  # type: ignore[arg-type]
        secret_id="test-id",
        secret_key="test-key",
        requisition_id="req-123",
    )
    # Pre-set token to skip auth call in most tests
    collector._access_token = "test-token"
    return collector


async def test_revolut_collect_balances_and_transactions(repo, pricing):
    pricing._set_cache("EUR", Decimal("1.08"))
    pricing._set_cache("GBP", Decimal("1.27"))
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = _make_collector(pricing)

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if "/requisitions/req-123" in path:
            return _mock_response({"accounts": ["acc-001"]})
        if "/acc-001/balances" in path:
            return _mock_response(
                {
                    "balances": [
                        {
                            "balanceAmount": {"amount": "1500.50", "currency": "EUR"},
                            "balanceType": "closingAvailable",
                        },
                        {
                            "balanceAmount": {"amount": "250.00", "currency": "GBP"},
                            "balanceType": "interimAvailable",
                        },
                        # Zero balance — should be skipped
                        {
                            "balanceAmount": {"amount": "0", "currency": "USD"},
                            "balanceType": "closingAvailable",
                        },
                    ]
                }
            )
        if "/acc-001/transactions" in path:
            return _mock_response(
                {
                    "transactions": {
                        "booked": [
                            {
                                "transactionAmount": {"amount": "500.00", "currency": "EUR"},
                                "bookingDate": "2024-06-14",
                                "transactionId": "tx-1",
                            },
                            {
                                "transactionAmount": {"amount": "-100.00", "currency": "EUR"},
                                "bookingDate": "2024-06-13",
                                "transactionId": "tx-2",
                            },
                            # Zero amount — should be skipped
                            {
                                "transactionAmount": {"amount": "0", "currency": "EUR"},
                                "bookingDate": "2024-06-12",
                                "transactionId": "tx-3",
                            },
                        ]
                    }
                }
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 2
    assert result.transactions_count == 2
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    assert len(snapshots) == 2

    transactions = await repo.get_transactions(source="revolut")
    assert len(transactions) == 2
    assert transactions[0].tx_type.value == "deposit"
    assert transactions[1].tx_type.value == "withdrawal"


async def test_revolut_skips_duplicate_currencies(pricing):
    """When multiple balance types exist for the same currency, take only the first."""
    pricing._set_cache("EUR", Decimal("1.08"))
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = _make_collector(pricing)

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if "/requisitions/" in path:
            return _mock_response({"accounts": ["acc-001"]})
        if "/balances" in path:
            return _mock_response(
                {
                    "balances": [
                        {
                            "balanceAmount": {"amount": "1000.00", "currency": "EUR"},
                            "balanceType": "closingAvailable",
                        },
                        {
                            "balanceAmount": {"amount": "1000.00", "currency": "EUR"},
                            "balanceType": "interimAvailable",
                        },
                    ]
                }
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "EUR"


async def test_revolut_no_accounts(pricing):
    """Requisition with no linked accounts returns empty results."""
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = _make_collector(pricing)

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if "/requisitions/" in path:
            return _mock_response({"accounts": []})
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert snapshots == []

    transactions = await collector.fetch_transactions()
    assert transactions == []


async def test_revolut_token_acquisition(pricing):
    """Test that _ensure_token obtains an access token."""
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = RevolutCollector(
        pricing,  # type: ignore[arg-type]
        secret_id="my-id",
        secret_key="my-key",
        requisition_id="req-123",
    )
    assert collector._access_token is None

    async def mock_post(path: str, **kwargs: object) -> MagicMock:
        if "/token/new" in path:
            return _mock_response({"access": "new-token", "refresh": "refresh-token"})
        return _mock_response({})

    collector._client.post = mock_post  # type: ignore[assignment]

    await collector._ensure_token()
    assert collector._access_token == "new-token"

    # Second call is a no-op (token already set)
    await collector._ensure_token()
    assert collector._access_token == "new-token"


async def test_revolut_ignores_unsupported_balance_types(pricing):
    """Balance types other than closingAvailable/interimAvailable/expected are skipped."""
    pricing._set_cache("EUR", Decimal("1.08"))
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = _make_collector(pricing)

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if "/requisitions/" in path:
            return _mock_response({"accounts": ["acc-001"]})
        if "/balances" in path:
            return _mock_response(
                {
                    "balances": [
                        {
                            "balanceAmount": {"amount": "500.00", "currency": "EUR"},
                            "balanceType": "openingBooked",
                        },
                        {
                            "balanceAmount": {"amount": "800.00", "currency": "EUR"},
                            "balanceType": "expected",
                        },
                    ]
                }
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].amount == Decimal("800.00")
