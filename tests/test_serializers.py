"""Tests for the server serializers module."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from pfm.db.models import CollectorResult, Snapshot, Source
from pfm.server.serializers import (
    _str_decimal,
    analytics_to_dict,
    asset_type_for_snapshot,
    build_asset_type_map,
    collector_result_to_dict,
    decimal_default,
    mask_secret,
    parse_cached_ai_commentary,
    parse_cached_ai_commentary_model,
    parse_net_worth_usd,
    snapshot_to_dict,
    source_to_dict,
)


class TestMaskSecret:
    def test_short_value(self):
        assert mask_secret("abc") == "***"

    def test_exactly_8_chars(self):
        assert mask_secret("12345678") == "***"

    def test_long_value(self):
        assert mask_secret("abcdefghij") == "abc...hij"


class TestSourceToDict:
    def test_masked(self):
        src = Source(name="wise-main", type="wise", credentials='{"token": "secret12345678"}')
        result = source_to_dict(src, mask_secrets=True)
        assert result["name"] == "wise-main"
        assert result["type"] == "wise"
        assert result["credentials"]["token"] == "sec...678"

    def test_unmasked(self):
        src = Source(name="wise-main", type="wise", credentials='{"token": "secret12345678"}')
        result = source_to_dict(src, mask_secrets=False)
        assert result["credentials"]["token"] == "secret12345678"


class TestSnapshotToDict:
    def test_basic(self):
        snap = Snapshot(
            date=date(2024, 1, 1),
            source="okx",
            asset="BTC",
            amount=Decimal("1.5"),
            usd_value=Decimal(45000),
        )
        result = snapshot_to_dict(snap)
        assert result["date"] == "2024-01-01"
        assert result["source"] == "okx"
        assert result["source_name"] == "okx"
        assert result["asset"] == "BTC"
        assert result["amount"] == "1.5"
        assert result["usd_value"] == "45000"
        assert result["price"] == "0"
        assert result["apy"] == "0"

    def test_with_apy(self):
        snap = Snapshot(
            date=date(2024, 1, 1),
            source="okx",
            asset="USDT",
            amount=Decimal(500),
            usd_value=Decimal(500),
            price=Decimal(1),
            apy=Decimal("0.1049"),
        )
        result = snapshot_to_dict(snap)
        assert result["apy"] == "0.1049"


class TestCollectorResultToDict:
    def test_basic(self):
        r = CollectorResult(
            source="okx",
            snapshots_count=5,
            snapshots_usd_total=Decimal(1000),
            transactions_count=10,
            errors=["some error"],
            duration_seconds=2.5,
        )
        result = collector_result_to_dict(r)
        assert result["source"] == "okx"
        assert result["snapshots_count"] == 5
        assert result["snapshots_usd_total"] == "1000"
        assert result["errors"] == ["some error"]


class TestAnalyticsToDict:
    def test_parses_json(self):
        metrics = {
            "net_worth": '{"usd": "45000"}',
            "pnl": '{"daily": {}}',
        }
        result = analytics_to_dict(metrics)
        assert result["net_worth"] == {"usd": "45000"}
        assert result["pnl"] == {"daily": {}}

    def test_fallback_on_invalid_json(self):
        result = analytics_to_dict({"bad": "not json{"})
        assert result["bad"] == "not json{"


class TestAssetTypeForSnapshot:
    def test_defi(self):
        assert asset_type_for_snapshot("blend", "USDC") == "defi"

    def test_fiat_source(self):
        assert asset_type_for_snapshot("wise", "USD") == "fiat"

    def test_stock(self):
        assert asset_type_for_snapshot("ibkr", "AAPL") == "stocks"

    def test_ibkr_fiat(self):
        assert asset_type_for_snapshot("ibkr", "USD") == "fiat"

    def test_crypto(self):
        assert asset_type_for_snapshot("okx", "BTC") == "crypto"

    def test_crypto_rabby(self):
        assert asset_type_for_snapshot("rabby", "ETH") == "crypto"

    def test_defi_yo(self):
        assert asset_type_for_snapshot("yo", "YOETH") == "defi"

    def test_defi_bitget_wallet(self):
        assert asset_type_for_snapshot("bitget_wallet", "USDC") == "defi"

    def test_fiat_asset(self):
        assert asset_type_for_snapshot("unknown", "USD") == "fiat"

    def test_other(self):
        assert asset_type_for_snapshot("unknown", "UNKNOWN") == "other"


class TestBuildAssetTypeMap:
    def test_basic(self):
        snaps = [
            Snapshot(date=date(2024, 1, 1), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 1), source="wise", asset="USD", amount=Decimal(500), usd_value=Decimal(500)),
        ]
        result = build_asset_type_map(snaps)
        assert result["BTC"] == "crypto"
        assert result["USD"] == "fiat"


class TestParseNetWorthUsd:
    def test_valid(self):
        assert parse_net_worth_usd('{"usd": "45000.50"}') == Decimal("45000.50")

    def test_invalid_json(self):
        assert parse_net_worth_usd("not json") == Decimal(0)

    def test_not_dict(self):
        assert parse_net_worth_usd('"hello"') == Decimal(0)


class TestParseCachedAiCommentary:
    def test_dict_format(self):
        raw = json.dumps({"text": "Great week", "model": "gemini"})
        assert parse_cached_ai_commentary(raw) == "Great week"

    def test_none(self):
        assert parse_cached_ai_commentary(None) is None

    def test_string_format(self):
        raw = json.dumps("Some commentary")
        assert parse_cached_ai_commentary(raw) == "Some commentary"


class TestParseCachedAiCommentaryModel:
    def test_dict_format(self):
        raw = json.dumps({"text": "x", "model": "gemini-pro"})
        assert parse_cached_ai_commentary_model(raw) == "gemini-pro"

    def test_none(self):
        assert parse_cached_ai_commentary_model(None) is None

    def test_no_model(self):
        raw = json.dumps({"text": "x"})
        assert parse_cached_ai_commentary_model(raw) is None


class TestStrDecimal:
    def test_large_value(self):
        assert _str_decimal(Decimal("45000.123")) == "45000.123"

    def test_integer(self):
        assert _str_decimal(Decimal(1000)) == "1000"

    def test_small_value(self):
        assert _str_decimal(Decimal("0.00001234")) == "0.00001234"

    def test_trailing_zeros_stripped(self):
        assert _str_decimal(Decimal("0.50000000")) == "0.5"


class TestDecimalDefault:
    def test_decimal(self):
        assert decimal_default(Decimal("1.23")) == "1.23"  # no rounding, full precision

    def test_date(self):
        assert decimal_default(date(2024, 1, 1)) == "2024-01-01"

    def test_unsupported(self):
        import pytest

        with pytest.raises(TypeError):
            decimal_default(set())
