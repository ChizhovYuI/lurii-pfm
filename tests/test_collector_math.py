"""Tests for the collector math helpers."""

from __future__ import annotations

from decimal import Decimal

from pfm.collectors._math import apr_to_apy


class TestAprToApy:
    def test_zero_apr(self):
        assert apr_to_apy(Decimal(0)) == Decimal(0)

    def test_daily_compounding(self):
        # 10% APR daily → APY ≈ 10.52%
        apy = apr_to_apy(Decimal("0.10"), periods=365)
        assert Decimal("0.1051") < apy < Decimal("0.1053")

    def test_weekly_compounding(self):
        # 10% APR weekly → APY ≈ 10.51%
        apy = apr_to_apy(Decimal("0.10"), periods=52)
        assert Decimal("0.1050") < apy < Decimal("0.1052")

    def test_default_is_daily(self):
        apy_default = apr_to_apy(Decimal("0.05"))
        apy_daily = apr_to_apy(Decimal("0.05"), periods=365)
        assert apy_default == apy_daily

    def test_small_apr(self):
        # 1% APR → APY ≈ 1.005%
        apy = apr_to_apy(Decimal("0.01"))
        assert apy > Decimal("0.01")
        assert apy < Decimal("0.0101")

    def test_high_apr(self):
        # 100% APR → APY ≈ 171.5%
        apy = apr_to_apy(Decimal("1.0"))
        assert apy > Decimal("1.7")
        assert apy < Decimal("1.72")
