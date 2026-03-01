"""Shared math helpers for collector APR/APY calculations."""

from __future__ import annotations

from decimal import Decimal


def apr_to_apy(apr: Decimal, *, periods: int = 365) -> Decimal:
    """Convert APR to APY with given compounding periods per year.

    OKX/Bybit use ``periods=365`` (daily compounding).
    Blend uses ``periods=52`` (weekly compounding per SDK convention).
    """
    if apr == 0:
        return Decimal(0)
    return (1 + apr / periods) ** periods - 1
