"""Shared ``prices.source`` tag values.

Lives in its own import-free module so both the pricing service (writer) and the
repository readers (which must exclude the miss sentinel) can reference the same
literals without a circular import.

- ``REAL_PRICE_SOURCE`` — a live spot price (date = today).
- ``HISTORICAL_PRICE_SOURCE`` — a back-dated price written by the usd_value
  backfill. Kept distinct from the live source so a historical row can never be
  served by the live cache read, even when ``on_date == today``.
- ``MISS_PRICE_SOURCE`` — a "no price available" sentinel (price = '0'). Readers
  that value holdings MUST exclude it so a $0 sentinel is never read as a price.
"""

from __future__ import annotations

from typing import Final

REAL_PRICE_SOURCE: Final[str] = "coingecko"
HISTORICAL_PRICE_SOURCE: Final[str] = "coingecko-history"
MISS_PRICE_SOURCE: Final[str] = "coingecko-miss"
