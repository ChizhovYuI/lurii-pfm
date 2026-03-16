"""Internal transfer detection between sources."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.db.models import TransactionType

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Transaction

# ── Stablecoin equivalence ─────────────────────────────────────────────

_STABLECOIN_GROUPS: list[frozenset[str]] = [
    frozenset({"USDC", "USDC-BASE", "USDC-OP", "USDC-ARB", "USDC-SOL", "USDC-AVAX", "USDC-MATIC"}),
    frozenset({"USDT", "USDT-TRC20", "USDT-BEP20", "USDT-SOL", "USDT-ARB"}),
    frozenset({"DAI", "XDAI"}),
]


def _normalize_asset(asset: str) -> str:
    """Normalize asset ticker to base form for comparison."""
    upper = asset.upper()
    for group in _STABLECOIN_GROUPS:
        if upper in group:
            return next(iter(sorted(group)))
    return upper


def _assets_equivalent(a: str, b: str) -> bool:
    """Check whether two asset tickers are equivalent (stablecoin-aware)."""
    return _normalize_asset(a) == _normalize_asset(b)


# ── Amount tolerance ───────────────────────────────────────────────────

_AMOUNT_TOLERANCE = Decimal("0.05")  # 5%


def _amounts_within_tolerance(a: Decimal, b: Decimal) -> float:
    """Return a similarity score 0..1 for two amounts, 0 if outside tolerance."""
    if a == 0 and b == 0:
        return 1.0
    larger = max(abs(a), abs(b))
    if larger == 0:
        return 1.0
    diff_pct = abs(a - b) / larger
    if diff_pct > _AMOUNT_TOLERANCE:
        return 0.0
    return float(1 - diff_pct / _AMOUNT_TOLERANCE)


# ── Date window ────────────────────────────────────────────────────────

_DATE_WINDOW_DAYS = 3


def _date_proximity_score(d1: date, d2: date) -> float:
    """Return a score 0..1 for date proximity within the window."""
    days_apart = abs((d1 - d2).days)
    if days_apart > _DATE_WINDOW_DAYS:
        return 0.0
    return 1.0 - (days_apart / (_DATE_WINDOW_DAYS + 1))


# ── Transfer direction types ──────────────────────────────────────────

_OUTFLOW_TYPES = frozenset({TransactionType.WITHDRAWAL, TransactionType.TRANSFER})
_INFLOW_TYPES = frozenset({TransactionType.DEPOSIT, TransactionType.TRANSFER})


@dataclass(frozen=True, slots=True)
class TransferPair:
    """A detected internal transfer pair with confidence score."""

    tx_id_a: int  # outflow (withdrawal/transfer)
    tx_id_b: int  # inflow (deposit/transfer)
    score: float  # 0..1 combined confidence


def detect_transfer_pairs(txs: list[Transaction]) -> list[TransferPair]:
    """Detect internal transfer pairs from a list of transactions.

    Matches where:
    - One is an outflow (withdrawal/transfer), other is inflow (deposit/transfer)
    - Different source_name
    - Same asset or stablecoin equivalent
    - Amount within 5% tolerance (to account for fees)
    - Date within 3-day window
    """
    outflows = [tx for tx in txs if tx.tx_type in _OUTFLOW_TYPES and tx.id is not None]
    inflows = [tx for tx in txs if tx.tx_type in _INFLOW_TYPES and tx.id is not None]

    candidates: list[TransferPair] = []

    for out_tx in outflows:
        for in_tx in inflows:
            # Must be different sources.
            out_source = out_tx.source_name or out_tx.source
            in_source = in_tx.source_name or in_tx.source
            if out_source == in_source:
                continue

            # Asset equivalence check.
            if not _assets_equivalent(out_tx.asset, in_tx.asset):
                continue

            # Amount similarity.
            amount_score = _amounts_within_tolerance(out_tx.amount, in_tx.amount)
            if amount_score == 0.0:
                continue

            # Date proximity.
            date_score = _date_proximity_score(out_tx.date, in_tx.date)
            if date_score == 0.0:
                continue

            combined_score = amount_score * 0.6 + date_score * 0.4

            assert out_tx.id is not None  # noqa: S101
            assert in_tx.id is not None  # noqa: S101
            candidates.append(
                TransferPair(
                    tx_id_a=out_tx.id,
                    tx_id_b=in_tx.id,
                    score=round(combined_score, 4),
                )
            )

    # Greedy matching: pick highest-scored pairs, no tx used twice.
    candidates.sort(key=lambda p: p.score, reverse=True)
    used: set[int] = set()
    result: list[TransferPair] = []
    for pair in candidates:
        if pair.tx_id_a in used or pair.tx_id_b in used:
            continue
        used.add(pair.tx_id_a)
        used.add(pair.tx_id_b)
        result.append(pair)

    return result
