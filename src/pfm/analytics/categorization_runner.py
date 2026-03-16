"""Orchestrates the full categorization pipeline: rules → transfers → AI."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

from pfm.analytics.categorizer import categorize_batch
from pfm.analytics.transfer_detector import detect_transfer_pairs
from pfm.db.models import TransactionMetadata

if TYPE_CHECKING:
    from pfm.ai.base import LLMProvider
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.models import Transaction
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)

_EMPTY_SUMMARY: dict[str, int] = {"total": 0, "categorized": 0, "transfers": 0, "ai_categorized": 0}


def _filter_uncategorized(
    all_txs: list[Transaction],
    existing: dict[int, TransactionMetadata],
    *,
    force: bool,
) -> list[Transaction]:
    """Return transactions that still need categorization."""
    result: list[Transaction] = []
    for tx in all_txs:
        if tx.id is None:
            continue
        meta = existing.get(tx.id)
        if not force and meta and (meta.reviewed or meta.category):
            continue
        result.append(tx)
    return result


def _apply_transfer_to_batch(  # noqa: PLR0913
    metadata_batch: list[TransactionMetadata],
    existing: dict[int, TransactionMetadata],
    tx_id: int,
    pair_id: int,
    category: str,
    score: float,
) -> None:
    """Mark a transaction as part of an internal transfer in the batch."""
    for i, m in enumerate(metadata_batch):
        if m.transaction_id == tx_id:
            metadata_batch[i] = replace(
                m,
                is_internal_transfer=True,
                transfer_pair_id=pair_id,
                transfer_detected_by="auto",
            )
            return
    existing_meta = existing.get(tx_id)
    if existing_meta and existing_meta.is_internal_transfer:
        return
    metadata_batch.append(
        TransactionMetadata(
            transaction_id=tx_id,
            category=category,
            category_source="auto",
            category_confidence=score,
            is_internal_transfer=True,
            transfer_pair_id=pair_id,
            transfer_detected_by="auto",
        )
    )


async def _run_ai_stage(
    ai_provider: LLMProvider,
    uncategorized: list[Transaction],
    metadata_store: MetadataStore,
    metadata_batch: list[TransactionMetadata],
) -> int:
    """Run AI categorization and return count of items categorized."""
    from pfm.ai.categorizer import ai_categorize_batch

    categories = await metadata_store.get_categories()
    ai_results = await ai_categorize_batch(ai_provider, uncategorized[:50], categories)
    ai_count = 0
    for tx_id, category, confidence in ai_results:
        if any(m.transaction_id == tx_id for m in metadata_batch):
            continue
        metadata_batch.append(
            TransactionMetadata(
                transaction_id=tx_id,
                category=category,
                category_source="ai",
                category_confidence=confidence,
            )
        )
        ai_count += 1
    return ai_count


async def run_categorization(
    repo: Repository,
    metadata_store: MetadataStore,
    *,
    ai_provider: LLMProvider | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Run the full categorization pipeline.

    Returns a summary dict with counts of actions taken.
    """
    all_txs = await repo.get_transactions()
    if not all_txs:
        return dict(_EMPTY_SUMMARY)

    tx_ids = [tx.id for tx in all_txs if tx.id is not None]
    existing = await metadata_store.get_metadata_batch(tx_ids)
    to_categorize = _filter_uncategorized(all_txs, existing, force=force)

    # Stage 1: Rule-based categorization (compound DB rules).
    rules = await metadata_store.get_category_rules()
    results = categorize_batch(to_categorize, rules, existing)

    categorized_count = 0
    metadata_batch: list[TransactionMetadata] = []
    uncategorized: list[Transaction] = []

    for tx, cat_result in results:
        if tx.id is None:
            continue
        # Only persist rule-based matches. Heuristic guesses are left
        # undefined so the user can review and manually assign categories.
        if cat_result and cat_result.source == "rule":
            metadata_batch.append(
                TransactionMetadata(
                    transaction_id=tx.id,
                    category=cat_result.category,
                    category_source=cat_result.source,
                    category_confidence=cat_result.confidence,
                )
            )
            categorized_count += 1
        else:
            uncategorized.append(tx)

    # Stage: Transfer detection.
    pairs = detect_transfer_pairs(all_txs)
    for pair in pairs:
        _apply_transfer_to_batch(
            metadata_batch, existing, pair.tx_id_a, pair.tx_id_b, "internal_transfer_out", pair.score
        )
        _apply_transfer_to_batch(
            metadata_batch, existing, pair.tx_id_b, pair.tx_id_a, "internal_transfer_in", pair.score
        )

    # Stage: AI categorization.
    ai_count = 0
    if ai_provider and uncategorized:
        ai_count = await _run_ai_stage(ai_provider, uncategorized, metadata_store, metadata_batch)

    if metadata_batch:
        await metadata_store.upsert_metadata_batch(metadata_batch)

    return {
        "total": len(all_txs),
        "categorized": categorized_count,
        "transfers": len(pairs),
        "ai_categorized": ai_count,
    }
