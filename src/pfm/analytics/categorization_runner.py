"""Orchestrates the categorization pipeline: types → transfers → categories."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

from pfm.analytics.categorizer import categorize_batch
from pfm.analytics.transfer_detector import detect_transfer_pairs
from pfm.analytics.type_resolver import resolve_type_batch
from pfm.db.models import TransactionMetadata, TransactionType

if TYPE_CHECKING:
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.models import Transaction
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)

_EMPTY_SUMMARY: dict[str, int] = {"total": 0, "type_resolved": 0, "transfers": 0, "categorized": 0}


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


async def run_categorization(
    repo: Repository,
    metadata_store: MetadataStore,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Run the categorization pipeline: types → transfers → categories.

    Returns a summary dict with counts of actions taken.
    """
    all_txs = await repo.get_transactions()
    if not all_txs:
        return dict(_EMPTY_SUMMARY)

    # Stage 0: Resolve unknown types via DB rules.
    unknown_txs = [tx for tx in all_txs if tx.tx_type == TransactionType.UNKNOWN]
    type_resolved = 0
    if unknown_txs:
        type_rules = await metadata_store.get_type_rules()
        type_updates = resolve_type_batch(unknown_txs, type_rules)
        if type_updates:
            await repo.update_transaction_types(type_updates)
            type_resolved = len(type_updates)
            all_txs = await repo.get_transactions()  # refresh

    # Stage 1: Transfer detection (needs resolved types for inflow/outflow).
    tx_ids = [tx.id for tx in all_txs if tx.id is not None]
    existing = await metadata_store.get_metadata_batch(tx_ids)

    transfer_batch: list[TransactionMetadata] = []
    pairs = detect_transfer_pairs(all_txs)
    for pair in pairs:
        _apply_transfer_to_batch(transfer_batch, existing, pair.tx_id_a, pair.tx_id_b, "transfer", pair.score)
        _apply_transfer_to_batch(transfer_batch, existing, pair.tx_id_b, pair.tx_id_a, "transfer", pair.score)
    if transfer_batch:
        await metadata_store.upsert_metadata_batch(transfer_batch)
        existing = await metadata_store.get_metadata_batch(tx_ids)  # refresh

    # Stage 2: Rule-based categorization (skips already-categorized transfers).
    to_categorize = _filter_uncategorized(all_txs, existing, force=force)
    rules = await metadata_store.get_category_rules()
    results = categorize_batch(to_categorize, rules, existing)

    categorized_count = 0
    category_batch: list[TransactionMetadata] = []

    for tx, cat_result in results:
        if tx.id is None:
            continue
        if cat_result and cat_result.source == "rule":
            category_batch.append(
                TransactionMetadata(
                    transaction_id=tx.id,
                    category=cat_result.category,
                    category_source=cat_result.source,
                    category_confidence=cat_result.confidence,
                )
            )
            categorized_count += 1

    if category_batch:
        await metadata_store.upsert_metadata_batch(category_batch)

    return {
        "total": len(all_txs),
        "type_resolved": type_resolved,
        "transfers": len(pairs),
        "categorized": categorized_count,
    }
