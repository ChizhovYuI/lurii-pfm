"""AI-powered transaction categorization for ambiguous transactions."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfm.ai.base import LLMProvider
    from pfm.db.models import Transaction, TransactionCategory

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a personal finance transaction categorizer. "
    "Given a list of transactions and a taxonomy of categories, "
    "assign the most likely category to each transaction. "
    "Respond with a JSON array of objects, each with 'tx_id' (int), "
    "'category' (str matching a taxonomy category), and 'confidence' (float 0-1)."
)


def _extract_description(tx: Transaction) -> str:
    """Extract description from raw_json if present."""
    if not tx.raw_json:
        return ""
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            return str(parsed.get("description", ""))
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


def _format_tx_line(tx: Transaction) -> str:
    """Format a single transaction for the AI prompt, including description if available."""
    line = (
        f"  - id={tx.id}, date={tx.date}, source={tx.source_name or tx.source}, "
        f"type={tx.tx_type.value}, asset={tx.asset}, amount={tx.amount}, "
        f"usd_value={tx.usd_value}"
    )
    desc = _extract_description(tx)
    if desc:
        line += f', description="{desc}"'
    return line


def build_categorization_prompt(
    transactions: list[Transaction],
    categories: list[TransactionCategory],
) -> str:
    """Build a user prompt for AI categorization."""
    taxonomy_lines = [f"  - {cat.category} ({cat.display_name}) [tx_type: {cat.tx_type}]" for cat in categories]
    taxonomy_str = "\n".join(taxonomy_lines)

    tx_lines = [_format_tx_line(tx) for tx in transactions]
    tx_str = "\n".join(tx_lines)

    return (
        f"Categories:\n{taxonomy_str}\n\n"
        f"Transactions to categorize:\n{tx_str}\n\n"
        f"Return JSON array: [{{'tx_id': int, 'category': str, 'confidence': float}}, ...]"
    )


async def ai_categorize_batch(
    provider: LLMProvider,
    transactions: list[Transaction],
    categories: list[TransactionCategory],
) -> list[tuple[int, str, float]]:
    """Use AI to categorize a batch of transactions.

    Returns list of (transaction_id, category, confidence).
    """
    if not transactions:
        return []

    user_prompt = build_categorization_prompt(transactions, categories)

    try:
        result = await provider.generate_commentary(
            _SYSTEM_PROMPT,
            user_prompt,
            max_output_tokens=2048,
        )
    except Exception:
        logger.exception("AI categorization failed")
        return []

    return _parse_ai_response(result.text)


def _parse_ai_response(text: str) -> list[tuple[int, str, float]]:
    """Parse the AI JSON response into (tx_id, category, confidence) tuples."""
    # Extract JSON from markdown code fences if present.
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        json_lines = [line for line in lines[1:] if not line.startswith("```")]
        stripped = "\n".join(json_lines)

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        logger.warning("Failed to parse AI categorization response as JSON")
        return []

    if not isinstance(parsed, list):
        return []

    results: list[tuple[int, str, float]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        tx_id = item.get("tx_id")
        category = item.get("category")
        confidence = item.get("confidence", 0.7)
        if tx_id is not None and category:
            results.append((int(tx_id), str(category), float(confidence)))
    return results
