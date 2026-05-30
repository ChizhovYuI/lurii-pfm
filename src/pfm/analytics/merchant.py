"""Derive a normalized merchant token from a transaction's raw_json.

Rule suggestions key off the most common ``(field, value)`` across a user's
category choices. Raw fields are often poor discriminators: kbank's ``channel``
is the payment rail ("K PLUS" spans restaurants, top-ups, groceries) and its
``details`` buries the merchant behind a volatile ref code. A cleaned merchant
token clusters recurring payees so suggestions become discriminating.

This is consumed both at suggestion time (injected into snapshots) and at
categorization time (the ``merchant_name`` virtual rule field), so it must be
deterministic — identical input always yields identical output.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

# Strip a leading reference token: "Paid for Ref X6847 ...", "Ref Code EDC50445".
# The ``\b`` after the ref keyword is required so a word that merely STARTS with
# "ref" ("Reform", "Refinery", "referral") is not matched and gutted.
_REF_PREFIX = re.compile(r"^(?:paid\s+for\s+ref|ref(?:\s+code)?)\b\s*\S+\s*", re.IGNORECASE)
# Strip embedded UPPERCASE alphanumeric reference codes (e.g. "EDC50445",
# "X6847"). Case-sensitive on purpose: with IGNORECASE this also ate ordinary
# lowercase letter+digit tokens ("abc123", product/model codes). The
# leading-letter requirement (1-4 letters) keeps a bare digit run, so legitimate
# numeric tokens in a merchant name ("Store 365", a year, a branch number)
# survive instead of being deleted.
_STANDALONE_REF = re.compile(r"\b[A-Z]{1,4}\d{3,}\b")

# Per-source field priority. The first field yielding a non-empty cleaned token
# wins. Sources with no merchant-like field (crypto exchanges) fall through to
# the default and typically yield None.
_SOURCE_FIELDS: dict[str, tuple[str, ...]] = {
    "kbank": ("details",),
    "wise": ("merchant", "payeeName", "payerName"),
    "revolut": ("merchant", "description"),
    "bunq": ("counterparty", "description"),
}
_DEFAULT_FIELDS: tuple[str, ...] = (
    "merchant",
    "merchant_name",
    "payee",
    "payeeName",
    "payerName",
    "counterparty",
    "name",
    "description",
)


def _normalize_merchant(value: str) -> str:
    """Strip reference noise and collapse whitespace."""
    text = _REF_PREFIX.sub("", value.strip())
    text = _STANDALONE_REF.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def derive_merchant_name(source: str, raw: Mapping[str, object]) -> str | None:
    """Return a normalized merchant token, or None when none can be derived.

    Callers must pass a mapping; both call sites (the categorizer virtual field
    and the rule-suggestion path) guard that the parsed ``raw_json`` is a dict
    before calling, so a non-object JSON snapshot never reaches here.
    """
    fields = _SOURCE_FIELDS.get(source.lower(), _DEFAULT_FIELDS)
    for field in fields:
        value = raw.get(field)
        if isinstance(value, str) and value.strip():
            cleaned = _normalize_merchant(value)
            if cleaned:
                return cleaned
    return None
