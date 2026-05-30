"""Tests for merchant_name derivation and its use in matching/suggestions."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from pfm.analytics.categorizer import categorize_transaction
from pfm.analytics.merchant import derive_merchant_name
from pfm.db.metadata_store import _find_common_field, _snapshot_with_merchant
from pfm.db.models import CategoryRule, Transaction, TransactionType


def test_snapshot_with_merchant_tolerates_non_object_json():
    # A field_snapshot that is valid JSON but not an object (or unparseable, or
    # empty) must yield {} instead of crashing the suggestion sweep.
    assert _snapshot_with_merchant("kbank", "[1, 2, 3]") == {}
    assert _snapshot_with_merchant("kbank", "42") == {}
    assert _snapshot_with_merchant("kbank", "not json") == {}
    assert _snapshot_with_merchant("kbank", "") == {}
    assert _snapshot_with_merchant("kbank", None) == {}
    # A normal object still parses and gets the derived merchant token injected.
    assert _snapshot_with_merchant("kbank", '{"details": "Tesco EDC50445"}') == {
        "details": "Tesco EDC50445",
        "merchant_name": "Tesco",
    }


def test_kbank_details_strips_ref_prefix_to_reveal_merchant():
    raw = {"details": "Paid for Ref X6847 บจก.บางจากกรีนเนท", "channel": "K PLUS"}
    assert derive_merchant_name("kbank", raw) == "บจก.บางจากกรีนเนท"


def test_kbank_ref_code_only_yields_none():
    # "Ref Code EDC50445" is pure reference noise — no merchant.
    assert derive_merchant_name("kbank", {"details": "Ref Code EDC50445"}) is None


def test_wise_prefers_payee_then_payer():
    debit = {"merchant": "", "payeeName": "Bridge Building Sp.z.o.o.", "payerName": ""}
    assert derive_merchant_name("wise", debit) == "Bridge Building Sp.z.o.o."
    credit = {"merchant": "", "payeeName": "", "payerName": "DR - WALTER GmbH"}
    assert derive_merchant_name("wise", credit) == "DR - WALTER GmbH"


def test_crypto_source_has_no_merchant():
    assert derive_merchant_name("okx", {"notes": "From: Funding", "ccy": "USDC"}) is None


def test_numeric_tokens_in_merchant_name_are_preserved():
    # A bare digit run (store/branch number, year) is NOT a ref code and must
    # survive — only alphanumeric ref codes are stripped.
    assert derive_merchant_name("kbank", {"details": "Store 365"}) == "Store 365"
    assert derive_merchant_name("kbank", {"details": "Cafe 2024 Bangkok"}) == "Cafe 2024 Bangkok"


def test_embedded_alnum_ref_code_is_stripped():
    # Letter-prefixed ref codes are still removed wherever they appear.
    assert derive_merchant_name("kbank", {"details": "Tesco EDC50445"}) == "Tesco"
    assert derive_merchant_name("kbank", {"details": "X6847 Grab"}) == "Grab"


def test_ref_prefix_does_not_eat_words_merely_starting_with_ref():
    # The ``\b`` after the ref keyword: a word that only STARTS with "ref" must
    # not be matched and gutted (regression for the missing word boundary).
    assert derive_merchant_name("kbank", {"details": "Reform Pilates Studio"}) == "Reform Pilates Studio"
    assert derive_merchant_name("kbank", {"details": "Refinery Coffee"}) == "Refinery Coffee"
    assert derive_merchant_name("revolut", {"merchant": "referral bonus Acme"}) == "referral bonus Acme"


def test_lowercase_alnum_tokens_are_not_stripped():
    # The standalone-ref pattern is case-sensitive (uppercase ref codes only), so
    # ordinary lowercase letter+digit product/model tokens survive.
    assert derive_merchant_name("revolut", {"merchant": "abc123 store"}) == "abc123 store"
    assert derive_merchant_name("revolut", {"merchant": "iphone15 case"}) == "iphone15 case"


def test_merchant_name_virtual_field_matches_rule():
    tx = Transaction(
        date=date(2026, 4, 1),
        source="kbank",
        source_name="kbank",
        tx_type=TransactionType.SPEND,
        asset="THB",
        amount=Decimal(120),
        usd_value=Decimal(0),
        raw_json=json.dumps({"details": "Paid for Ref X6847 STARBUCKS", "channel": "K PLUS"}),
    )
    rule = CategoryRule(
        type_match="spend",
        result_category="coffee",
        field_name="merchant_name",
        field_operator="eq",
        field_value="STARBUCKS",
    )
    result = categorize_transaction(tx, [rule])
    assert result is not None
    assert result.category == "coffee"


async def test_suggested_merchant_rule_matches_categorizer(tmp_path):
    """A merchant_name rule learned from choices must match the source txs.

    Records choices the way the categorize flow does (source TYPE + full
    raw_json), then asserts the suggested merchant_name rule actually applies.
    """
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    raw = json.dumps({"details": "Paid for Ref X6847 STARBUCKS", "channel": "K PLUS"})
    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(
            [
                Transaction(
                    date=date(2026, 4, day),
                    source="kbank",
                    source_name="kbank-main",
                    tx_type=TransactionType.SPEND,
                    asset="THB",
                    amount=Decimal(120),
                    usd_value=Decimal(0),
                    tx_id=f"s{day}",
                    raw_json=raw,
                )
                for day in range(1, 4)
            ]
        )
        saved = await repo.get_transactions()
        for tx in saved:
            assert tx.id is not None
            await store.record_category_choice(tx.id, tx.source, "spend", "coffee", field_snapshot=tx.raw_json)

        suggestions = await store.get_category_suggestions(min_evidence=2)
        merchant_sugg = next(
            s
            for s in suggestions
            if isinstance(s["suggested_rule"], dict) and s["suggested_rule"].get("field_name") == "merchant_name"
        )
        rule_dict = merchant_sugg["suggested_rule"]
        assert isinstance(rule_dict, dict)
        assert rule_dict["field_value"] == "STARBUCKS"

        rule = CategoryRule(
            type_match=str(rule_dict["type_match"]),
            result_category=str(rule_dict["result_category"]),
            source_type=str(rule_dict["source_type"]),
            field_name=str(rule_dict["field_name"]),
            field_operator=str(rule_dict["field_operator"]),
            field_value=str(rule_dict["field_value"]),
        )
        result = categorize_transaction(saved[0], [rule])
        assert result is not None
        assert result.category == "coffee"


def test_find_common_field_prefers_merchant_over_noisy_channel():
    # channel and merchant_name both appear in every snapshot, but merchant wins.
    snaps: list[dict[str, object]] = [
        {"channel": "K PLUS", "merchant_name": "STARBUCKS"},
        {"channel": "K PLUS", "merchant_name": "STARBUCKS"},
        {"channel": "K PLUS", "merchant_name": "STARBUCKS"},
    ]
    field, value = _find_common_field(snaps)
    assert field == "merchant_name"
    assert value == "STARBUCKS"
