"""Tests for category rule matching and the _parse_values utility."""

from __future__ import annotations

import pytest

from pfm.analytics.categorizer import _match_values, _parse_values
from pfm.db.metadata_store import _validate_regex_value


class TestParseValues:
    def test_json_array(self) -> None:
        assert _parse_values('["a", "b"]') == ["a", "b"]

    def test_plain_string(self) -> None:
        assert _parse_values("hello") == ["hello"]

    def test_invalid_json_treated_as_string(self) -> None:
        assert _parse_values("[invalid") == ["[invalid"]


class TestMatchValues:
    def test_eq_plain(self) -> None:
        assert _match_values("Payment", "Payment", "eq")
        assert not _match_values("Other", "Payment", "eq")

    def test_eq_array(self) -> None:
        assert _match_values("Payment", '["Payment", "Refund"]', "eq")
        assert _match_values("Refund", '["Payment", "Refund"]', "eq")
        assert not _match_values("Other", '["Payment", "Refund"]', "eq")

    def test_contains_plain(self) -> None:
        assert _match_values("swap_token", "swap", "contains")
        assert not _match_values("deposit", "swap", "contains")

    def test_contains_array(self) -> None:
        assert _match_values("swap_token", '["swap", "trade"]', "contains")
        assert _match_values("my_trade_x", '["swap", "trade"]', "contains")
        assert not _match_values("deposit", '["swap", "trade"]', "contains")

    def test_unknown_operator(self) -> None:
        assert not _match_values("x", "x", "unknown")

    def test_regex_plain(self) -> None:
        assert _match_values("Card Spending 1234", r"Card Spending \d+", "regex")
        assert not _match_values("Card Spending abc", r"Card Spending \d+", "regex")

    def test_regex_array(self) -> None:
        assert _match_values("ATM Withdrawal", r'["^ATM\\b", "^POS\\b"]', "regex")
        assert _match_values("POS Buy", r'["^ATM\\b", "^POS\\b"]', "regex")
        assert not _match_values("Refund", r'["^ATM\\b", "^POS\\b"]', "regex")

    def test_regex_invalid_pattern_no_match(self) -> None:
        assert not _match_values("anything", "([unclosed", "regex")

    def test_regex_case_sensitive_by_default(self) -> None:
        assert not _match_values("payment", "PAYMENT", "regex")
        assert _match_values("payment", "(?i)PAYMENT", "regex")


class TestValidateRegexValue:
    def test_valid_plain(self) -> None:
        _validate_regex_value(r"^FX \d+")

    def test_valid_array(self) -> None:
        _validate_regex_value(r'["^ATM", "^POS"]')

    def test_invalid_plain_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid regex"):
            _validate_regex_value("([unclosed")

    def test_invalid_in_array_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid regex"):
            _validate_regex_value(r'["^ok$", "([bad"]')
