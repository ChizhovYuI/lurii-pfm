"""Tests for HMAC signing utilities."""

from pfm.collectors._auth import sign_binance, sign_bybit, sign_coinex, sign_okx


def test_sign_okx_produces_base64():
    sig = sign_okx("2024-01-15T00:00:00.000Z", "GET", "/api/v5/account/balance", "", "secret123")
    assert isinstance(sig, str)
    assert len(sig) > 0


def test_sign_okx_deterministic():
    args = ("2024-01-15T00:00:00.000Z", "GET", "/api/v5/account/balance", "", "secret")
    assert sign_okx(*args) == sign_okx(*args)


def test_sign_binance_produces_hex():
    sig = sign_binance("symbol=BTCUSDT&timestamp=1234567890", "secret123")
    assert isinstance(sig, str)
    assert all(c in "0123456789abcdef" for c in sig)


def test_sign_binance_deterministic():
    args = ("timestamp=123", "secret")
    assert sign_binance(*args) == sign_binance(*args)


def test_sign_bybit_produces_hex():
    sig = sign_bybit("1234567890", "api-key", "20000", "accountType=UNIFIED", "secret123")
    assert isinstance(sig, str)
    assert all(c in "0123456789abcdef" for c in sig)


def test_sign_bybit_deterministic():
    args = ("123", "key", "20000", "payload", "secret")
    assert sign_bybit(*args) == sign_bybit(*args)


def test_sign_coinex_produces_hex():
    sig = sign_coinex(
        "GET",
        "/v2/assets/spot/transcation-history?type=trade&page=1&limit=10",
        "",
        "1700490703564",
        "secret123",
    )
    assert isinstance(sig, str)
    assert all(c in "0123456789abcdef" for c in sig)


def test_sign_coinex_deterministic():
    args = ("GET", "/v2/assets/spot/balance", "", "1700490703564", "secret")
    assert sign_coinex(*args) == sign_coinex(*args)
