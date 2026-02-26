"""HMAC signing helpers for exchange APIs."""

from __future__ import annotations

import base64
import hashlib
import hmac


def sign_okx(timestamp: str, method: str, path: str, body: str, secret: str) -> str:
    """Generate OKX API signature.

    Signature = Base64(HMAC-SHA256(timestamp + method + path + body, secret))
    """
    message = f"{timestamp}{method}{path}{body}"
    mac = hmac.new(secret.encode(), message.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def sign_binance(query_string: str, secret: str) -> str:
    """Generate Binance API signature.

    Signature = HMAC-SHA256(query_string, secret) as hex
    """
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()


def sign_bybit(timestamp: str, api_key: str, recv_window: str, payload: str, secret: str) -> str:
    """Generate Bybit V5 API signature.

    Signature = HMAC-SHA256(timestamp + api_key + recv_window + payload, secret) as hex
    """
    message = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
