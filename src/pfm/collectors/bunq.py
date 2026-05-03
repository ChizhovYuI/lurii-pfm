"""Bunq collector — reads monetary accounts via signed REST API."""

from __future__ import annotations

import base64
import json
import logging
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from pfm.collectors import register_collector
from pfm.collectors._retry import retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL_PRODUCTION = "https://api.bunq.com"
_BASE_URL_SANDBOX = "https://public-api.sandbox.bunq.com"

_USER_AGENT = "lurii-pfm/0.22"
_DESCRIPTION = "lurii-pfm"
_PAYMENT_PAGE_SIZE = 200
_RSA_KEY_BITS = 2048


def generate_keypair_pem() -> tuple[str, str]:
    """Generate an RSA-2048 keypair and return (private_pem, public_pem) strings."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=_RSA_KEY_BITS)
    priv_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    pub_pem = (
        key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return priv_pem, pub_pem


@register_collector
class BunqCollector(BaseCollector):
    """Collector for bunq monetary accounts via the public API.

    bunq requires a signed-request handshake: install client public key,
    register a device-server, then open a session. Each subsequent call
    is signed with the client RSA private key.
    """

    source_name = SourceName.BUNQ

    def __init__(
        self,
        pricing: PricingService,
        *,
        api_key: str,
        private_key_pem: str,
        public_key_pem: str,
        environment: str = "production",
    ) -> None:
        super().__init__(pricing)
        self._api_key = api_key
        self._public_key_pem = public_key_pem
        loaded = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
        if not isinstance(loaded, RSAPrivateKey):
            msg = "bunq: private_key_pem must be an RSA key"
            raise TypeError(msg)
        self._private_key: RSAPrivateKey = loaded

        env = (environment or "production").strip().lower()
        base = _BASE_URL_SANDBOX if env == "sandbox" else _BASE_URL_PRODUCTION
        self._client = httpx.AsyncClient(base_url=base, timeout=30.0)
        self._session_token: str | None = None
        self._user_id: int | None = None

    async def close(self) -> None:
        await self._client.aclose()

    # ── Signing helpers ──────────────────────────────────────────────

    def _sign(self, body: bytes) -> str:
        sig = self._private_key.sign(body, padding.PKCS1v15(), hashes.SHA256())
        return base64.b64encode(sig).decode()

    @staticmethod
    def _common_headers() -> dict[str, str]:
        return {
            "User-Agent": _USER_AGENT,
            "X-Bunq-Client-Request-Id": str(uuid.uuid4()),
            "X-Bunq-Geolocation": "0 0 0 0 000",
            "X-Bunq-Language": "en_US",
            "X-Bunq-Region": "en_US",
            "Cache-Control": "no-cache",
        }

    # ── Handshake ────────────────────────────────────────────────────

    @retry()
    async def _install(self) -> str:
        body = json.dumps({"client_public_key": self._public_key_pem}).encode()
        resp = await self._client.post(
            "/v1/installation",
            content=body,
            headers={**self._common_headers(), "Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return _extract_token(resp.json())

    @retry()
    async def _register_device(self, install_token: str) -> None:
        body = json.dumps({"description": _DESCRIPTION, "secret": self._api_key}).encode()
        resp = await self._client.post(
            "/v1/device-server",
            content=body,
            headers={
                **self._common_headers(),
                "Content-Type": "application/json",
                "X-Bunq-Client-Authentication": install_token,
                "X-Bunq-Client-Signature": self._sign(body),
            },
        )
        if resp.status_code == httpx.codes.CONFLICT:
            # Device already registered for this API key + IP — fine, continue.
            logger.debug("bunq: device-server already registered, continuing")
            return
        resp.raise_for_status()

    @retry()
    async def _open_session(self, install_token: str) -> tuple[str, int]:
        body = json.dumps({"secret": self._api_key}).encode()
        resp = await self._client.post(
            "/v1/session-server",
            content=body,
            headers={
                **self._common_headers(),
                "Content-Type": "application/json",
                "X-Bunq-Client-Authentication": install_token,
                "X-Bunq-Client-Signature": self._sign(body),
            },
        )
        resp.raise_for_status()
        return _extract_session(resp.json())

    async def _handshake(self) -> None:
        install_token = await self._install()
        await self._register_device(install_token)
        self._session_token, self._user_id = await self._open_session(install_token)
        logger.info("bunq: session established for user_id=%s", self._user_id)

    # ── Authenticated GET ────────────────────────────────────────────

    async def _signed_get(self, path: str, *, _retry_on_401: bool = True) -> dict[str, Any]:
        if self._session_token is None:
            await self._handshake()
        headers = {
            **self._common_headers(),
            "X-Bunq-Client-Authentication": self._session_token or "",
            "X-Bunq-Client-Signature": self._sign(b""),
        }
        resp = await self._client.get(path, headers=headers)
        if resp.status_code == httpx.codes.UNAUTHORIZED and _retry_on_401:
            logger.info("bunq: 401 on %s, re-handshaking", path)
            self._session_token = None
            self._user_id = None
            return await self._signed_get(path, _retry_on_401=False)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    # ── Balances ─────────────────────────────────────────────────────

    async def fetch_raw_balances(self) -> list[RawBalance]:
        if self._session_token is None:
            await self._handshake()
        accounts_data = await self._signed_get(f"/v1/user/{self._user_id}/monetary-account")
        raw: list[RawBalance] = []
        for account in _iter_accounts(accounts_data):
            if account.get("status") != "ACTIVE":
                continue
            balance = account.get("balance") or {}
            currency = str(balance.get("currency", "")).upper()
            try:
                amount = Decimal(str(balance.get("value", "0")))
            except (TypeError, ValueError):
                continue
            if amount == 0 or not currency:
                continue
            raw.append(
                RawBalance(
                    asset=currency,
                    amount=amount,
                    raw_json=json.dumps(account),
                )
            )
        logger.info("bunq: found %d non-zero balances", len(raw))
        return raw

    # ── Transactions ─────────────────────────────────────────────────

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        if self._session_token is None:
            await self._handshake()
        today = self._pricing.today()
        start_date = since or date(today.year, today.month, 1)

        accounts_data = await self._signed_get(f"/v1/user/{self._user_id}/monetary-account")
        all_txs: list[Transaction] = []
        for account in _iter_accounts(accounts_data):
            account_id = account.get("id")
            if not account_id:
                continue
            account_txs = await self._fetch_account_payments(int(account_id), start_date)
            all_txs.extend(account_txs)

        logger.info("bunq: parsed %d transactions", len(all_txs))
        return all_txs

    async def _fetch_account_payments(self, account_id: int, start_date: date) -> list[Transaction]:
        path = f"/v1/user/{self._user_id}/monetary-account/{account_id}/payment?count={_PAYMENT_PAGE_SIZE}"
        out: list[Transaction] = []
        while True:
            data = await self._signed_get(path)
            stop = False
            for item in data.get("Response", []):
                payment = item.get("Payment")
                if not payment:
                    continue
                tx = self._parse_payment(payment)
                if tx is None:
                    continue
                if tx.date < start_date:
                    stop = True
                    break
                out.append(tx)
            older = (data.get("Pagination") or {}).get("older_url")
            if stop or not older:
                break
            path = older
        return out

    @staticmethod
    def _parse_payment(payment: dict[str, Any]) -> Transaction | None:
        amount_data = payment.get("amount") or {}
        try:
            amount = Decimal(str(amount_data.get("value", "0")))
        except (TypeError, ValueError):
            return None
        currency = str(amount_data.get("currency", "")).upper()
        if amount == 0 or not currency:
            return None

        created = str(payment.get("created", ""))
        try:
            tx_date = date.fromisoformat(created.split(" ", 1)[0])
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        tx_id = str(payment.get("id", ""))
        annotated = {**payment, "_amount_sign": "positive" if amount > 0 else "negative"}
        return Transaction(
            date=tx_date,
            source=SourceName.BUNQ,
            tx_type=TransactionType.UNKNOWN,
            asset=currency,
            amount=abs(amount),
            usd_value=Decimal(0),
            tx_id=tx_id,
            raw_json=json.dumps(annotated),
        )


# ── Response parsers ──────────────────────────────────────────────────


def _extract_token(payload: dict[str, Any]) -> str:
    """Pull the Token.token value from a bunq Response array."""
    for item in payload.get("Response", []):
        token = item.get("Token")
        if token and "token" in token:
            return str(token["token"])
    msg = "bunq: no Token in installation response"
    raise ValueError(msg)


def _extract_session(payload: dict[str, Any]) -> tuple[str, int]:
    """Pull session token + user_id from a session-server response."""
    token: str | None = None
    user_id: int | None = None
    for item in payload.get("Response", []):
        if "Token" in item:
            token = str(item["Token"]["token"])
        for user_key in ("UserPerson", "UserCompany", "UserLight"):
            if user_key in item and "id" in item[user_key]:
                user_id = int(item[user_key]["id"])
        if "UserApiKey" in item:
            requested = item["UserApiKey"].get("requested_by_user") or {}
            for user_key in ("UserPerson", "UserCompany", "UserLight"):
                if user_key in requested and "id" in requested[user_key]:
                    user_id = int(requested[user_key]["id"])
    if token is None or user_id is None:
        msg = "bunq: session response missing Token or User id"
        raise ValueError(msg)
    return token, user_id


def _iter_accounts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a monetary-account list response into a list of inner account dicts."""
    out: list[dict[str, Any]] = []
    for item in payload.get("Response", []):
        out.extend(account for account in item.values() if isinstance(account, dict))
    return out
