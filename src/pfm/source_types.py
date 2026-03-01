"""Credential schemas for each known source type."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CredentialField:
    """Definition of a single credential field."""

    name: str
    prompt: str
    required: bool = True
    default: str = ""
    secret: bool = True


# Credential fields per source type.
# Order matters — wizard prompts in this order.
SOURCE_TYPES: dict[str, list[CredentialField]] = {
    "okx": [
        CredentialField("api_key", "API Key"),
        CredentialField("api_secret", "API Secret"),
        CredentialField("passphrase", "Passphrase"),
    ],
    "binance": [
        CredentialField("api_key", "API Key"),
        CredentialField("api_secret", "API Secret"),
    ],
    "binance_th": [
        CredentialField("api_key", "API Key"),
        CredentialField("api_secret", "API Secret"),
    ],
    "bybit": [
        CredentialField("api_key", "API Key"),
        CredentialField("api_secret", "API Secret"),
    ],
    "lobstr": [
        CredentialField("stellar_address", "Stellar public address (G...)", secret=False),
    ],
    "blend": [
        CredentialField("stellar_address", "Stellar public address (G...)", secret=False),
        CredentialField("pool_contract_id", "Blend pool contract ID", secret=False),
        CredentialField(
            "soroban_rpc_url",
            "Soroban RPC URL",
            required=False,
            default="https://soroban-rpc.mainnet.stellar.gateway.fm",
            secret=False,
        ),
    ],
    "wise": [
        CredentialField("api_token", "Personal API token"),
    ],
    "kbank": [
        CredentialField("gmail_address", "Gmail address", secret=False),
        CredentialField("gmail_app_password", "Gmail App Password"),
        CredentialField(
            "kbank_sender_email",
            "KBank sender email",
            required=False,
            default="K-ElectronicDocument@kasikornbank.com",
            secret=False,
        ),
        CredentialField("pdf_password", "PDF password (DDMMYYYY)"),
    ],
    "ibkr": [
        CredentialField("flex_token", "Flex Web Service token"),
        CredentialField("flex_query_id", "Flex Query ID", secret=False),
    ],
    "revolut": [
        CredentialField("secret_id", "GoCardless Secret ID"),
        CredentialField("secret_key", "GoCardless Secret Key"),
        CredentialField("requisition_id", "GoCardless Requisition ID", secret=False),
    ],
}


def validate_credentials(source_type: str, credentials: dict[str, str]) -> list[str]:
    """Validate credentials against the schema for a source type.

    Returns a list of error messages (empty if valid).
    """
    fields = SOURCE_TYPES.get(source_type)
    if fields is None:
        return [f"Unknown source type: {source_type!r}"]

    return [f"Missing required field: {f.name}" for f in fields if f.required and not credentials.get(f.name)]
