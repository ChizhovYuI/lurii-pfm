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
    tip: str = ""


# Credential fields per source type.
# Order matters — wizard prompts in this order.
SOURCE_TYPES: dict[str, list[CredentialField]] = {
    "okx": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to okx.com\n"
                "2. Go to API Management\n"
                "3. Create API key with Read Only permission\n"
                "4. Copy API Key, Secret, and Passphrase"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
        CredentialField("passphrase", "Passphrase"),
    ],
    "binance": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to binance.com\n"
                "2. Go to API Management\n"
                "3. Create API with Read Only permissions\n"
                "4. Copy API Key and Secret (shown once)"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
    ],
    "binance_th": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to binance.th\n"
                "2. Go to API Management\n"
                "3. Create a read-only API key\n"
                "4. Copy API Key and Secret (shown once)"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
    ],
    "bybit": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to bybit.com\n"
                "2. Go to API Management\n"
                "3. Create API key with Read Only permissions\n"
                "4. Copy API Key and Secret (shown once)"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
    ],
    "mexc": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to mexc.com\n"
                "2. Open API Management\n"
                "3. Create API key with Read-only permissions\n"
                "4. Copy API Key and Secret"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
    ],
    "mexc_earn": [
        CredentialField(
            "uid",
            "MEXC digitalId (UID)",
            secret=False,
            tip=(
                "1. Open mexc.com while logged in\n"
                "2. Call POST /ucenter/api/user_info from browser session\n"
                "3. Use response.data.digitalId as UID"
            ),
        ),
    ],
    "bitget_wallet": [
        CredentialField(
            "wallet_address",
            "Wallet address (0x...)",
            secret=False,
            tip=(
                "1. Open Bitget Wallet and copy your EVM address (0x...)\n"
                "2. PFM reads Stablecoin Earn Plus position on Base automatically"
            ),
        ),
        CredentialField(
            "solana_address",
            "Solana address for SOL staking (optional)",
            required=False,
            secret=False,
            tip=(
                "1. Open Bitget Wallet and switch to Solana network\n"
                "2. Copy your Solana address\n"
                "3. PFM reads native SOL staking positions automatically"
            ),
        ),
    ],
    "lobstr": [
        CredentialField(
            "stellar_address",
            "Stellar public address (G...)",
            secret=False,
            tip=(
                "1. Open the Lobstr app\n"
                "2. Tap Receive or your account icon\n"
                "3. Copy your Stellar public address (starts with G)"
            ),
        ),
    ],
    "blend": [
        CredentialField(
            "stellar_address",
            "Stellar public address (G...)",
            secret=False,
            tip=(
                "1. Get your Stellar public address from your wallet"
                " (starts with G)\n"
                "2. Find pool contract ID at mainnet.blend.capital"
            ),
        ),
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
        CredentialField(
            "api_token",
            "Personal API token",
            tip=(
                "1. Log in to wise.com\n"
                "2. Go to Settings → API tokens\n"
                "3. Create a new personal token\n"
                "4. Copy the token"
            ),
        ),
    ],
    "kbank": [
        CredentialField(
            "gmail_address",
            "Gmail address",
            secret=False,
            tip=(
                "1. In K PLUS app, request e-statement"
                " (sends PDF to your email)\n"
                "2. Create Gmail App Password at"
                " myaccount.google.com → Security → App Passwords\n"
                "3. Requires 2-Step Verification enabled"
            ),
        ),
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
        CredentialField(
            "flex_token",
            "Flex Web Service token",
            tip=(
                "1. Log in to IBKR Client Portal\n"
                "2. Go to Performance & Reports → Flex Queries\n"
                "3. Create Activity Query and generate"
                " Flex Web Service token\n"
                "4. Copy both the token and Query ID"
            ),
        ),
        CredentialField("flex_query_id", "Flex Query ID", secret=False),
    ],
    "rabby": [
        CredentialField(
            "wallet_address",
            "Wallet address (0x...)",
            secret=False,
            tip=(
                "1. Open Rabby and copy your main wallet address (0x...)\n"
                "2. Add it as source in PFM\n"
                "3. Access key is optional and only for legacy DeBank Pro setups"
            ),
        ),
        CredentialField("access_key", "DeBank AccessKey (optional)", required=False),
    ],
    "revolut": [
        CredentialField(
            "secret_id",
            "GoCardless Secret ID",
            tip=(
                "1. Register at bankaccountdata.gocardless.com\n"
                "2. Create a secret (copy ID and Key)\n"
                "3. Create requisition for Revolut"
                " (REVOLUT_REVOGB21)\n"
                "4. Authorize in Revolut app"
                " and copy Requisition ID"
            ),
        ),
        CredentialField("secret_key", "GoCardless Secret Key"),
        CredentialField("requisition_id", "GoCardless Requisition ID", secret=False),
    ],
    "trading212": [
        CredentialField(
            "api_key",
            "API Key",
            tip=(
                "1. Log in to trading212.com\n"
                "2. Open Invest API settings\n"
                "3. Create an API key with account data and history access\n"
                "4. Copy API Key and API Secret"
            ),
        ),
        CredentialField("api_secret", "API Secret"),
    ],
    "yo": [
        CredentialField(
            "network",
            "Network (e.g. base, ethereum, arbitrum)",
            secret=False,
            tip=(
                "1. Open app.yo.xyz and select your vault\n"
                "2. Copy network and vault contract address\n"
                "3. Use your wallet address as user address"
            ),
        ),
        CredentialField("vault_address", "Vault contract address (0x...)", secret=False),
        CredentialField("user_address", "Wallet address (0x...)", secret=False),
    ],
    "emcd": [
        CredentialField(
            "email",
            "EMCD account email",
            secret=False,
            tip=(
                "1. Log in to emcd.io\n"
                "2. Use the email address of your EMCD account\n"
                "3. Data is synced via the Lurii Finance Chrome extension"
            ),
        ),
    ],
}


@dataclass(frozen=True, slots=True)
class ApyRulesProtocol:
    """Describes which protocol+coins support APY rules for a source type."""

    protocol: str
    coins: tuple[str, ...]


APY_RULES_TYPES: dict[str, tuple[ApyRulesProtocol, ...]] = {
    "bitget_wallet": (ApyRulesProtocol(protocol="aave", coins=("usdc", "usdt")),),
}


def validate_credentials(source_type: str, credentials: dict[str, str]) -> list[str]:
    """Validate credentials against the schema for a source type.

    Returns a list of error messages (empty if valid).
    """
    fields = SOURCE_TYPES.get(source_type)
    if fields is None:
        return [f"Unknown source type: {source_type!r}"]

    return [f"Missing required field: {f.name}" for f in fields if f.required and not credentials.get(f.name)]
