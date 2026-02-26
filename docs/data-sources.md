# Data Sources

Overview of all financial data sources, recommended integration method, and access details.

**Base currency:** USD

## Source Map

| # | Source | Type | Assets | Recommended Method |
|---|--------|------|--------|--------------------|
| 1 | OKX | Crypto exchange | Various | REST API (read-only key) |
| 2 | Binance | Crypto exchange (global) | Various | REST API (read-only key) |
| 3 | Binance TH | Crypto exchange (Thailand) | Various + THB | REST API (read-only key) |
| 4 | Bybit | Crypto exchange | Various | REST API (read-only key) |
| 5 | Uphold | Fiat-to-crypto bridge | GBP, USDC | REST API (Personal Access Token) |
| 6 | Lobstr | Stellar wallet | XLM, USDC | Stellar Horizon API (public address) |
| 7 | Blend | Stellar DeFi lending | USDC (fixed pool) | Soroban RPC (contract call) |
| 8 | Wise | Multi-currency fiat | GBP + others | REST API (personal token) |
| 9 | KBank | Thai bank | THB | PDF statement parsing |
| 10 | IBKR | Broker (stocks/ETFs) | USD | Flex Query (automated) |

## Money Flow

```
Salary → Anna Money → GBP → Wise → GBP → Uphold → USDC (Stellar) → Lobstr → Blend (fixed pool yield)
```

---

## 1. OKX

**Method:** REST API v5 with read-only API key

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Trading balance | `GET /api/v5/account/balance` | 10 req/2s |
| Funding balance | `GET /api/v5/asset/balances` | 10 req/2s |
| Earn positions | `GET /api/v5/earning/*` | varies |
| Bills (7 days) | `GET /api/v5/account/bills` | 5 req/s |
| Bills (3 months) | `GET /api/v5/account/bills-archive` | 5 req/2s |
| Bills (since 2021) | `GET /api/v5/account/bills-history-archive` | 10 req/2s |
| Asset valuation | `GET /api/v5/asset/asset-valuation` | - |

### Setup

1. OKX → API Management → Create API key
2. Permissions: **Read only** (no trade, no withdraw)
3. IP whitelist recommended

### Python Library

```
pip install okx  # official: python-okx
```

### Fallback

CSV export available via web UI (monthly statements).

---

## 2. Binance (Global)

**Method:** REST API with read-only API key

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Account balance | `GET /api/v3/account` | 20 weight |
| Order history | `GET /api/v3/allOrders` | 20 weight |
| Trade history | `GET /api/v3/myTrades` | 20 weight |
| Deposit history | `GET /sapi/v1/capital/deposit/hisrec` | - |
| Withdraw history | `GET /sapi/v1/capital/withdraw/history` | - |

Global rate limit: **6,000 request weight per minute**.

### Setup

1. Binance → API Management → Create API
2. Permissions: **Read only** (`enableReading=true`, all others false)
3. IP whitelist recommended
4. Keys unused 90 days without IP whitelist auto-reset to read-only

### Python Library

```
pip install binance-connector-python  # official
# or
pip install python-binance  # community, most popular
```

### Fallback

CSV export via web UI (Wallet → Transaction History → Export). Max 15 exports/month, up to 1 year per export.

---

## 3. Binance TH (Thailand)

**Method:** REST API with read-only API key

### Differences from Binance Global

- Spot trading only (no futures/margin/leverage)
- THB trading pairs (BTC/THB, ETH/THB, USDT/THB)
- Some endpoint names and response formats differ
- API docs: `https://www.binance.th/api-docs/en/`

### Setup

Same process as Binance Global — create read-only API key on Binance TH platform.

### Python Library

May need a custom client or adapted `binance-connector-python` pointing to Binance TH base URL.

---

## 4. Bybit

**Method:** REST API v5 with read-only API key

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Wallet balance | `GET /v5/account/wallet-balance` | 50 req/s |
| Transaction log | `GET /v5/account/transaction-log` | 50 req/s |
| Coin balance | `GET /v5/asset/transfer/query-account-coins-balance` | varies |

Auth: HMAC SHA256 signature via headers (`X-BAPI-SIGN`, `X-BAPI-API-KEY`, `X-BAPI-TIMESTAMP`).

### Setup

1. Bybit → API Management → Create API key
2. Permissions: Read only (Orders, Positions, Trade, Account Transfer)

### Python Library

```
pip install pybit  # official
```

### Fallback

CSV export via Account → Data Export. Max 6-month range, 10k entries, 5 exports/day. Processing takes 1-3 days.

---

## 5. Uphold

**Method:** REST API with Personal Access Token (PAT)

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Transaction history | `GET /core/accounts/{id}/transactions` | 250 req/min |
| Card balances | via card/account endpoints | 250 req/min |

### Setup

1. Uphold → Settings → Personal Access Token
2. OAuth scopes: `accounts:read`, `cards:read`, `transactions:read`

### Python Library

```
pip install uphold-sdk-python  # community
```

### Fallback

CSV export via Activity → Generate Report (sent to email).

---

## 6. Lobstr (Stellar Wallet)

**Method:** Stellar Horizon API — no authentication needed, just the public address

Lobstr is a standard Stellar wallet. All data is on-chain.

### Endpoints

Base URL: `https://horizon.stellar.org`

| Purpose | Endpoint |
|---------|----------|
| Account balances | `GET /accounts/{public_key}` |
| Transaction history | `GET /accounts/{public_key}/transactions` |
| Payment history | `GET /accounts/{public_key}/payments` |
| Operations | `GET /accounts/{public_key}/operations` |

Rate limit: **3,600 req/hour** per IP (public node).

### Balance Response

The `/accounts/{public_key}` response includes a `balances` array with:
- `asset_type`: `native` (XLM), `credit_alphanum4`, `credit_alphanum12`
- `asset_code`, `asset_issuer`, `balance`

### Setup

1. Get your Stellar public address (`G...`) from Lobstr
2. No API key needed

### Python Library

```
pip install stellar-sdk  # official, supports async with aiohttp
```

```python
from stellar_sdk import Server

server = Server("https://horizon.stellar.org")
account = server.load_account("GABC...")  # includes balances
payments = server.payments().for_account("GABC...").call()
```

---

## 7. Blend (Stellar DeFi — Fixed Pool)

**Method:** Soroban RPC contract call via `stellar-sdk`

Blend has **no REST API**. It's a Soroban smart contract. Positions are read by calling `get_positions(address)` on the pool contract.

### Approach

1. Use `stellar-sdk` with `SorobanServer`
2. Build a transaction invoking `get_positions(your_stellar_address)` on the Blend pool contract
3. Call `simulate_transaction()` — returns position data without submitting
4. Parse result: bToken amounts × `b_rate` = underlying asset value

### Key Details

- Pool contract IDs must be known (from Blend app or docs)
- `b_rate` / `d_rate` change every ledger — fetch fresh each time
- No Python Blend SDK exists — replicate the JS SDK logic via raw Soroban calls
- Reference: `https://docs.blend.capital/tech-docs/integrations/integrate-pool`

### Python Library

```
pip install stellar-sdk[aiohttp]  # same SDK, Soroban support included
```

```python
from stellar_sdk import SorobanServer

soroban = SorobanServer("https://mainnet.stellar.validationcloud.io/v1/...")
# build + simulate transaction invoking get_positions()
```

### Supplementary

- **StellarExpert API** (`stellar.expert/openapi.html`) — asset metadata, supply, ratings (free, no auth)

---

## 8. Wise

**Method:** REST API with personal API token

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Balances | via balance endpoints | 500 req/min |
| Balance statement | `GET /balance-statement` | 500 req/min |
| Transaction history | via statement endpoints | 500 req/min |

### Setup

1. Wise → Settings → Integrations and Tools → API tokens
2. Available for personal accounts
3. Read-only access to balance and transaction endpoints
4. Note: EU/UK PSD2 may restrict some actions with personal tokens

### Python Library

```
pip install wise-api  # community, Python >= 3.10
```

No official SDK from Wise.

### Fallback

CSV/PDF/XLSX/JSON export via web UI (Transactions page). Up to 365-day periods. Also supports MT940, QIF, CAMT.053 formats.

---

## 9. Kasikorn Bank (KBank)

**Method:** PDF statement parsing (no personal API available)

### Why No API

- KBank API portal exists but restricted to approved business partners (50k-100k THB/month)
- Thailand Open Banking ("Your Data" regulation) mandated Oct 2025, but implementation deadline is end of 2026
- No Thai bank has live open banking data sharing for individuals yet
- Western aggregators (Plaid, Tink) do not cover Thai banks

### Approach

1. Enable **K-eMail Statement** (monthly auto-delivery, PDF) from K PLUS app
2. Parse the PDF with Python (`pdfplumber` or `camelot-py`)
3. Ingest structured data into the tracker

### Statement Options

| Source | Format | Period |
|--------|--------|--------|
| K PLUS → Request Statement | PDF (email) | Up to 12 months |
| K-eMail Statement (web/app) | PDF (recurring) | Monthly |
| MAKE by KBank app | PDF | Up to 6 months |

No native CSV export from any KBank product.

### Python Libraries

```
pip install pdfplumber  # or camelot-py
```

### Future

Monitor BOT "Your Data" implementation (expected late 2026): `bot.or.th/en/financial-innovation/digital-finance/open-data.html`

---

## 10. Interactive Brokers (IBKR)

**Method:** Flex Query (automated HTTP retrieval) — best for daily snapshots, no daemon needed

### Flex Query Workflow

**Step 1 — Trigger report:**
```
GET https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest
    ?t={TOKEN}&q={QUERY_ID}&v=3
```
Returns a `<ReferenceCode>`.

**Step 2 — Fetch report:**
```
GET https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement
    ?t={TOKEN}&q={REFERENCE_CODE}&v=3
```

### Rate Limits

| Layer | Limit |
|-------|-------|
| Flex Web Service | 1 req/sec, 10 req/min per token |
| Client Portal Web API | ~50 req/sec |

### Setup

1. Client Portal → Reports → Flex Queries → create Activity query (positions, trades, cash, dividends)
2. Flex Web Service → generate token (6-hour default expiry, IP-lockable)
3. Schedule daily cron job ~30 min after market close

### Python Library

```
pip install ibflex  # Flex Query parser + HTTP client
```

```python
from ibflex import client, parser

response = client.download(token="YOUR_TOKEN", query_id="YOUR_QUERY_ID")
statement = parser.parse(response)
```

### Alternative: Client Portal REST API

| Purpose | Endpoint |
|---------|----------|
| List accounts | `GET /portfolio/accounts` |
| Account summary (NAV) | `GET /portfolio/{accountId}/summary` |
| Account ledger | `GET /portfolio/{accountId}/ledger` |
| Positions | `GET /portfolio/{accountId}/positions/0` |
| P&L | `GET /iserver/account/pnl/partitioned` |

Requires OAuth or local CP Gateway proxy. Better for real-time; overkill for daily snapshots.

### Alternative: TWS API

```
pip install ib_async  # active replacement for ib_insync (unmaintained)
```

Requires IB Gateway or TWS running. Not recommended for simple daily snapshots.

### Fallback

Activity Statements via Client Portal → Reports → Statements (CSV/Excel). Can be auto-emailed daily/weekly/monthly.

---

## Summary: Integration Complexity

| Source | Complexity | Auth | Real-time? |
|--------|-----------|------|------------|
| OKX | Low | API key | Yes |
| Binance | Low | API key | Yes |
| Binance TH | Medium | API key | Yes |
| Bybit | Low | API key | Yes |
| Uphold | Low | PAT/OAuth | Yes |
| Lobstr | Low | None (public) | Yes |
| Blend | High | None (public) | Yes |
| Wise | Low | Personal token | Yes |
| KBank | High | None (PDF parse) | No (batch) |
| IBKR | Medium | Flex token | No (EOD) |

### Recommended Implementation Order

1. **Lobstr** — zero auth, instant (just need public address)
2. **Wise** — personal token, simple REST
3. **Uphold** — PAT, simple REST
4. **OKX / Binance / Bybit** — API keys, well-documented SDKs
5. **Binance TH** — similar to Binance but needs testing
6. **IBKR** — Flex Query setup, cron-based
7. **Blend** — Soroban contract calls, most complex
8. **KBank** — PDF parsing, least automatable
