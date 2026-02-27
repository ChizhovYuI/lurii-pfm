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
| 5 | Lobstr | Stellar wallet | XLM, USDC | Stellar Horizon API (public address) |
| 6 | Blend | Stellar DeFi lending | USDC (fixed pool) | Soroban RPC (contract call) |
| 7 | Wise | Multi-currency fiat | GBP + others | REST API (personal token) |
| 8 | KBank | Thai bank | THB | PDF parsing (Gmail IMAP auto-fetch) |
| 9 | IBKR | Broker (stocks/ETFs) | USD | Flex Query (automated) |

## Money Flow

```
Salary → Anna Money → GBP → Wise → GBP → USDC (Stellar) → Lobstr → Blend (fixed pool yield)
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

1. Log in to [OKX](https://www.okx.com/) → [API Management](https://www.okx.com/account/my-api) → Create API key
2. Permissions: **Read only** (no trade, no withdraw)
3. IP whitelist recommended
4. Add to pfm: `pfm source add` → select `okx` → enter API key, secret, passphrase

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

1. Log in to [Binance](https://www.binance.com/) → [API Management](https://www.binance.com/en/my/settings/api-management) → Create API
2. Permissions: **Read only** (`enableReading=true`, all others false)
3. IP whitelist recommended
4. Keys unused 90 days without IP whitelist auto-reset to read-only
5. Add to pfm: `pfm source add` → select `binance` → enter API key, secret

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
- Uses v1 API endpoints (not v3) — see [API docs](https://www.binance.th/en/binance-api)

### Setup

1. Log in to [Binance TH](https://www.binance.th/) → [API Management](https://www.binance.th/en/my/settings/api-management) → Create read-only API key
2. Add to pfm: `pfm source add` → select `binance_th` → enter API key, secret

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

1. Log in to [Bybit](https://www.bybit.com/) → [API Management](https://www.bybit.com/app/user/api-management) → Create API key
2. Permissions: Read only (Orders, Positions, Trade, Account Transfer)
3. Add to pfm: `pfm source add` → select `bybit` → enter API key, secret

### Python Library

```
pip install pybit  # official
```

### Fallback

CSV export via Account → Data Export. Max 6-month range, 10k entries, 5 exports/day. Processing takes 1-3 days.

---

## 5. Lobstr (Stellar Wallet)

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

1. Open [Lobstr](https://lobstr.co/) app → copy your Stellar public address (`G...`)
2. No API key needed — all Stellar data is public on-chain
3. Add to pfm: `pfm source add` → select `lobstr` → enter Stellar address
4. Horizon API docs: [developers.stellar.org](https://developers.stellar.org/docs/data/horizon)

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

## 6. Blend (Stellar DeFi — Fixed Pool)

**Method:** Soroban RPC contract call via `stellar-sdk`

Blend has **no REST API**. It's a Soroban smart contract. Positions are read by calling `get_positions(address)` on the pool contract.

### Approach

1. Use `stellar-sdk` with `SorobanServer`
2. Build a transaction invoking `get_positions(your_stellar_address)` on the Blend pool contract
3. Call `simulate_transaction()` — returns position data without submitting
4. Parse result: bToken amounts × `b_rate` = underlying asset value

### Setup

1. Find your pool contract ID via [Blend app](https://mainnet.blend.capital/) or [docs](https://docs.blend.capital/)
2. Add to pfm: `pfm source add` → select `blend` → enter Stellar address, pool contract ID (optional: Soroban RPC URL)

### Key Details

- `b_rate` / `d_rate` change every ledger — fetch fresh each time
- No Python Blend SDK exists — replicate the JS SDK logic via raw Soroban calls
- Integration reference: [docs.blend.capital/tech-docs/integrations/integrate-pool](https://docs.blend.capital/tech-docs/integrations/integrate-pool)
- Soroban RPC docs: [soroban.stellar.org](https://soroban.stellar.org/docs)

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

## 7. Wise

**Method:** REST API with personal API token

### Endpoints

| Purpose | Endpoint | Rate Limit |
|---------|----------|------------|
| Balances | via balance endpoints | 500 req/min |
| Balance statement | `GET /balance-statement` | 500 req/min |
| Transaction history | via statement endpoints | 500 req/min |

### Setup

1. Log in to [Wise](https://wise.com/) → Settings → [API tokens](https://wise.com/settings/api-tokens)
2. Available for personal accounts
3. Read-only access to balance and transaction endpoints
4. Add to pfm: `pfm source add` → select `wise` → enter API token
5. API docs: [docs.wise.com](https://docs.wise.com/)
6. Note: EU/UK PSD2 may restrict some actions with personal tokens

### Python Library

```
pip install wise-api  # community, Python >= 3.10
```

No official SDK from Wise.

### Fallback

CSV/PDF/XLSX/JSON export via web UI (Transactions page). Up to 365-day periods. Also supports MT940, QIF, CAMT.053 formats.

---

## 8. Kasikorn Bank (KBank)

**Method:** PDF statement parsing via Gmail IMAP auto-fetch (no personal API available)

### Why No API

- KBank API portal exists but restricted to approved business partners (50k-100k THB/month)
- Thailand Open Banking ("Your Data" regulation) mandated Oct 2025, but implementation deadline is end of 2026
- No Thai bank has live open banking data sharing for individuals yet
- Western aggregators (Plaid, Tink) do not cover Thai banks

### Approach

Two modes supported:

1. **Auto (Gmail IMAP):** Collector connects to Gmail via IMAP, searches for emails from `K-ElectronicDocument@kasikornbank.com`, downloads the latest PDF attachment, and parses it automatically.
2. **Manual:** `pfm import-kbank /path/to/statement.pdf` — parse a local PDF file directly.

Balance handling:
- Ending balance is parsed in THB and converted to USD at collect time for analytics.
- Transactions remain recorded in THB (`usd_value=0` currently; historical FX conversion is deferred).

### Setup (Gmail auto-fetch)

1. In [K PLUS](https://www.kasikornbank.com/en/personal/Digital-banking/Pages/KPLUS.aspx) app → request statement (sends password-protected PDF to your email)
2. Create a [Gmail App Password](https://myaccount.google.com/apppasswords) (requires [2-Step Verification](https://myaccount.google.com/signinoptions/two-step-verification) enabled)
3. Add to pfm: `pfm source add` → select `kbank` → enter Gmail address, app password, PDF password (DDMMYYYY)

### PDF Format

KBank PDFs are password-protected (date of birth in DDMMYYYY format). Structure per page:
- Table 1 (page 1 only): header with account info and ending balance
- Table 2+: transactions packed into newline-delimited cells (not one-per-row)

### Statement Options

| Source | Format | Period |
|--------|--------|--------|
| K PLUS → Request Statement | PDF (email) | Up to 12 months |
| K-eMail Statement (web/app) | PDF (recurring) | Monthly |
| MAKE by KBank app | PDF | Up to 6 months |

No native CSV export from any KBank product.

### Python Libraries

```
pip install pdfplumber  # PDF table extraction
```

### Future

Monitor BOT "Your Data" implementation (expected late 2026): [bot.or.th/en/financial-innovation/digital-finance/open-data](https://bot.or.th/en/financial-innovation/digital-finance/open-data.html)

---

## 9. Interactive Brokers (IBKR)

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

1. Log in to [IBKR Client Portal](https://www.interactivebrokers.com/sso/Login) → [Performance & Reports](https://www.interactivebrokers.com/en/index.php?f=4700) → Flex Queries → create Activity query (positions, trades, cash, dividends)
2. Flex Web Service → generate token (6-hour default expiry, IP-lockable)
3. Add to pfm: `pfm source add` → select `ibkr` → enter flex token and query ID
4. Schedule daily cron job ~30 min after market close
5. API docs: [interactivebrokers.github.io/tws-api](https://interactivebrokers.github.io/tws-api/)

### Collector Runtime Notes

- `SendRequest` calls are throttled with a minimum delay between requests.
- The collector reuses a short-lived statement cache across `fetch_balances()` and `fetch_transactions()` within one run.
- This reduces `ErrorCode 1018` ("Too many requests") during concurrent collect runs.

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
| Lobstr | Low | None (public) | Yes |
| Blend | High | None (public) | Yes |
| Wise | Low | Personal token | Yes |
| KBank | Medium | Gmail App Password | No (batch) |
| IBKR | Medium | Flex token | No (EOD) |

### Recommended Implementation Order

1. **Lobstr** — zero auth, instant (just need public address)
2. **Wise** — personal token, simple REST
3. **OKX / Binance / Bybit** — API keys, well-documented SDKs
4. **Binance TH** — similar to Binance but needs testing
5. **IBKR** — Flex Query setup, cron-based
6. **Blend** — Soroban contract calls, most complex
7. **KBank** — PDF parsing, least automatable
