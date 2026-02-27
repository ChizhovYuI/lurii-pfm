# Requirements

## Overview

Personal Financial Management system that aggregates assets and statements from crypto exchanges, wallets, DeFi protocols, banks, and brokers. Produces a weekly Telegram report with total net worth, PnL, allocation breakdown, and AI-generated investment commentary.

## User Profile

- Base currency: **USD**
- Reporting: **Weekly push via Telegram bot**
- Storage: **Local SQLite**
- Secrets: **`.env` for global settings**, **SQLite for source credentials**
- AI provider: **Claude API** (Anthropic)

---

## Data Sources (9)

### Crypto Exchanges (4)

| Source | Method | Auth |
|--------|--------|------|
| OKX | REST API v5 | Read-only API key |
| Binance (global) | REST API | Read-only API key |
| Binance TH | REST API | Read-only API key |
| Bybit | REST API v5 | Read-only API key |

### Fiat / Banking (2)

| Source | Method | Auth |
|--------|--------|------|
| Wise | REST API | Personal token |
| KBank | PDF parsing (Gmail IMAP auto-fetch or manual) | Gmail App Password |

### Stellar Ecosystem (2)

| Source | Method | Auth |
|--------|--------|------|
| Lobstr (wallet) | Stellar Horizon API | Public address (no auth) |
| Blend (DeFi yield) | Soroban RPC contract call | Public address (no auth) |

### Broker (1)

| Source | Method | Auth |
|--------|--------|------|
| IBKR | Flex Query (HTTP) | Flex token |

---

## Functional Requirements

### F0 — Source Management

- F0.1: Dynamic source configuration via CLI (`pfm source add/list/show/delete`)
- F0.2: Sources stored in SQLite `sources` table (name, type, credentials JSON, enabled flag)
- F0.3: Interactive wizard for adding sources (pick type → name → fill credentials)
- F0.4: Named instances — multiple accounts per source type (e.g. `okx-main`, `okx-trading`)
- F0.5: 9 hardcoded source types: okx, binance, binance_th, bybit, lobstr, blend, wise, kbank, ibkr
- F0.6: `pfm source show` masks secrets in output
- F0.7: `pfm source enable/disable` toggles source activity
- F0.8: `pfm collect` auto-discovers enabled sources from DB

### F1 — Data Collection

- F1.1: Fetch current balances from all configured sources
- F1.2: Fetch transaction history (deposits, withdrawals, trades, yields)
- F1.3: Convert all balances to USD using live exchange rates
- F1.4: Handle KBank PDF import (manual trigger or email-based)
- F1.5: Handle IBKR Flex Query (scheduled, EOD data)
- F1.6: Handle Blend Soroban contract position reading
- F1.7: Store raw responses for auditability

### F2 — Storage

- F2.1: Local SQLite database
- F2.2: Daily snapshots of all positions (historical tracking)
- F2.3: Transaction log (normalized across all sources)
- F2.4: Price history cache (for PnL calculations)
- F2.5: Schema migrations (alembic or similar)
- F2.6: Source configurations in `sources` table (credentials as JSON, plain text)

### F3 — Portfolio Analytics

- F3.1: **Total net worth** (sum of all assets in USD)
- F3.2: **PnL** — daily, weekly, monthly, all-time
- F3.3: **Asset allocation** — by asset, by source, by category (crypto/fiat/stocks/DeFi)
- F3.4: **Yield tracking** — Blend fixed pool returns
- F3.5: **Cost basis** — per asset (for tax/gain tracking)
- F3.6: **Currency exposure** — breakdown by currency (USD, GBP, THB, BTC, ETH, XLM, USDC, etc.)
- F3.7: **Risk metrics** — concentration %, largest positions

### F4 — AI Analysis

- F4.1: Feed portfolio snapshot + recent changes to Claude API
- F4.2: Generate weekly investment commentary:
  - Market context for held assets
  - Portfolio health assessment
  - Rebalancing suggestions
  - Risk alerts (over-concentration, correlated assets, yield changes)
  - Actionable recommendations
- F4.3: Keep prompts version-controlled and tunable

### F5 — Telegram Reporting

- F5.1: Push-only bot (no interactive commands)
- F5.2: Weekly scheduled report containing:
  - Total net worth (USD)
  - Week-over-week PnL (absolute + %)
  - Top gainers / losers
  - Asset allocation pie chart or breakdown
  - Yield summary (Blend)
  - AI-generated commentary and recommendations
- F5.3: Configurable schedule (day of week, time)
- F5.4: Error alerts (if a data source fails to fetch)

---

## Non-Functional Requirements

### NF1 — Code Quality

- Strict ruff linting (35+ rule sets) — already configured
- Strict mypy (`strict = true`) — already configured
- Pre-commit hooks (ruff + mypy + security checks) — already configured
- 80% minimum test coverage — already configured
- All functions typed

### NF2 — Security

- Global secrets (Telegram, Claude API, CoinGecko) in `.env` (never committed)
- Source credentials in SQLite `sources` table (local file, gitignored `data/` directory)
- API keys are read-only where possible
- No plaintext secrets in logs; `pfm source show` masks credential values
- PDF statements stored outside git (in `data/` — gitignored)
- Private keys / seed phrases are NEVER stored

### NF3 — Reliability

- Graceful degradation: if one source fails, report the rest + flag the error
- Retry with backoff for transient API failures
- Idempotent fetchers (safe to re-run)
- Logging with structured output

### NF4 — Performance

- Weekly batch job — no real-time requirements
- Target: full portfolio fetch < 5 minutes
- SQLite is sufficient for single-user

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.13+ |
| Package manager | [uv](https://docs.astral.sh/uv/) |
| Database | SQLite (via [aiosqlite](https://pypi.org/project/aiosqlite/)) |
| Migrations | alembic |
| HTTP client | [httpx](https://www.python-httpx.org/) (async) |
| Crypto exchanges | Raw httpx + HMAC signing (OKX, Binance, Bybit) |
| Stellar | [stellar-sdk](https://stellar-sdk.readthedocs.io/) (Horizon + Soroban) |
| Wise | Raw httpx (Bearer token) |
| IBKR | [ibflex](https://pypi.org/project/ibflex/) (Flex Query parser) |
| PDF parsing | [pdfplumber](https://github.com/jsvine/pdfplumber) |
| KBank email | Python stdlib (`imaplib` + `email`) |
| Pricing | [CoinGecko API](https://docs.coingecko.com/reference/introduction) (free tier) |
| AI | [anthropic](https://docs.anthropic.com/) (Claude API) |
| Telegram | Raw httpx (push-only bot) |
| Scheduler | cron / systemd timer (external) |
| Linting | [ruff](https://docs.astral.sh/ruff/) |
| Type checking | [mypy](https://mypy.readthedocs.io/) (strict) |
| Testing | pytest + pytest-cov + pytest-asyncio |
| Pre-commit | [pre-commit](https://pre-commit.com/) |

---

## Architecture (High Level)

```
┌─────────────┐
│   Scheduler  │  (cron: weekly)
│   (trigger)  │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│            Collector Layer              │
│                                         │
│  ┌─────┐ ┌───────┐ ┌─────┐ ┌───────┐  │
│  │ OKX │ │Binance│ │Bybit│ │Lobstr │  │
│  └──┬──┘ └───┬───┘ └──┬──┘ └───┬───┘  │
│  ┌──┴──┐ ┌───┴───┐ ┌──┴──┐ ┌───┴───┐  │
│  │Blend│ │ Wise  │ │IBKR │ │ KBank │  │
│  └──┬──┘ └───┬───┘ └──┬──┘ └───┬───┘  │
│     └────┬───┴────┬───┘────────┘       │
│          ▼        ▼                    │
│   ┌────────────────────┐               │
│   │  Normalizer Layer  │               │
│   │  (USD conversion)  │               │
│   └─────────┬──────────┘               │
└─────────────┼───────────────────────────┘
              ▼
┌─────────────────────────┐
│     SQLite Database     │
│  (snapshots, tx log,    │
│   prices, raw data)     │
└────────────┬────────────┘
             ▼
┌─────────────────────────┐
│    Analytics Engine     │
│  (PnL, allocation,     │
│   yield, cost basis)    │
└────────────┬────────────┘
             ▼
┌─────────────────────────┐
│   Claude API (AI)       │
│  (commentary, recs)     │
└────────────┬────────────┘
             ▼
┌─────────────────────────┐
│   Telegram Bot (push)   │
│  (weekly report)        │
└─────────────────────────┘
```

---

## Modules

```
src/pfm/
├── __init__.py
├── config.py              # Global settings (.env loading)
├── source_types.py        # Credential schemas per source type
├── cli.py                 # Entry point (source, collect, analyze, report)
├── logging.py             # Structured logging with secret redaction
├── db/
│   ├── __init__.py
│   ├── models.py          # SQLite schema / dataclass models
│   ├── source_store.py    # Source CRUD (sources table)
│   ├── repository.py      # Data access layer
│   └── migrations/        # Alembic migrations
├── collectors/
│   ├── __init__.py        # COLLECTOR_REGISTRY + auto-import
│   ├── base.py            # Abstract collector interface
│   ├── _auth.py           # HMAC signing (OKX, Binance, Bybit)
│   ├── _retry.py          # Retry decorator + rate limiter
│   ├── okx.py
│   ├── binance.py
│   ├── binance_th.py
│   ├── bybit.py
│   ├── lobstr.py          # Stellar Horizon
│   ├── blend.py           # Soroban RPC
│   ├── wise.py
│   ├── kbank.py           # PDF parser + Gmail IMAP
│   └── ibkr.py            # Flex Query
├── pricing/
│   ├── __init__.py
│   └── coingecko.py       # CoinGecko API (crypto prices + fiat rates)
├── analytics/
│   ├── __init__.py
│   ├── portfolio.py       # Net worth, allocation, exposure
│   ├── pnl.py             # PnL calculations
│   └── yield_tracker.py   # Blend yield tracking
├── ai/
│   ├── __init__.py
│   ├── analyst.py         # Claude API integration
│   └── prompts.py         # Version-controlled prompt templates
├── reporting/
│   ├── __init__.py
│   └── telegram.py        # Telegram bot (push only)
```

---

## CLI Commands

```bash
# ── Source management ──────────────────────────────────────────────
pfm source add              # Interactive wizard: pick type → name → credentials
pfm source list             # Table of all sources (name, type, enabled, created_at)
pfm source show <name>      # Details with masked secrets
pfm source delete <name>    # Remove with confirmation
pfm source enable <name>    # Enable a source
pfm source disable <name>   # Disable a source

# ── Data collection ───────────────────────────────────────────────
pfm collect                 # Fetch all enabled sources
pfm collect --source <name> # Fetch a single named source

# ── Analytics & reporting ─────────────────────────────────────────
pfm analyze                 # Run analytics on latest snapshot
pfm report                  # Generate and send Telegram report
pfm run                     # Full pipeline: collect → analyze → report

# ── Utilities ─────────────────────────────────────────────────────
pfm import-kbank /path/to/statement.pdf   # Import KBank PDF manually
```

---

## Decided

- **Price feed**: [CoinGecko](https://www.coingecko.com/en/api) free tier (crypto prices + fiat rates, 30 req/min)
- **AI commentary**: [Claude API](https://console.anthropic.com/) — get key at [console.anthropic.com/settings/keys](https://console.anthropic.com/settings/keys)
- **Telegram bot**: create via [@BotFather](https://t.me/BotFather), get chat ID via [@userinfobot](https://t.me/userinfobot)

## Open Questions

1. **IBKR token refresh** — Flex tokens expire after 6 hours. Automation strategy?
2. ~~**Blend pool IDs**~~ — resolved: `BLEND_POOL_CONTRACT_ID` env var
3. ~~**KBank statement format**~~ — resolved: parser handles newline-delimited cells, password-protected PDFs, Gmail IMAP auto-fetch
4. ~~**Binance TH API differences**~~ — resolved: uses v1 API endpoints instead of v3
5. **Tax reporting** — future scope? (capital gains, FIFO/LIFO cost basis methods)
