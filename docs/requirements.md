# Requirements

## Overview

Personal Financial Management system that aggregates assets and statements from crypto exchanges, wallets, DeFi protocols, banks, and brokers. Produces a weekly Telegram report with total net worth, PnL, allocation breakdown, and AI-generated investment commentary.

## User Profile

- Base currency: **USD**
- Reporting: **Weekly push via Telegram bot**
- Storage: **Local SQLite**
- Secrets: **`.env` file**
- AI provider: **Claude API** (Anthropic)

---

## Data Sources (10)

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
| KBank | PDF statement parsing | None (manual PDF) |

### Stellar Ecosystem (3)

| Source | Method | Auth |
|--------|--------|------|
| Lobstr (wallet) | Stellar Horizon API | Public address (no auth) |
| Uphold (bridge) | REST API | Personal Access Token |
| Blend (DeFi yield) | Soroban RPC contract call | Public address (no auth) |

### Broker (1)

| Source | Method | Auth |
|--------|--------|------|
| IBKR | Flex Query (HTTP) | Flex token |

---

## Functional Requirements

### F1 — Data Collection

- F1.1: Fetch current balances from all 10 sources
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

- All secrets in `.env` (never committed)
- API keys are read-only where possible
- No plaintext secrets in logs
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
| Package manager | uv |
| Database | SQLite (via aiosqlite or sqlite3) |
| Migrations | alembic |
| HTTP client | httpx (async) |
| Crypto exchange SDKs | python-okx, binance-connector-python, pybit |
| Stellar | stellar-sdk |
| Wise | wise-api or raw httpx |
| Uphold | uphold-sdk-python or raw httpx |
| IBKR | ibflex |
| PDF parsing | pdfplumber |
| AI | anthropic (Claude API) |
| Telegram | python-telegram-bot or raw httpx |
| Scheduler | cron / systemd timer (external) |
| Linting | ruff |
| Type checking | mypy (strict) |
| Testing | pytest + pytest-cov + pytest-asyncio |
| Pre-commit | pre-commit |

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
│  │ OKX │ │Binance│ │Bybit│ │Uphold │  │
│  └──┬──┘ └───┬───┘ └──┬──┘ └───┬───┘  │
│  ┌──┴──┐ ┌───┴───┐ ┌──┴──┐ ┌───┴───┐  │
│  │Lobstr│ │ Blend │ │Wise │ │ IBKR  │  │
│  └──┬──┘ └───┬───┘ └──┬──┘ └───┬───┘  │
│     │    ┌───┴───┐    │        │       │
│     │    │ KBank │    │        │       │
│     │    └───┬───┘    │        │       │
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
├── config.py              # Settings, .env loading
├── db/
│   ├── __init__.py
│   ├── models.py          # SQLite schema / ORM models
│   ├── migrations/        # Alembic migrations
│   └── repository.py      # Data access layer
├── collectors/
│   ├── __init__.py
│   ├── base.py            # Abstract collector interface
│   ├── okx.py
│   ├── binance.py
│   ├── binance_th.py
│   ├── bybit.py
│   ├── uphold.py
│   ├── lobstr.py          # Stellar Horizon
│   ├── blend.py           # Soroban RPC
│   ├── wise.py
│   ├── kbank.py           # PDF parser
│   └── ibkr.py            # Flex Query
├── pricing/
│   ├── __init__.py
│   └── fx.py              # USD conversion, price feeds
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
└── cli.py                 # Entry point (collect, analyze, report)
```

---

## CLI Commands (planned)

```bash
# Fetch all sources and store snapshot
pfm collect

# Fetch a single source
pfm collect --source okx

# Run analytics on latest snapshot
pfm analyze

# Generate and send Telegram report
pfm report

# Full pipeline: collect → analyze → report
pfm run

# Import KBank PDF manually
pfm import-kbank /path/to/statement.pdf
```

---

## Decided

- **Price feed**: CoinGecko free tier (crypto prices + fiat rates, 30 req/min)

## Open Questions

1. **IBKR token refresh** — Flex tokens expire after 6 hours. Automation strategy?
2. **Blend pool IDs** — need to confirm mainnet contract addresses
3. **KBank statement format** — need a sample PDF to build the parser
4. **Binance TH API differences** — need to test which endpoints differ from global
5. **Tax reporting** — future scope? (capital gains, FIFO/LIFO cost basis methods)
