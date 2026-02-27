# Implementation Plan

48 tasks across 9 phases. Each phase delivers something testable.

**Effort key:** S = half day, M = 1 day, L = 2 days

---

## Phase 0 — Foundation (Infrastructure, Config, DB, CLI Skeleton)

Everything wired and testable, but no real data yet.

### Task 0.1 — Configuration module `src/pfm/config.py` [S] ✅

**Dependencies:** None

**Files:** create `src/pfm/config.py`, modify `pyproject.toml` (add `pydantic-settings`)

`Settings` class using `pydantic-settings` reading from `.env`. Global fields only: DB path, Telegram bot token/chat ID, Claude API key, CoinGecko API key, log level. Source-specific credentials managed separately via `pfm source` CLI (stored in SQLite). All secrets are `SecretStr`. Cached `get_settings()` factory.

**Acceptance:**
- Loads from `.env.example` without errors
- Env var override works
- No plaintext secrets in `repr()`
- mypy + ruff pass

### Task 0.2 — Update `.env.example` [S] ✅

**Dependencies:** 0.1

**Files:** modify `.env.example`

Add placeholders for every secret field in `Settings`.

### Task 0.3 — Database models `src/pfm/db/models.py` [M] ✅

**Dependencies:** 0.1

**Files:** create `src/pfm/db/__init__.py`, `src/pfm/db/models.py`, modify `pyproject.toml` (add `aiosqlite`)

Dataclass models for tables:
- `snapshots` — daily portfolio snapshots (id, date, source, asset, amount, usd_value, raw_json, created_at)
- `transactions` — normalized tx log (id, date, source, tx_type enum [deposit/withdrawal/trade/yield/dividend/interest], asset, amount, usd_value, counterparty_asset, counterparty_amount, tx_id, raw_json, created_at)
- `prices` — price cache (id, date, asset, currency, price, source, created_at)
- `raw_responses` — raw API responses (id, date, source, endpoint, response_body, created_at)
- `analytics_cache` — pre-computed analytics (id, date, metric_name, metric_json, created_at)

`schema.sql` constant + `async def init_db(path: Path) -> None`.

**Acceptance:**
- `init_db` creates SQLite file with all tables
- Round-trip test: insert row, read back, fields match

### Task 0.4 — Repository / data access `src/pfm/db/repository.py` [M] ✅

**Dependencies:** 0.3

**Files:** create `src/pfm/db/repository.py`

Async repository wrapping `aiosqlite`:
- `save_snapshot`, `save_snapshots` (batch), `save_transaction`, `save_raw_response`, `save_price`
- `get_snapshots_by_date`, `get_latest_snapshots`, `get_snapshots_for_range`
- `get_prices_by_date`, `get_transactions(source, start, end)`

Context manager pattern for connection lifecycle.

**Acceptance:**
- All methods work against in-memory SQLite
- Batch insert is atomic
- Unit tests with >90% coverage on this module

### Task 0.5 — Abstract collector interface `src/pfm/collectors/base.py` [S] ✅

**Dependencies:** 0.3

**Files:** create `src/pfm/collectors/__init__.py`, `src/pfm/collectors/base.py`

`BaseCollector(ABC)`:
- `source_name: str`
- `async def fetch_balances() -> list[Snapshot]` (abstract)
- `async def fetch_transactions(since: date | None) -> list[Transaction]` (abstract)
- `async def collect(repo, since) -> CollectorResult` — calls both, saves via repo, catches exceptions

`CollectorResult` dataclass: source, snapshots_count, transactions_count, errors, duration_seconds.

`CollectorRegistry` dict mapping source names → classes.

**Acceptance:**
- Cannot instantiate `BaseCollector` directly
- Mock subclass runs `collect()` end-to-end against in-memory DB
- Errors in one fetch don't prevent the other

### Task 0.6 — Pricing service `src/pfm/pricing/coingecko.py` [M] ✅

**Dependencies:** 0.1, 0.4

**Files:** create `src/pfm/pricing/__init__.py`, `src/pfm/pricing/coingecko.py`, modify `pyproject.toml` (add `httpx`)

CoinGecko client:
- `async def get_prices(asset_ids, vs_currency="usd") -> dict[str, Decimal]`
- `async def get_fiat_rates(base="usd") -> dict[str, Decimal]` — THB, GBP, EUR
- `async def convert_to_usd(amount, asset) -> Decimal`
- Ticker → CoinGecko ID mapping
- Rate limiting (30 req/min)
- Cache in `prices` DB table (serve if < 1 hour old)
- Retry with exponential backoff

**Acceptance:**
- Mocked httpx tests return correct prices
- Rate limiter prevents exceeding 30 req/min
- Cache hit avoids HTTP call
- Unknown asset raises typed error

### Task 0.7 — CLI entry point `src/pfm/cli.py` [M] ✅

**Dependencies:** 0.1, 0.4, 0.5

**Files:** create `src/pfm/cli.py`, modify `pyproject.toml` (add `click`, `[project.scripts]` entry)

CLI using `click`:
- `pfm source add/list/show/delete/enable/disable` — source management (Phase 0.5)
- `pfm collect [--source NAME]` — fetch from DB-configured sources
- `pfm analyze`
- `pfm report`
- `pfm run` — full pipeline
- `pfm import-kbank PATH`

Stubs for analytics/reporting initially. Wire up settings, DB init, structured logging.

**Acceptance:**
- `pfm --help` shows all commands including `source` group
- `pfm source --help` shows subcommands
- Each command logs start/end
- Exit code 0 for stubs

### Task 0.8 — Structured logging `src/pfm/logging.py` [S] ✅

**Dependencies:** None

**Files:** create `src/pfm/logging.py`

`setup_logging(level="INFO")`. Structured output (key=value or JSON lines). Secret redaction filter.

**Acceptance:**
- Output includes timestamp and level
- Secret patterns redacted in logs

### Task 0.9 — Test infrastructure [S] ✅

**Dependencies:** 0.1–0.8

**Files:** create `tests/conftest.py`, `tests/test_config.py`, `tests/test_db.py`, `tests/test_collector_base.py`, `tests/test_pricing.py`, `tests/test_cli.py`

Shared fixtures: in-memory DB, test settings, mock httpx client. Tests for all Phase 0 modules.

**Acceptance:**
- `uv run pytest` passes
- Coverage >= 80%
- `mypy --strict` + `ruff` pass

---

## Phase 0.5 — Dynamic Source Management (CLI + DB)

Migrate source credentials from `.env` to SQLite. Interactive CLI for adding/managing sources. Global settings (Telegram, Claude API, CoinGecko, logging) stay in `.env`.

### Task 0.5.1 — Source model + DB schema [S] ✅

**Dependencies:** 0.3

**Files:** modify `src/pfm/db/models.py`, modify schema SQL

Add `sources` table:
- `id` INTEGER PK auto
- `name` TEXT UNIQUE — user-chosen instance name (e.g. `okx-main`)
- `type` TEXT — one of 9 known types (okx, binance, binance_th, bybit, lobstr, blend, wise, kbank, ibkr)
- `credentials` TEXT — JSON blob of credential key-value pairs
- `enabled` BOOLEAN — default true
- `created_at` TEXT — ISO timestamp

`Source` dataclass in models.py.

**Acceptance:**
- `init_db` creates `sources` table
- `Source` round-trip: insert, read back, fields match
- UNIQUE constraint on `name`

### Task 0.5.2 — Source store CRUD `src/pfm/db/source_store.py` [M] ✅

**Dependencies:** 0.5.1

**Files:** create `src/pfm/db/source_store.py`

Async CRUD operations:
- `add_source(name, type, credentials) -> Source`
- `get_source(name) -> Source | None`
- `list_sources() -> list[Source]`
- `list_enabled_sources() -> list[Source]`
- `delete_source(name) -> bool`
- `update_source(name, credentials=None, enabled=None) -> Source`

Validate `type` against known source types. Raise typed errors for duplicates, not found.

**Acceptance:**
- All CRUD operations tested against in-memory SQLite
- Duplicate name raises error
- Unknown type raises error
- Enable/disable toggle works

### Task 0.5.3 — Credential schemas per source type [S] ✅

**Dependencies:** 0.5.2

**Files:** modify `src/pfm/db/source_store.py` or new `src/pfm/source_types.py`

Define required credential fields per source type:
- `okx`: api_key, api_secret, passphrase
- `binance`: api_key, api_secret
- `binance_th`: api_key, api_secret
- `bybit`: api_key, api_secret
- `lobstr`: stellar_public_address
- `blend`: stellar_public_address, blend_pool_contract_id, soroban_rpc_url (optional, has default)
- `wise`: api_token
- `kbank`: gmail_address, gmail_app_password, kbank_sender_email (optional), kbank_pdf_password
- `ibkr`: flex_token, flex_query_id

Used by wizard (prompt for each field) and validation.

**Acceptance:**
- Each type has defined required/optional fields
- Validation rejects missing required fields
- Optional fields have defaults where applicable

### Task 0.5.4 — CLI `pfm source` commands [M] ✅

**Dependencies:** 0.5.2, 0.5.3, 0.7

**Files:** modify `src/pfm/cli.py`

**Additional dep:** `click` (already planned)

CLI group `pfm source` with subcommands:
- `pfm source add` — interactive wizard: pick type from list → enter name → prompt for each credential field
- `pfm source list` — table output (name, type, enabled, created_at)
- `pfm source show <name>` — details with masked secrets (show first/last 3 chars)
- `pfm source delete <name>` — confirmation prompt before deletion
- `pfm source enable <name>` / `pfm source disable <name>` — toggle

**Acceptance:**
- `pfm source add` walks through wizard and saves to DB
- `pfm source list` shows table
- `pfm source show` masks API keys/secrets
- `pfm source delete` requires confirmation
- All subcommands handle errors gracefully (not found, duplicate, etc.)

### Task 0.5.5 — Wire `pfm collect` to registry dispatch [M] ✅

**Dependencies:** 0.5.2

**Files:** modify `src/pfm/cli.py`, modify `src/pfm/collectors/__init__.py`

Collectors already accept keyword arguments matching credential field names — no refactoring needed. Main work: wire `pfm collect` to load sources from DB, dispatch via `COLLECTOR_REGISTRY`, run concurrently.

- `collectors/__init__.py` auto-imports all 9 collector modules to populate registry
- `cli.py` `collect` command: loads enabled sources from DB, looks up collector class by type, instantiates with `**credentials`, runs `asyncio.gather`, prints summary table

**Acceptance:**
- All existing collector tests pass
- `pfm collect` discovers sources from DB and runs them
- `pfm collect --source NAME` runs a single named source
- Summary table shows snapshot/transaction counts, errors, timing

### Task 0.5.6 — Clean up config.py and .env.example [S] ✅

**Dependencies:** 0.5.5

**Files:** modify `src/pfm/config.py`, modify `.env.example`

Remove all source-specific fields from `Settings` (OKX, Binance, Bybit, Stellar, Wise, KBank Gmail, IBKR). Keep only global settings:
- `database_path`
- `telegram_bot_token`, `telegram_chat_id`
- `anthropic_api_key`
- `coingecko_api_key`
- `log_level`

Update `.env.example` to match.

**Acceptance:**
- `Settings` has no source-specific fields
- `.env.example` only has global vars
- All tests pass

### Task 0.5.7 — Source management tests [M] ✅

**Dependencies:** 0.5.1–0.5.6

**Files:** create `tests/test_source_store.py`, modify `tests/test_cli.py`, modify `tests/test_collectors.py`

- Source store CRUD tests
- CLI wizard test (mocked input)
- Collector instantiation from credentials dict
- Enable/disable flow

**Acceptance:**
- All new tests pass
- Coverage >= 80% on new modules
- `mypy --strict` + `ruff` pass

---

## Phase 1 — First Collectors (Lobstr + Wise)

Prove the full pipeline with the two simplest sources.

### Task 1.1 — Lobstr collector `src/pfm/collectors/lobstr.py` [M] ✅

**Dependencies:** Phase 0

**Files:** create `src/pfm/collectors/lobstr.py`, modify `pyproject.toml` (add `stellar-sdk`)

Stellar Horizon API:
- `fetch_balances()`: GET `/accounts/{public_key}`, parse `balances` array (XLM + USDC), convert to USD
- `fetch_transactions()`: GET `/accounts/{public_key}/payments`, paginate, normalize
- Map `asset_type=native` → XLM, `credit_alphanum4` → by `asset_code`

### Task 1.2 — Wise collector `src/pfm/collectors/wise.py` [M] ✅

**Dependencies:** Phase 0

**Files:** create `src/pfm/collectors/wise.py`

Wise REST API with personal token (raw httpx, no SDK):
- `fetch_balances()`: GET profile → GET balances for all currencies, convert to USD
- `fetch_transactions()`: GET statement, normalize
- Auth: Bearer token

### Task 1.3 — Wire `collect` command [S] ✅

**Dependencies:** 1.1, 1.2

**Files:** modify `src/pfm/cli.py`, `src/pfm/collectors/__init__.py`

Register collectors. `pfm collect` discovers enabled sources from DB, runs concurrently via `asyncio.gather`. `--source NAME` filters by instance name.

*Completed as part of Task 0.5.5 — all 9 collectors auto-registered via `@register_collector` decorator and `__init__.py` auto-import.*

### Task 1.4 — Integration tests [S] ✅

**Dependencies:** 1.3

**Files:** create `tests/test_lobstr.py`, `tests/test_wise.py`, `tests/test_collect_integration.py`

---

## Phase 2 — Crypto Exchange Collectors (OKX, Binance, Binance TH, Bybit)

Four HMAC-signed REST API collectors.

### Task 2.5 — HMAC signing utility `src/pfm/collectors/_auth.py` [S] ✅

**Dependencies:** None (used by 2.1–2.4)

**Files:** create `src/pfm/collectors/_auth.py`

- `sign_okx(timestamp, method, path, body, secret) -> str`
- `sign_binance(query_string, secret) -> str`
- `sign_bybit(timestamp, api_key, recv_window, query_string, secret) -> str`

### Task 2.1 — OKX collector `src/pfm/collectors/okx.py` [M] ✅

**Dependencies:** Phase 0, 2.5

**Files:** create `src/pfm/collectors/okx.py`

- `fetch_balances()`: `/api/v5/account/balance` (trading) + `/api/v5/asset/balances` (funding) + earn positions
- `fetch_transactions()`: `/api/v5/account/bills` or bills-archive
- Raw httpx with signing helper (not the `okx` SDK)

### Task 2.2 — Binance collector `src/pfm/collectors/binance.py` [M] ✅

**Dependencies:** Phase 0, 2.5

**Files:** create `src/pfm/collectors/binance.py`

- `fetch_balances()`: GET `/api/v3/account`, filter non-zero (`free` + `locked`)
- `fetch_transactions()`: deposit + withdrawal history
- Base URL configurable (for Binance TH reuse)

### Task 2.3 — Binance TH collector `src/pfm/collectors/binance_th.py` [S] ✅

**Dependencies:** 2.2

**Files:** create `src/pfm/collectors/binance_th.py`

Subclass/compose with `BinanceCollector`, override base URL + THB-specific handling.

### Task 2.4 — Bybit collector `src/pfm/collectors/bybit.py` [M] ✅

**Dependencies:** Phase 0, 2.5

**Files:** create `src/pfm/collectors/bybit.py`

- `fetch_balances()`: GET `/v5/account/wallet-balance`
- `fetch_transactions()`: GET `/v5/account/transaction-log`
- Auth via headers (`X-BAPI-*`)

### Task 2.6 — Exchange collector tests [M] ✅

**Dependencies:** 2.1–2.5

**Files:** create `tests/test_okx.py`, `tests/test_binance.py`, `tests/test_binance_th.py`, `tests/test_bybit.py`, `tests/test_auth.py`, `tests/fixtures/*.json`

---

## Phase 3 — Remaining Collectors (IBKR, Blend, KBank)

### Task 3.2 — IBKR collector `src/pfm/collectors/ibkr.py` [M] ✅

**Dependencies:** Phase 0

**Additional dep:** `ibflex`

Flex Query two-step workflow:
1. Send request → get reference code
2. Poll for statement → parse with `ibflex.parser`
- Retry loop for "statement not ready"
- Token expiry warning

### Task 3.3 — Blend collector `src/pfm/collectors/blend.py` [L] ✅

**Dependencies:** Phase 0, stellar-sdk

Soroban RPC:
- Build simulated tx calling `get_positions(user_address)` on pool contract
- `simulate_transaction()` via `SorobanServer`
- Parse XDR → bToken amounts × `b_rate` → USDC value → USD
- Transactions: empty for now (yield tracked via balance diffs)

### Task 3.4 — KBank PDF parser `src/pfm/collectors/kbank.py` [L] ✅

**Dependencies:** Phase 0

**Additional dep:** `pdfplumber`

- Manual trigger via `pfm import-kbank /path/to/statement.pdf`
- Auto-fetch from Gmail IMAP (searches for KBank sender, downloads latest PDF)
- Parse newline-delimited cells: dates, descriptions, amounts, balances
- Password-protected PDF support (DDMMYYYY date of birth)
- THB → USD conversion
- `fetch_balances()`: ending balance from most recent import

### Task 3.5 — Wire remaining collectors [S] ✅

**Dependencies:** 3.2–3.4

Register all 9 collector types. `pfm collect` auto-discovers from DB. Add `--category` filter option.

*Completed as part of Task 0.5.5 — all 9 collectors auto-registered via `@register_collector` decorator.*

### Task 3.6 — Tests [M] ✅

**Dependencies:** 3.2–3.4

**Files:** `tests/test_collectors.py` (consolidated)

---

## Phase 4 — Analytics Engine

### Task 4.1 — Portfolio analytics `src/pfm/analytics/portfolio.py` [M] ✅

**Dependencies:** Phase 0

- `compute_net_worth(repo, date) -> Decimal`
- `compute_allocation_by_asset(repo, date)` — per-asset breakdown (amount, usd_value, %)
- `compute_allocation_by_source(repo, date)` — per-source
- `compute_allocation_by_category(repo, date)` — crypto/fiat/stocks/DeFi
- `compute_currency_exposure(repo, date)`
- `compute_risk_metrics(repo, date)` — concentration %, top 5, HHI index

### Task 4.2 — PnL calculations `src/pfm/analytics/pnl.py` [L] ✅

**Dependencies:** 4.1

- `compute_pnl(repo, date, period: PnlPeriod) -> PnlResult`
- Periods: DAILY, WEEKLY, MONTHLY, ALL_TIME
- `PnlResult`: start_value, end_value, absolute_change, percentage_change, top_gainers, top_losers
- Cost basis tracking (average cost method)
- Handle missing historical data gracefully

### Task 4.3 — Yield tracker `src/pfm/analytics/yield_tracker.py` [M] ✅

**Dependencies:** 4.1

- `compute_yield(repo, source, asset, start, end) -> YieldResult`
- YieldResult: principal_estimate, current_value, yield_amount, yield_percentage, annualized_rate
- Compare balance snapshots over time for Blend + OKX earn

### Task 4.4 — Wire `analyze` command [S] ✅

**Dependencies:** 4.1–4.3

`pfm analyze`: run all analytics on latest snapshot, print summary, cache in DB.

### Task 4.5 — Analytics tests [M] ✅

**Dependencies:** 4.1–4.3

**Files:** `tests/test_portfolio.py`, `tests/test_pnl.py`, `tests/test_yield_tracker.py`

Seed DB with fixture data across multiple dates, verify all computations.

---

## Phase 5 — AI Commentary (Claude API)

### Task 5.1 — Prompt templates `src/pfm/ai/prompts.py` [S] ✅

**Dependencies:** None

Version-controlled prompt templates:
- `WEEKLY_REPORT_SYSTEM_PROMPT` — role as personal financial advisor
- `WEEKLY_REPORT_USER_PROMPT_TEMPLATE` — placeholders for all analytics data
- Instructs Claude to produce: market context, health assessment, rebalancing, risk alerts, recommendations

### Task 5.2 — Claude API analyst `src/pfm/ai/analyst.py` [M] ✅

**Dependencies:** 5.1, Phase 4

**Additional dep:** `anthropic`

- `async def generate_commentary(analytics: AnalyticsSummary) -> str`
- Model: `claude-sonnet-4-20250514`
- Max tokens: 1024
- Graceful fallback on API error
- Log token usage

### Task 5.3 — AI tests [S]

**Dependencies:** 5.1, 5.2

Mocked Anthropic client. Test prompt rendering + fallback.

---

## Phase 6 — Telegram Reporting

### Task 6.1 — Telegram bot client `src/pfm/reporting/telegram.py` [M]

**Dependencies:** Phase 0

Push-only using raw `httpx`:
- `async def send_message(chat_id, text, parse_mode="HTML") -> bool`
- `async def send_report(report: WeeklyReport) -> bool`
- `async def send_error_alert(errors: list[str]) -> bool`
- Handle 4096 char limit (split messages)

### Task 6.2 — Report formatter `src/pfm/reporting/formatter.py` [M]

**Dependencies:** 6.1, Phase 4, Phase 5

Format analytics + AI commentary into Telegram HTML:
- Header: date, net worth
- PnL: week-over-week with arrows
- Allocation: top 10 holdings
- Yield: Blend returns
- AI commentary
- Footer: errors/warnings

### Task 6.3 — Wire `report` and `run` commands [S]

**Dependencies:** 6.1, 6.2

`pfm report`: load analytics → generate AI commentary → format → send Telegram.

`pfm run`: collect → analyze → report (full pipeline). Aggregate errors, send alert if any source failed.

### Task 6.4 — Reporting tests [S]

**Dependencies:** 6.1–6.3

**Files:** `tests/test_telegram.py`, `tests/test_formatter.py`, `tests/test_report_integration.py`

---

## Phase 7 — Production Hardening

### Task 7.1 — Alembic migrations [M]

**Dependencies:** 0.3

**Additional dep:** `alembic`

**Files:** `alembic.ini`, `src/pfm/db/migrations/`

### Task 7.2 — Retry and rate limiting `src/pfm/collectors/_retry.py` [S]

**Dependencies:** Phase 0

- `@retry(max_attempts=3, backoff_base=2.0)` decorator
- `RateLimiter(requests_per_minute)` class

### Task 7.3 — Scheduling docs [S]

**Dependencies:** Phase 6

**Files:** create `docs/deployment.md`, `scripts/pfm-weekly.sh`

Crontab example, shell script, systemd timer alternative.

### Task 7.4 — Error handling audit [M]

**Dependencies:** All phases

Audit all modules: graceful degradation, clear error messages, no unhandled exceptions, Telegram error alerts.

### Task 7.5 — Final test pass [M]

**Dependencies:** All phases

Fill coverage gaps to 80%+. Edge cases: empty portfolio, all sources failing, large numbers, date boundaries.

---

## Parallelism

Phases 1, 2, 3 are independent (all depend only on Phase 0). Phase 0.5 depends on Phase 0 (DB + CLI). Phases 1–3 collectors need Phase 0.5 (credentials from DB). Phase 4 needs Phase 0 + fixture data. Phase 5 needs Phase 4. Phase 6 needs Phase 4 + 5. Phase 7 needs everything.

```
Phase 0 ── Phase 0.5 (Source Management)
               ├── Phase 1 (Lobstr + Wise) ──────────────┐
               ├── Phase 2 (Crypto Exchanges) ────────────┤
               ├── Phase 3 (IBKR, Blend, KBank) ──────────┤
               └── Phase 4 (Analytics) ──────────────────>├── Phase 5 (AI) ── Phase 6 (Telegram) ── Phase 7 (Hardening)
```

## Effort Summary

| Phase | Tasks | Effort | Status |
|-------|-------|--------|--------|
| 0 — Foundation | 9 | 4S + 5M | ✅ |
| 0.5 — Source Management | 7 | 3S + 4M | ✅ |
| 1 — Lobstr + Wise | 4 | 2S + 2M | ✅ |
| 2 — Crypto Exchanges | 6 | 2S + 4M | ✅ |
| 3 — Remaining Sources | 5 | 1S + 2M + 2L | ✅ |
| 4 — Analytics | 5 | 1S + 3M + 1L | Pending |
| 5 — AI Commentary | 3 | 2S + 1M | Pending |
| 6 — Telegram Reporting | 4 | 2S + 2M | Pending |
| 7 — Hardening | 5 | 2S + 3M | Pending |
| **Total** | **48** | | **~41/48 done** |

## File Manifest

```
src/pfm/
  config.py                        # 0.1 (global settings only)
  logging.py                       # 0.8
  cli.py                           # 0.7 + 0.5.4
  source_types.py                  # 0.5.3 (credential schemas per type)
  db/
    __init__.py                    # 0.3
    models.py                      # 0.3 + 0.5.1
    source_store.py                # 0.5.2 (source CRUD)
    repository.py                  # 0.4
    migrations/                    # 7.1
  collectors/
    __init__.py                    # 0.5
    base.py                        # 0.5
    _auth.py                       # 2.5
    _retry.py                      # 7.2
    okx.py                         # 2.1
    binance.py                     # 2.2
    binance_th.py                  # 2.3
    bybit.py                       # 2.4
    lobstr.py                      # 1.1
    blend.py                       # 3.3
    wise.py                        # 1.2
    kbank.py                       # 3.4
    ibkr.py                        # 3.2
  pricing/
    __init__.py                    # 0.6
    coingecko.py                   # 0.6
  analytics/
    __init__.py                    # 4.1
    portfolio.py                   # 4.1
    pnl.py                         # 4.2
    yield_tracker.py               # 4.3
  ai/
    __init__.py                    # 5.1
    prompts.py                     # 5.1
    analyst.py                     # 5.2
  reporting/
    __init__.py                    # 6.1
    telegram.py                    # 6.1
    formatter.py                   # 6.2

tests/
  conftest.py                      # 0.9
  test_config.py                   # 0.9
  test_db.py                       # 0.9
  test_source_store.py             # 0.5.7
  test_collector_base.py           # 0.9
  test_pricing.py                  # 0.9
  test_cli.py                      # 0.9
  test_auth.py                     # 2.6
  test_lobstr.py                   # 1.4
  test_wise.py                     # 1.4
  test_okx.py                      # 2.6
  test_binance.py                  # 2.6
  test_binance_th.py               # 2.6
  test_bybit.py                    # 2.6
  test_ibkr.py                     # 3.6
  test_blend.py                    # 3.6
  test_kbank.py                    # 3.6
  test_portfolio.py                # 4.5
  test_pnl.py                      # 4.5
  test_yield_tracker.py            # 4.5
  test_prompts.py                  # 5.3
  test_analyst.py                  # 5.3
  test_telegram.py                 # 6.4
  test_formatter.py                # 6.4
  test_report_integration.py       # 6.4
  test_collect_integration.py      # 1.4
  fixtures/                        # 2.6
    okx_balance.json
    binance_account.json
    bybit_wallet.json
    horizon_account.json
    wise_balances.json
    ibkr_flex_statement.xml
    blend_positions.json

docs/
  data-sources.md                  # (done)
  requirements.md                  # (done)
  implementation-plan.md           # (this file)
  deployment.md                    # 7.3

scripts/
  pfm-weekly.sh                    # 7.3

alembic.ini                        # 7.1
```
