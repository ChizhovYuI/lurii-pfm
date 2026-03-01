# Architecture Decision Records

Documenting dropped sources, rejected approaches, and other decisions that shaped the project.

---

## ADR-001: Drop Uphold integration

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Uphold was originally included as source #5 (fiat-to-crypto bridge, GBP → USDC). The collector was fully implemented (`src/pfm/collectors/uphold.py`) using their REST API with a Personal Access Token (PAT).

**Problem:** Uphold requires registering an OAuth application to obtain API access. Personal Access Tokens are only available to approved developer applications — there is no self-service "read-only API key" flow like other exchanges provide. The application review process adds friction and may not be approved for personal use.

**Decision:** Remove Uphold as a data source entirely rather than maintain dead code waiting for access approval.

**Consequences:**
- Source count reduced from 10 to 9
- Money flow simplified: `Wise → GBP → USDC (Stellar) → Lobstr` (Uphold bridge step removed)
- Deleted: `src/pfm/collectors/uphold.py`, Uphold tests, config fields, env vars
- If Uphold access is obtained in the future, the collector can be restored from git history

---

## ADR-002: Migrate source credentials from .env to SQLite with CLI management

**Date:** 2026-02-27

**Status:** Accepted

**Context:** All 9 source credentials were stored as flat environment variables in `.env`, loaded via `pydantic-settings` into a monolithic `Settings` class. Adding or removing a source required manually editing `.env` and restarting. Multiple accounts of the same type (e.g. two OKX accounts) were not supported.

**Problem:**
- No way to manage sources dynamically at runtime
- No support for multiple instances of the same source type
- Credentials mixed with global settings (Telegram, Gemini API, CoinGecko) in one flat file
- No enable/disable mechanism — all configured sources always run

**Decision:** Move source credentials to a `sources` table in SQLite (same `pfm.db`). Manage via interactive CLI (`pfm source add/list/show/delete/enable/disable`). Keep global settings (Telegram, Gemini API, CoinGecko, logging) in `.env`.

**Design choices:**
- **SQLite over YAML/TOML config file** — single storage backend, already have migrations, no file format parsing
- **Plain text secrets** — DB file is local and gitignored, same security posture as `.env`
- **Hardcoded 9 source types** — not plugin-based, avoids over-engineering for single-user
- **Named instances** — each source has a unique user-chosen name (e.g. `okx-main`), enabling multiple accounts per type
- **Interactive wizard** — `pfm source add` prompts for type, name, and each credential field
- **Manual re-add** — no automatic migration from `.env`; clean break, user re-adds sources via CLI
- **Auto-discover for collection** — `pfm collect` reads enabled sources from DB, no hardcoded list

**Consequences:**
- New Phase 0.5 in implementation plan (7 tasks)
- `config.py` shrinks to global settings only
- All 9 collectors refactored to accept `credentials: dict` instead of `Settings`
- `pfm collect` becomes dynamic — runs whatever sources are in the DB
- `.env.example` reduced to ~10 lines (global settings only)

---

## ADR-003: Add persistent CoinGecko cache in SQLite

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Price conversion is used by multiple collectors in one `pfm collect` run. In-memory cache helps within a single process, but repeated runs (or process restarts) still trigger fresh CoinGecko requests and increase `429 Too Many Requests` risk.

**Problem:**
- CoinGecko free-tier rate limits are easy to hit during concurrent collection
- In-memory cache is lost between runs
- Repeated requests for the same asset/rate within a short time window are unnecessary

**Decision:** Use `prices` table as a persistent cache layer for CoinGecko. `PricingService` now checks SQLite for a recent cached USD price first (TTL 1 hour), then falls back to HTTP only when needed, and writes fetched results back to SQLite (write-through cache).

**Design choices:**
- **SQLite-backed cache** over external Redis/memcached — no extra infra, single-user local setup
- **TTL by `created_at`** — cache freshness window enforced at query time
- **Layered cache** — keep existing in-memory cache for fastest repeated lookups in-process
- **Serialized HTTP + 429 backoff** — requests are lock-serialized and retried with backoff to reduce rate-limit failures

**Consequences:**
- Fewer CoinGecko API calls across repeated runs
- Better resilience during `pfm collect` bursts
- Price cache has durable history in the same DB used for analytics

---

## ADR-004: Make snapshot writes idempotent per source and date

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Running `pfm collect` multiple times on the same day previously appended new snapshot rows, causing duplicate `(date, source, asset)` entries and inflated net worth/allocation analytics unless manually cleaned.

**Problem:**
- Duplicate same-day snapshots for the same source
- Latest analytics could be overstated due to additive duplicates
- Manual cleanup required after repeated collect runs

**Decision:** Snapshot persistence is now idempotent per `source + date`. Before inserting a new snapshot batch for a source/date, existing rows for that source/date are deleted. The new batch becomes the effective snapshot set for that source/day.

**Design choices:**
- **Repository-level replacement** (`save_snapshots`) instead of ad-hoc cleanup scripts
- **Delete by `(date, source)` then insert batch** to preserve complete replacement semantics
- **Transactions unaffected** — only snapshot dedup is enforced

**Consequences:**
- Re-running `pfm collect` on the same day does not inflate portfolio totals
- "Latest snapshot" semantics become deterministic and easier to reason about
- Existing historical duplicates can still be cleaned once; new duplicates are prevented going forward

---

## ADR-005: Decouple AI generation from report send and cache model metadata

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Calling Gemini during `pfm report` made report delivery sensitive to Gemini quota/rate-limit errors and introduced latency at send time. It also made it harder to audit which model generated a given commentary block.

**Decision:** `pfm report` only reads cached `ai_commentary` for the analysis date. `pfm comment` is the command responsible for generating commentary and storing:
- `text`
- `model` (when a Gemini model succeeded)

**Consequences:**
- Report delivery is decoupled from Gemini uptime/quota
- Commentary provenance is visible (`AI model: ...`) and persisted for auditability
- Scheduling can explicitly control when AI calls happen (`collect -> analyze -> comment -> report`)

---

## ADR-006: Gemini 429 handling uses immediate model failover

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Free-tier Gemini quotas frequently return `HTTP 429`, especially for `gemini-2.5-pro`. Retrying the same model introduced long delays and often still failed.

**Decision:** On `429`, skip retries for the current model and immediately try the next model in order:
1. `gemini-2.5-pro`
2. `gemini-2.5-flash`
3. `gemini-2.5-flash-lite`

If all fail, use fallback commentary text.

**Consequences:**
- Faster response under quota pressure
- Higher chance of receiving an AI response in a single run
- Commentary style may vary by fallback model, but output remains available

---

## ADR-007: HTTP backend with aiohttp and launchd daemon

**Date:** 2026-02-28

**Status:** Accepted

**Context:** The SwiftUI macOS app (Phase 3) needs a local API to consume all Lurii Finance functionality. The Python process must be persistent (not spawned per-request), support real-time progress events during collection, and be manageable without manual process supervision.

**Decision:** Add an aiohttp HTTP server exposing REST + WebSocket endpoints on `127.0.0.1:19274`, managed as a macOS launchd daemon.

**Design choices:**
- **aiohttp over FastAPI/Flask** — already async, native WebSocket support, lightweight, no ASGI server needed
- **Local-only binding** (`127.0.0.1`) — middleware rejects non-loopback requests; no auth needed for single-user local daemon
- **launchd over systemd/supervisord** — macOS-native, `KeepAlive` + `RunAtLoad` for reliability, standard `~/Library/LaunchAgents/` path
- **Application factory pattern** — `create_app(db_path)` with startup/cleanup hooks for shared resources (Repository, PricingService, EventBroadcaster)
- **Background collection task** — `POST /api/v1/collect` returns 202 immediately, spawns `asyncio.ensure_future` task, rejects concurrent requests with 409
- **WebSocket EventBroadcaster** — broadcasts collection progress events to all connected clients
- **Serializers extracted from CLI** — shared `serializers.py` module avoids duplicating JSON conversion logic between CLI and API
- **CLI thin-client pattern** — existing commands check `is_daemon_reachable()` first, proxy via HTTP if daemon is up, fall back to inline execution if not
- **DB path migration** — auto-copy from `data/pfm.db` to `~/Library/Application Support/Lurii Finance/lurii.db` at daemon startup

**Consequences:**
- 18 new source files in `src/pfm/server/`, 10 new test files (83 tests)
- CLI commands work identically whether daemon is running or not
- SwiftUI app can consume all endpoints without touching Python internals
- Port 19274 is configurable but fixed by default (unlikely to conflict)
- No breaking changes to existing CLI behavior

---

## ADR-008: Multi-provider LLM abstraction

**Date:** 2026-02-28

**Status:** Accepted

**Context:** AI commentary was hard-coupled to Gemini API via `google-genai` SDK. Users wanting local/private LLM inference (Ollama) or access to other models (Claude, GPT via OpenRouter) had no option.

**Decision:** Replace the Gemini-only `ai/analyst.py` with a pluggable `LLMProvider` protocol. Four providers implemented: Gemini, Ollama, OpenRouter, Grok. Multiple providers can be configured simultaneously with zero or one active at a time.

**Design choices:**
- **Protocol-based abstraction** — `LLMProvider` protocol with `generate_commentary()` and `close()` methods
- **Provider registry** — `PROVIDER_REGISTRY` dict mapping names to classes
- **Dedicated `ai_providers` table** — one row per provider type (`type` as PK), with `api_key`, `model`, `base_url`, `active` columns. Replaces the earlier single-row `ai_settings` approach. Multiple providers coexist; only one can be `active = 1` at a time
- **`AIProviderStore` CRUD** — `add()` (upsert), `get()`, `get_active()`, `list_all()`, `activate()`, `deactivate()`, `remove()`, modeled after `SourceStore`
- **`_ensure_table()`** — auto-creates `ai_providers` table for databases created before this migration, avoiding a formal schema migration step
- **Auto-migration from legacy keys** — `migrate_from_legacy()` reads old `ai_provider*` and `gemini_api_key` from `app_settings`, inserts into `ai_providers`, activates the migrated provider. Idempotent (no-op if providers already exist)
- **Backward-compat aliases** — `AIConfig = AIProvider`, `AIStore = AIProviderStore` preserve imports in existing code
- **Ollama native API** — direct `/api/chat` HTTP calls instead of OpenAI-compatible endpoint, for full model management (list, pull)
- **OpenAI-compatible clients** — OpenRouter and Grok share a common base using `openai` SDK pattern
- **Error propagation** — `CommentaryResult` includes an optional `error` field. Providers set it on failure (e.g. `"openrouter API error 401"`). The error is surfaced in `POST /api/v1/ai/commentary` response alongside the fallback text, so the UI can display the failure reason

**DB schema:**
```sql
CREATE TABLE IF NOT EXISTS ai_providers (
    type TEXT PRIMARY KEY,
    api_key TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    base_url TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

**REST API:**
- `GET /api/v1/ai/config` — active provider (legacy compat)
- `PUT /api/v1/ai/config` — set active provider (legacy compat)
- `GET /api/v1/ai/providers` — list all configured providers (api_key masked)
- `PUT /api/v1/ai/providers/{type}` — add/update provider
- `DELETE /api/v1/ai/providers/{type}` — remove provider
- `POST /api/v1/ai/providers/{type}/activate` — set active
- `POST /api/v1/ai/providers/deactivate` — clear active

**CLI commands:**
- `pfm ai set` — configure and activate a provider
- `pfm ai show` — display active provider
- `pfm ai list` — show all configured providers with active marker
- `pfm ai activate <type>` — switch active provider
- `pfm ai deactivate` — clear active
- `pfm ai remove <type>` — delete a provider config
- `pfm ai clear` — alias for deactivate

**Consequences:**
- Gemini remains the default provider with existing model failover chain
- Ollama enables fully local/private AI commentary (no API key needed)
- OpenRouter provides access to Claude, GPT, Mistral, etc. via single API key
- Legacy `pfm gemini set/show/clear` commands still work as aliases
- Switching providers no longer requires re-entering credentials — just `pfm ai activate <type>`

---

## ADR-009: SQLCipher database encryption with locked/unlocked daemon state

**Date:** 2026-02-28

**Status:** Accepted

**Context:** The SQLite database stores source credentials and financial data in plain text. Phase 4 of the v2 evolution spec calls for encrypting the database at rest using SQLCipher, with the key stored in macOS Keychain and supplied to the daemon at startup (or via an unlock endpoint from the SwiftUI app).

**Problem:**
- Database file is readable by any process with file access
- Source credentials (API keys, tokens) are stored in plain text in SQLite
- No mechanism for the SwiftUI app to unlock an encrypted daemon post-launch

**Decision:** Use `sqlcipher3` (coleifer, v0.6.2) for transparent database encryption, with an opt-in locked/unlocked daemon state machine and a `/api/v1/unlock` endpoint.

**Design choices:**
- **`sqlcipher3` over `pysqlcipher3` / Rotki fork** — `sqlcipher3` ships pre-built wheels for Python 3.13 with a self-contained SQLCipher build (no system-level library dependency). It is DB-API 2.0 compatible and maintained by the `peewee` author. `pysqlcipher3` requires linking against a separately-installed `libsqlcipher`, and the Rotki fork adds custom patches we don't need.
- **Connector injection for aiosqlite** — `aiosqlite.Connection` accepts any `Callable[[], sqlite3.Connection]` as its connector. We inject a factory that calls `sqlcipher3.connect()` + `PRAGMA key` instead of monkey-patching or subclassing. This keeps the encryption layer contained in `db/encryption.py`.
- **`connect_db()` helper** — returns either encrypted or plain `aiosqlite.Connection` based on whether a key is supplied, so `Repository`, `init_db`, and stores don't need to know the encryption state.
- **Opt-in encryption** — DB starts plain. Encryption is enabled via a future `pfm db encrypt` CLI command or SwiftUI settings. Once encrypted, the daemon requires a key on startup.
- **Locked state machine** — if encryption is enabled but no key is available at startup, the daemon enters a locked state where all endpoints except `/api/v1/health`, `/api/v1/unlock`, and `/api/v1/encryption/status` return `423 Locked`. The SwiftUI app detects this via health, reads the key from Keychain, and POSTs to `/api/v1/unlock`.
- **Key via env var or unlock endpoint** — `PFM_DB_KEY` env var for headless/CLI use, `POST /api/v1/unlock` for interactive SwiftUI flow.
- **`PRAGMA cipher_compatibility = 4`** — ensures forward compatibility with SQLCipher 4.x format.
- **Plain-to-encrypted migration** — `migrate_to_encrypted()` uses `sqlite3.backup()` to stream from plain to encrypted DB. Original file is preserved.

**Consequences:**
- New `src/pfm/db/encryption.py` module (~90 lines)
- `Repository` and `init_db` accept optional `key_hex` parameter
- New `db_locked_middleware` gates data endpoints behind unlock
- Three new HTTP endpoints: `POST /unlock`, `GET /encryption/status`, updated `GET /health` with `locked` field
- Phase 4 proper (Swift Keychain, `pfm db encrypt/decrypt` CLI) can build on this foundation
- No breaking changes — plain DB continues to work identically when no key is configured

---

## ADR-010: Settings endpoint includes AI provider metadata for UI rendering

**Date:** 2026-03-01

**Status:** Accepted

**Context:** The SwiftUI app settings screen needs to render a combo box of available AI providers and dynamically show input fields (api_key, model, base_url) based on which provider is selected. Each provider has different required/optional fields and class-level defaults (e.g. Ollama has a `default_base_url`, Gemini has a `default_model`).

**Decision:** Enrich `GET /api/v1/settings` response with two AI-related arrays: `ai_providers` (configured instances) and `ai_providers_available` (all registered provider types with field schemas). Provider field metadata is derived at startup by introspecting `PROVIDER_REGISTRY` class `__init__` signatures.

**Design choices:**
- **Registry introspection via `inspect.signature()`** — provider fields are derived from `__init__` parameter names and defaults, not a separate schema definition. This keeps the field list in sync with actual provider classes automatically
- **`_PROVIDER_FIELDS` ordering** — `("api_key", "model", "base_url")` defines UI rendering order; only fields present in a provider's `__init__` are included
- **`secret` flag on fields** — `_SECRET_FIELDS` set marks fields like `api_key` as `"secret": true` in the metadata. The UI uses this generically to: mask the value, show a placeholder, and make the input optional when a value already exists. No field names are hardcoded in the UI
- **Masked secret values** — secret fields return a masked string (e.g. `"sk-...269"`) or empty string. The UI can check for a non-empty masked value to know a secret exists. When saving, the UI omits the field if unchanged, so the backend preserves the existing value
- **Dynamic field inclusion** — configured `ai_providers` only include fields that the provider type actually supports (derived from `ai_providers_available` metadata), so Gemini won't have `base_url` and Ollama won't have `api_key`
- **Class-level defaults** — `default_model`, `default_base_url` class attributes are exposed as `"default"` hints so the UI can pre-fill inputs
- **Cached metadata** — `_AI_PROVIDERS_META` is computed once since the provider registry is static after import

**Response shape:**
```json
{
  "ai_providers": [
    {
      "type": "gemini",
      "active": true,
      "api_key": "AIz...coA",
      "model": "gemini-2.5-pro"
    },
    {
      "type": "ollama",
      "active": false,
      "model": "llama3.1:8b",
      "base_url": "http://localhost:11434"
    }
  ],
  "ai_providers_available": [
    {
      "type": "gemini",
      "fields": [
        {"name": "api_key", "required": true, "secret": true},
        {"name": "model", "required": false, "secret": false, "default": "gemini-2.5-pro"}
      ]
    },
    {
      "type": "ollama",
      "fields": [
        {"name": "model", "required": true, "secret": false},
        {"name": "base_url", "required": false, "secret": false, "default": "http://localhost:11434"}
      ]
    }
  ]
}
```

**Consequences:**
- SwiftUI settings screen can dynamically render provider-specific forms without hardcoding field names
- Adding a new provider class automatically makes it available in the UI (no endpoint changes)
- Configured and available providers are separate arrays — UI can distinguish "add new" vs "edit existing"
- Secret handling is driven by metadata (`secret` flag), not UI-side field name checks

---

## ADR-011: Temporarily remove PnL from analytics, reports, and AI prompts

**Date:** 2026-03-01

**Status:** Accepted

**Context:** PnL computations (daily/weekly/monthly/all_time) compare two snapshot dates, but often there is no data for the earlier date — producing zero or misleading values. This was particularly common for newly added sources or when collection gaps existed.

**Problem:**
- PnL frequently showed $0 or incorrect values due to missing historical snapshots
- Misleading PnL data was passed to the AI prompt, producing unreliable commentary
- Telegram reports displayed PnL arrows and percentages based on incomplete data

**Decision:** Remove all PnL from `AnalyticsSummary`, AI prompt templates, report formatter, analytics routes, CLI output, and client proxy. Keep `pnl.py` and `test_pnl.py` intact for future re-addition with proper edge-case handling.

**What was removed:**
- `pnl` and `weekly_pnl_by_asset` fields from `AnalyticsSummary` dataclass
- "PnL summary" and "Top weekly movers by asset" sections from AI prompt template
- PnL helper functions: `_compact_pnl_summary`, `_compact_pnl_period`, `_compact_weekly_movers`, `_fmt_usd_signed`
- `GET /api/v1/analytics/pnl` route and `proxy_analytics_pnl()` client function
- `pnl_result_to_dict()` serializer
- PnL computation calls in `analytics_helper.py` and `cli.py`
- PnL display in report formatter (weekly/monthly arrows, per-holding 7d PnL column)
- `AssetPnl`, `PnlPeriod`, `PnlResult`, `compute_pnl` from `analytics/__init__.py` exports

**What was kept:**
- `src/pfm/analytics/pnl.py` — computation logic (will return with proper handling)
- `tests/test_pnl.py` — unit tests for the computation logic

**Consequences:**
- Reports show holdings without PnL columns (simpler, more honest)
- AI commentary focuses on allocation and risk metrics (no misleading PnL data)
- 13 files changed, ~300 lines removed
- PnL will be re-added later with proper edge-case handling (missing data detection, minimum data requirements, confidence indicators)

---

## ADR-012: Return ungrouped holdings in portfolio summary (source per row)

**Date:** 2026-03-01

**Status:** Accepted

**Context:** `GET /api/v1/portfolio/summary` returned an `allocation` array where rows were grouped by `(asset, asset_type)` with a `sources: list[str]` field. This grouping was done server-side via `compute_allocation_by_asset()`.

**Problem:**
- The SwiftUI app needs to display holdings grouped in different ways (by source, by asset, by category) depending on the view
- Server-side grouping forced one specific aggregation, making other groupings require additional API calls or client-side re-computation
- Per-source detail (e.g. BTC on OKX vs BTC on Binance) was lost in the grouped response

**Decision:** Replace the grouped `allocation` array with an ungrouped `holdings` array. Each row represents a single `(source, asset)` snapshot with `source: str` instead of `sources: list[str]`. Grouping is delegated to the UI.

**Response shape change:**
```
# Before
"allocation": [{"asset": "BTC", "sources": ["okx", "binance"], "amount": "1.5", ...}]

# After
"holdings": [
  {"source": "okx",     "asset": "BTC", "amount": "1.0", ...},
  {"source": "binance", "asset": "BTC", "amount": "0.5", ...}
]
```

**Consequences:**
- UI can group/filter holdings by any dimension without additional API calls
- `compute_allocation_by_asset()` is no longer called by this endpoint (still used by other analytics endpoints)
- Per-source granularity preserved in the response
- `asset_type` is computed per-snapshot row using existing `asset_type_for_snapshot()` helper
- `percentage` removed from response — UI computes it from `usd_value` / `net_worth`

---

## ADR-013: Per-source snapshot resolution and data warnings

**Date:** 2026-03-01

**Status:** Accepted

**Context:** Portfolio analytics query snapshots for a single date (`MAX(date)` across all sources). Sources like KBank produce monthly PDF statements whose snapshot date lags behind daily API sources by days or weeks. When OKX/Bybit snapshots are dated March 3 but KBank's latest is March 1, `get_snapshots_by_date(March 3)` excludes KBank entirely — silently dropping fiat holdings from net worth, allocation, and reports.

**Problem:**
- KBank (and any infrequently-updated source) disappears from analytics whenever a fresher source exists
- No visibility into which sources are missing or stale in API responses or AI commentary
- `latest[0].date` assumed all resolved snapshots share the same date, which broke after introducing per-source resolution

**Decision:** Replace exact-date snapshot queries in analytics with per-source date resolution. Add a `warnings` field to API responses and AI prompts for stale/missing source detection. Shift KBank snapshot date to statement period end + 1 day.

**Design choices:**
- **`get_snapshots_resolved(target_date)`** — new Repository method using a self-join: for each source, finds `MAX(date) WHERE date <= target_date`, then returns all rows for those source+date combos. Single SQL query, no N+1
- **`get_latest_snapshots()` delegates to resolved** — finds `MAX(date)` globally, then calls `get_snapshots_resolved(max_date)`. All existing callers automatically get per-source resolution
- **Analytics use resolved queries** — all 5 `compute_*` functions in `portfolio.py` switched from `get_snapshots_by_date` to `get_snapshots_resolved`
- **`max(s.date for s in latest)` for analysis_date** — since resolved snapshots contain mixed dates, the analysis date is derived from the newest snapshot, not `latest[0].date`. Fixed in all 7 call sites (portfolio, analytics, report, ai, cli)
- **`compute_data_warnings(snapshots, enabled_types, analysis_date)`** — pure function, two warning types:
  1. Missing sources: enabled source types with zero snapshot rows → `"No snapshot data for source: X"`
  2. Stale KBank: snapshot > 35 days behind analysis date → `"KBank statement is outdated (date, N days old)"`
- **Warnings surfaced in 4 places:** `GET /portfolio/summary`, `GET /analytics/allocation`, `AnalyticsSummary.warnings` tuple (used by report and AI), and AI prompt template ("Data warnings:" section)
- **KBank +1 day** — statement period "01/02 - 28/02" means the ending balance is effective March 1 (the day after the period closes), so `snapshot_date = statement_date + timedelta(days=1)`
- **35-day threshold** — `_KBANK_STALE_DAYS = 35` accounts for months up to 31 days plus a few days of email/processing delay

**SQL for resolved query:**
```sql
SELECT s.* FROM snapshots s
INNER JOIN (
  SELECT source, MAX(date) AS max_date
  FROM snapshots WHERE date <= ?
  GROUP BY source
) latest ON s.source = latest.source AND s.date = latest.max_date
ORDER BY s.source, s.asset
```

**Consequences:**
- KBank fiat holdings are always included in analytics regardless of collection frequency
- Missing/stale sources are visible to users and AI (actionable warnings)
- `get_snapshots_by_date` still exists for exact-date queries (e.g. historical lookups)
- No breaking API changes — `warnings` is a new additive field (empty array when no issues)
- 14 files changed, 8 new tests (3 DB + 5 portfolio)

---

## ADR-014: Dedicated earn summary endpoint for yield-bearing positions

**Date:** 2026-03-01

**Status:** Accepted

**Context:** The SwiftUI app needs a dedicated page to display yield-earning positions as a table. The existing `GET /api/v1/portfolio/summary` returns all holdings, leaving the UI to filter and compute yield-specific aggregates client-side.

**Decision:** Add `GET /api/v1/earn/summary` that filters latest snapshots to positions with `apy > 0` and returns aggregate totals alongside the filtered positions.

**Design choices:**
- **Server-side filtering** (`apy > 0`) — avoids sending all holdings and re-filtering in Swift; keeps yield logic in one place
- **Weighted average APY** — `sum(apy * usd_value) / total_usd_value` gives a meaningful portfolio-level yield metric that accounts for position size
- **Reuses `get_latest_snapshots()`** — same per-source resolved snapshot query as portfolio summary (ADR-013), no new DB queries
- **Empty vs missing distinction** — 404 when no snapshots exist at all (DB is empty); 200 with `positions: []` and zero totals when snapshots exist but none are earning. This lets the UI distinguish "no data" from "no yield positions"
- **Same serialization helpers** — `_str_decimal`, `asset_type_for_snapshot` from `serializers.py`, consistent with portfolio response shape

**Response shape:**
```json
{
  "date": "2024-01-07",
  "total_usd_value": "12500.50",
  "weighted_avg_apy": "0.0832",
  "positions": [
    {
      "source": "okx",
      "asset": "USDT",
      "asset_type": "crypto",
      "amount": "10000",
      "usd_value": "10000",
      "price": "1",
      "apy": "0.1049"
    }
  ]
}
```

**Consequences:**
- New `src/pfm/server/routes/earn.py` module (single endpoint)
- Registered in `setup_routes()` alongside existing route modules
- 3 new tests covering happy path, empty DB, and no-earning-positions cases
- No changes to existing endpoints or DB schema

---

## ADR-015: Merge semantics for AI provider updates and `activate` → `active` rename

**Date:** 2026-03-01

**Status:** Accepted

**Context:** `PUT /api/v1/ai/config` replaced all provider fields on every call — sending `{"provider": "gemini", "model": "gemini-2.5-flash"}` would erase the existing `api_key` because `store.add()` defaulted missing fields to empty strings.

**Problem:**
- The SwiftUI settings screen saves individual field changes (e.g. user picks a new model from a dropdown). Sending a partial update wiped other configured fields

**Decision:** Add merge semantics to `PUT /api/v1/ai/config`. Only fields present in the request are updated; missing fields preserve their existing DB values. Also rename `AIProviderStore.add(activate=)` parameter to `active=` for consistency with the `AIProvider` dataclass and API response field name.

**Design choices:**
- **Merge via fetch-then-upsert** — fetch existing row with `store.get()`, build kwargs from existing fields via `dataclasses.asdict()`, overlay request fields, then call `store.add()`
- **No hardcoded field names in merge logic** — existing fields are read dynamically from the `AIProvider` dataclass, so adding a new field to the dataclass automatically includes it in merges
- **`activate` → `active` rename** — `AIProviderStore.add()` parameter renamed from `activate` to `active` to match the `AIProvider.active` field and the `active` key in API request/response JSON. Eliminates field name remapping in all callers
- **`PUT /api/v1/settings` unchanged** — stays as plain `app_settings` key-value store; AI provider updates go through `PUT /api/v1/ai/config`

**Callers updated for `activate` → `active`:**
- `AIProviderStore.add()` signature and internals
- `AIProviderStore.migrate_from_legacy()` (2 call sites)
- `cli.py` `ai_set` command
- `routes/ai.py` `update_ai_config` and `upsert_provider` endpoints
- `tests/test_ai_store.py` (4 call sites)

**Example request:**
```json
PUT /api/v1/ai/config
{"provider": "gemini", "model": "gemini-2.5-flash"}
```
This updates only `model` for Gemini; `api_key`, `base_url`, and `active` are preserved.

**Consequences:**
- SwiftUI settings screen can save individual field changes without data loss
- Consistent `active` naming across dataclass, DB column, API JSON, and store method parameter

---

## ADR-016: Add Revolut as data source via GoCardless Bank Account Data API

**Date:** 2026-03-02

**Status:** Proposed

**Context:** Revolut is widely used for multi-currency personal banking in Europe but has no self-service API for individuals. Revolut's own Open Banking API requires TPP (Third Party Provider) registration with eIDAS/OBIE certificates — designed for regulated financial institutions, not personal finance tools.

**Problem:**
- No direct Revolut API access for individual developers
- Ponto (Isabel Group/Ibanity) was evaluated as an intermediary but rejected (see alternatives below)
- Need a way to read Revolut account balances and transaction history programmatically

**Alternatives evaluated:**

1. **Revolut Open Banking API (direct)** — requires TPP registration with eIDAS or OBIE certificate. Not accessible to individual developers. Rejected.

2. **Ponto (Isabel Group)** — PSD2-regulated AISP connecting 2,000+ EU banks. Rejected because:
   - Enterprise pricing (pay-per-linked-account, no free tier, ~€2,400/yr for Ibanity platform)
   - Complex auth stack (mTLS + HTTP Signatures + OAuth2)
   - Python SDK (`ibanity-python`) is 7 years unmaintained, not on PyPI
   - Revolut not explicitly confirmed in their supported bank list
   - Targeted at B2B platform integrators, not individual developers

3. **GoCardless Bank Account Data (formerly Nordigen)** — free open banking API for developers. Selected because:
   - Free tier (50 connections/month) sufficient for personal use
   - Revolut explicitly supported (institution ID: `REVOLUT_REVOGB21`)
   - Python SDK (`nordigen`) on PyPI, Python >= 3.8
   - Simple auth (secret_id + secret_key, no certificates)
   - Up to 730 days transaction history for Revolut
   - 2,500+ banks across UK/EU (potential for adding more sources later)

**Decision:** Add Revolut as source #10 using GoCardless Bank Account Data API (Nordigen) as the open banking intermediary.

**Design choices:**
- **GoCardless over Ponto** — free, simpler auth, confirmed Revolut support, maintained Python SDK
- **`nordigen` PyPI package** — official SDK, though no longer actively maintained by GoCardless (still functional). Alternative: raw HTTP calls to the REST API if SDK breaks
- **Browser-based initial auth** — GoCardless requires a one-time redirect flow where the user authorizes bank access in a browser. `pfm source add revolut` opens the authorization link via `webbrowser.open()`, user completes consent, callback provides requisition ID
- **Credentials in SQLite** — store `secret_id`, `secret_key`, and `requisition_id` in the `sources` table (same pattern as other sources, ADR-002)
- **90-day re-authorization** — PSD2 SCA requires re-consent every 90 days. The collector detects expired access and prompts re-auth. Store `authorized_at` timestamp to proactively warn before expiry
- **Data collected** — account balances (multi-currency), transaction history with date range filtering. Balances converted to USD via existing CoinGecko/PricingService for fiat pairs
- **Source type name** — `revolut` (not `gocardless` or `nordigen`) since the source identity is the bank, not the intermediary

**API flow:**
```
1. Register at bankaccountdata.gocardless.com → get secret_id, secret_key
2. pfm source add revolut → prompts for credentials
3. CLI calls GoCardless API to create requisition → returns auth link
4. User opens link, authorizes in Revolut app → callback with account IDs
5. pfm collect → fetches balances + transactions via GoCardless API
6. Every 90 days → re-authorize via browser flow
```

**Consequences:**
- Source count increases from 9 to 10
- New `src/pfm/collectors/revolut.py` module
- New dependency: `nordigen` (or raw `aiohttp` calls to GoCardless REST API)
- Revolut multi-currency balances (EUR, GBP, USD, etc.) included in portfolio analytics
- Pattern is reusable for adding other EU banks via GoCardless in the future
- 90-day re-auth adds a maintenance task that other API-key sources don't have
