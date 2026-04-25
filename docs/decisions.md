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
- **`sqlcipher3` over `pysqlcipher3` — `sqlcipher3` ships pre-built wheels for Python 3.13 with a self-contained SQLCipher build (no system-level library dependency). It is DB-API 2.0 compatible and maintained by the `peewee` author. `pysqlcipher3` requires linking against a separately-installed `libsqlcipher`
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

---

## ADR-017: Add `tip` field to credential field schemas for UI hints

**Date:** 2026-03-02

**Status:** Accepted

**Context:** The SwiftUI app renders a source-add form with credential input fields. Users need guidance on where to obtain each credential value (e.g. which settings page, what permissions to set). This information exists in `docs/data-sources.md` but is not accessible to the UI.

**Decision:** Add an optional `tip: str = ""` field to the `CredentialField` frozen dataclass. Populate a tip string on the **first** credential field of each source type only. The UI renders this as an info icon with a popover before the input fields.

**Design choices:**
- **Tip on first field only** — each source type gets one tip containing all setup steps, displayed before the inputs. Avoids per-field tip clutter
- **Newline-separated steps** — tips use `\n`-delimited numbered steps (e.g. `"1. Log in to okx.com\n2. Go to API Management\n..."`) so the UI can render them as a list
- **Empty string default** — non-first fields use `tip=""`, keeping the dataclass change backward-compatible
- **Included in `/api/v1/source-types` response** — `"tip"` key added to each field dict alongside `name`, `prompt`, `required`, `secret`

**Consequences:**
- `CredentialField` gains one new field (`tip`), no schema migration needed (Python-only dataclass)
- `/api/v1/source-types` response includes `tip` for all fields (empty string for non-first fields)
- SwiftUI source-add form can display setup instructions without hardcoding them client-side
- Tip content derived from `docs/data-sources.md` setup sections, kept in sync in one place (`source_types.py`)

---

## ADR-018: Structured JSON sections for AI commentary

**Date:** 2026-03-02

**Status:** Accepted

**Context:** The AI commentary was generated as plain text (no markdown) optimized for Telegram delivery. The SwiftUI app needs to render the commentary as distinct titled blocks with rich formatting (bold, bullet lists, numbers).

**Problem:**
- Plain text output cannot be rendered as structured UI cards in SwiftUI
- Telegram-only formatting constraints (no markdown) limited the richness of AI output
- The UI had no way to separate the 5 report sections for independent rendering

**Decision:** Change the AI prompt to request a JSON array of `{"title", "description"}` objects where `description` uses GitHub-flavored Markdown. Parse the JSON response in the analyst layer and expose structured `sections` alongside the flattened `text` in the API response.

**Design choices:**
- **JSON array output format** — the LLM returns `[{"title": "Market Context", "description": "BTC at **$95k**..."}]` instead of free-form text. System prompt explicitly says "respond ONLY with a valid JSON array"
- **`CommentarySection` dataclass** — frozen `(title: str, description: str)` in `base.py`, stored as a tuple on `CommentaryResult.sections`
- **`_parse_sections()` with code fence handling** — LLMs sometimes wrap JSON in ` ```json ``` ` fences; the parser strips these before `json.loads()`. Returns empty tuple on parse failure (graceful fallback to plain text)
- **`_flatten_sections()` for Telegram** — converts sections back to `"Title\nDescription\n\n"` plain text for the existing Telegram formatter, preserving backward compatibility
- **`text` field preserved** — `CommentaryResult.text` always contains a flat string (either flattened sections or raw LLM output if JSON parsing fails). Telegram and any other plain-text consumer works unchanged
- **`sections` in API and cache** — both `GET` and `POST /api/v1/ai/commentary` return `sections: [{"title", "description"}]`. Cached to `analytics_metrics` alongside `text` and `model`
- **Markdown in `description`** — GitHub-flavored Markdown (bold, bullets, numbered lists) gives the SwiftUI app rich rendering while remaining human-readable as fallback

**API response shape:**
```json
{
  "date": "2026-03-02",
  "text": "Market Context\nBTC at $95k...",
  "model": "gemini-2.5-flash",
  "sections": [
    {"title": "Market Context", "description": "BTC is trading at **$95,432**...\n\n- Portfolio exposure: 45%"},
    {"title": "Portfolio Health Assessment", "description": "..."},
    {"title": "Rebalancing Opportunities", "description": "..."},
    {"title": "Risk Alerts", "description": "..."},
    {"title": "Actionable Recommendations for Next 7 Days", "description": "..."}
  ]
}
```

**Files changed:**
- `src/pfm/ai/prompts.py` — system and user prompts request JSON array with markdown descriptions
- `src/pfm/ai/base.py` — added `CommentarySection` dataclass, `sections` tuple field on `CommentaryResult`
- `src/pfm/ai/analyst.py` — added `_parse_sections()`, `_flatten_sections()`, wired into generation flow
- `src/pfm/server/routes/ai.py` — GET and POST return `sections` array, cached to DB
- `src/pfm/ai/__init__.py` — exported `CommentarySection`
- `tests/test_analyst.py` — 6 new tests for parsing/flattening, fixed pre-existing `activate→active` bug
- `tests/test_prompts.py` — updated assertions for new prompt wording

**Consequences:**
- SwiftUI app can render 5 distinct cards with titles and markdown-formatted descriptions
- Telegram delivery continues to work via flattened `text` field (no breaking change)
- If the LLM returns non-JSON output (e.g. plain text), the system falls back gracefully — `sections` will be empty and `text` contains the raw output
- Cached commentary in `analytics_metrics` includes `sections` for future reads without re-generation

---

## ADR-019: Pass `db_path` to `send_report` in report notify endpoint

**Date:** 2026-03-02

**Status:** Accepted

**Context:** `POST /api/v1/report/notify` calls `send_report(report_payload)` without passing `db_path`. Inside `send_report`, `resolve_telegram_credentials` falls back to `settings.database_path` which defaults to the relative path `data/pfm.db`.

**Problem:**
- When the daemon runs via launchd (Homebrew install), its working directory is not the project root
- `data/pfm.db` resolves to a non-existent path, causing `sqlite3.OperationalError: unable to open database file`
- The report endpoint always fails with 500 in the daemon, even though `is_telegram_configured` (which does pass `db_path`) succeeds

**Decision:** Pass `db_path=db_path` to `send_report()` in the report notify endpoint, consistent with all other DB-accessing calls in the same handler.

**Files changed:**
- `src/pfm/server/routes/report.py` — added `db_path=db_path` to `send_report()` call (line 41)

**Consequences:**
- Report notify endpoint works correctly when daemon runs from any working directory
- Consistent with how `is_telegram_configured` and `build_analytics_summary` already receive `db_path` in the same handler

---

## ADR-020: Integrate `instructor` for structured AI output

**Date:** 2026-03-02

**Status:** Accepted

**Context:** LLM providers (Ollama, OpenRouter, Grok) returned raw text that the analyst orchestrator manually parsed as JSON. This parsing was fragile — it broke with `<think>` blocks, preamble text, unescaped newlines in JSON strings, and markdown code fences. Each failure mode required a dedicated workaround in `_parse_sections()`.

**Problem:**
- Manual JSON parsing required ~80 lines of defensive code (`_parse_sections`, `_escape_newlines_in_json_strings`, `_try_json_loads`, code fence stripping, preamble scanning)
- Each new LLM quirk (e.g. Qwen3 `<think>` blocks) required a new parsing workaround
- No validation that the JSON structure matched the expected `[{title, description}]` schema
- No automatic retry on malformed output — a single bad response produced fallback commentary

**Decision:** Integrate the `instructor` library to enforce structured output via Pydantic models at the API call level, with automatic validation and retries. Migrate Ollama, OpenRouter, and Grok providers; keep Gemini on its native `google-genai` SDK.

**Design choices:**
- **`instructor` + `openai` SDK** — `instructor.from_openai(AsyncOpenAI(...))` patches the OpenAI client to accept a `response_model` parameter. The LLM response is automatically parsed into a Pydantic model with validation and retry on failure
- **Gemini excluded** — Gemini's 300-line failover/rate-limiting logic in `providers/gemini.py` is too risky to rewrite. `instructor.from_gemini()` targets the old SDK, not `google.genai`. Gemini continues through the existing manual `_parse_sections` path
- **`CommentaryResponse` Pydantic model** — `sections: list[ReportSection]` with `min_length=1, max_length=10` provides lenient bounds while the prompt still requests exactly 5 sections
- **`ReportSection` bridges to `CommentarySection`** — `to_commentary_sections()` converts Pydantic models to the existing frozen dataclass format, keeping the rest of the codebase unchanged
- **`flatten_sections()` extracted to `base.py`** — both providers and analyst import from `base.py`, avoiding circular dependencies
- **Pre-parsed sections short-circuit** — analyst orchestrator checks `result.sections` first; if populated (instructor path), skips manual JSON parsing entirely. Gemini results (empty `sections`) continue through the existing path
- **Ollama uses `instructor.Mode.JSON`** — not `TOOLS` mode, maximizing compatibility with local models that lack function-calling support
- **Ollama dual-client pattern** — instructor-patched `AsyncOpenAI` for generation (hits `/v1`), raw `httpx.AsyncClient` for auto-pull (`/api/pull`)
- **OpenRouter and Grok unchanged** — they inherit from `OpenAICompatibleProvider` and only define class attributes. `default_base_url` values work as-is (constructor appends `/v1`)
- **Specific exception handling** — catches `openai.APIError` and `pydantic.ValidationError` (covers HTTP errors + schema validation failures after instructor retry exhaustion)

**New dependencies:**
- `openai>=1.82.0` — OpenAI Python SDK (used as transport layer for OpenAI-compatible APIs)
- `instructor>=1.8.0` — structured output enforcement via Pydantic models

**Files changed:**
- `pyproject.toml` — added `openai`, `instructor` dependencies + mypy override for `instructor`
- `src/pfm/ai/schemas.py` — **new** `CommentaryResponse` and `ReportSection` Pydantic models
- `src/pfm/ai/base.py` — extracted `flatten_sections()` as public function
- `src/pfm/ai/__init__.py` — exported `flatten_sections`
- `src/pfm/ai/providers/openai_compat.py` — replaced `httpx` with instructor-patched `AsyncOpenAI`
- `src/pfm/ai/providers/ollama.py` — instructor `AsyncOpenAI` for generation + `httpx` for pull
- `src/pfm/ai/analyst.py` — added `result.sections` short-circuit, uses `flatten_sections` from base
- `tests/test_ai_schemas.py` — **new** (4 tests)
- `tests/test_provider_openai_compat.py` — rewritten with mock instructor client
- `tests/test_provider_ollama.py` — rewritten with mock instructor + httpx for pull
- `tests/test_provider_openrouter.py` — updated (removed `_build_headers` test)
- `tests/test_provider_grok.py` — updated (removed `_build_headers` test)
- `tests/test_analyst.py` — added preparsed sections test, fixed `flatten_sections` import

**Files NOT changed:**
- `src/pfm/ai/providers/gemini.py` — stays on native `google-genai` SDK
- `src/pfm/ai/providers/openrouter.py` — inherits changes from base class
- `src/pfm/ai/providers/grok.py` — inherits changes from base class
- `src/pfm/ai/prompts.py` — system prompt unchanged (still needed for Gemini)

**Consequences:**
- Providers return `CommentaryResult` with pre-populated `sections` — no manual JSON parsing needed
- Pydantic validation catches malformed LLM output at the provider level with automatic retry
- Manual parsing in `analyst.py` preserved as fallback for Gemini (and any future provider not using instructor)
- Test count unchanged (42 AI tests); all pass with mypy and ruff clean
- Future Gemini migration to instructor possible once `instructor.from_gemini()` supports `google.genai`

---

## ADR-021: Graceful WebSocket shutdown and AI commentary progress events

**Date:** 2026-03-02

**Status:** Accepted

**Context:** Two related issues discovered during SwiftUI app integration:

1. **Daemon can't stop while UI is connected** — stopping the daemon (SIGTERM via `launchctl unload` or `pfm server stop`) hung indefinitely when the SwiftUI app had an active WebSocket connection.

2. **No visibility into AI commentary generation** — `POST /api/v1/ai/commentary` blocks for 10–120 seconds depending on the LLM provider. The UI had no way to show a loading state, detect concurrent requests, or know when generation completed.

**Problem 1 — Shutdown deadlock:**
- aiohttp's shutdown sequence: `on_shutdown` → wait `shutdown_timeout` for handlers → `on_cleanup`
- `EventBroadcaster.close()` (which sends WebSocket close frames) was in `on_cleanup` — *after* the timeout wait
- WebSocket handlers blocked in `async for msg in ws`, waiting for messages that would never come
- The close signal that would unblock them was scheduled to run after they finished — circular deadlock
- Default `shutdown_timeout` of 60 seconds made the hang feel permanent

**Problem 2 — No commentary progress:**
- `POST /api/v1/ai/commentary` was a synchronous request-response with no status signaling
- No way for the UI to know generation was in progress (no app flag, no WebSocket event)
- No guard against concurrent generation requests (two taps on "Generate" would run two LLM calls)

**Decision:** Fix the shutdown sequence and add commentary progress events following the existing collection progress pattern.

**Design choices:**

*Graceful shutdown:*
- **Move `broadcaster.close()` from `on_cleanup` to `on_shutdown`** — close frames are sent *before* the shutdown timeout, so WebSocket handlers receive the CLOSE message and exit naturally during the grace period
- **`shutdown_timeout=5.0`** in `web.run_app()` (was aiohttp default of 60s) — server force-closes remaining connections after 5 seconds if a client doesn't acknowledge the close frame
- **`heartbeat=30.0`** on `WebSocketResponse` — enables ping/pong to detect dead connections, preventing zombie WebSockets from blocking shutdown
- **Per-connection 2s timeout** in `broadcaster.close()` — one stuck client can't block shutdown of other connections

*Commentary progress:*
- **`app["generating_commentary"]` flag** — mirrors the existing `app["collecting"]` pattern
- **409 rejection** — `POST /api/v1/ai/commentary` returns `409 Conflict` if generation is already in progress, same as `POST /api/v1/collect`
- **WebSocket events** — `commentary_started`, `commentary_completed`, `commentary_failed` broadcast to all connected clients, matching the collection event pattern (`collection_started`, `collection_completed`, `collection_failed`)
- **Polling endpoint** — `GET /api/v1/ai/commentary/status` → `{"generating": true|false}`, mirrors `GET /api/v1/collect/status`

**Files changed:**
- `src/pfm/server/ws.py` — added `heartbeat=30.0`, per-connection close timeout, moved from `on_cleanup` to `on_shutdown`
- `src/pfm/server/app.py` — added `on_shutdown` handler for broadcaster, initialized `generating_commentary` flag, removed broadcaster close from `on_cleanup`
- `src/pfm/server/run.py` — set `shutdown_timeout=5.0`
- `src/pfm/server/routes/ai.py` — added `GET /ai/commentary/status`, 409 guard, broadcast events around generation

**Consequences:**
- Daemon stops cleanly within ~5 seconds regardless of connected WebSocket clients
- SwiftUI app receives real-time commentary progress via existing WebSocket connection
- Concurrent generation requests are rejected with a clear error
- UI can poll `GET /ai/commentary/status` as fallback if WebSocket reconnects mid-generation
- No breaking changes — existing endpoints and event types unchanged

---

## ADR-022: Async AI commentary generation with background task

**Date:** 2026-03-02

**Status:** Accepted

**Context:** `POST /api/v1/ai/commentary` ran the full LLM generation synchronously within the HTTP request handler. Depending on the provider and model, this took 30–150+ seconds. The SwiftUI app's `URLSession` timed out (`NSURLErrorDomain Code=-1001`) before the response arrived.

**Problem:**
- Ollama (llama3.1:8b) took ~52–148 seconds for commentary generation
- OpenRouter latency varied widely depending on model and queue
- iOS/macOS `URLSession` default timeout is 60 seconds
- Extending the client timeout is fragile — any network hiccup during a 2-minute request causes a retry flood

**Decision:** Convert `POST /api/v1/ai/commentary` to an async background task pattern, matching the existing `POST /api/v1/collect` design. The endpoint returns 202 immediately, and the UI receives results via WebSocket.

**Design choices:**
- **Same pattern as collection** — `asyncio.ensure_future(_run_commentary(app))` spawns a background coroutine, task reference stored in `app["_commentary_task"]` to prevent GC
- **Validation before spawning** — snapshot existence check runs synchronously in the handler (fast DB query); only the slow LLM call runs in background
- **Result in WebSocket event** — `commentary_completed` event includes `date`, `text`, `model`, `sections` so the UI can update immediately without a follow-up GET request
- **Fallback read path** — `GET /api/v1/ai/commentary` reads cached result from DB, so the UI can also poll or read on reconnect
- **`generating_commentary` flag in `finally:`** — always reset even if the background task crashes, preventing permanent 409 lockout
- **No HTTP response body** — 202 returns `{"status": "started"}` only; all result data flows through WebSocket

**API flow:**
```
1. UI sends POST /api/v1/ai/commentary → receives 202 {"status": "started"}
2. UI receives WebSocket event: {"type": "commentary_started"}
3. Background task runs LLM generation (30-150s)
4. UI receives WebSocket event: {"type": "commentary_completed", "date": "...", "text": "...", ...}
   OR: {"type": "commentary_failed", "error": "..."}
5. On reconnect: GET /api/v1/ai/commentary reads cached result from DB
```

**Files changed:**
- `src/pfm/server/routes/ai.py` — extracted `_run_commentary()` background task, handler returns 202

**Consequences:**
- No more client-side timeouts — HTTP round-trip completes in <50ms
- UI shows loading state via `commentary_started` event, updates on `commentary_completed`
- Background task failure is communicated via `commentary_failed` event (not a 500 response)
- The 409 guard still prevents concurrent generation
- Breaking change for API consumers expecting a synchronous response body — must switch to WebSocket or polling

---

## ADR-023: Ollama validation failures skip auto-pull

**Date:** 2026-03-02

**Status:** Accepted

**Context:** The Ollama provider has an auto-pull mechanism: if `_call_chat()` returns an empty result, `generate_commentary()` assumes the model isn't installed, pulls it via `/api/pull`, and retries. This was designed for first-run scenarios where the user configures a model name but hasn't pulled it yet.

**Problem:**
- `llama3.1:8b` frequently generates malformed JSON — it places `description` content as a JSON key instead of `"description": "value"`. This is a known limitation of small local models with long structured output
- All 3 instructor retry attempts fail with the same `ValidationError` (field `description` missing)
- `_call_chat()` caught all exceptions uniformly and returned `CommentaryResult(text="")` — empty text
- `generate_commentary()` interpreted empty text as "model not found" and triggered `/api/pull`
- The pull was a no-op (model already installed), but the subsequent retry accidentally provided 3 more chances, sometimes succeeding by luck
- False auto-pull added ~1 second of latency and misleading log messages (`"attempting model pull"`)

**Decision:** Split exception handling in `_call_chat()` to distinguish API errors (model may not exist) from validation errors (model exists but generated bad output).

**Design choices:**
- **`APIError` → empty text** — signals potential model-not-found, allows auto-pull to proceed. Covers HTTP 404, connection refused, timeout
- **`ValidationError | InstructorRetryException` → `FALLBACK_COMMENTARY`** — model responded but output was structurally invalid. Returns fallback text with `error="structured_output_failed"`. Non-empty text skips auto-pull in `generate_commentary()`
- **No additional retries** — instructor already retried 3 times internally. A 4th round via auto-pull is unlikely to help for the same prompt. The fallback commentary is returned instead
- **Consistent with `OpenAICompatibleProvider`** — the base class already returns `FALLBACK_COMMENTARY` on all failures. Ollama now matches this behavior for validation errors

**Files changed:**
- `src/pfm/ai/providers/ollama.py` — split `except` clause into `APIError` vs `ValidationError | InstructorRetryException`
- `tests/test_provider_ollama.py` — added `test_ollama_validation_failure_skips_pull`

**Consequences:**
- Auto-pull only triggers on genuine API errors (model not found, connection refused)
- Validation failures return fallback commentary immediately — no misleading pull attempt
- Log messages accurately reflect the failure type (`"Ollama API error"` vs `"Ollama structured output validation failed"`)
- Small models (llama3.1:8b) that struggle with JSON schema will return fallback instead of silently retrying via pull

---

## ADR-024: Add native SOL staking to Bitget Wallet collector

**Date:** 2026-03-04

**Status:** Accepted

**Context:** The Bitget Wallet collector only tracked Aave V3 supply positions on Base (EVM). Bitget Wallet also supports native SOL staking via its self-operated Solana validator (`7tKWFaaLi2FJSqukHxUrnXph8M3ynrqn3kEkKPpgcNHZ`, "Bitget Wallet", 0% commission).

**Problem:**
- Staked SOL was not tracked in the portfolio
- EVM address and Solana address are different keys (secp256k1 vs ed25519), so the existing `wallet_address` cannot derive the Solana address

**Decision:** Extend `BitgetWalletCollector` to fetch native SOL staking positions via Solana RPC, with validator APY from Stakewiz API.

**Design choices:**
- **Optional `solana_address` credential** — added to `source_types.py` as `required=False`. Existing configs without it continue to work unchanged (backward compatible)
- **Solana RPC `getProgramAccounts`** — queries the Stake program (`Stake11111111111111111111111111111111111111`) with `memcmp` filter at offset 44 (withdrawer pubkey) and `jsonParsed` encoding. Returns all stake accounts owned by the wallet
- **Aggregated single SOL snapshot** — all stake accounts are summed into one `Snapshot(asset="SOL")`. Multiple validators are supported but APY is taken from the first active voter
- **Lamports → SOL conversion** — `Decimal(total_lamports) / Decimal(10^9)`, using `Decimal` throughout to avoid float precision loss
- **Stakewiz API for APY** — `GET https://api.stakewiz.com/validator/{voter}` returns `apy_estimate` as a percentage (e.g. 6.13). Converted to decimal (0.0613) for consistency with Aave APY storage. Best-effort: logs warning and returns 0 on failure
- **Voter extraction from parsed data** — reads `account.data.parsed.info.stake.delegation.voter` from the first active stake account. Skips accounts with no delegation (inactive/warming-up) so a later active account can still provide the voter
- **Refactored `fetch_balances()`** — split into `_fetch_aave_balances()` + `_fetch_sol_staking()`, combined in `fetch_balances()`. Both return `list[Snapshot]`
- **Base58 address validation** — `_SOLANA_ADDRESS_RE` matches `[1-9A-HJ-NP-Za-km-z]{32,44}` (standard base58 alphabet, valid Solana pubkey length range)
- **Public Solana RPC** — uses `https://api.mainnet-beta.solana.com` (rate-limited but sufficient for single daily collection)

**Raw JSON payload includes:**
- `solana_address`, `voter`, `total_lamports`, `stake_accounts` (array of `{pubkey, lamports}`)

**Files changed:**
- `src/pfm/collectors/bitget_wallet.py` — added Solana constants, `solana_address` param, `_fetch_sol_staking()`, `_fetch_validator_apy()`, `_normalize_solana_address()`, refactored `fetch_balances()`
- `src/pfm/source_types.py` — added `solana_address` credential field to `bitget_wallet`
- `tests/test_bitget_wallet.py` — 5 new tests with real stake account data fixtures (address `BcbaVrK3...`, ~7.89 SOL, validator `7tKWFaa...`)

**Consequences:**
- SOL staking positions appear in portfolio analytics, earn summary, and AI commentary
- Existing Bitget Wallet configs without `solana_address` are unaffected
- Stakewiz dependency is best-effort — APY defaults to 0 if the API is unreachable
- Source count remains 10 (Bitget Wallet gains a capability, not a new source type)

---

## ADR-025: User-configurable APY rules for Bitget Wallet

**Date:** 2026-03-04

**Status:** Accepted

**Context:** Bitget Wallet's "Stablecoin Earn Plus" product offers tiered APY (10% for 0–5000 USDC, 2.97% above) and temporary bonus boosts (+18.8% for 7 days). The Aave GraphQL API returns only the protocol APY (3.1%), so the actual yield is not reflected in snapshots. The SwiftUI app also needs to discover which source types support APY rules and which protocol+coin combinations are valid.

**Problem:**
- Aave API reports ~3.1% APY, but the user earns 10% on the first 5000 USDC via Bitget's product
- Temporary bonus boosts (e.g. +18.8% for 7 days) are not reflected in any API
- Historical snapshots retain the incorrect protocol APY and cannot be corrected retroactively
- No signal in the API for whether a source type supports APY rules — the UI would need to hardcode valid protocol/coin values

**Decision:** Add user-configurable APY rules stored in `app_settings` that override/supplement protocol APY during collection and retroactively recalculate historical snapshots when rules change. Expose valid protocol+coin combinations per source type in `/api/v1/source-types`.

**Design choices:**

*Storage and computation:*
- **`app_settings` storage** — rules stored as JSON array under key `apy_rules:{source_name}`. Reuses existing key-value table, no schema migration needed
- **Two rule types** — `base` replaces the protocol APY with a tiered bracket; `bonus` adds on top. Multiple bonus rules stack additively
- **Tiered brackets** — each rule has `limits: [{from_amount, to_amount, apy}]` where `from_amount` is exclusive lower bound, `to_amount` is inclusive upper bound (None = infinity). Matches the Earn Plus tier structure
- **Date-scoped rules** — `started_at`/`finished_at` define the active period. Temporary boosts naturally expire without manual deletion
- **Pure computation function** — `compute_effective_apy(protocol_apy, rules, protocol, coin, amount, date)` is a pure function with no I/O, easy to test
- **Retroactive recalculation** — after any rule CRUD, affected snapshots (union date range of old + new rules) are queried and their APY is recomputed from `raw_json.apy.value` (the original protocol APY). This preserves the protocol APY as ground truth
- **Collector integration** — `BitgetWalletCollector.apy_rules` is populated from the store before collection. Rules are applied inline during `_fetch_aave_balances()`, so new snapshots immediately reflect the effective APY
- **Validation** — known protocol (`aave`), coin (`usdc`, `usdt`), type (`base`, `bonus`) enums. Non-empty limits, valid dates, `started_at <= finished_at`

*UI discovery:*
- **`supported_apy_rules: [{protocol, coins}]`** in `/api/v1/source-types` response — provides the valid protocol+coin combinations for combobox rendering. Empty array means APY rules are not available for that source type
- **`ApyRulesProtocol` dataclass** in `source_types.py` — `(protocol: str, coins: tuple[str, ...])`, single source of truth for both API response and route validation
- **`APY_RULES_TYPES` dict** — maps source type name to tuple of `ApyRulesProtocol`. Route validation checks `source.type in APY_RULES_TYPES` instead of hardcoding source type names
- **Restructured `/api/v1/source-types` response** — changed from `{name: [fields]}` to `{name: {fields: [...], supported_apy_rules: [...]}}` to accommodate the new metadata

**Data model:**
```python
@dataclass(frozen=True, slots=True)
class RuleLimit:
    from_amount: Decimal   # exclusive lower bound
    to_amount: Decimal | None  # inclusive upper bound, None = infinity
    apy: Decimal           # decimal fraction (0.10 = 10%)

@dataclass(frozen=True, slots=True)
class ApyRule:
    id: str                # uuid4
    protocol: str          # "aave"
    coin: str              # "usdc"
    type: str              # "base" | "bonus"
    limits: tuple[RuleLimit, ...]
    started_at: date
    finished_at: date
```

**REST API:**

| Method | Path | Action |
|--------|------|--------|
| GET | `/api/v1/sources/{name}/apy-rules` | List rules |
| POST | `/api/v1/sources/{name}/apy-rules` | Add rule, recalculate |
| PUT | `/api/v1/sources/{name}/apy-rules/{id}` | Update rule, recalculate |
| DELETE | `/api/v1/sources/{name}/apy-rules/{id}` | Delete rule, recalculate |

**Source-types response shape:**
```
"bitget_wallet": {
  "fields": [{"name": "wallet_address", "prompt": "...", ...}],
  "supported_apy_rules": [
    {"protocol": "aave", "coins": ["usdc", "usdt"]}
  ]
}
"okx": {
  "fields": [{"name": "api_key", "prompt": "...", ...}],
  "supported_apy_rules": []
}
```

**Files changed:**
- `src/pfm/db/apy_rules_store.py` — **new** `RuleLimit`, `ApyRule` dataclasses, `compute_effective_apy()`, `ApyRulesStore` CRUD
- `src/pfm/server/routes/apy_rules.py` — **new** 4 route handlers with recalculation
- `src/pfm/db/repository.py` — added `get_snapshots_by_source_name_and_date_range()`, `update_snapshot_apy()`
- `src/pfm/collectors/bitget_wallet.py` — added `apy_rules` attribute, applies rules in `_fetch_aave_balances()`
- `src/pfm/server/routes/collect.py` — injects rules into collectors for supported source types
- `src/pfm/server/routes/__init__.py` — registered `apy_rules_routes`
- `src/pfm/source_types.py` — added `ApyRulesProtocol` dataclass and `APY_RULES_TYPES` dict
- `src/pfm/server/routes/sources.py` — restructured `/api/v1/source-types` response to include `supported_apy_rules`
- `tests/test_apy_rules_store.py` — **new** 27 tests (computation + CRUD + validation)
- `tests/test_apy_rules_routes.py` — **new** 9 route tests
- `tests/test_bitget_wallet.py` — added APY rules override test
- `tests/test_routes_sources.py` — updated for new response shape

**Consequences:**
- Earn summary and portfolio analytics reflect the actual yield, not just the protocol APY
- Historical snapshots are corrected when rules are added/changed/removed
- Temporary bonus boosts are modeled with date-scoped rules that naturally expire
- Protocol APY is preserved in `raw_json` as ground truth for recalculation
- No schema migration — reuses existing `app_settings` table
- UI renders protocol and coin comboboxes from API data, no hardcoded values
- Adding APY rule support for a new source type requires only a new entry in `APY_RULES_TYPES`
- Breaking change for `/api/v1/source-types` consumers — response shape changed from `[fields]` to `{fields, supported_apy_rules}`

---

## ADR-027: MCP server for portfolio data access

**Date:** 2026-03-07

**Status:** Accepted

See `adr-027-mcp-server.md`. FastMCP-based stdio server reusing
`Repository`, analytics, and pricing. 10 tools, 6 resources, 2 prompts.
Read-only by design (later narrowed by ADR-028 to allow categorization
writes).

---

## ADR-028: Categorization tools in MCP server

**Date:** 2026-04-25

**Status:** In progress (Phases 1–5 accepted, Phase 6 proposed)

See `adr-028-categorization-mcp-tools.md`. Adds a `regex` operator to
the rule engine and a categorization tool surface (rule CRUD, dry-run,
manual category override, re-run) so a Claude Code skill can drive the
type/category workflow end-to-end. Narrows the ADR-027 read-only
contract to allow writes scoped to categorization metadata only.

**Phase 1 (done):**
- `regex` operator in `_match_values` (case-sensitive; `(?i)` flag for ci); compiled patterns are cached via `functools.lru_cache`
- Pattern validation at rule-create time (`_validate_regex_value`) — invalid pattern raises `ValueError`
- Runtime tolerance — malformed pattern silently fails to match instead of breaking the categorization pass
- Files: `src/pfm/analytics/categorizer.py`, `src/pfm/db/metadata_store.py`, `tests/test_type_rules.py`, `tests/test_categorizer.py`

**Phase 2 (done):**
- `MetadataStore.get_categorization_summary(source_name?)` — per-source counts: `total`, `unknown_type`, `no_category`, `internal_transfer`. Single GROUP BY query.
- `MetadataStore.get_uncategorized_transactions(source_name?, missing_type, missing_category, limit, offset)` — paginated `(Transaction, TransactionMetadata|None)`. Default (both flags False) is OR-logic; both True is AND.
- Files: `src/pfm/db/metadata_store.py`, `tests/test_metadata_store_helpers.py` (11 new tests).

**Phase 3 (done):**
- `src/pfm/analytics/rule_dryrun.py` — `dry_run_category_rule(...)` and `dry_run_type_rule(...)`. Mirrors the corresponding `MetadataStore.create_*_rule` signatures plus `scope_source` / `limit=200`. No DB writes.
- Output schema: `{matched, unchanged, changed, overlapping_rules, raw_field_samples}`. `overlapping_rules` lists existing non-deleted rules already winning for matched txs. `raw_field_samples` is ≤5 deduped values, each ≤200 chars.
- Reuses `_match_category_rule`, `_resolve_field`, `categorize_transaction` (categorizer), `match_type_rule`, `_resolve_raw_field` (type_resolver), and `_validate_regex_value` (metadata_store) — no logic duplication.
- Pre-validates regex (`field_operator="regex"`) before any DB read.
- Files: `src/pfm/analytics/rule_dryrun.py`, `tests/test_rule_dryrun.py` (12 new tests).

**Phase 4 (done):**
- `AppContext` extended with `metadata_store: MetadataStore`, built once per lifespan from the shared `aiosqlite.Connection`. `_ctx_store` helper added next to `_ctx_repo` / `_ctx_db_path`.
- 16 categorization tools registered in `src/pfm/mcp_server.py`: inspection (`list_category_rules`, `list_type_rules`, `list_categories`, `categorization_summary`, `get_rule_suggestions`), discovery (`list_uncategorized_transactions`, `get_transaction_detail`), mutation (`create_category_rule`, `delete_category_rule`, `create_type_rule`, `delete_type_rule`, `set_transaction_category`, `link_transfer`, `unlink_transfer`), dry-run (`dry_run_category_rule`, `dry_run_type_rule` wiring `rule_dryrun`), and `apply_categorization` wrapping `run_categorization`.
- All tools return JSON via `_json`. Boolean args are keyword-only (FBT discipline). Mutation tools accept integer row ids; listing tools expose both `id` and `tx_id` so the skill can round-trip ids.
- Files: `src/pfm/mcp_server.py`.

**Phase 5 (done):**
- `tests/test_mcp_server.py` extended with `TestCategorizationTools` (16 tests) using a real `Repository` + `MetadataStore` over `tmp_path` SQLite. Covers happy paths, regex validation, not-found envelope, link/unlink round-trip, `set_transaction_category` recording a `user_category_choices` row, and dry-run wiring. Smokes `apply_categorization` returning the counts dict.
- Files: `tests/test_mcp_server.py`.

**Phase 6 (proposed):** Claude Code skill in `../lurii-portfolio`.
