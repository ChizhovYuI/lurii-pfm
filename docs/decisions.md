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

**Decision:** Replace the Gemini-only `ai/analyst.py` with a pluggable `LLMProvider` protocol. Four providers implemented: Gemini, Ollama, OpenRouter, Grok. User selects one active provider via `pfm ai set`.

**Design choices:**
- **Protocol-based abstraction** — `LLMProvider` protocol with `generate_commentary()` and `close()` methods
- **Provider registry** — `PROVIDER_REGISTRY` dict mapping names to classes
- **Per-provider config in SQLite** — `ai_settings` table (provider, api_key, model, base_url), not `.env`
- **Ollama native API** — direct `/api/chat` HTTP calls instead of OpenAI-compatible endpoint, for full model management (list, pull)
- **OpenAI-compatible clients** — OpenRouter and Grok share a common base using `openai` SDK pattern

**Consequences:**
- Gemini remains the default provider with existing model failover chain
- Ollama enables fully local/private AI commentary (no API key needed)
- OpenRouter provides access to Claude, GPT, Mistral, etc. via single API key
- Legacy `pfm gemini set/show/clear` commands still work as aliases

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
