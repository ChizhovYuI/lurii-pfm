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
- Credentials mixed with global settings (Telegram, Claude API, CoinGecko) in one flat file
- No enable/disable mechanism — all configured sources always run

**Decision:** Move source credentials to a `sources` table in SQLite (same `pfm.db`). Manage via interactive CLI (`pfm source add/list/show/delete/enable/disable`). Keep global settings (Telegram, Claude API, CoinGecko, logging) in `.env`.

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
