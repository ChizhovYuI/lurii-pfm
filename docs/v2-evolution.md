# Lurii Finance v2 — Evolution Specification

## Overview

Transform Lurii Finance from a Python CLI batch tool into a native macOS application with a SwiftUI frontend, persistent Python backend, encrypted storage, multi-LLM support, DeFi yield optimization, and natural-language search over financial data.

**Brand:** Lurii Finance (formerly PFM)
**UI reference:** [Rotki](https://github.com/rotki/rotki) — open-source portfolio tracker with privacy focus.

---

## Phases

| Phase | Scope | Dependencies |
|-------|-------|-------------|
| 1 | LLM provider abstraction | None |
| 2 | HTTP backend (aiohttp) | Phase 1 |
| 3 | SwiftUI app (macOS native) | Phase 2 |
| 4 | Encrypted SQLite (SQLCipher) | Phase 2 |
| 5 | Semantic search + chat | Phase 1, Phase 2 |
| 6 | DefiLlama yield optimization | Phase 2, Phase 3 |

---

## Phase 1 — LLM Provider Abstraction

### Goal

Replace the Gemini-coupled `ai/analyst.py` with a pluggable provider system. User selects one active provider in settings.

### Providers

| Provider | SDK / Protocol | Auth | Notes |
|----------|---------------|------|-------|
| Gemini | `google-genai` | API key | Current default, model failover chain |
| Ollama | HTTP REST (`localhost:11434`) | None | Privacy-first, local-only |
| OpenRouter | OpenAI-compatible REST | API key | Access to many models via single key |
| Grok (xAI) | OpenAI-compatible REST | API key | xAI models |

### Architecture

```
src/pfm/ai/
├── __init__.py
├── base.py              # Abstract LLMProvider protocol
├── providers/
│   ├── __init__.py      # PROVIDER_REGISTRY
│   ├── gemini.py        # Existing logic, refactored
│   ├── ollama.py        # Local Ollama REST client
│   ├── openrouter.py    # OpenAI-compatible client
│   └── grok.py          # OpenAI-compatible client (xai-* models)
├── analyst.py           # Orchestrator: resolve provider → generate
├── prompts.py           # Unchanged
└── config.py            # Provider selection + per-provider settings
```

### Provider Protocol

```python
from typing import Protocol

class LLMProvider(Protocol):
    name: str

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult: ...

    async def close(self) -> None: ...
```

### Settings

New `ai_settings` table in SQLite (or extend `app_settings`):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ai_provider` | enum | `gemini` | Active provider: gemini / ollama / openrouter / grok |
| `ai_provider_api_key` | str | null | API key (Gemini, OpenRouter, Grok) |
| `ai_provider_model` | str | null | Override default model for active provider |
| `ai_provider_base_url` | str | null | Custom endpoint (Ollama, OpenRouter) |

### CLI Changes

```bash
pfm ai set                # Interactive: pick provider → configure
pfm ai show               # Show current provider config (key masked)
pfm ai clear              # Remove AI provider config
pfm ai providers          # List available providers
# Deprecate: pfm gemini set/show/clear (keep as aliases for backwards compat)
```

### Default Models

| Provider | Default model |
|----------|--------------|
| Gemini | `gemini-2.5-pro` (failover: flash → flash-lite) |
| Ollama | `llama3.1:8b` |
| OpenRouter | `anthropic/claude-sonnet-4` |
| Grok | `grok-3-mini` |

### Migration

- Existing Gemini key in `app_settings` is auto-migrated to new schema
- `pfm gemini set/show/clear` become aliases for `pfm ai set --provider gemini`

---

## Phase 2 — HTTP Backend (aiohttp)

### Goal

Expose all Lurii Finance functionality via a local HTTP API. The Python process runs as a persistent daemon managed by launchd.

### Server Architecture

```
src/pfm/
├── server/
│   ├── __init__.py
│   ├── app.py           # aiohttp Application factory
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── sources.py   # /api/sources CRUD
│   │   ├── collect.py   # /api/collect (trigger + progress via WS)
│   │   ├── portfolio.py # /api/portfolio (snapshots, net worth, allocation)
│   │   ├── analytics.py # /api/analytics (PnL, yield, exposure)
│   │   ├── ai.py        # /api/ai (commentary, provider config)
│   │   ├── report.py    # /api/report (trigger notification)
│   │   ├── settings.py  # /api/settings (app config)
│   │   └── unlock.py    # /api/unlock (Phase 4: DB decryption key)
│   ├── ws.py            # WebSocket handler for real-time events
│   ├── middleware.py     # Local-only guard, error handling
│   └── daemon.py        # launchd integration, PID file, lifecycle
```

### API Design

All endpoints prefixed with `/api/v1/`. JSON request/response.

#### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/sources` | List all sources |
| POST | `/sources` | Add a source |
| GET | `/sources/{name}` | Get source details (secrets masked) |
| DELETE | `/sources/{name}` | Delete source |
| PATCH | `/sources/{name}` | Update source (enable/disable, credentials) |
| POST | `/collect` | Trigger collection (all or specific source) |
| GET | `/portfolio/summary` | Latest net worth + allocation |
| GET | `/portfolio/snapshots` | Historical snapshots (date range) |
| GET | `/portfolio/holdings` | Current holdings with PnL |
| GET | `/analytics/pnl` | PnL data (daily/weekly/monthly) |
| GET | `/analytics/allocation` | Asset allocation breakdown |
| GET | `/analytics/exposure` | Currency exposure |
| GET | `/analytics/yield` | Blend yield tracking |
| GET | `/ai/commentary` | Latest AI commentary |
| POST | `/ai/commentary` | Generate new commentary |
| GET | `/ai/config` | Current AI provider config |
| PUT | `/ai/config` | Update AI provider |
| POST | `/report/notify` | Send push notification with weekly summary |
| GET | `/settings` | App settings |
| PUT | `/settings` | Update settings |

#### WebSocket

`ws://localhost:{port}/api/v1/ws`

Event types:
```json
{"event": "collection_started", "source": "okx-main"}
{"event": "collection_progress", "source": "okx-main", "pct": 50}
{"event": "collection_completed", "source": "okx-main", "assets": 12}
{"event": "collection_failed", "source": "okx-main", "error": "..."}
{"event": "snapshot_updated", "date": "2026-02-28"}
{"event": "commentary_ready", "date": "2026-02-28", "model": "llama3.1:8b"}
```

### Security

- **Local-only binding**: `127.0.0.1:{port}` — never exposed to network
- **No auth** initially (local daemon, single-user) — reconsider if remote access is ever needed
- Middleware rejects non-loopback requests

### Daemon Management

```
~/Library/LaunchAgents/com.lurii-finance.daemon.plist
```

```bash
pfm daemon start          # Load launchd plist
pfm daemon stop           # Unload
pfm daemon status         # Check if running
pfm daemon logs           # Tail daemon logs
```

Port: configurable, default `19274` (arbitrary, unlikely to conflict).

### CLI Preservation

All existing CLI commands (`pfm collect`, `pfm report`, etc.) continue to work. They become thin HTTP clients calling the daemon API when the daemon is running, or run inline (current behavior) when the daemon is not running.

---

## Phase 3 — SwiftUI App (macOS Native)

### Goal

Native macOS application providing visual dashboards, source management, and AI chat. Inspired by Rotki's layout.

### Tech Stack

| Component | Technology |
|-----------|-----------|
| UI framework | SwiftUI |
| Target | macOS 14+ (Sonoma) |
| HTTP client | URLSession |
| WebSocket | URLSessionWebSocketTask |
| Charts | Swift Charts |
| Keychain | Security framework |
| Crypto | CryptoKit |
| Distribution | Direct .app (or Homebrew cask later) |

### App Structure

```
LuriiFinance/
├── LuriiFinanceApp.swift              # App entry point
├── Models/
│   ├── Portfolio.swift       # Portfolio, Holding, Snapshot models
│   ├── Source.swift           # Source configuration models
│   ├── Analytics.swift        # PnL, Allocation, Exposure
│   └── Chat.swift             # Chat message models
├── Services/
│   ├── APIClient.swift        # REST client for Python backend
│   ├── WebSocketClient.swift  # WebSocket event handler
│   ├── KeychainService.swift  # Keychain read/write for DB key
│   └── DaemonManager.swift    # Start/stop/monitor Python daemon
├── Views/
│   ├── Sidebar/
│   │   └── SidebarView.swift  # Navigation sidebar (Rotki-style)
│   ├── Dashboard/
│   │   ├── DashboardView.swift    # Main overview
│   │   ├── NetWorthCard.swift     # Total net worth + trend
│   │   ├── AllocationChart.swift  # Pie/donut chart
│   │   └── PnLChart.swift         # Line chart (daily/weekly/monthly)
│   ├── Assets/
│   │   ├── HoldingsTable.swift    # Asset list with sorting/filtering
│   │   └── AssetDetail.swift      # Per-asset history + charts
│   ├── Sources/
│   │   ├── SourceList.swift       # Data source management
│   │   ├── AddSourceWizard.swift  # Source configuration wizard
│   │   └── CollectionStatus.swift # Live collection progress
│   ├── Analytics/
│   │   ├── PnLView.swift          # PnL breakdown
│   │   ├── AllocationView.swift   # Detailed allocation
│   │   ├── ExposureView.swift     # Currency exposure
│   │   └── YieldView.swift        # Blend yield tracking
│   ├── AI/
│   │   ├── ChatPanel.swift        # Chat interface (Phase 5)
│   │   ├── CommentaryView.swift   # Weekly commentary display
│   │   └── AISettingsView.swift   # Provider configuration
│   └── Settings/
│       ├── GeneralSettings.swift
│       ├── NotificationSettings.swift  # Push notification config
│       └── SecuritySettings.swift # Encryption config
└── Utilities/
    ├── Formatters.swift       # Currency, date, percentage formatters
    └── Theme.swift            # Colors, fonts, spacing constants
```

### Navigation (Rotki-inspired sidebar)

```
┌──────────────────────────────────────────────────────┐
│ 📊 Lurii Finance                                     │
├──────────┬───────────────────────────────────────────┤
│          │                                           │
│ Dashboard│  Net Worth: $XX,XXX         ▲ +2.3%      │
│          │  ┌─────────┐ ┌─────────────────────┐     │
│ Assets   │  │  Donut  │ │    PnL Line Chart    │     │
│          │  │  Chart  │ │                       │     │
│ Sources  │  └─────────┘ └─────────────────────┘     │
│          │                                           │
│ Analytics│  ┌─────────────────────────────────┐     │
│          │  │  Holdings Table                   │     │
│ AI Chat  │  │  Asset | Amount | Value | PnL    │     │
│          │  │  BTC     0.5    $32,500  +5.2%   │     │
│ Reports  │  │  ETH     12.0   $21,600  -1.1%   │     │
│          │  │  USDC    8,000   $8,000   0.0%   │     │
│ Settings │  │  ...                              │     │
│          │  └─────────────────────────────────┘     │
└──────────┴───────────────────────────────────────────┘
```

### Startup Flow

1. SwiftUI app launches
2. `DaemonManager` checks if Python daemon is running (PID file / health endpoint)
3. If not running, starts daemon via `launchctl`
4. `KeychainService` reads encryption key from Keychain (Phase 4)
5. Calls `POST /api/v1/unlock` with the key (Phase 4)
6. `WebSocketClient` connects to daemon
7. `APIClient` loads initial portfolio data
8. Dashboard renders

---

## Phase 4 — Encrypted SQLite (SQLCipher)

### Goal

Encrypt the SQLite database at rest. Key stored in macOS Keychain, passed to Python backend at startup.

### Components

#### Swift Side (Key Management)

```swift
// KeychainService.swift
struct KeychainService {
    static let account = "pfm-db-key"
    static let service = "com.lurii-finance.app"

    static func generateKey() -> Data {
        let key = SymmetricKey(size: .bits256)
        return key.withUnsafeBytes { Data($0) }
    }

    static func store(key: Data) throws { /* SecItemAdd */ }
    static func read() throws -> Data { /* SecItemCopyMatching */ }
    static func delete() throws { /* SecItemDelete */ }
}
```

#### Python Side (SQLCipher)

Replace `aiosqlite` with `pysqlcipher3` (API-compatible):

```python
# db/encrypted.py
import pysqlcipher3.dbapi2 as sqlcipher

async def open_encrypted_db(path: str, key: bytes) -> Connection:
    conn = sqlcipher.connect(path)
    conn.execute(f"PRAGMA key = \"x'{key.hex()}'\"")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn
```

#### Unlock Flow

```
SwiftUI app start
  → KeychainService.read() → 32-byte key
  → POST /api/v1/unlock {"key": "<hex-encoded>"}
  → Python daemon opens DB with SQLCipher PRAGMA key
  → Returns {"status": "unlocked"}
  → All endpoints become available
```

Pre-unlock state: daemon responds with `423 Locked` to all data endpoints.

### Migration Path

- New installs: SwiftUI generates key, stores in Keychain, creates encrypted DB
- Existing installs: one-time migration tool (`pfm db encrypt`) reads plain DB, creates encrypted copy, stores key in Keychain
- CLI-only users: key can be provided via env var `PFM_DB_KEY` for headless operation

### Dependencies

| Package | Purpose |
|---------|---------|
| `pysqlcipher3` | SQLCipher Python binding (replaces aiosqlite for encrypted path) |
| `sqlcipher` (brew) | SQLCipher library (macOS: `brew install sqlcipher`) |

---

## Phase 5 — Semantic Search + Chat

### Goal

Natural-language queries over financial data using embeddings. Dedicated chat panel in SwiftUI.

### Architecture

```
src/pfm/
├── embeddings/
│   ├── __init__.py
│   ├── base.py           # Abstract EmbeddingProvider protocol
│   ├── providers/
│   │   ├── ollama.py     # Local embeddings (nomic-embed-text)
│   │   └── openai.py     # API embeddings (text-embedding-3-small)
│   ├── indexer.py         # Converts snapshots/transactions → text → vectors
│   ├── store.py           # sqlite-vec read/write
│   └── search.py          # Query → embedding → vector search → results
├── chat/
│   ├── __init__.py
│   ├── engine.py          # Chat orchestrator: query → search → LLM → response
│   └── memory.py          # Conversation context management
```

### What Gets Embedded

| Data | Text Template | Frequency |
|------|--------------|-----------|
| Daily snapshot | `"On {date}, {source} held {amount} {asset} worth ${usd_value}"` | After each collect |
| Transaction | `"{date}: {type} {amount} {asset} on {source} (${usd_value})"` | After each collect |

### Embedding Provider Protocol

```python
class EmbeddingProvider(Protocol):
    name: str
    dimensions: int

    async def embed(self, texts: list[str]) -> list[list[float]]: ...
    async def embed_query(self, query: str) -> list[float]: ...
    async def close(self) -> None: ...
```

### Default Embedding Models

| Provider | Model | Dimensions |
|----------|-------|-----------|
| Ollama (default) | `nomic-embed-text` | 768 |
| OpenAI | `text-embedding-3-small` | 1536 |

### Vector Storage (sqlite-vec)

Same DB file, new virtual table:

```sql
CREATE VIRTUAL TABLE vec_items USING vec0(
    embedding float[768]
);

-- Metadata in regular table, joined by rowid
CREATE TABLE embedding_metadata (
    id INTEGER PRIMARY KEY,
    data_type TEXT NOT NULL,   -- 'snapshot' | 'transaction'
    source_id TEXT,
    date TEXT NOT NULL,
    asset TEXT,
    text TEXT NOT NULL,         -- Original text that was embedded
    created_at TEXT NOT NULL
);
```

### Chat Flow

```
User: "When did I have the most ETH?"
  │
  ├─ 1. Embed query via EmbeddingProvider
  ├─ 2. Vector search in sqlite-vec (top-K similar chunks)
  ├─ 3. Retrieve matching snapshot/transaction text
  ├─ 4. Build LLM prompt: system context + retrieved data + user question
  ├─ 5. Send to active LLM provider (from Phase 1)
  └─ 6. Return response to SwiftUI chat panel
```

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/chat` | Send chat message, get response |
| GET | `/api/v1/chat/history` | Conversation history |
| DELETE | `/api/v1/chat/history` | Clear conversation |
| POST | `/api/v1/embeddings/reindex` | Rebuild embedding index |
| GET | `/api/v1/embeddings/status` | Index stats (count, last updated) |

### Chat Settings

| Key | Type | Default |
|-----|------|---------|
| `embedding_provider` | enum | `ollama` |
| `embedding_model` | str | `nomic-embed-text` |
| `chat_context_window` | int | `10` (messages) |
| `search_top_k` | int | `20` |

### Example Queries & Expected Behavior

| Query | Search strategy |
|-------|----------------|
| "когда у меня было больше всего ETH?" | Vector search for ETH snapshots → sort by amount → LLM formats answer |
| "покажи периоды когда USDC на Blend < 1000" | Vector search for Blend + USDC → filter by value → LLM describes periods |
| "что происходило когда биткоин упал в январе?" | Vector search for BTC + January → correlate with other assets → LLM analysis |
| "сравни крипто сейчас и 3 месяца назад" | Vector search for recent + 3-months-ago snapshots → LLM comparison |

---

## Phase 6 — DefiLlama Yield Optimization

### Goal

Integrate [DefiLlama](https://defillama.com/) yield data to compare current portfolio positions against higher-yield alternatives, track historical APY/TVL, and provide actionable move-your-coins recommendations.

### Data Source

[DefiLlama Yields API](https://yields.llama.fi) — free, no auth required.

| Endpoint | URL | Data |
|----------|-----|------|
| All pools | `GET https://yields.llama.fi/pools` | 18,700+ pools: APY, TVL, chain, project, risk metrics |
| Pool history | `GET https://yields.llama.fi/chart/{pool_id}` | Daily APY + TVL history per pool |

Response fields per pool:
```
chain, project, symbol, tvlUsd, apy, apyBase, apyReward,
rewardTokens, underlyingTokens, stablecoin, ilRisk, exposure,
predictions {predictedClass, predictedProbability, binnedConfidence},
mu, sigma, apyMean30d, apyPct1D, apyPct7D, apyPct30D
```

### Architecture

```
src/pfm/
├── defi/
│   ├── __init__.py
│   ├── defillama.py       # DefiLlama API client (httpx async)
│   ├── yield_scanner.py   # Match portfolio assets → higher-yield pools
│   ├── risk_scorer.py     # TVL + history + IL risk → safety score
│   └── cache.py           # SQLite cache with TTL (same pattern as CoinGecko)
```

### SQLite Tables

```sql
-- Cached pool snapshots (TTL: 1 hour for fresh data)
CREATE TABLE defillama_pools (
    pool_id TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    project TEXT NOT NULL,
    symbol TEXT NOT NULL,
    tvl_usd REAL,
    apy REAL,
    apy_base REAL,
    apy_reward REAL,
    apy_mean_30d REAL,
    stablecoin INTEGER,
    il_risk TEXT,
    exposure TEXT,
    predicted_class TEXT,
    predicted_probability REAL,
    mu REAL,
    sigma REAL,
    fetched_at TEXT NOT NULL
);

-- Historical APY/TVL for tracked pools
CREATE TABLE defillama_pool_history (
    pool_id TEXT NOT NULL,
    date TEXT NOT NULL,
    apy REAL,
    apy_base REAL,
    tvl_usd REAL,
    PRIMARY KEY (pool_id, date)
);
```

### Yield Scanner Logic

For each asset in the user's portfolio:

1. **Find matching pools** — filter DefiLlama pools by `symbol` matching the held asset (e.g., USDC, XLM, ETH)
2. **Filter by chain compatibility** — prioritize chains the user already uses (Stellar, Ethereum, etc.), flag others as "requires bridge"
3. **Rank by risk-adjusted yield**:
   - `apy` (higher is better)
   - `tvlUsd` (higher = safer, minimum threshold e.g. $1M)
   - `sigma` (lower = more stable APY)
   - `predictions.predictedClass` (prefer "Stable/Up")
   - `ilRisk` (prefer "no")
   - `exposure` (prefer "single")
4. **Compare against current position** — user's Blend USDC at 8.71% vs alternatives
5. **Generate recommendations** with confidence score

### Risk Scoring

```python
def compute_safety_score(pool: Pool) -> float:
    """0.0 (risky) to 1.0 (safe)"""
    score = 0.0
    # TVL component (0-0.3): log scale, max at $100M+
    score += min(log10(pool.tvl_usd) / 8, 0.3) if pool.tvl_usd > 0 else 0
    # APY stability (0-0.25): lower sigma = more stable
    score += max(0.25 - pool.sigma * 0.05, 0) if pool.sigma else 0.1
    # IL risk (0-0.2)
    score += 0.2 if pool.il_risk == "no" else 0.05
    # Single exposure (0-0.15)
    score += 0.15 if pool.exposure == "single" else 0.05
    # Prediction (0-0.1)
    if pool.predicted_class == "Stable/Up":
        score += 0.1
    return min(score, 1.0)
```

### Recommendation Output

```python
@dataclass
class YieldRecommendation:
    held_asset: str              # e.g. "USDC"
    current_source: str          # e.g. "blend-main"
    current_apy: float           # e.g. 8.71
    alternatives: list[YieldAlternative]

@dataclass
class YieldAlternative:
    pool_id: str
    project: str                 # e.g. "aave-v3"
    chain: str                   # e.g. "Ethereum"
    apy: float                   # e.g. 12.5
    apy_delta: float             # e.g. +3.79
    tvl_usd: float
    safety_score: float          # 0.0-1.0
    requires_bridge: bool        # True if not on user's current chains
    apy_mean_30d: float
    predicted_trend: str         # "Stable/Up", "Down", etc.
```

### Integration Points

#### AI Commentary (Phase 1)
DefiLlama recommendations are included in the data fed to the LLM for weekly commentary:
```
Current yields:
- USDC on Blend (Stellar): 8.71% APY, TVL $11.5M

Higher-yield alternatives (safety score > 0.7):
- USDC on Aave v3 (Ethereum): 12.5% APY, TVL $2.1B, safety 0.92 [requires bridge]
- USDC on Morpho (Ethereum): 10.8% APY, TVL $850M, safety 0.88 [requires bridge]
```

The LLM then provides nuanced recommendations considering bridge costs, gas fees, and risk appetite.

#### SwiftUI Dashboard (Phase 3)
Dedicated "Yield Opportunities" section:

```
┌─────────────────────────────────────────────────────┐
│ Yield Opportunities                                  │
├─────────────────────────────────────────────────────┤
│                                                      │
│ USDC (currently 8.71% on Blend)                     │
│ ┌───────────────────────────────────────────────┐   │
│ │ ▲ +3.79%  Aave v3 (Ethereum)    12.50% APY   │   │
│ │           TVL $2.1B  Safety ████████░░ 0.92   │   │
│ │           ⚠ Requires bridge from Stellar       │   │
│ ├───────────────────────────────────────────────┤   │
│ │ ▲ +2.09%  Morpho (Ethereum)     10.80% APY   │   │
│ │           TVL $850M  Safety ████████░░ 0.88   │   │
│ │           ⚠ Requires bridge from Stellar       │   │
│ ├───────────────────────────────────────────────┤   │
│ │ ▲ +0.85%  Blend v2 (Stellar)     9.56% APY   │   │
│ │           TVL $69M   Safety ███████░░░ 0.79   │   │
│ │           ✓ Same chain                         │   │
│ └───────────────────────────────────────────────┘   │
│                                                      │
│ XLM (currently 0.12% on Blend)                      │
│ ┌───────────────────────────────────────────────┐   │
│ │ ▲ +4.88%  StellarX LP (Stellar)  5.00% APY   │   │
│ │           TVL $5M    Safety ██████░░░░ 0.65   │   │
│ │           ⚠ Impermanent loss risk              │   │
│ └───────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

#### Historical Yield Charts
Track APY trends for pools the user cares about:
- Daily APY line chart for current position vs top alternatives
- TVL trend overlay (declining TVL = warning signal)
- 30-day APY mean comparison

### API Endpoints (Phase 2 backend)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/yields/opportunities` | Yield recommendations for current holdings |
| GET | `/api/v1/yields/pools?symbol=USDC&chain=Stellar` | Browse pools with filters |
| GET | `/api/v1/yields/pool/{pool_id}/history` | Historical APY/TVL for a pool |
| POST | `/api/v1/yields/refresh` | Force refresh DefiLlama cache |
| GET | `/api/v1/yields/tracked` | Pools user is monitoring |
| POST | `/api/v1/yields/track/{pool_id}` | Add pool to watchlist |
| DELETE | `/api/v1/yields/track/{pool_id}` | Remove from watchlist |

### Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `defillama_cache_ttl` | int | `3600` | Cache TTL in seconds (1 hour) |
| `yield_min_tvl` | float | `1000000` | Minimum TVL to consider ($1M) |
| `yield_min_safety` | float | `0.6` | Minimum safety score to show |
| `yield_chains` | list | `["Stellar"]` | Preferred chains (no bridge warning) |
| `yield_assets` | list | `auto` | Assets to scan (auto = all held assets) |

---

## Cross-Cutting Concerns

### Dependencies (New)

| Package | Phase | Purpose |
|---------|-------|---------|
| `aiohttp` | 2 | HTTP server + WebSocket |
| `pysqlcipher3` | 4 | Encrypted SQLite |
| `sqlite-vec` | 5 | Vector storage |
| `httpx` | 1 | Already present; used for Ollama/OpenRouter/Grok/DefiLlama REST calls |

### macOS Requirements

| Requirement | Phase |
|-------------|-------|
| Xcode 15+ | 3 |
| macOS 14 (Sonoma)+ | 3 |
| `brew install sqlcipher` | 4 |
| Ollama installed | 1 (optional), 5 (for local embeddings) |

### Data Directory

```
~/Library/Application Support/Lurii Finance/
├── lurii.db            # Encrypted SQLite (Phase 4)
├── lurii.db.bak        # Pre-encryption backup (one-time)
├── daemon.pid          # Daemon PID file
└── logs/
    └── daemon.log      # Daemon logs
```

Migrate from current `data/pfm.db` to standard macOS Application Support path in Phase 2.

### Testing Strategy

- Phase 1: Unit tests for each provider (mock HTTP), integration test with Ollama if available
- Phase 2: aiohttp test client for API endpoints, WebSocket tests
- Phase 3: SwiftUI previews + XCTest for ViewModels
- Phase 4: Test encrypted DB open/close, migration from plain → encrypted
- Phase 5: Test embedding indexer, vector search accuracy, chat flow
- Phase 6: Mock DefiLlama responses, test yield scanner matching, risk scorer edge cases

---

## Resolved Questions

1. **Ollama model management** — Auto-pull required models (`ollama pull`) if not found locally. Seamless experience, user doesn't need manual setup.
2. **SwiftUI distribution** — Homebrew cask. Requires Apple Developer ID code signing (free). Standard macOS install/uninstall experience.
3. **Chat language** — Always English responses regardless of input query language. Simpler prompts, consistent output.
4. **Data migration** — Move DB from `data/pfm.db` to `~/Library/Application Support/Lurii Finance/` in Phase 2 (HTTP backend), since the daemon needs a stable, standard path.
5. **Telegram** — Replace with macOS native push notifications from the daemon. Drop Telegram bot dependency. Reporting module becomes `reporting/notifications.py` using `UserNotifications` framework via Python-objc bridge or a small Swift helper.
