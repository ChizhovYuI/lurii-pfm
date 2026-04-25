# ADR-027: MCP Server for Portfolio Data Access

**Status:** Proposed
**Date:** 2026-03-07

## Context

Lurii Finance collects portfolio data from 10 sources into a local SQLite database.
The SwiftUI app and CLI consume this data through the aiohttp HTTP backend.

AI assistants (Claude Desktop, Codex, ChatGPT, etc.) cannot access this data directly.
Users must copy-paste numbers or screenshots to discuss investments.

The **Model Context Protocol (MCP)** is an open standard that lets AI assistants
connect to local data sources through a structured tool/resource interface.

## Decision

Add an MCP server as a new entry point in the `pfm` package. The server reuses
existing `Repository`, analytics, and pricing code. It exposes portfolio data
as MCP **tools** (parameterised queries) and **resources** (current-state endpoints).

### Architecture

```
Claude Desktop / Codex / any MCP client
        |
        | stdio (local pipe)
        |
   pfm-mcp-server  (FastMCP, runs as subprocess)
        |
        | imports pfm.db, pfm.analytics, pfm.pricing
        |
   ~/Library/Application Support/Lurii Finance/lurii.db  (read-only)
```

- **Transport:** `stdio` (default for local AI assistants). Optional `streamable-http` flag for remote/testing.
- **Process model:** Spawned by the MCP client as a subprocess. No daemon dependency.
  The server opens the SQLite DB directly (read-only) — it does NOT call the HTTP backend.
- **Package:** `src/pfm/mcp_server.py` (single module). Entry point: `pfm-mcp` (console script).

### Lifespan

```python
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    db_path = get_db_path()          # ~/Library/Application Support/Lurii Finance/lurii.db
    repo = Repository(db_path)
    try:
        yield AppContext(repo=repo, db_path=db_path)
    finally:
        pass  # aiosqlite closes on GC; no explicit teardown needed
```

### Tools (10)

Tools accept parameters and run computations. Best for questions like
"What was my PnL last week?" or "Show transactions from Wise."

| Tool | Parameters | Returns | Use case |
|------|-----------|---------|----------|
| `get_portfolio_summary` | `date?: str` | Net worth, top 10 holdings, allocation by category, risk metrics, warnings | "How is my portfolio doing?" |
| `get_allocation` | `by: asset\|source\|category`, `date?: str` | Allocation breakdown with USD values and percentages | "Where is my money allocated?" |
| `get_currency_exposure` | `date?: str` | Currency breakdown (USD, GBP, THB, etc.) | "What is my currency risk?" |
| `get_risk_metrics` | `date?: str` | Concentration %, HHI index, top 5 assets | "How concentrated is my portfolio?" |
| `get_pnl` | `period: daily\|weekly\|monthly\|all_time`, `date?: str` | Start/end values, absolute/% change, top gainers/losers | "How did I perform this week?" |
| `get_transactions` | `source?: str`, `start?: str`, `end?: str`, `limit?: int` | Transaction list (date, source, type, asset, amount) | "Show my recent Wise withdrawals" |
| `get_snapshots` | `source?: str`, `start: str`, `end: str` | Historical balance snapshots | "What did my OKX account look like in January?" |
| `get_yield_positions` | `date?: str` | All earn/DeFi positions with APY and portfolio % | "What yield am I earning?" |
| `get_yield_history` | `source: str`, `asset: str`, `start: str`, `end: str` | Yield tracking for one position | "How has my Blend USDC yield performed?" |
| `get_sources` | — | List of configured sources (names, types, enabled status) | "Which accounts do I have connected?" |

**Parameter conventions:**
- All dates are ISO format strings (`YYYY-MM-DD`). Default: today.
- Results are JSON dicts/lists with string-formatted decimals (2dp for USD, 2dp for %).
- Tool descriptions include example questions to help AI assistants select the right tool.

### Resources (6)

Resources are parameterless current-state snapshots. MCP clients can list and
read them to get a quick overview without knowing which tool to call.

| URI | Description |
|-----|-------------|
| `lurii://portfolio/summary` | Net worth + top holdings + category allocation |
| `lurii://portfolio/allocation` | Full allocation by asset (all holdings) |
| `lurii://portfolio/risk` | Risk metrics + top concentrated assets |
| `lurii://portfolio/earn` | Active yield positions |
| `lurii://portfolio/transactions/recent` | Last 7 days of deposits/withdrawals/transfers |
| `lurii://sources` | Configured sources (no credentials) |

Resources return JSON text. They use the same compute functions as tools
but with default parameters (today's date, no filters).

### Prompts (2)

Pre-built prompt templates that AI assistants can use to start a conversation.

| Prompt | Parameters | Description |
|--------|-----------|-------------|
| `investment_review` | `focus?: str` | Fetches full analytics summary and asks for a comprehensive portfolio review. Optional focus area (e.g., "risk", "rebalancing", "yield"). |
| `weekly_check_in` | — | Fetches summary + PnL + recent transactions for a weekly review discussion. |

Prompts call the tools internally to gather data, then return a structured
user message with all the context embedded.

### Module Structure

```
src/pfm/mcp_server.py          # Single module: lifespan, tools, resources, prompts
```

```python
# Entry point in pyproject.toml
[project.scripts]
pfm-mcp = "pfm.mcp_server:main"

def main():
    mcp.run(transport="stdio")
```

### Client Configuration

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "lurii-finance": {
      "command": "/path/to/pfm-mcp"
    }
  }
}
```

**Claude Code** (`~/.claude/settings.json`):
```json
{
  "mcpServers": {
    "lurii-finance": {
      "command": "pfm-mcp"
    }
  }
}
```

### Implementation Plan

**Phase 1 — Core tools (MVP)**
1. Add `mcp` dependency to pyproject.toml (`mcp[cli]>=1.0`)
2. Create `src/pfm/mcp_server.py` with lifespan + 4 core tools:
   `get_portfolio_summary`, `get_allocation`, `get_pnl`, `get_transactions`
3. Add `pfm-mcp` console script entry point
4. Test with Claude Code MCP configuration

**Phase 2 — Full tools + resources**
5. Add remaining 6 tools: `get_currency_exposure`, `get_risk_metrics`,
   `get_yield_positions`, `get_yield_history`, `get_snapshots`, `get_sources`
6. Add 6 resources (parameterless current-state)
7. Add 2 prompts (`investment_review`, `weekly_check_in`)

**Phase 3 — Distribution**
8. Include `pfm-mcp` in Homebrew formula
9. Document client configuration in README

### Design Decisions

**Why stdio, not HTTP?**
MCP clients (Claude Desktop, Codex) spawn the server as a subprocess over stdio.
This is simpler, requires no port management, and works without the daemon running.

**Why bypass the HTTP backend?**
The MCP server opens SQLite directly (read-only). This avoids requiring the daemon
to be running and eliminates HTTP overhead. SQLite supports concurrent readers safely.

**Why a single module?**
The MCP server is a thin adapter layer. All business logic lives in `pfm.analytics`,
`pfm.db`, and `pfm.pricing`. A single file keeps it simple with ~200-300 lines.

**Why not expose write operations (collect, add source)?**
The MCP server is read-only by design. Collection and source management require
credentials and have side effects — these stay in the HTTP backend and SwiftUI app.

> **Update (ADR-028):** the read-only contract has been narrowed to allow
> writes to categorization metadata only (rule CRUD, manual category
> overrides). Collection, source management, and prices remain
> read-only. See `adr-028-categorization-mcp-tools.md`.

**Why string decimals instead of floats?**
Financial data must not lose precision. String-formatted decimals ("12345.67")
are safe for JSON serialization and prevent floating-point display artifacts.

## Consequences

**Positive:**
- Users can discuss portfolio data naturally with any MCP-compatible AI assistant
- No copy-pasting numbers — AI has direct read access to live portfolio data
- Reuses all existing analytics code with zero duplication
- Read-only: no risk of accidental data modification

**Negative:**
- New dependency (`mcp` package)
- Another entry point to maintain (though it is thin)
- SQLite direct access means MCP server sees raw data, not filtered by daemon logic
  (acceptable since both run locally for the same user)
