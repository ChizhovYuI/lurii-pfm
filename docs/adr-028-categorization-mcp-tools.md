# ADR-028: Categorization Tools in MCP Server

**Status:** Accepted (Phases 1–6 done, including Phase 3.1 priority-aware dry-run fix)
**Date:** 2026-04-25

## Context

Transactions land in the database with a normalized `tx_type` and an
optional `metadata.category`. Type resolution and categorization are driven
by two rule tables (`type_rules`, `category_rules`) and a manual-override
overlay (`transaction_metadata`). Existing operators on rule fields:
`eq`, `contains`.

Bootstrapping rules for a new source (or fixing an "unknown" backlog) is a
multi-step loop:
1. Inspect transactions with `tx_type = unknown` or `category IS NULL`.
2. Read `raw_json` to find a discriminating field.
3. Author a rule, dry-run it to confirm scope, then save.
4. Re-run categorization to apply.

ADR-027 declared the MCP server **read-only**. That was correct for the
analytics tools, but it blocks the categorization workflow above. A
Claude Code skill that operates on the user's portfolio cannot iterate
without a tight write surface for *categorization metadata only*.

A second gap: rule matching supports only literal `eq` and `contains`.
Real-world bank descriptions need anchoring and grouping (e.g.
`^FX \d+`, `^(POS|ATM)\b`).

## Decision

Add a categorization tool surface to the MCP server, scoped to:

- Rule CRUD (`category_rules`, `type_rules`).
- Manual metadata writes (per-transaction category override).
- Re-run of the existing `run_categorization` pipeline.

Add a `regex` operator to the rule engine alongside `eq` and `contains`.

The MCP server's read-only contract from ADR-027 is **narrowed**, not
revoked: collection, source management, and prices remain read-only.
Writes are limited to user-authored categorization metadata, which the
user already controls via the SwiftUI app.

### Scope

```
Claude Code skill ──► pfm-mcp ──► metadata_store / repository
                                    │
                                    ├── category_rules     (write)
                                    ├── type_rules         (write)
                                    ├── transaction_meta   (write — category only)
                                    └── transactions       (read)
```

Out of scope: editing transaction amounts, dates, or `tx_type` outside
the existing type-resolver pipeline.

### Engine: `regex` operator (Phase 1 — done)

`_match_values(field_val, rule_val, "regex")` compiles `rule_val` (or
each element of a JSON-array) with `re.compile` and returns
`bool(pattern.search(field_val))`.

- **Case-sensitive by default.** Users opt in with the inline flag
  `(?i)`. Avoids the lossy `lower()` shortcut used by `contains`.
- **Compile cache.** `functools.lru_cache(maxsize=512)` on the compile
  function. Hot path stays cheap when the same rule scans thousands of
  transactions.
- **Runtime tolerance.** A rule with a malformed pattern silently
  fails to match — never raises. Avoids breaking the whole
  categorization pass over one bad rule.
- **Create-time validation.** `MetadataStore.create_category_rule` and
  `create_type_rule` call `_validate_regex_value(field_value)` and
  raise `ValueError` for malformed patterns. Surfaces problems
  before they reach the engine.

JSON-array values keep working: `["^ATM\\b", "^POS\\b"]` is treated as
"match any of these patterns". Same parsing as `eq` / `contains`.

### MCP tools (Phase 2–4 — proposed)

**Inspection**

| Tool | Purpose |
|------|---------|
| `list_category_rules(source?, include_deleted=False)` | List active rules, ordered by priority. |
| `list_type_rules(source?, include_deleted=False)` | Same for type rules. |
| `list_categories(tx_type?)` | Valid category values per `tx_type`. Skill needs this to author rules. |
| `categorization_summary(source?)` | Per-source counts: total, `unknown_type`, `no_category`, `internal_transfer`. |
| `get_rule_suggestions(min_evidence=2)` | Wraps existing `MetadataStore.get_category_suggestions` (learning from `user_category_choices`). |

**Discovery**

| Tool | Purpose |
|------|---------|
| `list_uncategorized_transactions(source?, missing_type=False, missing_category=False, limit=100, offset=0)` | Returns transactions plus parsed `raw_json` keys, value samples truncated to 200 chars. |
| `get_transaction_detail(transaction_id)` | Full transaction + metadata + parsed `raw_json` + currently-winning `rule_id`. |

**Mutation**

| Tool | Purpose |
|------|---------|
| `create_category_rule(...)` | Mirrors `MetadataStore.create_category_rule` signature. Validates regex. |
| `delete_category_rule(rule_id)` | Soft-delete for builtins, hard-delete otherwise. |
| `create_type_rule(...)` / `delete_type_rule(rule_id)` | Same shape, type-rule side. |
| `set_transaction_category(transaction_id, category)` | Manual override. Records to `user_category_choices` for `get_rule_suggestions`. |
| `link_transfer(tx_id_a, tx_id_b)` / `unlink_transfer(tx_id)` | Cross-source transfer pairing the auto-detector misses. |

**Dry-run (Phase 3)**

| Tool | Purpose |
|------|---------|
| `dry_run_category_rule(...)` | Same args as `create_category_rule` plus `scope_source`, `limit=200`. Evaluates over candidate transactions without saving. |
| `dry_run_type_rule(...)` | Same shape, type-rule side. |

Dry-run output:

```json
{
  "matched": 42,
  "unchanged": [{"tx_id": 1, "current_category": "fx"}],
  "changed":   [{"tx_id": 7, "current_category": null, "proposed_category": "fx"}],
  "overlapping_rules": [{"id": 12, "field_name": "description", "field_value": "FX", "result_category": "fx", "priority": 100}],
  "raw_field_samples": ["FX 1234.56 USD", "FX 99.00 EUR"]
}
```

`overlapping_rules` lists existing rules that already win for the same
matched transactions. Lets the skill drop duplicate rules during
auto-compaction passes.

**Apply**

| Tool | Purpose |
|------|---------|
| `apply_categorization(force=False)` | Wraps `run_categorization`. Skill must call after `create_*_rule` or the new rule has no observable effect on already-stored metadata. |

### Module structure

```
src/pfm/analytics/
  categorizer.py              # +regex operator (done)
  rule_dryrun.py              # new, Phase 3
src/pfm/db/
  metadata_store.py           # +get_categorization_summary, +get_uncategorized_transactions
src/pfm/mcp_server.py         # +AppContext.metadata_store, +tools
```

`AppContext` extended with `metadata_store: MetadataStore` reusing the
shared `aiosqlite.Connection` from `Repository`.

### Design decisions

**Why narrow the read-only contract instead of forking a "write" server?**
A second binary doubles the configuration burden on the user. The
write surface is small and bounded to categorization metadata. Risk
budget allows it.

**Why expose `apply_categorization` as a tool, not run it implicitly
on rule create?**
Implicit re-run hides cost. The skill can batch several rule edits
before paying for a single re-run. It also leaves the door open to a
narrower future tool (`recategorize_source(source)`).

**Why surface `overlapping_rules` in dry-run?**
The Claude Code skill's main failure mode is rule sprawl — the same
description gets categorized by multiple builtin and user rules. Naming
overlaps in the dry-run output makes deduplication a one-step decision
instead of a separate audit pass.

**Why keep `raw_json` in tool output despite the token cost?**
The skill cannot author rules without seeing field names and sample
values. Truncating values at 200 chars and emitting top-level keys
separately is the agreed budget.

**Why silent-fail malformed regex at runtime, but raise at create-time?**
Two different audiences. Create-time errors reach an interactive user
who can fix the pattern. Runtime errors would abort an entire
categorization pass over a single bad row. Silent-fail at runtime is
the conservative default.

### Implementation phases

1. ✅ **Engine — regex operator.** `categorizer.py:_match_values` +
   `_validate_regex_value` in `metadata_store.py`. Tests in
   `tests/test_type_rules.py` and `tests/test_categorizer.py`.
2. ✅ **Store helpers.** `get_categorization_summary(source_name?)` and
   `get_uncategorized_transactions(source_name?, missing_type,
   missing_category, limit, offset)` in `metadata_store.py`. Tests in
   `tests/test_metadata_store_helpers.py`. Default (both flags False)
   is OR-logic; both True is AND. Skill consumes the summary for the
   per-source dashboard and the paginated list for discovery passes.
3. ✅ **Dry-run module** (priority-aware as of Phase 3.1).
   `src/pfm/analytics/rule_dryrun.py` exposes
   `dry_run_category_rule(...)` and `dry_run_type_rule(...)`. Both
   accept `scope_source` and `limit` (default 200), reuse
   `_match_category_rule` / `match_type_rule` and `_validate_regex_value`
   without DB writes, and return `{matched, unchanged, changed,
   overlapping_rules, raw_field_samples}` per the schema above. Samples
   capped at 5 entries, deduped, truncated to 200 chars. Tests in
   `tests/test_rule_dryrun.py` (12 cases).
4. ✅ **MCP wiring.** `AppContext` extended with `metadata_store: MetadataStore`
   built once per lifespan from `repo.connection`. New `_ctx_store` helper.
   16 categorization tools registered in `src/pfm/mcp_server.py`:
   inspection (`list_category_rules`, `list_type_rules`, `list_categories`,
   `categorization_summary`, `get_rule_suggestions`), discovery
   (`list_uncategorized_transactions`, `get_transaction_detail`), mutation
   (`create_category_rule`, `delete_category_rule`, `create_type_rule`,
   `delete_type_rule`, `set_transaction_category`, `link_transfer`,
   `unlink_transfer`), dry-run (`dry_run_category_rule`,
   `dry_run_type_rule`) wiring `rule_dryrun`, and `apply_categorization`
   wrapping `run_categorization`. All return JSON via `_json`. Boolean args
   are keyword-only (FBT discipline). Mutation tools accept integer row
   ids; listing tools expose both `id` and `tx_id`.
5. ✅ **MCP smoke tests.** `tests/test_mcp_server.py` extended with
   `TestCategorizationTools` (16 tests) using a real `Repository` +
   `MetadataStore` over `tmp_path` SQLite. Covers happy paths, regex
   validation, not-found envelope, link/unlink round-trip, and dry-run
   wiring.
6. ✅ **Skill.** `categorization-curator` skill in `../lurii-portfolio`
   (sibling repo) drives the workflow end-to-end via MCP. Allow-list
   updated for the 17 new categorization tools.

### Phase 3.1 — priority-aware dry-run (post-Phase-6 fix)

Initial dry-run evaluated the candidate in isolation — `changed` listed
every match regardless of whether an existing higher-precedence rule
already wins for that tx. Concretely: a candidate with `priority=300`
(catch-all) matching 165 rows would appear to flip all 165, but real
apply only writes the subset that no higher-precedence existing rule
already covers.

Fix: simulate the engine. Sort `[*existing, candidate]` by
`priority ASC, id ASC` (candidate's missing id sentinels to last in
ties — matches reality, since the new rule gets the highest id at
create time). Run `categorize_transaction` / `_resolve_type_winner`
per matched tx. Output schema gains `shadowed_by_higher`:

```json
{
  "shadowed_by_higher": [
    {
      "tx_id": 7,
      "current_category": "fx",
      "winning_rule_id": 12,
      "winning_priority": 100,
      "winning_category": "fx"
    }
  ]
}
```

`changed` and `unchanged` now reflect the candidate's post-priority
production effect. `matched` count is unchanged (it remains the
isolation-match count — useful for sanity-checking pattern correctness).

### Phase 6.1 — surface winning type rule on `get_transaction_detail`

The Phase 4 wiring of `get_transaction_detail` returned only
`winning_rule_id` (the category-rule winner). Skill consumers asking
"why is this tx mapped to type X" had no first-class answer — they had
to iterate through type rules manually.

Fix: `get_transaction_detail` now returns two rule snapshots —
`winning_category_rule` (id, priority, field_name, field_value,
result_category) and `winning_type_rule` (id, priority, field_name,
field_value, result_type). Each is `null` when no rule applies
(e.g. `winning_type_rule` is `null` when the source already supplies
a concrete `tx_type` and no rule needs to fire). The legacy
`winning_rule_id` field is kept as a back-compat alias for
`winning_category_rule.id`.

Implementation: promoted `_resolve_type_winner` from `rule_dryrun.py`
to a public `resolve_type_winner` in `pfm.analytics.type_resolver`;
both `rule_dryrun` and `mcp_server` import it. Tests in
`tests/test_mcp_server.py::TestCategorizationTools` now assert the
two new fields.

### Phase 6.2 — validate_rule_args + JSON envelope on validation errors

Invalid-regex `ValueError` previously propagated as raw exceptions
out of `create_*_rule` and `dry_run_*_rule`, blocking the skill from
surfacing structured errors. All four tools now wrap the validation
site in `try/except ValueError` and return
`{"error": "validation", "message": ...}` instead of raising — same
envelope shape used elsewhere in the surface (`{"error": "not found"}`,
etc.).

A new `validate_rule_args(field_operator, field_value)` tool runs the
same regex compile pre-check without a DB scan. Skill should call it
before `dry_run_*_rule` when authoring a regex rule, so malformed
patterns short-circuit before paying for the dry-run query (~100 ms
on a 200-tx scope).

Schema-wise: callers that relied on the old `ValueError` path must
switch to checking `parsed.get("error") == "validation"`. Soft break —
only known caller is the skill, updated in lockstep.

### Schema impact

None. `field_operator` is plain `TEXT` — `"regex"` is a new accepted
value, not a new column. No migration.

## Consequences

**Positive:**
- Claude Code skill can iterate on rules without manual SQL.
- Regex unlocks anchored and grouped patterns (`^FX`, `^(POS|ATM)\b`)
  that `contains` cannot express cleanly.
- Dry-run + `overlapping_rules` makes auto-compaction passes safe.
- Compile cache keeps regex matching cheap on the hot path.

**Negative:**
- MCP server is no longer purely read-only (ADR-027). Mitigated by
  scoping writes to categorization metadata.
- `raw_json` exposure expands. Already exposed via `get_transactions`,
  but `list_uncategorized_transactions` widens the surface.
- Regex authored via MCP could be expensive (catastrophic
  backtracking). `re.compile` does not bound this. Acceptable
  short-term — single-user database, worst case is a slow
  `apply_categorization` call.
