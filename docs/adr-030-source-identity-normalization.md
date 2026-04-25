# ADR-030 — Source identity normalization (source_id FK)

**Status:** Stages 1–3 shipped.
**Date:** 2026-04-25
**Supersedes parts of:** ADR-028 "Source filter semantics" addendum
(documents the matching rule unambiguously; this ADR replaces the
underlying schema so the addendum is no longer load-bearing).

## Context

`transactions` and `snapshots` carried two denormalized text columns
(`source` — coarse type, `source_name` — instance handle), neither
linked to `sources` by foreign key. Three concrete pains:

1. **Two parallel identifiers per physical source.** Live evidence:
   `coinex` had 22 rows under `source_name='coinex'` and 21 under
   `source_name='coinex-main'`, plus a duplicate `sources` row.
   Other splits (kbank/kbank-main, etc.) were historical drift from a
   collector default change.
2. **Rule matching ORs both columns.** `_match_category_rule` and
   `match_type_rule` matched against either `tx.source` or
   `tx.source_name`. Rules could not distinguish two accounts of the
   same type.
3. **Renames touch every data table.** No `rename_source` code path
   existed. `delete_source_cascade` was the only operator that knew
   about source identity at scale.

## Decision

Adopt **Option B** from the design doc: integer FK `source_id` on data
and rule tables, drop `source_name` text column long-term, keep `source`
as a denormalized cache of `sources.type` (renamed `source_type` in
stage 3). Rule tables get `source_type` + `source_id` (XOR check), with
a 6-tier auto-priority scheme.

Rejected: Option A (FK only, keep both text columns) was insufficient —
did not fix the rules problem. Option C (full asset/chain
normalization) deferred to a future ADR when Phase 5/6 (chat / yield
optimization) creates a concrete consumer.

## Staged migration

### Stage 1 — Foundation (this commit)

Two additive migrations and supporting code. Production read paths
unchanged. Backwards-compatible with every existing call site.

**Schema:**

```sql
-- Migration h8i9j0k1l2m3 — additive only
ALTER TABLE transactions          ADD COLUMN source_id INTEGER REFERENCES sources(id);
ALTER TABLE snapshots             ADD COLUMN source_id INTEGER REFERENCES sources(id);
ALTER TABLE user_category_choices ADD COLUMN source_id INTEGER REFERENCES sources(id);
ALTER TABLE category_rules        ADD COLUMN source_id INTEGER REFERENCES sources(id);
ALTER TABLE type_rules            ADD COLUMN source_id INTEGER REFERENCES sources(id);

CREATE INDEX idx_transactions_source_id          ON transactions(source_id);
CREATE INDEX idx_snapshots_source_id             ON snapshots(source_id);
CREATE INDEX idx_choices_source_id               ON user_category_choices(source_id);
CREATE INDEX idx_category_rules_source_id        ON category_rules(source_id);
CREATE INDEX idx_type_rules_source_id            ON type_rules(source_id);
```

**Migration i9j0k1l2m3n4 — backfill** for transactions, snapshots,
user_category_choices:

1. Exact-match pass: `source_id = sources.id WHERE sources.name =
   table.source_name`. Handles every modern row.
2. Fallback pass: when `source_name` does not match any `sources.name`
   and exactly one `sources` row exists for that type, link to that
   row. Handles historical rows pre-dating the `<source>-main` naming
   convention.

Rule tables stay with `source_id IS NULL` — engine semantics unchanged
in stage 1.

**Code:**

- `models.Snapshot.source_id`, `models.Transaction.source_id` —
  nullable `int | None` field.
- `Repository._resolve_source_id(name)` — cached lookup of `sources.id`
  by `sources.name` per Repository instance.
- `Repository.save_transaction(s)` and `save_snapshot(s)` populate
  `source_id` from the resolver on every insert. Rows whose
  `source_name` does not match a `sources` row get `source_id = NULL`
  (backwards compatible — no error).
- `Repository.rename_source(old, new)` — single entry point that
  updates `sources.name`, `transactions.source_name`,
  `snapshots.source_name`. The FK `source_id` is invariant under
  rename. In stage 3 this collapses to a single `UPDATE sources`.

**Coverage:** five new tests in `tests/test_db.py` covering insert
backfill, missing-source NULL, `rename_source` happy path, and the
backfill migration on legacy rows.

**Out of stage 1:** PRAGMA foreign_keys=ON enforcement, NOT NULL
constraints, source_name drop, rule semantics rewrite, coinex merge.

### Stage 2 — Read path migration (this commit)

Read paths now hydrate `source_name` from the `sources` table via JOIN,
falling back to the denormalized cache when `source_id IS NULL` (legacy
rows). All categorization-curator MCP surfaces additionally expose
`source_id` so the skill can pin rules to a specific account before
Stage 3 rules-rewrite lands.

**Code:**

- `Repository.list_sources_with_counts()` — returns `[{id, name, type,
  enabled, tx_count, snap_count}]` keyed by FK joins on `transactions` /
  `snapshots`. Surfaces via the new `list_sources` MCP tool.
- `Repository.delete_source_cascade(name)` — switched to `source_id`
  lookups internally. Signature unchanged. Legacy rows whose Stage 1
  backfill could not link (`source_id IS NULL`) are not removed by the
  cascade — orphans surface for a future cleanup tool.
- `MetadataStore.get_categorization_summary` — JOIN `sources` and group
  by canonical `s.name` (with COALESCE fallback). Adds `source_id` to
  every row.
- `MetadataStore.get_uncategorized_transactions` and
  `get_transaction_by_id` — JOIN `sources`, project
  `s.name AS canonical_source_name`. `Repository.row_to_transaction` and
  `_row_to_snapshot` prefer `canonical_source_name` when present.
- MCP tools `categorization_summary`, `list_uncategorized_transactions`,
  `get_transaction_detail` surface `source_id` additively.

**Coverage:** four new tests in `tests/test_db.py` covering
`list_sources_with_counts`, FK-based cascade after a `source_name`
drift, and canonical-name preference on `get_transaction_by_id`. Two
new tests in `tests/test_mcp_server.py` lock the `list_sources` tool
shape and the `source_id` surfacing on summary/list/detail.

**Out of stage 2:** rules rewrite, drop of `source_name`,
`PRAGMA foreign_keys=ON` (all stage 3).

### Stage 3 — Rules + cleanup (this commit)

Migration `j0k1l2m3n4o5_stage3_source_id_normalize` runs the destructive
schema rewrite as a single transaction:

1. **Pre-flight: orphan check.** Aborts when any data row still has
   `source_id IS NULL` after Stage 1's two-pass backfill. Operator
   either deletes the orphans or adds the missing `sources` row, then
   re-runs.
2. **Pre-flight: duplicate-type tx_id collision.** When two `sources`
   rows share a `type` (live: coinex 22+21 split), aborts if any
   `tx_id` collides between them — mirrors the `f2c7e6a9d1b4` KBank
   empties pattern. Resolve duplicates manually, then re-run.
3. **Source merge.** For each duplicate-type group, pick the canonical
   row (prefers `<type>-main`, else lowest id), repoint
   `transactions` / `snapshots` / `user_category_choices` / rule
   tables, delete the others.
4. **Rule rewrite.** Map every `category_rules.source` /
   `type_rules.source` text value into the new pair:
   - `"*"` → both NULL (catch-all)
   - matches `sources.name` → `source_id` populated
   - else → `source_type` populated (covers `sources.type` plus
     pre-source historical strings such as `"revolut"` / `"trading212"`)
5. **Drop `source_name`** from `transactions` and `snapshots`.
6. **Tighten `source_id` NOT NULL** on data tables.
7. **Swap dedup index** — `idx_transactions_source_name_tx_id_unique`
   (`source_name`, `tx_id`) → `idx_transactions_source_id_tx_id_unique`
   (`source_id`, `tx_id`).
8. **Drop the rule `source` text column** + add `CHECK (NOT
   (source_type IS NOT NULL AND source_id IS NOT NULL))` so the new
   pair stays mutually exclusive.

**Auto-priority** becomes 6-tier (`MetadataStore._auto_priority`):

| filter shape          | priority |
|-----------------------|----------|
| field + `source_id`   | 80       |
| field + `source_type` | 100      |
| field only            | 150      |
| `source_id` only      | 180      |
| `source_type` only    | 200      |
| catch-all             | 300      |

**Engine** (`categorizer._match_category_rule` /
`type_resolver.match_type_rule`) collapses to:

```python
if rule.source_id is not None and rule.source_id != tx.source_id:
    return False
if rule.source_type is not None and rule.source_type != tx.source:
    return False
```

**MCP surface** — `create_*_rule`, `dry_run_*_rule`, `list_*_rules`,
`audit_*_rules` all accept `source_type: str | None` and
`source_id: int | None` (XOR — passing both → validation envelope).
Legacy `source: str | None` stays as a kw-only deprecation alias —
`_resolve_legacy_source` rewrites a `sources.name` to `source_id` and
anything else to `source_type`. `list_sources` already shipped in
Stage 2.

**Repository** (`Repository`):

- `_ensure_source(type, name)` — auto-create a sources row if missing.
  Keeps tests / CLI stable now that `source_id NOT NULL` is enforced.
- `save_transactions` / `save_snapshots` resolve `source_id` via
  `_ensure_source`, drop the `source_name` column from the insert
  tuple, and (snapshots only) replace by `(date, source_id)` instead
  of `(date, source, source_name)`.
- `rename_source` collapses to a single `UPDATE sources` — data tables
  hydrate `source_name` from the JOIN.
- `delete_snapshots_by_source_names` / `get_snapshots_*` /
  `get_transactions` / `get_latest_transaction_date` / `_row_to_*` all
  switched to FK-resolved JOINs that project
  `s.name AS canonical_source_name`. The dataclass `source_name` is a
  read-only hydration field; nothing writes it.
- `__aenter__` enables `PRAGMA foreign_keys = ON`.

**Column-rename deferral.** ADR originally also called for renaming
`transactions.source` / `snapshots.source` columns to `source_type`.
This is purely cosmetic — the column already stores the source type
string — and would have churned ~150 read sites for no semantic gain,
so the rename was deferred. Python `Transaction.source` / `Snapshot.source`
keep their existing names; the matcher reads `tx.source` directly.

**Sibling skill** (`../lurii-portfolio/.claude/skills/categorization-curator/SKILL.md`)
ships in lock-step — drops the source-aliasing footnote, adds Step 1
`list_sources` parallel call and Step 2 account-vs-type guidance, and
hard-rule #1 now mentions the `source_type` / `source_id` pair.

**Coverage.** Two new tests in `tests/test_db.py` lock the Stage 3
migration: `test_init_db_reaches_stage3_head_and_drops_source_name`
(schema smoke check) and `test_stage3_merges_duplicate_type_sources`
(coinex-shaped duplicate-type merge with FK repoint). The full Stage
1+2 test set was rewritten against the new schema (canonical-name
hydration via JOIN, no `source_name` column, `_ensure_source` covers
the missing-source path). 976 tests pass.

## Risks / unknowns

- **Coinex merge.** Two `sources` rows (id=19 `coinex-main`, id=20
  `coinex`), 22+21 txs split. Stage 1 leaves both rows intact; both
  txs sets get correct `source_id` via exact-match. Stage 3 will pick a
  canonical row and merge — pre-flight tx_id collision check required.
- **SwiftUI app reads via HTTP backend.** Stage 1 read path unchanged,
  no impact. Stage 3 re-checks before destructive migration.
- **PRAGMA foreign_keys.** Decorative until stage 3. Inserting bad
  `source_id=999` would silently succeed in stage 1 — production code
  paths only set `source_id` from the resolver, so no in-tree path can
  trigger this.

## Out of scope

- Asset normalization (Option C from the design doc).
- Chain / network table.
- Promoting `transaction_metadata.category` to FK on
  `transaction_categories`.
- Soft-delete on `sources` (current cascade hard-deletes; orthogonal).
