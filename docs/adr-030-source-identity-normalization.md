# ADR-030 — Source identity normalization (source_id FK)

**Status:** Stage 1 shipped. Stages 2–3 deferred.
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

### Stage 2 — Read path migration (deferred)

- Hydrate `source_name` via JOIN on `sources` rather than reading the
  cached column.
- Add `list_sources()` MCP tool exposing `(id, name, type, tx_count,
  snap_count)` for skill survey.
- Convert `delete_source_cascade` to FK-based.
- Update `categorization_summary`, `list_uncategorized_transactions`,
  `get_transaction_detail` to surface `source_id` (additive).

### Stage 3 — Rules + cleanup (deferred)

- Pre-flight: tx_id collision check on the coinex source merge (mirror
  the `f2c7e6a9d1b4` KBank empties pattern).
- Migration 3 (destructive): tighten `source_id NOT NULL`, drop
  `source_name`, rename `source` → `source_type`, swap dedup index
  `(source_name, tx_id) → (source_id, tx_id)`, drop rule `source` text
  column.
- Rules: `source_type` + `source_id` columns with XOR check.
- Auto-priority becomes 6-tier:
  - field + account = 80
  - field + type    = 100
  - field only      = 150
  - account only    = 180
  - type only       = 200
  - catch-all       = 300
- Categorizer / type_resolver simplify to `if rule.source_id is not
  None and rule.source_id != tx.source_id: return False; if
  rule.source_type is not None and rule.source_type !=
  tx.source_type: return False`.
- MCP `create_*_rule`, `dry_run_*_rule`, `list_*_rules` accept
  `source_id` and `source_type`; legacy `source` arg becomes a
  deprecation alias.
- `categorization-curator` skill (`../lurii-portfolio`) ships in lock
  step — drops the source-aliasing footnote, replaces with
  account-vs-type guidance.
- PRAGMA foreign_keys=ON enabled in Repository.__aenter__.

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
