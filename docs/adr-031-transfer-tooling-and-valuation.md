# ADR-031: Transfer tooling, transaction valuation backfill, and merchant-name matching

**Status:** Accepted (Phases 1–5 done)
**Date:** 2026-05-30

## Context

A real curation session against the ADR-028/029/030 tools (run from Claude
cowork against the live MCP server) surfaced one hard blocker and a cluster of
gaps:

1. **No general transaction list exposes row `id`s.** `link_transfer`,
   `unlink_transfer`, `set_transaction_category`, and `get_transaction_detail`
   all take the integer `id`, but only `list_uncategorized_transactions`
   (uncategorized rows) and `get_transaction_detail` (already needs the id)
   surfaced it. `get_transactions` returned no `id` and no metadata overlay, so
   every categorized row — every transfer — was unreachable except by guessing
   ids. The transfer sub-flow was effectively undriveable.

2. **No server-side transfer matcher.** `detect_transfer_pairs` existed but was
   only callable internally by the categorization runner. Matching cross-source
   pairs by hand does not scale.

3. **Asymmetric transfer links in live data.** Two rows had
   `transfer_pair_id` set on one side but `is_internal_transfer=0` /
   `transfer_pair_id=NULL` on the other. Root cause: Stage-2 categorization
   rebuilt `transaction_metadata` from scratch and the upsert nulled the
   transfer overlay; a second detection pass could also re-pair an
   already-linked row to a new partner, leaving one side stale.

4. **`usd_value=0` for 96% of transactions (1219/1269).** Most collectors defer
   pricing and store `0`. Without USD valuation there is no value-based transfer
   matching and no spend math on crypto.

5. **`categorization_summary` could not separate linked from unpaired
   transfers** — a single `internal_transfer` count.

6. **Noisy rule suggestions for free-form sources.** `get_rule_suggestions`
   falls back to the most common raw field; for kbank that is `channel` (the
   payment rail — "K PLUS" spans restaurants, top-ups, groceries), producing
   non-discriminating mappings. The merchant is buried in `details` behind a
   volatile reference code.

## Decision

Ship in five phases. New tools are MCP-only (consistent with the v0.23.3
MCP-only rule-management convention); `link_transfer` / `unlink_transfer` keep
their existing HTTP routes.

### Phase 1 — Transfer & listing tooling

- **`list_transactions`** — general listing over `get_transactions_paginated`.
  Every row carries its integer `id` plus the transfer/category overlay
  (`category`, `category_source`, `is_internal_transfer`, `transfer_pair_id`,
  `transfer_detected_by`). Filters: `source` (type), `source_name`, `tx_type`,
  `category`, `start`/`end`, `search`, `is_internal_transfer`, `has_pair`,
  `limit`/`offset`. Returns `{count, total, offset, limit, transactions}`. This
  is the unblock.
- **`get_transactions` enriched** — routed through the metadata-aware paginated
  path; now returns `id` + the same overlay (was metadata-blind).
- **`get_transactions_paginated` filters** — added `source_type`,
  `is_internal_transfer`, `has_pair` (filter-building extracted to
  `_build_paginated_filters` to keep complexity bounded).
- **`suggest_transfer_links`** — read-only cross-source matcher wrapping
  `detect_transfer_pairs` over unpaired rows (already-linked excluded). Returns
  candidate `(id_a, id_b, score)` with hydrated asset/amount/source/date. The
  scalable fix; feed accepted pairs to `bulk_link_transfers`.
- **`bulk_link_transfers([[a,b],…])`** — batched symmetric link
  (`link_transfers_batch`), mirror of `bulk_delete_*_rules`. The tool skips and
  reports three classes of bad input: malformed entries (not exactly two
  integers; booleans are rejected because `isinstance(True, int)` is `True`),
  ids that do not exist in `transactions` (one missing id would otherwise raise
  a FOREIGN KEY error and roll back the whole `executemany`), and an id reused
  by an earlier pair in the same batch (which would leave a one-sided link).
- **`link_transfer(dry_run=True)`** — before/after preview of both sides, plus
  an existence check per id, without writing (consistent with
  `dry_run_category_rule`).

### Phase 2 — Pairing integrity

- **Root-cause fix** in `categorization_runner`: transfer detection now runs
  over **unpaired transactions only** (no re-pairing of linked rows, preserves
  manual links); `_filter_uncategorized` **skips** `is_internal_transfer` rows
  even under `force`. Stage-2 category writes touch only the category columns;
  `upsert_metadata_batch` keeps its `ON CONFLICT` to
  `category`/`category_source`/`category_confidence`, so the existing overlay
  (`is_internal_transfer`, `transfer_pair_id`, `transfer_detected_by`,
  `type_override`, `reviewed`, `notes`) is preserved at the SQL layer — no
  manual field-copy is needed.
- **`repair_transfer_pairs`** (store method + MCP tool) — restores symmetry for
  one-sided links and clears orphans. Handles both broken states: a missing
  back-link (restore) and a flagged-but-unpaired row left by a partner delete
  (FK `transfer_pair_id` is `ON DELETE SET NULL`, `transaction_id` is
  `ON DELETE CASCADE`). The restore keeps **each side's own** detection source
  (a side with none inherits the claimer's), so a manual link is never
  downgraded to `auto`, and it sets `category_source` to match — a
  previously-categorized partner is not left with a stale source under the
  forced `category='transfer'`. Existence of claimed partners is resolved with
  one `existing_transaction_ids` query (no per-row lookup). Fixes the two live
  asymmetric rows.

### Phase 3 — Summary split

`get_categorization_summary` keeps `internal_transfer` (total) and adds
`transfer_linked` (`transfer_pair_id` set) and `transfer_unpaired` (flagged,
no pair — real work remaining).

### Phase 4 — Transaction USD valuation

- **`PricingService.get_price_usd_on(ticker, date)`** — historical price via
  CoinGecko `/coins/{id}/history` (crypto direct; fiat via the bitcoin bridge).
  Stablecoins/USD → 1; unknown → `None`. Persists real prices to the date-keyed
  `prices` cache so reruns never re-fetch the same `(asset, date)`. A definitive
  "no price" records a **time-limited miss sentinel** (`source='coingecko-miss'`,
  7-day retry window); a transient error is **not** recorded, so it retries.
  Cache rows are tagged by `source`: the historical read and the live-price read
  both filter `source='coingecko'` (and the live read pins `date=today`), so a
  historical or sentinel row can never be served as the current price.
- **`backfill_transaction_usd_values`** (`analytics/usd_value_backfill.py`) —
  values `usd_value=0` rows as `abs(amount) * price(asset, date)`, deduplicating
  lookups by `(asset, date)`. `limit`, `newest_first`, and `max_lookups`
  parameters (`max_lookups` bounds the number of distinct price lookups, and so
  the wall-clock, regardless of backlog).
- **`backfill_usd_values`** MCP tool (full oldest-first sweep, optional `limit`)
  and a **bounded post-collect forward-fill** in
  `run_parallel_pipeline._run_post_import` (newest-first,
  `_FORWARD_FILL_LIMIT=200` rows, `_FORWARD_FILL_MAX_LOOKUPS=20` distinct
  lookups, best-effort/non-fatal) so new imports get valued without blocking
  collection. The lookup budget caps added collect latency at ~45 s
  (≈ 20 × 2.1 s rate limit); the backlog drains across successive collects.

### Phase 5 — Merchant-name matching

- **`analytics/merchant.py:derive_merchant_name(source, raw)`** — deterministic
  per-source merchant token. kbank: strip a leading reference prefix
  ("Paid for Ref X6847 …", "Ref Code …") and embedded ref codes from `details`,
  revealing the (often Thai) merchant; wise: `merchant` → `payeeName` →
  `payerName`; crypto sources yield `None`. The embedded-ref pattern requires a
  leading letter (`[A-Z]{1,4}\d{3,}`), so an alphanumeric ref code is stripped
  but a bare digit run (a branch number, a year, "Store 365") is kept.
- **`merchant_name` virtual field** in the categorizer's `_resolve_field`, so a
  rule with `field_name="merchant_name"` matches against the derived token —
  closing the loop so a suggested merchant rule actually applies.
- **Suggestion bias**: `get_category_suggestions` injects the derived
  `merchant_name` into each choice snapshot, and `_find_common_field` prefers a
  discriminating `merchant_name` over noisy raw fields (e.g. `channel`). It
  skips non-string snapshot values, since a choice snapshot is the full
  `raw_json` whose nested objects are neither hashable nor matchable as a
  top-level rule field. Both record paths — the MCP `set_transaction_category`
  and the HTTP quick-category route — store the **source type** and the **full
  `raw_json`**, so the merchant token derived at suggestion time equals the one
  the categorizer derives at match time.

## Consequences

- Any transaction — categorized or transfer — is now reachable by `id`; the
  transfer linking, audit, and ad-hoc fix flows are driveable.
- Transfer pairing is symmetric by construction and self-healable; the two live
  asymmetric rows are repaired by running `repair_transfer_pairs`.
- A full first `backfill_usd_values` run is slow: ~1200 unvalued rows at the
  CoinGecko free-tier limit (~30 req/min) ≈ 40 minutes. Run with `limit` in
  batches, or let the post-collect forward-fill chip away. The backlog persists
  until a sweep is run.
- Stablecoins are valued at 1.0 historically (peg approximation); assets unknown
  to CoinGecko are left at 0 and counted under `no_price`. A miss sentinel stops
  re-fetching an unpriceable `(asset, date)` for 7 days, so the row stays at 0
  but no longer costs a request on every collect.
- A historical price is keyed by UTC calendar day (CoinGecko `/history` is UTC).
  A transaction whose local timestamp falls near a UTC day boundary can be
  valued against the adjacent day's price — accepted, since stored transaction
  dates carry no intraday time.
- Merchant derivation is heuristic and source-specific; sources without a
  merchant-bearing field gain nothing, but no suggestion regresses.

## Post-review hardening

A recall-mode code review of the change set found correctness and robustness
gaps. The fixes below shipped with the feature; they did not change the public
tool surface.

- **Live price cache could be poisoned by the backfill.** The backfill writes
  historical rows (old `date`, fresh `created_at`) into the same `prices` table
  the live cache reads. The live read ordered by `created_at` with no `date`
  filter, so a just-written historical price could be returned as the current
  price for the whole 1-hour TTL. Fix: the live read pins `date=today` and
  filters `source='coingecko'`.
- **Post-collect forward-fill could block collection for minutes.** A row cap
  alone did not bound wall-clock, because each distinct `(asset, date)` costs a
  serialized ~2.1 s CoinGecko request. Fix: `max_lookups` budget (20
  post-collect) caps the lookups, so the backlog drains across collects instead
  of stalling one.
- **Unpriceable rows were re-fetched on every collect.** A miss was not cached,
  so the same delisted/unknown `(asset, date)` hit CoinGecko on each forward-fill
  and never converged. Fix: the time-limited miss sentinel (see Phase 4).
- **`bulk_link_transfers` could abort a whole batch or create one-sided links.**
  One non-existent id raised a FOREIGN KEY error that rolled back every pair;
  booleans coerced to ids; an id reused across pairs left an asymmetric link.
  Fix: pre-filter on `existing_transaction_ids`, reject booleans, and drop a
  pair that reuses an already-claimed id (see Phase 1).
- **`repair_transfer_pairs` downgraded manual links.** The restore wrote the
  claimer's detection source onto both sides and left a stale `category_source`.
  Fix: per-side source preservation (see Phase 2).
- **Merchant ref-stripping deleted legitimate numbers.** `[A-Z]{0,4}\d{3,}`
  stripped any digit run; now requires a leading letter (see Phase 5).
- **Suggested merchant rules did not always match.** The HTTP record path stored
  a flattened field set and the source instance name, diverging from match time.
  Fix: both record paths store the source type and full `raw_json` (see Phase 5).

## Second-review hardening (xhigh recall pass)

A follow-up xhigh-recall review surfaced further correctness and robustness gaps;
all fixes shipped with tests and did not change the public tool surface.

- **`repair_transfer_pairs` could clobber a real categorized row.** A stale
  one-sided `transfer_pair_id` pointing at a genuine non-transfer transaction
  caused the restore branch to overwrite that row's category/type with transfer
  values. Fix: the repair now fetches the claimed partner's metadata and clears
  the stale claim instead of clobbering a row that carries a real categorization;
  the algorithm was also rewritten to a deterministic decide-then-apply pass so
  results no longer depend on row iteration order.
- **Miss sentinel and backfill rows leaked into valuation readers.** The
  `prices` readers `get_prices_by_date` / `get_price` had no `source` filter, so
  a `coingecko-miss` price `0` sentinel could be read as a real price (zeroing a
  unit price / short-circuiting `build_price_map`). Fix: both readers exclude
  `MISS_PRICE_SOURCE` (shared constant in `pfm.pricing.constants`).
- **Same-day backfill could still poison the live price.** A today-dated backfill
  wrote a `date=today, source='coingecko'` row that the live read accepted as the
  spot price. Fix: backfilled historical rows use a distinct
  `source='coingecko-history'`; the live read stays pinned to `source='coingecko'`
  so no historical write of any date can win it.
- **Bounded forward-fill could starve priceable rows.** The lookup budget was
  spent by free cache/miss hits and the loop `break`-ed on exhaustion, abandoning
  cache-hit rows; with `newest_first` a wall of permanent misses re-consumed the
  budget every collect. Fix: a no-network `peek_price_usd_on` resolves cache/miss
  hits for free, only genuine network calls count against `max_lookups`, and the
  loop `continue`s so cached rows past the budget still get valued.
- **Merchant ref-stripping over-stripped.** `_REF_PREFIX` lacked a word boundary
  (so "Reform" → "form") and `_STANDALONE_REF` ran case-insensitively (eating
  lowercase letter+digit tokens). Fix: `\b` after the ref keyword and a
  case-sensitive standalone-ref pattern.
- **A transfer's partner was orphaned on delete.** Deleting one side left the
  survivor flagged `is_internal_transfer=1` with a NULL pair (FK SET NULL). Fix:
  a `BEFORE DELETE` trigger on `transactions` clears the partner's transfer
  overlay before the FK fires (migration `l2m3n4o5p6q7`); `repair_transfer_pairs`
  remains for rows broken before the trigger existed.
- **Smaller fixes.** Self-pairs (`a == b`) are rejected by `link_transfer` /
  `bulk_link_transfers`; the `link_transfer` dry-run `after` projection now
  carries `reviewed`/`notes` (which a real link preserves); a non-object JSON
  `field_snapshot` no longer crashes suggestions; `get_review_queue` now shares
  the `_metadata_from_aliased_row` projection (`_ALIASED_TX_META_COLS`);
  `get_transactions` skips the discarded `COUNT(*)` (`include_total=False`);
  `suggest_transfer_links` accepts an optional date window and the post-collect
  scan pushes its `limit` into SQL.

## Files

- `src/pfm/mcp_server.py` — `list_transactions`, `suggest_transfer_links`,
  `bulk_link_transfers` (existence/bool/reuse filtering), `repair_transfer_pairs`,
  `backfill_usd_values` tools; `link_transfer` `dry_run`; `get_transactions`
  enrichment; `_transaction_row_dict`, `_link_after_dict`, `_is_int_id` helpers.
- `src/pfm/db/metadata_store.py` — `get_transactions_paginated` filters +
  `_build_paginated_filters`; shared `_metadata_from_aliased_row`;
  `link_transfers_batch`, `existing_transaction_ids`, `repair_transfer_pairs`
  (per-side source, batch existence), shared `_LINK_SIDE_SQL` /
  `_CLEAR_TRANSFER_SQL` / `_RESTORE_BACKLINK_SQL`; summary
  `transfer_linked`/`transfer_unpaired`; `merchant_name` injection +
  `_find_common_field` bias (skips non-string values).
- `src/pfm/db/repository.py` — `get_transactions_missing_usd_value`,
  `update_transaction_usd_values`.
- `src/pfm/analytics/categorization_runner.py` — unpaired-only detection,
  transfer-skip filter (overlay preserved by the upsert `ON CONFLICT`).
- `src/pfm/analytics/categorizer.py` — `merchant_name` virtual field.
- `src/pfm/analytics/merchant.py` (new) — `derive_merchant_name`.
- `src/pfm/analytics/usd_value_backfill.py` (new) — backfill job + `max_lookups`.
- `src/pfm/pricing/coingecko.py` — `get_price_usd_on`, miss sentinel + source
  pinning, shared `_write_price_row`, historical helpers.
- `src/pfm/collectors/pipeline.py` — post-collect forward-fill (lookup budget).
- `src/pfm/server/routes/transactions.py` — quick-category records source type +
  full `raw_json` (suggestion/match parity).
- Tests: `test_db.py`, `test_mcp_server.py`, `test_type_resolver.py`,
  `test_pricing.py`, `test_usd_value_backfill.py` (new), `test_merchant.py`
  (new). 880 pass, coverage 75.8%.
