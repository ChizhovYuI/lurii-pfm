# Categorization Tools — Open Follow-ups

This file tracks open work captured during the first real curation
session against ADR-028 / ADR-029 tools. Most findings from that
session have already shipped (Phases 6.1–6.5 of ADR-028 and the full
ADR-029). What remains here is genuinely deferred — either because it
needs schema work, the cost-of-flexibility is high, or the pain has
not yet been validated against real usage.

## Open

### Source identity normalization — Stage 3

ADR-030 ships in stages. Stages 1 (additive `source_id` FK + backfill
+ rename helper) and 2 (read-path JOIN, `list_sources()`, FK-based
cascade, `source_id` on categorization MCP surfaces) are in. Stage 3
is deferred until the foundation has soaked through a real curation
session.

**Stage 3** — destructive: tx_id collision pre-flight on the coinex
merge (22+21 split), drop `source_name`, rename `source` →
`source_type`, swap dedup index, rule tables get
`source_type`+`source_id` (XOR), 6-tier auto-priority,
categorizer/type_resolver rewrite, MCP tool surface rename with
deprecation alias, skill rewrite, `PRAGMA foreign_keys=ON`.

**Why staged:** ~150 read sites of `tx.source_name` across src+tests,
plus a coordinated rename in the sibling `categorization-curator`
skill repo. Splitting reduces risk and lets the stage-1+2 surface
soak in production for a real curation session before destructive
migrations land.

**See also:** `docs/adr-030-source-identity-normalization.md` —
detailed design + stage breakdown. ADR-028 "Source filter
semantics" addendum stays load-bearing only until stage 3 lands.

## Closed (reference)

The following findings from the same feedback round shipped already;
listed here so the next session has a one-stop map of what was done.

| Item | Phase / ADR |
|---|---|
| Priority-aware dry-run (`shadowed_by_higher` bucket) | ADR-028 Phase 3.1 |
| `categorization-curator` skill | ADR-028 Phase 6 |
| Surface winning category & type rule on `get_transaction_detail` | ADR-028 Phase 6.1 |
| `validate_rule_args` + JSON envelope on validation errors | ADR-028 Phase 6.2 |
| `bulk_delete_*_rules` + skill batch-confirm | ADR-028 Phase 6.3 |
| `audit_*_rules` + dead-rule sub-flow | ADR-028 Phase 6.4 |
| `dry_run` `summary_only` flag | ADR-028 Phase 6.5 |
| Source filter & priority semantics docs | ADR-028 (clarifications) |
| Opt-in `raw_sample` on `list_uncategorized_transactions` | ADR-029 |
| Filter non-discriminating rule suggestions | ADR-029 |
| Source identity normalization Stage 1 (`source_id` FK foundation) | ADR-030 Stage 1 |
| Source identity normalization Stage 2 (read-path JOIN, `list_sources`, FK cascade, `source_id` on MCP tools) | ADR-030 Stage 2 |

## How to use this file

When something in the closed list breaks or behaves differently than
expected, that's a regression — open an issue against the relevant
ADR phase, don't reopen here. New deferred work goes under "Open"
with a one-paragraph context block plus options-and-tradeoffs (so
the next pass can pick up without re-deriving the analysis).
