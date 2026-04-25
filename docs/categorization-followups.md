# Categorization Tools — Open Follow-ups

This file tracks open work captured during the first real curation
session against ADR-028 / ADR-029 tools. Most findings from that
session have already shipped (Phases 6.1–6.5 of ADR-028 and the full
ADR-029). What remains here is genuinely deferred — either because it
needs schema work, the cost-of-flexibility is high, or the pain has
not yet been validated against real usage.

## Open

_(none — ADR-030 is fully shipped as of Stage 3.)_

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
| Source identity normalization Stage 3 (coinex merge, drop `source_name`, `source_id` NOT NULL, rule `source_type`+`source_id` XOR with deprecation alias, 6-tier auto-priority, `PRAGMA foreign_keys=ON`) | ADR-030 Stage 3 |

## How to use this file

When something in the closed list breaks or behaves differently than
expected, that's a regression — open an issue against the relevant
ADR phase, don't reopen here. New deferred work goes under "Open"
with a one-paragraph context block plus options-and-tradeoffs (so
the next pass can pick up without re-deriving the analysis).
