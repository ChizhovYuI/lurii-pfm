# Categorization Tools — Open Follow-ups

This file tracks open work captured during the first real curation
session against ADR-028 / ADR-029 tools. Most findings from that
session have already shipped (Phases 6.1–6.5 of ADR-028 and the full
ADR-029). What remains here is genuinely deferred — either because it
needs schema work, the cost-of-flexibility is high, or the pain has
not yet been validated against real usage.

## Open

### MCP #10 — source naming canonicalization

**Symptom:** the database holds parallel source identifiers
(`kbank` / `kbank-main`, `coinex` / `coinex-main`,
`bitget_wallet` / `bitget-wallet-live`). Rules use exact-equality
matching against either `tx.source` or `tx.source_name`, so the
same conceptual source under two identifiers needs duplicate rules.
Skill cleanup passes look like dedup but really chase aliases.

**Options under consideration:**

1. New `list_sources()` MCP tool exposing canonical names plus a
   list of known aliases per source. Read-only; no schema change.
   Skill could surface aliases during the survey/discover passes.
2. Write-time normalization — store a canonical `source` on every
   transaction during ingest, derived from the configured source
   account. Requires a migration plus careful handling for sources
   already in the database.
3. Document aliases per integration in `docs/data-sources.md` and
   leave the database as-is. Cheapest; relies on the operator to
   know which identifiers are co-conspirators.

**Why deferred:** option (2) is a schema change (ADR-grade
decision). Option (1) is a small tool but only useful if option (2)
is not happening. Option (3) is what the skill already does
informally. Need a real second curation session to learn whether
the alias confusion is structural or one-off.

**See also:** ADR-028 "Source filter semantics" addendum
documents the matching rule unambiguously, which removes the
ambiguity at runtime even if naming stays inconsistent.

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

## How to use this file

When something in the closed list breaks or behaves differently than
expected, that's a regression — open an issue against the relevant
ADR phase, don't reopen here. New deferred work goes under "Open"
with a one-paragraph context block plus options-and-tradeoffs (so
the next pass can pick up without re-deriving the analysis).
