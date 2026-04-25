# ADR-029: Opt-in raw_sample + non-discriminating suggestion filter

**Status:** Accepted
**Date:** 2026-04-25

## Context

ADR-028 shipped the categorization MCP tools. Two payload-level
problems surfaced after the first real curation session:

1. `list_uncategorized_transactions(limit=100)` returned ~84 KB of
   JSON — large enough to crowd out other tool calls in the same
   context window. Bulk of the size came from `raw_sample` (each
   `raw_json` value truncated to 200 chars across all keys). A skill
   pass that only needs `raw_keys` for a quick survey paid the full
   cost regardless.

2. `get_rule_suggestions(min_evidence=2)` returned ~32 entries for a
   typical session, of which ~20 were noise: the same
   `(source, field_name, field_value)` tuple appeared multiple times
   under different `result_category` values
   (`_balance_direction == decrease → dining (149 ev)`,
   `→ groceries (137 ev)`, `→ shopping (19 ev)`). The field is not
   predictive — converting any of those to a rule would be wrong.

Both gaps showed up in the categorization-curator skill workflow
(ADR-028 Phase 6) and forced expensive workarounds (`jq` over a
spilled tool-output file in case 1; manual eyeballing in case 2).

## Decision

**1. Make `raw_sample` opt-in.** Add
`include_raw_sample: bool = False` to
`list_uncategorized_transactions`. Default response keeps `raw_keys`
(cheap, sufficient for survey) and drops `raw_sample`. Pass the flag
to opt in for pattern discovery.

**2. Filter non-discriminating suggestions by default.**
`MetadataStore.get_category_suggestions` post-processes the suggestion
list: groups by `(source, field_name, field_value)`; if a group has
more than one distinct `result_category`, the field is non-predictive
and all members are dropped. New
`include_non_discriminating: bool = False` flag surfaces them flagged
with `non_discriminating: true` and `conflicting_categories: [...]`
for transparency.

The MCP tool `get_rule_suggestions` exposes the same flag.

### Why opt-in, not a separate tool?

Skill workflow already calls `list_uncategorized_transactions` twice
(survey + discovery). A second tool name (`list_uncategorized_keys_only`)
splits the tool surface for a single boolean — cost-of-flexibility
losing battle. Boolean param keeps the surface flat.

### Why filter by default and offer opt-in, not the reverse?

Default-show would push the noise filter into every caller. The skill
already has to pre-filter today; the server is the right place. Opt-in
exposes the suppressed entries for explicit audit/debug — matches the
posture of `include_deleted` on rule listings.

### Why not just lower the truncation length?

Trimming `raw_sample` values from 200 → 50 chars is lossy without a
clear gain: discovery still wants full snippets when it runs, and
survey pass doesn't want any. The clean split is by call, not by
length.

## Schema impact

None. New parameters are additive; existing callers unchanged. The
suggestion filter changes default output shape for one tool —
acceptable since the only known consumer (categorization-curator
skill) is co-shipped.

## Implementation

| Change | File |
|---|---|
| `_uncategorized_item_dict` accepts `include_raw_sample` keyword | `src/pfm/mcp_server.py` |
| `list_uncategorized_transactions` MCP tool gets the flag | `src/pfm/mcp_server.py` |
| `MetadataStore.get_category_suggestions` post-filters via `_annotate_non_discriminating` helper | `src/pfm/db/metadata_store.py` |
| `get_rule_suggestions` MCP tool gets the flag | `src/pfm/mcp_server.py` |
| Tests | `tests/test_mcp_server.py` (split into `_default_keys_only` + `_with_raw_sample`); `tests/test_metadata_store_helpers.py` (new `TestSuggestionFilter`) |

Skill `categorization-curator` updated in same pass:
- Step 1 (Survey) — drop the manual filter advice (server now suppresses).
- Step 2 (Discover) — note default is keys-only; pass
  `include_raw_sample=True` for the discovery pass on a narrowed limit.

## Consequences

**Positive:**
- Survey pass payload drops by ~10× for 100-tx pages.
- `get_rule_suggestions` signal/noise improves on every call.
- No surface widening.

**Negative:**
- Default change to `list_uncategorized_transactions` is a soft
  break — any caller that read `raw_sample` without opting in needs
  to add the flag. Only known caller is the skill, updated in lockstep.

## Alternatives considered

- **Pagination of `dry_run.changed`** (separate finding from same
  feedback). Deferred — different problem, different fix.
- **Server-side hard suppression with no opt-in.** Rejected — losing
  the audit path makes it hard to investigate why a suggestion is
  missing.
