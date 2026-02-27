# Architecture Decision Records

Documenting dropped sources, rejected approaches, and other decisions that shaped the project.

---

## ADR-001: Drop Uphold integration

**Date:** 2026-02-27

**Status:** Accepted

**Context:** Uphold was originally included as source #5 (fiat-to-crypto bridge, GBP → USDC). The collector was fully implemented (`src/pfm/collectors/uphold.py`) using their REST API with a Personal Access Token (PAT).

**Problem:** Uphold requires registering an OAuth application to obtain API access. Personal Access Tokens are only available to approved developer applications — there is no self-service "read-only API key" flow like other exchanges provide. The application review process adds friction and may not be approved for personal use.

**Decision:** Remove Uphold as a data source entirely rather than maintain dead code waiting for access approval.

**Consequences:**
- Source count reduced from 10 to 9
- Money flow simplified: `Wise → GBP → USDC (Stellar) → Lobstr` (Uphold bridge step removed)
- Deleted: `src/pfm/collectors/uphold.py`, Uphold tests, config fields, env vars
- If Uphold access is obtained in the future, the collector can be restored from git history
