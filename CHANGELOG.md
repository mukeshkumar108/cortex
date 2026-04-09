# Changelog

## 2026-04-09

### Added
- Added migration `025_tenant_alias_consolidation.sql` to consolidate tenant alias data:
  - `sophie-prod` -> `default`
  - conflict-safe upsert rules for tenant-scoped tables
  - idempotent behavior for repeat runs
- Added expanded semantic rerank evaluation fixture pack:
  - `tests/fixtures/semantic_rerank_eval_pack.v2.json` (18 cases)
- Added diagnostics and schema docs:
  - `docs/SEMANTIC_RERANK_DIAGNOSTICS.md`
  - `docs/DERIVED_MODEL_AND_STEERING_SCHEMA.md`

### Changed
- Semantic memory reranking refined in runtime and evaluator:
  - query semantic interpretation used as primary query typing signal
  - sparse query-domain focus and low-certainty damping
  - intent-aware compatibility scoring for `make_commitment`/`reflect`
  - soft fallback tie-break only when semantic intent is generic
- Semantic classification confidence scoring updated to reduce compression and improve spread.
- `/session/startbrief` documentation aligned to current runtime response shape.
- README updated with tenant alias canonicalization and migration run instructions.
- Architecture decisions updated for tenant alias handling and validation policy.
- `/session/startbrief` now uses explicit precedence scoring for summary and loop selection:
  - score components: `recency`, `salience`, `importance`, `confidence`, `contradiction_penalty`
  - freshness skew tolerance prevents near-future timestamp drift from dropping newest summaries
  - contradiction handling demotes stale breakup claims when newer reconciliation evidence exists
  - loop ranking now uses the same scoring framework (instead of salience-only ordering)
- User-model narrative enrichment now runs post-LLM narrative sanitization:
  - drops stale relative-time drift unless currently evidenced
  - removes contradiction lines against newer relationship evidence
  - falls back to top scored evidence when generated narrative is over-filtered

### Validation Outcomes
- v1 eval pack (`10` cases): `improved=6`, `regressed=0`, `unchanged=4`
- v2 eval pack (`18` cases): `improved=6`, `regressed=0`, `unchanged=12`
- Embedding fallback/failed rates remained `0.0` in both packs.

### Database Operations
- Applied migrations successfully, including `025_tenant_alias_consolidation.sql`.
- Post-check: alias tenant `sophie-prod` rows reduced to `0` across consolidated tables.

### Added (Debugging and Traceability)
- New internal debugging endpoint:
  - `GET /internal/debug/startbrief/ranking`
  - returns full summary/loop candidate rankings (not just selected winners)
  - includes score components and definitions for root-cause analysis
- `/session/startbrief` response `evidence` now includes:
  - `claim_ranking`
  - `loop_ranking`
  - `claim_ranking_defs`
  - `loop_ranking_defs`
