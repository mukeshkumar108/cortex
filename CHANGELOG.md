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

### Validation Outcomes
- v1 eval pack (`10` cases): `improved=6`, `regressed=0`, `unchanged=4`
- v2 eval pack (`18` cases): `improved=6`, `regressed=0`, `unchanged=12`
- Embedding fallback/failed rates remained `0.0` in both packs.

### Database Operations
- Applied migrations successfully, including `025_tenant_alias_consolidation.sql`.
- Post-check: alias tenant `sophie-prod` rows reduced to `0` across consolidated tables.
