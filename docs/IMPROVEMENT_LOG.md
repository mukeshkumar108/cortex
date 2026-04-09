# Improvement Log

Purpose: running implementation log for memory quality and retrieval/startbrief evolution.

## 2026-04-09

### Startup Contract and Surface Clarity
- Canonicalized backend startup to `GET /session/startbrief`.
- Marked `POST /brief` as internal minimal/fallback mode.
- Marked `GET /session/brief` as legacy/internal compatibility mode.
- Added backend call-rules doc:
  - `docs/MEMORY_SURFACES_AND_CALL_RULES.md`

### Freshness/Precedence Stabilization in Startbrief
- Fixed summary selection drift where fresh summaries could be dropped due to timestamp skew.
- Added summary claim scoring components:
  - `recency`, `salience`, `importance`, `confidence`, `contradiction_penalty`
- Added loop scoring components with the same policy shape.
- Updated selection logic to rank by scored precedence instead of salience-only heuristics.
- Added contradiction demotion for stale breakup framing when newer reconciliation evidence exists.

### Narrative Hygiene
- Startbrief runtime narrative now prefers filtered, evidence-aligned current narrative.
- User-model enrichment narrative pass now sanitizes:
  - stale relative-time drift
  - contradiction lines against newer relationship evidence
  - fallback to top-scored evidence when needed

### Debugging/Observability
- Added `GET /internal/debug/startbrief/ranking`:
  - returns full summary and loop candidate rankings with score components
  - includes selected winners and scoring definitions
- Added scoring evidence in `/session/startbrief` response:
  - `evidence.claim_ranking`
  - `evidence.loop_ranking`
  - `evidence.claim_ranking_defs`
  - `evidence.loop_ranking_defs`

### Ambient Entity Grounding (v1)
- Added `entity_hints` to `/session/startbrief`:
  - compact fields only (`entityId`, `name`, `type`, `role`, `importance`, `salience`, `lastSeenAt`)
  - max 10 hints
- Added `POST /entities/profile`:
  - request by `entityId` or `name`
  - returns compact entity card + key facts + optional relevant loops + provenance
- Introduced shared internal entity candidate builder:
  - reused by both `/session/startbrief` and `/entities/profile`
  - uses existing Graphiti/user_model/loops surfaces (no new storage path)

### Why this matters
- Prevents stale memories from overriding fresh corrections.
- Makes precedence decisions inspectable and testable.
- Reduces “memory feels wrong/outdated” failures without lexical patching.
