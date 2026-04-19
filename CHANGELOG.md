# Changelog

## 2026-04-11

### Added
- Pass 1 and episodic memory schema/extensions:
  - `migrations/034_pass1_tension_and_memory_delta_embedding.sql`
  - `session_classifications.tension_signal`
  - `session_classifications.memory_delta_embedding vector(1536)`
  - `idx_sc_embedding` ivfflat cosine index
- New runtime memory endpoints:
  - `GET /session/handover`
  - `POST /memory/search`
- New memory scripts:
  - `scripts/run_episodic_embeddings.py` (backfill + ongoing embedding support)
  - `scripts/run_identity_synthesis.py`
  - `scripts/run_living_context.py`
- New memory docs:
  - `docs/MEMORY_SYSTEM_OVERVIEW.md`
  - `docs/MEMORY_OPERATIONS_RUNBOOK.md`
  - `docs/MEMORY_API_REFERENCE.md`
  - `docs/MEMORY_DATA_MODEL.md`
  - `docs/agent/AGENT_MEMORY_RULES.md`
  - `docs/agent/AGENT_HANDOVER_USAGE.md`
  - `docs/agent/AGENT_RECOVERY_PLAYBOOK.md`

### Changed
- `scripts/run_pass1_memory_triage_batch.py`
  - prompt now requests `tension_signal` for subtle unspoken/avoidance cues
  - stores `tension_signal` in `session_classifications`
  - now embeds memory deltas for newly processed memory-worthy sessions
- `src/main.py`
  - added handover packet builder integrating living context, threads, people, clustered projects, identity anchors, and episodic recall
  - added episodic semantic search wiring
- `scripts/run_scoring_update.py`
  - lifecycle-aware thread salience/decay behavior
  - persistent-goal promotion and stale thread auto-snooze behavior

### Data/Backfill Notes
- Episodic embedding backfill completed for current memory-worthy backlog:
  - candidates: `91`
  - embedded: `91`
  - skipped_empty: `0`
  - skipped_failed: `0`

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
- Shared entity builder relationship handling tightened:
  - known relationship names are promoted to `person` with role when role evidence exists
  - role evidence now merges user-model relationships with recent summary relationship mentions
  - startbrief fallback `entity_hints` now preserve detected relationship role

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
- New ambient entity surfaces:
  - `entity_hints` added to `/session/startbrief` (compact startup grounding)
  - `POST /entities/profile` for deep-dive identity cards (person/project/company/place/other)
  - shared internal entity-candidate builder reused by both surfaces
  - `POST /internal/debug/entities/profile` for full entity profile diagnostics
