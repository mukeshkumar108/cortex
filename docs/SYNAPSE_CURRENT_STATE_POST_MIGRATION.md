# Synapse Current State (Post-Migration)

Date audited: 2026-04-20
Audit basis: current repository code and schema (`src/`, `schema.sql`, tests)

## 1. What Synapse is now

Synapse is now a hybrid system with a canonical v2 truth substrate plus legacy-derived continuity/synthesis surfaces still in active use.

- Canonical layer (v2 truth substrate):
  - Evidence: `sessions_v2`, `turns_v2`
  - Semantic truth: `entities`, `entity_aliases`, `claims`, `claim_evidence`
  - Ordering/audit: `canonical_mutations`, `canonical_tenant_watermarks`
  - Extraction staging: `extract_results`, `claims_quarantine`
- Derived layer (continuity/synthesis/product UX):
  - `user_model`, `identity_profile`, `living_context`, `entity_profiles`, `open_threads`, `session_classifications`, and synthesis outputs (`/session/startbrief`, `/session/handover`, `/session/brief`)
- Retrieval layer:
  - Authoritative contract exists at `POST /v2/memory/query` with lane separation (`factual`, `episodic`, `continuity`, `hybrid`)
  - Legacy `POST /memory/query` is now an adapter over v2 service logic
- Rollout/observability/safety layer:
  - Shadow diffing (`retrieval_shadow_diffs`, `/internal/v2/retrieval-shadow/audit`)
  - Rollout controller and rollback evaluation (`retrieval_rollout_control`, `retrieval_rollout_events`, `/internal/v2/rollout/*`)
  - Continuous invariant detection/repair governance (`invariant_violations`, `invariant_repair_actions`, `/internal/v2/invariants/*`)

## 2. What is now canonical truth

### Canonical tables

Canonical truth tables currently implemented in `schema.sql`:

- `sessions_v2`
- `turns_v2`
- `entities`
- `entity_aliases`
- `claims`
- `claim_evidence`
- `canonical_mutations`
- `canonical_tenant_watermarks`

Supporting canonical pipeline tables:

- `extract_results` (pre-resolution durable extraction outputs)
- `claims_quarantine` (T4b containment)
- `predicate_policy_versions` / `predicate_policy`
- `turn_ingest_idempotency`

### Canonical write paths currently active

- Evidence ingest dual-write:
  - `SessionManager._dual_write_turn_v2(...)` in `src/session.py`
  - Triggered from ingest/session-ingest flows when `v2_dual_write_enabled` is on
- Durable extraction results:
  - `persist_extract_result(...)` in `src/extraction_results.py`
  - Invoked by post-ingest hook executor in `src/main.py` (`POST_INGEST_HOOK_EXTRACT_RESULTS`)
- Canonical mutation logging + watermark sequencing:
  - `CanonicalMutationLogger.append_mutation(...)` in `src/canonical_mutation_log.py`
  - Per-tenant monotonic sequence (`tenant_sequence`) and committed-only contract

### Lifecycle model

Claim lifecycle model is implemented in code and schema:

- `active`, `superseded`, `retracted`
- Resolver semantics implemented in `src/claim_resolution.py`:
  - same-slot equivalent claim -> reinforce existing active claim
  - exclusive/conflicting slot -> supersede prior active claim(s)
  - explicit retract candidate -> set matching active claim(s) to `retracted`

### Evidence model

Evidence-backed claims are enforced in retrieval and invariants:

- Claim evidence stored in `claim_evidence` with session/turn/span linkage
- `/v2/memory/query` factual lane drops rows without evidence links
- Invariants detect active claims lacking evidence (`factual_claim_missing_evidence`)

## 3. What is now derived/projection-only

### Current synthesis/projection surfaces

Active derived or mixed derived-first surfaces include:

- `/session/startbrief`
- `/session/handover`
- `/session/brief`
- `/entities/profile`
- continuity helper/query wrappers such as `_pg_search_nodes`, `_pg_search_continuity_facts`, `_pg_get_entity_role_hint`, `_pg_get_entity_continuity_facts`

### What remains derived-first

The following still prioritize derived/projection stores and then fall back:

- `session/startbrief`:
  - still heavily uses Graphiti session summaries and loop/user-model derived context
  - now includes canonical fallback signals and provenance payload
- `session/handover`:
  - uses derived tables (`living_context`, `open_threads`, `entity_profiles`, `identity_profile`) first
  - falls back to canonical claims/entities
- continuity wrappers:
  - still produce derived continuity/projection rows as primary continuity lane substrate

### Canonical fallback/provenance now present

- Startbrief and handover now emit canonical provenance with watermarks (`canonical_provenance` / `canonical_watermarks` metadata)
- Continuity helper paths include retrieval metadata fields that mark derived vs canonical signal origin
- Derived continuity remains explicitly tagged as derived in v2 continuity lane

## 4. What runtime endpoints are now active

### v2 endpoint(s)

- `POST /v2/memory/query`
  - authoritative lane-separated retrieval contract
  - rollout-gated; non-v2 cohorts receive explicit 409 (`V2_ROLLOUT_NOT_ENABLED_FOR_SCOPE`)

### Compatibility adapter endpoint(s)

- `POST /memory/query`
  - legacy response-shape compatibility adapter
  - routes through v2 service only (`_memory_query_v2_service`)
  - no legacy mixed-authority fallback

### Internal audit/rollout/invariants endpoints

- Shadow audit:
  - `GET /internal/v2/retrieval-shadow/audit`
- Rollout control:
  - `GET /internal/v2/rollout/state`
  - `POST /internal/v2/rollout/state`
  - `POST /internal/v2/rollout/evaluate`
  - `GET /internal/v2/rollout/events`
- Invariants:
  - `POST /internal/v2/invariants/run`
  - `GET /internal/v2/invariants/violations`
  - `GET /internal/v2/invariants/repairs`

### Legacy endpoints deprecated/disabled

- `POST /memory/search` -> disabled with `410`
- `POST /internal/debug/graphiti/query` -> disabled with `410`

## 5. What Graphiti-era components still remain in the repo

### Active Graphiti-era components

Still active in runtime paths:

- `src/graphiti_client.py` and Graphiti initialization during app startup
- Graphiti outbox + drain/session ingest job infrastructure (`graphiti_outbox`, `session.py`)
- Graphiti-backed summary and episodic operations used by startbrief/session flows
- Multiple internal debug Graphiti endpoints still available (episodes/session summaries metrics/lookups/views)

### Inactive/deactivated Graphiti-era retrieval components

- Old mixed-authority `memory_query` runtime branch has been removed (no reachable pre-v2 retrieval branch)
- `/internal/debug/graphiti/query` deprecated/disabled

### Deprecated/compatibility/history-only elements

- Legacy endpoint shape (`/memory/query`) retained as temporary compatibility adapter, but it is no longer Graphiti/mixed-authority retrieval logic
- Historical Graphiti-era docs and tests remain in repository for history/regression context

## 6. What the migration changed

### Old system vs current system (code reality)

Old pattern (pre-migration):

- mixed-authority retrieval paths
- factual and derived continuity content could be blended
- weaker canonical replay/audit substrate

Current pattern:

- v2 lane-separated retrieval contract implemented and enforced
- factual lane reads canonical claims and requires evidence links
- compatibility endpoint is v2-routed only
- canonical mutation log with explicit watermark semantics exists
- live shadow diffing + rollout controls + invariant governance are implemented

### Risks removed

- Silent fallback to legacy mixed-authority retrieval in production routing
- Factual results without evidence in v2 factual lane
- Undefined watermark semantics for canonical mutation ordering
- Lack of operational rollback criteria and observable regression signals

### New guarantees now present

- Per-tenant monotonic canonical watermark sequencing
- Committed-only canonical mutation read contract
- Deterministic extract run id generation for durable extraction results
- Explicit lane metadata (`derived`, `dataClassification`, lane labels)
- Rollout controller with threshold-based rollback and post-rollback integrity invocation

## 7. Known limitations / transitional areas

- Canonical resolvers (`EntityResolver`, `ClaimResolver`) are implemented and tested, but are primarily exercised via replay/audit tooling and tests; they are not yet fully wired as default live post-ingest claim/entity mutation path.
- Significant Graphiti-era operational and debug infrastructure is still active in session/startbrief ecosystem.
- Several synthesis surfaces remain derived-first and legacy-shaped, with canonical fallback/provenance rather than canonical-only input.
- Compatibility adapter (`/memory/query`) is still present by design for client/rollback safety.
- Legacy naming and docs remain in parts of the codebase (Graphiti-oriented terminology, debug routes, historical contracts).

## 8. Recommended next-phase backlog (post-migration, non-ticketed)

### Validation work

- Add an always-on production audit that periodically verifies `extract_results -> entity/claim resolution -> mutation log` end-to-end coverage by tenant.
- Add explicit SLO dashboards for:
  - factual evidence coverage
  - lane leakage checks
  - rollback trigger rates and false positives
- Expand golden-set replay corpus with scenario stratification (identity changes, contradictions, retractions, alias ambiguity).

### Product quality measurement

- Build systematic quality scorecards for `startbrief`, `handover`, and entity profile outputs:
  - continuity usefulness
  - factual grounding rate
  - contradiction incidence
- Add longitudinal parity tracking between served adapter outputs and direct v2 outputs by cohort.

### Optional future enhancements

- Wire canonical entity/claim resolution into live post-ingest pipeline with explicit feature gating and dry-run mode before serving dependency.
- Reduce Graphiti debug/ops surface to minimum required set once canonical path has sustained operational soak.
- Standardize provenance blocks across all synthesis endpoints (same schema, same watermark semantics).

### Possible graph/traversal reintroduction (derived index only)

- If graph traversal is reintroduced, constrain it to derived indexing/ranking roles only.
- Enforce policy: graph-derived text can guide continuity ranking but cannot become canonical factual authority without claim+evidence path.
- Require explicit source tags and lane constraints for any graph-derived retrieval contribution.
