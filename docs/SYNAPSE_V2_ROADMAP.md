# Synapse v2 Roadmap

Scope: execution plan derived from [SYNAPSE_V2_ARCHITECTURE.md](/opt/synapse/docs/SYNAPSE_V2_ARCHITECTURE.md).  
Standard: containment of legacy behavior is not completion.

## T0 — Canonicalization SDK and Shared Hashing/Normalization Rules
### 1. Description
Implement one shared library for normalization and key derivation (`claim_slot_key`, `claim_event_key`), timestamp normalization, and deterministic text canonicalization.

### 2. Why it exists
Without one canonical implementation, claim identity diverges across extraction/resolution/retrieval and replay is non-deterministic.

### 3. Dependencies
None.

### 4. Current status
Done.

### 5. Gaps
- None for T0 scope.

### 6. Acceptance criteria
- One module exports canonicalization primitives used by all current write/replay hashing paths. ✅
- Determinism test: same input + versions => identical keys/hashes across 1,000 randomized fixtures. ✅
- No duplicate local key derivation logic remains in current write path. ✅ (enforced by test; no `hashlib`/`uuid5` outside canonicalization module in `src/`).

### 7. Risks
- Silent key drift if any service bypasses SDK.
- Backfill mismatch with live writes.

### 8. Files/functions likely affected
- `src/utils.py`
- `src/main.py`
- new `src/canonicalization.py`
- resolver modules to be added in v2

---

## T1 — Stop Mixed-Authority Retrieval Behaviors
### 1. Description
Remove/disable retrieval behavior where derived prose is treated as factual authority.

### 2. Why it exists
Architecture requires factual lane = claims with evidence only.

### 3. Dependencies
None. This is a hard gate before all other work.

### 4. Current status
Done (containment).

### 5. Gaps
- No remaining mixed-authority factual injection in `/memory/query`.
- Legacy `/memory/search` behavior is disabled (HTTP 410); adapter-to-v2 remains deferred to T11/T10.

### 6. Acceptance criteria
- No code path in factual retrieval reads `user_model` or summary prose as fact candidates. ✅
- `/memory/search` is disabled or strict adapter to v2 with lane semantics. ✅ (disabled)
- Regression test asserts factual outputs are evidence-backed claim records only. ✅

### 7. Risks
- Recall quality drop if canonical claim lane not ready.
- Operators may re-enable legacy shortcuts during incidents.

### 8. Files/functions likely affected
- `src/main.py` (`memory_query`, `memory_search`, related helpers)
- `src/models.py` (response contracts)

---

## T2 — Add v2 Additive Schema and Indexes
### 1. Description
Create additive v2 canonical tables and indexes (`claims`, `claim_evidence`, canonical mutations, etc.) without destructive migration.

### 2. Why it exists
Current schema is legacy/mixed and not sufficient for canonical claim lifecycle model.

### 3. Dependencies
T0.

### 4. Current status
Done (additive schema implemented).

### 5. Gaps
- None for T2 schema scope.
- Operational requirement: database role must allow `pgcrypto` extension presence (`gen_random_uuid()` dependency).

### 6. Acceptance criteria
- Additive v2 migration exists and is runnable without destructive legacy changes. ✅
- Required PK/FK/unique constraints are present for tenant isolation, claim lifecycle support, and evidence linkage. ✅
- Indexes for factual lookup, episodic lookup, and projection lookup are present. ✅
- Post-audit hardening gaps (session/turn user integrity, entity merge lineage FK, policy-version anchoring, idempotency substrate) are closed. ✅

### 7. Risks
- Locking on large tables if constraints/indexes are not staged safely.
- FK validation stalls if backfill is not sequenced.

### 8. Files/functions likely affected
- `migrations/*.sql` (new v2 migrations)
- `schema.sql` (synchronized after migrations)
- `src/migrate.py`

---

## T3 — Dual-Write Evidence Ingest to v2 Surfaces
### 1. Description
Ingest pipeline writes canonical evidence (`sessions`, `turns`) to v2 while preserving existing path during rollout.

### 2. Why it exists
Evidence is canonical ground truth; must be established before extraction/resolution.

### 3. Dependencies
T2, T5.

### 4. Current status
Done (feature-flagged dual-write to v2 evidence surfaces).

### 5. Gaps
- T3b contract hardening (strict UTC/role/state validation and structured reject codes) remains pending.
- T4 extraction pipeline integration and parity telemetry thresholds remain pending.

### 6. Acceptance criteria
- Dual-write enabled by flag for ingest/session-ingest paths (`v2_dual_write_enabled`). ✅
- Idempotency test: duplicate ingest does not create duplicate turns in `turns_v2`. ✅
- Evidence writes remain append-only in v2 (`turns_v2` inserts only; idempotency via `turn_ingest_idempotency`). ✅
- Legacy ingest behavior preserved while dual-write is active. ✅

### 7. Risks
- Divergence between legacy and v2 evidence stores.
- Latency increase on ingest path.

### 8. Files/functions likely affected
- `src/ingestion.py`
- `src/session.py`
- `src/config.py`
- `tests/test_v2_dual_write_ingest.py`

---

## T3b — Evidence Contract Hardening
### 1. Description
Enforce strict schema and normalization rules on canonical evidence (`sessions`, `turns`) so downstream extraction/resolution consumes consistent inputs.

### 2. Why it exists
Canonical evidence quality determines all downstream correctness. If evidence is malformed or unordered, claim and replay correctness fail.

### 3. Dependencies
T2, T0.

### 4. Current status
Done (deterministic evidence contract enforced on v2 ingest writes).

### 5. Gaps
- T3b contract is enforced for v2 ingest/session-ingest runtime paths; broader legacy endpoint contract cleanup remains part of later deprecation scope (T15).

### 6. Acceptance criteria
- All ingested v2 timestamps are normalized to UTC. ✅
- Monotonic turn ordering is deterministic per `(tenant_id, user_id, session_id)` and out-of-order inserts are normalized deterministically. ✅
- Role/text/timestamp malformed turns are rejected fail-closed. ✅
- Session/user integrity mismatches are rejected to prevent cross-user/session contamination. ✅
- Invalid ingest payloads are rejected with structured error codes (`EVIDENCE_*`). ✅

### 7. Risks
- Existing traffic may trigger ingestion rejects due to latent data quality issues.
- Initial error-rate spike until clients conform to contract.

### 8. Files/functions likely affected
- `src/ingestion.py`
- `src/session.py`
- `src/main.py` (`/ingest`, `/session/ingest`)
- `tests/test_v2_dual_write_ingest.py`

---

## T4 — Durable Extraction-Results Pipeline
### 1. Description
Implement extraction jobs that persist candidate claims/results durably before resolution.

### 2. Why it exists
Extraction cannot be transient queue-only; must be auditable/replayable and versioned by policy/model.

### 3. Dependencies
T2, T3, T3b, T5.

### 4. Current status
Done (close-out validated; extraction-results tests now assert deterministic durable-write/no-claim contract without brittle outbox-pass assumptions).

### 5. Gaps
- Claim resolution consumption of extract results remains deferred to T7.

### 6. Acceptance criteria
- `extract_results` table is populated with structured candidates and structured failure rows. ✅
- Extraction persistence records model version, prompt version, and predicate policy version. ✅
- Missing/unknown policy version fails closed before persistence. ✅
- Extraction stage performs no claim or projection mutation. ✅
- Duplicate/retry behavior is deterministic via stable `extract_run_id` contract. ✅

### 7. Risks
- Queue replay without durable outputs causes silent data loss.
- Non-deterministic extraction config causes replay drift.

### 8. Files/functions likely affected
- `src/session.py` (post-ingest hook orchestration)
- `src/main.py` (post-ingest hook executor wiring)
- `src/extraction_results.py`
- `src/config.py`
- `tests/test_extract_results_pipeline.py`

---

## T4b — Quarantine Pipeline for Low-Confidence/Weakly Grounded Candidates
### 1. Description
Route weak candidates into quarantine store; prevent direct canonical claim promotion.

### 2. Why it exists
Backfill and live extraction noise must not pollute canonical claims.

### 3. Dependencies
T4, T5.

### 4. Current status
Partial (deterministic quarantine routing and persistence are implemented; review/promotion and ops tooling remain deferred).

### 5. Gaps
- Manual review/promotion tooling is intentionally deferred.
- Quarantine metrics dashboarding is deferred to later ops tickets.

### 6. Acceptance criteria
- Quarantine persistence captures tenant/user/session/extract-run linkage, candidate payload, reason, confidence, status, and timestamps. ✅
- Deterministic quarantine rules are implemented and tested for low confidence, weak grounding, malformed claim-candidate shape, and unsupported predicate/policy mismatch. ✅
- Quarantine routing is integrated into extraction-results persistence without claim/projection mutation. ✅
- Non-quarantined valid candidates still persist normally in `extract_results`. ✅
- No claim writes occur via quarantine path. ✅

### 7. Risks
- Quarantine backlog grows and blocks quality improvements.
- Manual promotion process becomes unbounded operational cost.

### 8. Files/functions likely affected
- `migrations/038_t4b_claims_quarantine_hardening.sql`
- `src/extraction_results.py`
- `tests/test_extract_results_pipeline.py`
- `tests/test_schema_migration.py`

---

## T5 — Predicate Policy Service + Versioning
### 1. Description
Implement versioned predicate policy (cardinality/conflict behavior/object equivalence requirements) and runtime enforcement.

### 2. Why it exists
Claim lifecycle correctness depends on explicit policy; replay requires version pinning.

### 3. Dependencies
T2, T0.

### 4. Current status
Done (policy service + versioned lookup/fail-closed semantics).

### 5. Gaps
- Extraction/resolver runtime integration is deferred to T4/T7 implementation work; T5 provides the enforceable lookup substrate and failure semantics.

### 6. Acceptance criteria
- Predicate policy persisted and versioned. ✅
- Runtime service enforces fail-closed policy lookup for unknown predicate/version. ✅
- Current version + explicit version request contract is implemented for downstream callers. ✅
- Tests cover known predicate lookup, unknown predicate failure, unknown version failure, and versioned behavior lookup. ✅

### 7. Risks
- Policy drift invalidates historical replay.
- Incorrect cardinality config causes truth conflicts.

### 8. Files/functions likely affected
- `src/predicate_policy.py`
- `migrations/037_predicate_policy_service_bootstrap.sql`
- `schema.sql`
- `tests/test_predicate_policy.py`

---

## T6 — Entity Resolution v2
### 1. Description
Build deterministic entity resolver with alias handling, ambiguity fail-closed, and merge lineage.

### 2. Why it exists
Claims depend on stable entity IDs; ambiguous alias auto-linking corrupts memory.

### 3. Dependencies
T0, T2, T4, T5.

### 4. Current status
Partial legacy behavior only.

### 5. Gaps
- Existing entity/profile paths are derived and mixed with retrieval.
- No canonical merge ledger and strict ambiguity handling in v2 contract.

### 6. Acceptance criteria
- Resolver outputs canonical entity IDs with confidence and ambiguity state.
- Ambiguous aliases do not auto-resolve.
- Merge operations are auditable and reversible.

### 7. Risks
- Entity collisions contaminate claims across relationships/topics.
- Over-merging creates irreversible semantic corruption.

### 8. Files/functions likely affected
- `src/main.py` (entity helpers)
- `src/graphiti_client.py` (legacy mapping boundaries)
- new entity resolver module + migrations

---

## T7 — Claim Resolution v2
### 1. Description
Implement deterministic claim lifecycle resolver using slot/event keys, policy rules, and evidence linkage.

### 2. Why it exists
Canonical semantic memory is claims; lifecycle errors directly corrupt factual recall.

### 3. Dependencies
T0, T2, T4, T5, T6.

### 4. Current status
Not implemented as v2 canonical resolver.

### 5. Gaps
- No canonical claim lifecycle table/logic in current runtime.
- No strict separation between extracted signals and finalized claims.

### 6. Acceptance criteria
- Resolver writes `active/superseded/retracted` deterministically.
- Every active claim has evidence spans.
- Exclusive predicate slots enforce single active claim invariant.

### 7. Risks
- Non-deterministic supersession under concurrency.
- Claims without evidence entering factual lane.

### 8. Files/functions likely affected
- new resolver module(s)
- `src/main.py` background job wiring
- migrations for claim tables and constraints

---

## T8 — Canonical Mutation Log + Watermarks
### 1. Description
Create canonical mutation log consumed by projections/retrieval via committed watermark semantics.

### 2. Why it exists
Projection freshness and replay correctness require ordered, committed canonical progression.

### 3. Dependencies
T2, T7.

### 4. Current status
Not implemented in v2 canonical form.

### 5. Gaps
- Existing async pipelines lack unified committed mutation watermark contract.
- Global vs per-tenant watermark semantics not fixed.

### 6. Acceptance criteria
- Mutation log exists and is written on every canonical claim/entity mutation.
- Watermark semantics are explicitly documented and tested.
- Projection builders consume committed watermarks only.

### 7. Risks
- Out-of-order or skipped watermarks causing stale or inconsistent projections.
- Ambiguous watermark semantics break multi-tenant rollout.

### 8. Files/functions likely affected
- migrations for mutation log
- resolver/projection job modules
- `src/main.py` orchestration logic

---

## T9a — Re-anchor Existing Synthesis to Canonical Layer
### 1. Description
Re-anchor existing synthesis outputs onto canonical v2 inputs so derived projections are built from canonical state only, versioned, and disposable.

### 2. Why it exists
Continuity data is necessary but must never become authority.

### 3. Dependencies
T2, T7, T8.

### 4. Current status
Legacy projection systems exist; v2 projection contract not implemented.

### 5. Gaps
- Existing projections (`user_model`, `living_context`, etc.) are mixed with retrieval authority.
- No strict v2 projection rebuild/watermark contract.

### 6. Acceptance criteria
- Projections include source watermarks and version.
- Rebuild-from-canonical produces same projection for same watermark.
- No projection table is queried for factual lane.
- Existing identity/living/thread/handover synthesis behavior is preserved during re-anchoring.
- No single-pass replacement of existing multi-pass synthesis is introduced.
- Re-anchored outputs meet or exceed current baseline quality.

### 7. Risks
- Stale projection serving continuity errors.
- Hidden projection-to-fact dependency reintroduced.

### 8. Files/functions likely affected
- `src/main.py` projection builders
- user model/living context modules
- migrations for snapshot/latest projection tables

---

## T9b — Optional Projection Rewrites (Only If Needed)
### 1. Description
Optional targeted projection/synthesis rewrites, only where re-anchoring cannot preserve quality or cannot operate correctly with canonical inputs.

### 2. Why it exists
Avoid unnecessary rewrites while allowing constrained intervention for proven quality or compatibility failures.

### 3. Dependencies
T9a, T12a.

### 4. Current status
Not started (deferred by default).

### 5. Gaps
- Rewrite scope, if any, is not yet justified by evidence.

### 6. Acceptance criteria
- Rewrite is explicitly justified by one of:
  - measured quality degradation vs baseline, or
  - demonstrated incompatibility with canonical inputs.
- Before/after quality comparison is documented on the same golden set used in T12a.
- Technical criteria from T9a remain satisfied (watermarks/versioning/factual-lane isolation).

### 7. Risks
- Unnecessary rewrites can degrade product continuity/tone quality.
- Local improvements can regress broader synthesis behavior.

### 8. Files/functions likely affected
- constrained subsets of `src/main.py` synthesis/projection paths
- selective synthesis modules only when justified
- evaluation harness artifacts in `tests/fixtures/` and `scripts/`

---

## T10 — `/v2/memory/query` Implementation
### 1. Description
Implement single retrieval contract with strict lanes: factual, episodic, continuity, hybrid.

### 2. Why it exists
Current retrieval is monolithic and mixed-authority.

### 3. Dependencies
T1, T7, T8, T9a.

### 4. Current status
Not implemented.

### 5. Gaps
- `/memory/query` mixes lane logic and policy heuristics in one path.
- No authoritative v2 lane contract in code.

### 6. Acceptance criteria
- `/v2/memory/query` exists and enforces lane separation.
- Factual lane rejects unsupported facts (no evidence => dropped).
- Response carries explicit source metadata for migration auditability.

### 7. Risks
- Hybrid mode may re-mix lanes if contracts are weak.
- Performance regression due to multi-lane assembly.

### 8. Files/functions likely affected
- `src/main.py` (new endpoint + helpers)
- `src/models.py` (v2 request/response models)
- episodic retrieval modules

---

## T11 — Legacy Client Compatibility Adapter Routed to v2 Only
### 1. Description
Keep legacy endpoint shape where required, but route retrieval logic exclusively to v2.

### 2. Why it exists
Support clients during migration without preserving mixed-authority engine.

### 3. Dependencies
T10.

### 4. Current status
Not implemented.

### 5. Gaps
- Legacy endpoints still execute legacy logic directly.

### 6. Acceptance criteria
- Legacy retrieval endpoints call v2 service internally.
- No adapter path can invoke old mixed-authority logic.
- Contract tests pass for legacy response compatibility.

### 7. Risks
- Silent fallback to old logic during incidents.
- Shape mismatch causing client regressions.

### 8. Files/functions likely affected
- `src/main.py` (legacy endpoint handlers)
- response mapping helpers

---

## T12a — Offline Replay, Diffing, and Audit Harness
### 1. Description
Build offline replay harness to compare deterministic v2 outputs over historical evidence sets.

### 2. Why it exists
Need pre-rollout confidence before serving v2.

### 3. Dependencies
T0, T5, T7, T8.

### 4. Current status
Not implemented.

### 5. Gaps
- No dedicated replay/diff harness tied to v2 claims lifecycle.

### 6. Acceptance criteria
- Replay job can run from evidence snapshots and produce stable claim-state hash.
- Diff reports include claim-level adds/removes/supersedes and evidence coverage deltas.
- Golden test set exists and is versioned for offline quality evaluation.
- Offline quality evaluation covers:
  - identity quality
  - living context quality
  - thread quality
  - handover usefulness
- T12a reports include before/after quality comparison for these product-baseline dimensions.

### 7. Risks
- Harness may diverge from production code path if not shared components.

### 8. Files/functions likely affected
- new replay tools in `scripts/` and `tests/fixtures/`
- shared resolver invocation wrappers

---

## T12b — Live Shadow-Read Diffing and Rollout Audit Dashboard
### 1. Description
Run v2 retrieval in shadow for live traffic, compare against current served outputs, and expose operational diffs.

### 2. Why it exists
Detect quality/latency regressions before cohort serve.

### 3. Dependencies
T10, T11, T12a.

### 4. Current status
Not implemented.

### 5. Gaps
- No production shadow diff dashboard with lane-specific metrics.

### 6. Acceptance criteria
- Shadow-read diff pipeline runs continuously for target tenants.
- Dashboard tracks evidence coverage, divergence, latency, and contradiction rates.
- Rollback thresholds wired to alerts.
- Live quality monitoring tracks:
  - continuity regressions
  - tone/interaction regressions
  - relationship-awareness degradation
- Quality alerts are incorporated into rollout stop/rollback controls.

### 7. Risks
- Diff noise if v1 and v2 contracts are not normalized before comparison.

### 8. Files/functions likely affected
- `src/main.py` request shadowing hooks
- telemetry/metrics modules
- ops dashboards configuration

---

## T13 — Continuous Invariants + Repair Jobs
### 1. Description
Implement scheduled invariant checks and repair workflows for violations.

### 2. Why it exists
Canonical memory correctness decays without continuous enforcement.

### 3. Dependencies
T7, T8, T9a.

### 4. Current status
Partial legacy checks exist; v2 invariant framework not implemented.

### 5. Gaps
- No v2 invariant job suite for claim/evidence/lane correctness.
- Repair governance (human vs auto) not encoded.

### 6. Acceptance criteria
- Continuous checks for documented invariants run and alert.
- Repair jobs implemented with explicit approval gates for semantic mutations.
- Audit logs for all repair actions.

### 7. Risks
- Over-aggressive auto-repairs mutate truth incorrectly.
- Silent invariant drift if checks are best-effort.

### 8. Files/functions likely affected
- new invariants/repair modules
- background scheduler in `src/main.py`
- ops scripts in `scripts/`

---

## T14 — Cohort Rollout + Rollback Controls
### 1. Description
Progressive rollout of v2 serve path with objective rollback triggers.

### 2. Why it exists
Need controlled cutover with fast containment for regressions.

### 3. Dependencies
T11, T12b, T13.

### 4. Current status
Not implemented.

### 5. Gaps
- No explicit cohort gating with lane-level SLO rollback automation.
- No explicit hard quality-parity rollout gate is defined.

### 6. Acceptance criteria
- Cohort progression plan enforced by feature flags.
- Automatic rollback on predefined thresholds.
- Post-rollback integrity checks run automatically.
- Hard rollout gate: cohort progression cannot proceed unless synthesis quality parity is demonstrated against baseline dimensions (identity/living/thread/handover).
- Rollback triggers include synthesis-quality regressions (not only technical/latency metrics).

### 7. Risks
- Manual rollout decisions delay rollback and increase blast radius.

### 8. Files/functions likely affected
- feature flag config paths
- `src/main.py` routing and gating
- ops runbook/dashboard alerts

---

## T15 — Legacy Graphiti-Era Deprecation and Cleanup
### 1. Description
Retire legacy mixed-authority code paths, endpoints, and stale schema/docs once v2 is stable.

### 2. Why it exists
Legacy paths are ongoing risk of authority regression.

### 3. Dependencies
T14 complete and stable for defined soak period.

### 4. Current status
Not implemented.

### 5. Gaps
- Graphiti-era retrieval/summary and debug surfaces still present.
- Schema/docs remain partially stale.

### 6. Acceptance criteria
- Legacy mixed-authority retrieval logic removed from production path.
- Deprecated endpoints disabled/removed with migration notice.
- Docs (`schema.sql`, architecture/runbooks/contracts) aligned with v2.
- Cleanup proceeds only after a 2–4 week soak period with no quality regressions.

### 7. Risks
- Premature cleanup can remove fallback during unstable rollout.
- Incomplete cleanup leaves hidden legacy re-entry points.

### 8. Files/functions likely affected
- `src/main.py`
- `src/graphiti_client.py`
- `src/briefing.py`
- `docs/*` (contracts/runbooks/schema references)

---

## Critical Path
Exact blocking sequence:
1. **T1**
2. **T0**
3. **T2**
4. **T5**
5. **T3**
6. **T3b**
7. **T4**
8. **T4b**
9. **T6**
10. **T7**
11. **T8**
12. **T12a**
13. **T9a**
14. **T10**
15. **T11**
16. **T12b**
17. **T13**
18. **T14**
19. **T15**

## Parallel Work
Can run independently once dependencies are met:
- **T2** and preparatory ops/dashboard scaffolding (non-serving).
- **T5** in parallel with early schema work.
- **T12a** can start once resolver contracts stabilize, before serve rollout.
- Portions of **T13** (check framework scaffolding) can start during T9a/T10.
- **T9b** is deferred by default and only activated if T12a evidence shows baseline-quality or compatibility failure.
- Documentation alignment portions of **T15** can start early, but destructive cleanup must wait.

## Immediate Next 3 Tickets
1. **T4b** — Quarantine pipeline for low-confidence/weakly grounded candidates (external test execution gate).
2. **T6** — Entity resolution v2.
3. **T7** — Claim resolution v2.

Rationale: T4b implementation is complete but must pass execution tests in a full runtime environment before closure. Then proceed on critical path to T6/T7.
