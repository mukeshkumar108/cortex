# Synapse v2 Migration Changelog

Purpose: factual, append-only progress log for Synapse v2 migration execution.

Scope: tickets defined in [SYNAPSE_V2_ROADMAP.md](/opt/synapse/docs/SYNAPSE_V2_ROADMAP.md).

## Entry Rules
- Append entries in reverse-chronological order (newest first).
- Record only implemented changes observed in code, migrations, tests, or config.
- Keep each field concise and specific.
- Do not include forward-looking plans or speculation.
- Ticket status must be exactly one of: `done`, `partially done`, `blocked`.

## Entry Template
```md
### YYYY-MM-DD HH:MM UTC — <TICKET_ID>
- Summary of what changed:
  - <bullet>
- Files changed:
  - `<path>`
- Tests added/updated:
  - `<test path>`
- Acceptance criteria satisfied:
  - <criterion now met>
- Known remaining gaps:
  - <gap still open>
- Status: <done|partially done|blocked>
```

## Entries

### 2026-04-19 17:38 UTC — T4b
- Summary of what changed:
  - Added deterministic quarantine routing in `persist_extract_result` so weak claim candidates are partitioned into quarantine instead of flowing forward as normal extraction candidates.
  - Implemented explicit, testable quarantine rules: `low_confidence`, `weak_grounding`, `malformed_candidate`, `unsupported_predicate`, and `policy_mismatch`.
  - Persisted quarantined candidates into `claims_quarantine` with run/session linkage, reason/reason-code, confidence/grounding fields, status, and metadata.
  - Preserved extraction stage safety: no claim writes and no retrieval/synthesis behavior changes.
  - Added additive migration to harden `claims_quarantine` contract and indexes for review/audit scans.
- Files changed:
  - `migrations/038_t4b_claims_quarantine_hardening.sql`
  - `src/extraction_results.py`
  - `tests/test_extract_results_pipeline.py`
  - `tests/test_schema_migration.py`
  - `schema.sql`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_extract_results_pipeline.py`:
    - low-confidence candidate is quarantined with required metadata
    - malformed claim-candidate shape is quarantined deterministically
    - valid candidate persists without quarantine
    - existing structured-failure behavior for malformed top-level payload remains
    - no claims writes from extraction/quarantine path
  - `tests/test_schema_migration.py`:
    - verifies `claims_quarantine` contract columns + quarantine index presence
- Acceptance criteria satisfied:
  - Weak/malformed extraction candidates are safely quarantined and auditable.
  - Quarantine routing is additive and does not mutate canonical claims.
  - Deterministic rules and metadata surfaces are covered by tests.
- Known remaining gaps:
  - Promotion/review tooling is intentionally deferred.
  - Ops dashboards/metrics for quarantine throughput are deferred to later tickets.
  - This environment lacks `pytest`/`asyncpg`; test execution must be completed externally before marking T4b fully done.
- Status: partially done

### 2026-04-19 17:27 UTC — T4
- Summary of what changed:
  - Added durable extraction-results persistence module and contract (`src/extraction_results.py`) that writes versioned rows to `extract_results`.
  - Wired extraction persistence into post-ingest hook orchestration via new hook `extract_results`.
  - Enforced policy-version binding and fail-closed behavior for missing/unknown predicate policy version.
  - Added deterministic retry/dedupe behavior via stable `extract_run_id` derivation and idempotent lookup.
  - Added structured failure capture for malformed candidate payloads (`status='failed'`, `error_text`, structured `raw_output`).
  - Kept extraction stage strictly non-mutating for claims/projections.
- Files changed:
  - `src/extraction_results.py`
  - `src/session.py`
  - `src/main.py`
  - `src/config.py`
  - `tests/test_extract_results_pipeline.py`
  - `tests/test_session_ingest.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_extract_results_pipeline.py`:
    - durable extraction row write with required metadata
    - missing policy version fails closed
    - malformed candidate payload captured as structured failure
    - no claim writes in extraction stage
    - deterministic duplicate/retry behavior
  - updated `tests/test_session_ingest.py::test_session_ingest_enqueues_only_summary_and_loops_hooks` for additive extraction hook.
  - regression run: `tests/test_v2_dual_write_ingest.py`
- Acceptance criteria satisfied:
  - Durable, policy-bound, versioned extraction-results persistence is implemented and wired.
  - Extraction stage remains contract-only and does not mutate claims/projections.
  - Malformed payload behavior is structured and auditable.
- Known remaining gaps:
  - T4b quarantine routing/promotion workflow is not implemented.
  - Resolver consumption of extraction results remains deferred to T7.
- Status: done

### 2026-04-19 17:18 UTC — T3b
- Summary of what changed:
  - Hardened v2 evidence ingest contract with fail-closed validation for role, non-empty text, and parseable timestamps.
  - Enforced deterministic per-session turn ordering in dual-write path by serializing writes on session row lock and assigning monotonic `turn_index`.
  - Added deterministic normalization for out-of-order timestamps (later insert with earlier timestamp is normalized forward by microsecond, audit flag in turn metadata).
  - Added UTC timestamp normalization for v2 evidence writes.
  - Added session/user integrity guards to reject cross-user contamination on `session_buffer`/`session_transcript` and v2 session ownership mismatches.
  - Added structured ingest rejection codes via `EVIDENCE_*` errors on `/ingest` and `/session/ingest`.
- Files changed:
  - `src/session.py`
  - `src/ingestion.py`
  - `src/main.py`
  - `tests/test_v2_dual_write_ingest.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_v2_dual_write_ingest.py`:
    - out-of-order insert normalization and deterministic monotonic ordering
    - malformed turn rejection with structured error code
    - UTC timestamp normalization assertion
    - duplicate ingest idempotency regression
    - v2 row creation and legacy-path preservation regressions
  - Regression runs:
    - `tests/test_session_ingest.py::test_session_ingest_enqueues_raw_job_and_transcript`
    - `tests/test_session_ingest.py::test_session_ingest_dedupe_key_idempotent`
    - `tests/test_session_lifecycle.py::test_ingest_without_session_id_autocreates`
- Acceptance criteria satisfied:
  - Deterministic evidence contract for v2 ingest writes is enforced.
  - Malformed evidence payloads are rejected fail-closed with structured error codes.
  - Duplicate ingest does not produce duplicate v2 turns.
  - Timestamp handling is normalized and consistent for v2 rows.
- Known remaining gaps:
  - T3b scope is confined to evidence ingest contract; extraction/claim/retrieval flows remain unchanged by design.
- Status: done

### 2026-04-19 16:54 UTC — T3
- Summary of what changed:
  - Implemented feature-flagged v2 evidence dual-write on `/ingest` and `/session/ingest` paths while preserving legacy writes.
  - Added canonical evidence writer in session manager that upserts `sessions_v2` and append-only inserts `turns_v2`.
  - Wired idempotency through `turn_ingest_idempotency` for both incremental ingest (`ingest_v1`) and session transcript ingest (`session_ingest_v1`).
  - Preserved legacy behavior (`session_buffer`, `session_transcript`, outbox/session hooks) with no retrieval/synthesis path changes.
  - Added dual-write flags in config: `v2_dual_write_enabled`, `v2_dual_write_fail_open`.
- Files changed:
  - `src/session.py`
  - `src/ingestion.py`
  - `src/config.py`
  - `tests/test_v2_dual_write_ingest.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_v2_dual_write_ingest.py`:
    - duplicate `/ingest` payload does not duplicate `turns_v2`
    - `/session/ingest` dual-write creates expected `sessions_v2` + `turns_v2` rows
    - legacy `/ingest` path still writes expected `session_buffer` rows
  - Regression runs:
    - `tests/test_session_ingest.py::test_session_ingest_enqueues_raw_job_and_transcript`
    - `tests/test_session_ingest.py::test_session_ingest_dedupe_key_idempotent`
    - `tests/test_session_lifecycle.py::test_ingest_without_session_id_autocreates`
    - `tests/test_schema_migration.py::test_t2_v2_constraints_and_indexes`
- Acceptance criteria satisfied:
  - Dual-write is implemented behind a feature flag for ingest/session evidence surfaces.
  - Duplicate ingest idempotency is enforced for v2 turn writes.
  - v2 evidence writes remain append-only for `turns_v2`; legacy ingest behavior remains intact.
- Known remaining gaps:
  - T3 parity metrics/threshold instrumentation is not yet implemented.
  - T3b evidence contract hardening (strict UTC/ordering/state reject semantics) is not yet implemented.
- Status: done

### 2026-04-19 16:48 UTC — T5_CHECKPOINT_VALIDATION
- Summary of what changed:
  - Ran post-ticket checkpoint validation for T5 migration/service/test/runtime surfaces.
- Files changed:
  - `docs/SYNAPSE_V2_CHANGELOG.md`
- Tests added/updated:
  - Executed `tests/test_predicate_policy.py` and `tests/test_schema_migration.py` (all passed in this environment).
  - Executed app lifespan startup/shutdown smoke run; migration path completed.
- Acceptance criteria satisfied:
  - T5 lookup/fail-closed tests and migration presence are validated in local runtime context.
- Known remaining gaps:
  - No isolated disposable database was provisioned for from-scratch migration replay; validation used existing local DB context.
  - No staging/production lock-impact validation for `ALTER TABLE ... SET NOT NULL`/constraint-add paths.
  - Architecture-level requirement “extraction/resolution reject runs without policy version” remains deferred to T4/T7 integration.
- Status: partially done

### 2026-04-19 16:38 UTC — T5
- Summary of what changed:
  - Added T5 policy service module with deterministic predicate lookup by `(policy_version, predicate)`.
  - Added fail-closed runtime behavior with explicit errors for unknown predicate and unknown policy version.
  - Added current-version contract via active `predicate_policy_versions` lookup and explicit caller-supplied version handling in service lookup.
  - Added additive migration `037_predicate_policy_service_bootstrap.sql` to:
    - harden `predicate_policy` contract columns (`conflict_mode`, `expected_subject_kind`, `expected_object_kind`, `object_equivalence_rule`)
    - enforce conflict-mode/value checks
    - seed versioned policy records (`v2.p1`, `v2.p2`) with deterministic behavior differences
  - Synchronized `schema.sql` policy contract columns with migration state.
- Files changed:
  - `src/predicate_policy.py`
  - `migrations/037_predicate_policy_service_bootstrap.sql`
  - `schema.sql`
  - `tests/test_predicate_policy.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_predicate_policy.py`:
    - known predicate lookup
    - unknown predicate failure
    - unknown version failure
    - versioned behavior lookup
    - current active version contract lookup
  - `tests/test_schema_migration.py` (re-run validation, unchanged assertions).
- Acceptance criteria satisfied:
  - Predicate policy is persisted and version-addressable with a runtime service in `src/`.
  - Unknown predicate and unknown policy version are rejected fail-closed.
  - Versioned policy behavior can coexist and be resolved deterministically by explicit version.
- Known remaining gaps:
  - Extraction and resolver wiring to require policy version at runtime is pending T4/T7 implementation.
  - Replay policy-compatibility harness work remains under T12a scope.
- Status: done

### 2026-04-19 15:33 UTC — T2
- Summary of what changed:
  - Added T2 hardening migration `036_synapse_v2_schema_hardening.sql` to close post-audit structural gaps.
  - Enforced session/turn user integrity with composite session key shape and tenant/session/user FK from `turns_v2`.
  - Added tenant-scoped self-FK lineage for entity merges (`entities.merged_into_entity_id`) with self-merge guard.
  - Added relational policy-version anchor via `predicate_policy_versions` and FKs from `predicate_policy`, `extract_results`, and `claims`.
  - Added dedicated idempotency substrate table `turn_ingest_idempotency` with non-empty key and tenant-scoped relational constraints.
  - Documented pgcrypto operational requirement in migration and roadmap.
- Files changed:
  - `migrations/036_synapse_v2_schema_hardening.sql`
  - `tests/test_schema_migration.py`
  - `schema.sql`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_schema_migration.py`:
    - table existence checks for `predicate_policy_versions` and `turn_ingest_idempotency`
    - constraint checks for `turns_v2_session_user_fk`, `entities_merged_into_fk`, `claims_policy_version_fk`
    - index checks for idempotency lookup index
- Acceptance criteria satisfied:
  - T2 post-audit structural blockers are resolved within additive schema scope.
  - Tenant isolation and lineage integrity are enforced for new canonical tables.
  - Schema now supports fail-closed policy-version reference and enforceable ingest idempotency contract for future T3/T5 work.
- Known remaining gaps:
  - Migration execution/validation in staging/production environment remains operational follow-through.
- Status: done

### 2026-04-19 15:17 UTC — T2
- Summary of what changed:
  - Added additive v2 canonical schema migration `035_synapse_v2_additive_schema.sql`.
  - Introduced canonical/supporting tables for v2: `sessions_v2`, `turns_v2`, `entities`, `entity_aliases`, `claims`, `claim_evidence`, `canonical_mutations`, `predicate_policy`, `extract_results`, `projection_snapshots`, `projection_latest`, `claims_quarantine`, and `v2_pipeline_checkpoints`.
  - Added tenant-scoped PK/FK/unique constraints for canonical lifecycle and evidence linkage.
  - Added index coverage for factual lookup (`claims`), episodic lookup (`turns_v2`), projection lookup (`projection_latest`/`projection_snapshots`), and pipeline scans.
  - Updated schema documentation section to include v2 substrate tables (without implying rollout completion).
  - Added migration tests validating table existence and key constraints/indexes.
- Files changed:
  - `migrations/035_synapse_v2_additive_schema.sql`
  - `schema.sql`
  - `tests/test_schema_migration.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_schema_migration.py`:
    - `test_t2_v2_additive_schema_objects_exist`
    - `test_t2_v2_constraints_and_indexes`
- Acceptance criteria satisfied:
  - Additive v2 schema substrate exists for T3/T5/T6/T7 without legacy table deletion.
  - Required tenant-scoped PK/FK/unique constraints are present for canonical claim/evidence flows.
  - Safe index strategy for factual, episodic, and projection lookup is implemented.
- Known remaining gaps:
  - Migration application/validation in staging and production environments is pending operational execution.
- Status: done

### 2026-04-19 15:09 UTC — T0
- Summary of what changed:
  - Replaced remaining `uuid5`-based local hashing in `main.py` with canonicalization SDK hashing.
  - Updated session digest payload construction to hash canonical structured payloads (timestamp passed as native value, normalized in SDK), removing preformatted timestamp hashing.
  - Added timestamp field semantics in canonicalization (`valid` / `missing` / `invalid`) so invalid and missing timestamps hash differently.
  - Refactored local text-normalization helpers in `main.py`, `episodic_memory.py`, and `memory_ontology.py` to delegate to canonicalization SDK.
  - Added enforcement test that blocks `hashlib`/`uuid5` usage outside `src/canonicalization.py`.
  - Added 1000+ fixture deterministic/variation stress tests.
- Files changed:
  - `src/canonicalization.py`
  - `src/main.py`
  - `src/session.py`
  - `src/episodic_memory.py`
  - `src/memory_ontology.py`
  - `tests/test_canonicalization.py`
  - `tests/test_canonicalization_enforcement.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_canonicalization.py`:
    - timestamp equivalence variants
    - missing vs invalid timestamp semantic separation
    - event-key missing-vs-invalid differentiation
    - 1200-fixture slot-key determinism stress test
    - 1000-fixture event-key variation sensitivity stress test
  - `tests/test_canonicalization_enforcement.py`:
    - bans `hashlib`/`uuid5` usage outside canonicalization module in `src/`
- Acceptance criteria satisfied:
  - Canonicalization SDK is the single hashing/key authority for current write/replay hashing paths.
  - Determinism stress coverage exceeds 1,000 randomized fixtures.
  - Local hashing primitives (`hashlib`/`uuid5`) are enforced out of non-canonicalization modules.
- Known remaining gaps:
  - None for T0 scope.
- Status: done

### 2026-04-19 15:03 UTC — T0
- Summary of what changed:
  - Added canonicalization SDK module with deterministic normalization and key generation primitives.
  - Implemented `generate_claim_slot_key(...)`, `generate_claim_event_key(...)`, timestamp normalization, subject/object normalization, and versioned deterministic hashing.
  - Migrated local session hash generation for episode/summary names to use shared canonicalization hash utility.
- Files changed:
  - `src/canonicalization.py`
  - `src/session.py`
  - `tests/test_canonicalization.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_canonicalization.py`:
    - deterministic key output tests
    - variation tests for object/timestamp changes
    - normalization tests for case/spacing/unicode and UTC timestamp formatting
- Acceptance criteria satisfied:
  - Shared canonicalization module exists with deterministic slot/event key generation and normalization primitives.
  - Deterministic and variation test coverage added for canonicalization behavior.
- Known remaining gaps:
  - Full adoption across all write/replay claim/entity paths is pending (resolver/extraction paths not yet implemented).
  - 1,000-fixture determinism stress test is not yet added.
- Status: partially done

### 2026-04-19 14:54 UTC — T1
- Summary of what changed:
  - Removed factual candidate injection from session-summary prose and `user_model` prose in `/memory/query`.
  - Hardened factual helper paths to fail closed unless explicitly called as derived (`include_derived=True`).
  - Disabled legacy `/memory/search` endpoint with HTTP 410.
- Files changed:
  - `src/main.py`
  - `tests/test_graphiti_native.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_graphiti_native.py`:
    - `test_memory_query_uses_evidence_backed_factual_rows`
    - `test_memory_query_blocks_session_summary_and_user_model_fact_injection`
    - `test_memory_query_factual_output_requires_evidence_backed_rows`
    - `test_memory_search_endpoint_is_disabled`
- Acceptance criteria satisfied:
  - Factual candidate construction no longer injects `graphiti_session_summary` or `user_model` prose.
  - Legacy `/memory/search` mixed-authority retrieval is disabled.
  - Regression coverage added for evidence-backed-only factual outputs.
- Known remaining gaps:
  - `/v2/memory/query` lane contract and legacy compatibility adapter remain unimplemented (T10/T11).
  - Canonical claim store is not implemented (T2/T7).
- Status: done

### 2026-04-19 00:00 UTC — CHANGELOG_BOOTSTRAP
- Summary of what changed:
  - Created Synapse v2 migration changelog file and standardized entry format.
- Files changed:
  - `docs/SYNAPSE_V2_CHANGELOG.md`
- Tests added/updated:
  - None.
- Acceptance criteria satisfied:
  - Changelog format established for ticket-by-ticket execution tracking.
- Known remaining gaps:
  - No ticket implementation progress entries logged yet.
- Status: partially done
