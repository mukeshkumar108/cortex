# Synapse Pipelines: Current Runtime State

Date audited: 2026-04-21
Audit basis: current repository code (`src/`), migrations/schema (`migrations/`, `schema.sql`), and runtime endpoints (`src/main.py`)

Update: Phase 1 live 6-pass hardening has been implemented additively. The derived synthesis pipeline now has an ingest-triggered async DAG, evidence traces, pipeline run state, Stage A quarantine, lifecycle fields, and Phase 2 memory-intelligence columns/jobs. Runtime `startbrief` and `handover` remain sourced from derived Postgres tables.

Governing philosophy: [SOPHIE_MEMORY_PHILOSOPHY.md](/opt/synapse/docs/SOPHIE_MEMORY_PHILOSOPHY.md). Synapse is meaning-first: the 6-pass Gemma/Postgres synthesis pipeline is the understanding and serving layer; canonical v2 is governance/audit/anchor-fact infrastructure.

Important proof status: live orchestration and traceability are implemented, but faithful reproduction of the original rich 6-pass Gemma behavior is not fully proven yet. Pass 4 and Pass 5 currently write evidence-backed assertion scaffolding and metadata, but they do not yet regenerate the full rich identity/living-context prose fields from the original manual pipeline.

## 1. Executive summary

Synapse currently has two distinct memory processes running in parallel:

1. Gemma/Postgres derived synthesis pipeline (the 6-pass system):
- Main tables: `session_classifications`, `entity_profiles`, `open_threads`, `identity_profile`, `living_context`
- Primary role: high-quality continuity/synthesis context for runtime surfaces
- Current production writes: integrated into live ingest hooks for Pass 1 / Pass 1.5 / Pass 3, with signal/time-ceiling triggers for Pass 4 / Pass 5. Historical scripts in `scripts/` remain useful for batch/backfill/operator runs.

2. Canonical v2 truth pipeline:
- Main tables: `sessions_v2`, `turns_v2`, `extract_results`, `claims_quarantine`, `entities`, `entity_aliases`, `claims`, `claim_evidence`, `canonical_mutations`, `canonical_tenant_watermarks`
- Primary role: deterministic evidence-backed truth, lifecycle control, replay/audit, lane-separated factual retrieval
- Current production writes: integrated into live ingest post-hook path (config-gated, enabled by default in `src/config.py`)

Result: this is a hybrid system. The two processes are both real, but they are not yet a single unified pipeline.

The intended hierarchy is explicit:
- Meaning synthesis leads runtime behavior.
- Canonical facts anchor and audit meaning.
- Facts update meaning through synthesis; they do not bypass it.

## 2. Process A: Gemma/Postgres derived synthesis pipeline

### 2.1 Purpose
Generate high-quality derived context for user-facing continuity surfaces:
- who/what matters
- unresolved threads
- identity synthesis
- current living tension/context

### 2.2 Inputs
Primary evidence input:
- `session_transcript.messages` (raw durable transcripts)

Primary intermediate input:
- prior derived outputs and recent triage outputs, depending on pass

### 2.3 Passes and storage

Pass 1 triage:
- Script: `scripts/run_pass1_memory_triage_batch.py`
- Live hook: `pass1_triage`
- Writes: `session_classifications`
- Typical fields: memory-worthiness, session kind, memory deltas, entity mentions, thread/identity signals, emotional/tension fields, `context_relevant`
- Also writes atomic `derived_assertions(surface='memory_delta'|'identity_signal'|'thread_signal'|'living_context_statement')` with `source_session_ids` and `source_turn_refs`

Pass 1.5 entity pipeline:
- Script: `scripts/run_entity_pipeline.py`
- Live hook: `pass1_5_entities` after successful Pass 1 when entity mentions exist
- Writes: `entity_profiles`
- Also writes atomic `derived_assertions(surface='entity_mention')`

Pass 3 threads pipeline:
- Script: `scripts/run_threads_pipeline.py`
- Live hook: `pass3_threads` after successful Pass 1 when thread signals exist
- Writes: `open_threads`
- Now includes `lifecycle_state`, `evidence_turn_refs`, extraction/validity confidence, memory layer, semantic category, retention floor, and access counters

Pass 4 identity synthesis:
- Script: `scripts/run_identity_synthesis.py`
- Live hook: `pass4_identity`
- Trigger: 3 identity-signal sessions since last successful Pass 4, or 14 days since last successful Pass 4, whichever comes first
- Writes: `identity_profile`
- Live hardening writes atomic assertion refs into `identity_profile.assertions` without replacing rich prose fields

Pass 5 living context synthesis:
- Script: `scripts/run_living_context.py`
- Live hook: `pass5_living_context`
- Trigger: 2 context-delta sessions since last successful Pass 5, or 10 days since last successful Pass 5, whichever comes first
- Writes: `living_context`
- Live hardening writes atomic assertion refs into `living_context.assertions` without replacing rich prose fields

Scoring refresh helpers:
- Script: `scripts/run_scoring_update.py`
- Updates salience/importance style fields on derived tables

### 2.4 Runtime consumption (current)

`GET /session/startbrief`:
- Reads from `session_classifications`, `entity_profiles`, `identity_profile`, loops
- Does not use Graphiti summary nodes in the main serving path after cutover

`GET /session/handover`:
- Reads from `living_context`, `open_threads`, `entity_profiles`, `identity_profile`
- Uses `_episodic_search` backed by `session_classifications` embeddings for episodic recall

Derived continuity retrieval helpers:
- `_pg_search_nodes`, `_pg_search_continuity_facts`, `_pg_get_entity_role_hint`, `_pg_get_entity_continuity_facts`
- Pull from derived tables (with some canonical supplementation in helper-level continuity functions)

### 2.5 Operational reality

This process is quality-proven in prior testing and now has a live ingest-triggered async DAG using the existing durable outbox. The full rich LLM scripts remain available for batch/backfill; live hardening is additive and preserves endpoint output shape.

Current limitation: the live DAG does not yet fully prove parity with the original rich script behavior. The next validation step is a pass-level parity harness that compares live hook outputs against the proven manual Gemma pipeline outputs for the same transcript/window.

## 3. Process B: Canonical v2 truth pipeline

### 3.1 Purpose
Provide deterministic, evidence-backed semantic truth with lifecycle and audit guarantees:
- canonical evidence records
- canonical entity identity
- canonical claims + evidence links
- canonical mutation ordering + watermarks

### 3.2 Live ingest path

Entry points:
- `POST /ingest`
- `POST /session/ingest`

Write and hook flow:
1. Transcript/session ingest writes operational transcript + outbox jobs (`session.py`)
2. Dual-write evidence to canonical evidence tables (`sessions_v2`, `turns_v2`) when `v2_dual_write_enabled`
3. Post-ingest `extract_results` hook executes `_execute_post_ingest_hook`
4. `persist_extract_result(...)` writes durable extraction result
5. Optional live canonical resolution (`canonical_live_resolution_enabled`) runs:
   - entity resolution (`EntityResolver`)
   - claim resolution (`ClaimResolver`)
   - claim evidence writes (`claim_evidence`)
   - canonical mutation log writes (`canonical_mutations` + `canonical_tenant_watermarks`)

### 3.3 Main canonical tables

Evidence:
- `sessions_v2`
- `turns_v2`

Extraction staging and quarantine:
- `extract_results`
- `claims_quarantine`

Canonical semantic truth:
- `entities`
- `entity_aliases`
- `claims`
- `claim_evidence`

Ordering/audit:
- `canonical_mutations`
- `canonical_tenant_watermarks`

### 3.4 Runtime consumption (current)

`POST /v2/memory/query`:
- factual lane: canonical claims only, evidence required
- episodic lane: episodic recall items
- continuity lane: derived-only continuity items

`POST /memory/query` (legacy adapter):
- routes to v2 service only
- no mixed-authority fallback path

### 3.5 Operational reality

Canonical pipeline is integrated in live ingest hooks and powers authoritative v2 factual retrieval. It is infrastructure-correctness-oriented and currently parallel to the Gemma derived synthesis pipeline rather than fully driving it.

## 4. Where the two processes connect today

Current concrete join points:
- Both processes share transcript evidence substrate (`session_transcript`, plus canonical evidence dual-write)
- `POST /v2/memory/query` hybrid lane can return canonical factual + episodic + derived continuity together (lane-labeled)
- Some continuity helper functions include canonical-signal supplementation for specific lookups
- Both processes now have explicit run/audit state: canonical uses `canonical_mutations`; derived synthesis uses `pipeline_runs`, `pipeline_checkpoints`, `derived_assertions`, and `derived_quarantine`

Current non-join (important):
- The Gemma 6-pass derived table writers (scripts) do not run as a first-class integrated stage inside the canonical post-ingest resolver flow
- Canonical claim/entity state is not yet the single upstream source feeding all derived pass generation

## 5. Endpoint authority matrix (current code)

`GET /session/startbrief`:
- Serving authority: derived tables (`session_classifications`, `entity_profiles`, `identity_profile`) + loops
- Graphiti summary-node path: not used in main serving path
- Canonical claims/entities: not driving this endpoint

`GET /session/handover`:
- Serving authority: derived tables (`living_context`, `open_threads`, `entity_profiles`, `identity_profile`) + episodic from `session_classifications`
- Canonical fallback in packet assembly: removed from active serving path

`POST /v2/memory/query`:
- factual: canonical claims + claim_evidence (hard evidence requirement)
- continuity: derived continuity only
- episodic: episodic recall path

`POST /memory/query`:
- compatibility adapter over `/v2/memory/query` service

`POST /memory/search`:
- disabled (410)

## 6. Graphiti status in current repo

Still present/active in parts of runtime:
- Graphiti client initialization on app startup
- Graphiti outbox infrastructure and jobs in session ingest/close workflows
- Several internal debug endpoints and debug ranking paths still reference Graphiti-oriented APIs/terminology

Disabled/deprecated for retrieval authority:
- Legacy Graphiti-era debug retrieval query endpoint (`/internal/debug/graphiti/query`) is disabled (410)
- Legacy mixed-authority `/memory/query` runtime branch is removed; adapter is v2-only

## 7. Conflict risk and current policy reality

Yes, there is still dual-track truth risk if not governed strictly:
- Derived serving outputs (startbrief/handover) come from Gemma-derived tables
- Canonical truth/factual retrieval comes from claims/evidence tables

Current practical policy in code:
- Runtime startbrief/handover are now pinned to derived Postgres synthesis tables
- Canonical pipeline remains authoritative for v2 factual retrieval and auditability
- No automatic mechanism currently guarantees that derived pass outputs are reconciled against canonical claims before serving

Meaning-first policy:
- Serving surfaces should not be sourced from canonical claims/entities directly.
- Canonical anchor facts may be used to audit or stabilize synthesis, but only through the synthesis layer.
- Contradictions between canonical anchor facts and synthesized understanding should be preserved and reviewed rather than silently overwritten.

## 8. What is clearly manual vs integrated

Integrated/live:
- ingest -> derived Pass 1 triage -> async Pass 1.5 entities / Pass 3 threads -> signal/time-ceiling Pass 4 identity and Pass 5 living context
- ingest -> extract_results -> optional live entity/claim resolution -> canonical mutation logging
- v2 retrieval + legacy adapter routing

Script/manual/operator-driven:
- backfill/batch reruns of pass-1 triage
- rich entity profile synthesis pipeline backfills
- rich open thread synthesis pipeline backfills
- rich identity synthesis backfills
- rich living context synthesis backfills
- scoring refresh scripts

## 9. Practical implication

Today’s system is not “one bad pipeline replacing one good pipeline.” It is:
- one quality-proven derived synthesis process, now live-orchestrated for freshness and evidence traceability, and
- one integrated canonical correctness/audit process,

running as complementary layers. The derived pipeline serves understanding surfaces; canonical v2 remains governance/audit and factual retrieval safety.

## 10. Remaining proof needed for "faithful 6-pass live parity"

The following are required before claiming the live pipeline faithfully reproduces the original rich Gemma/Postgres 6-pass system:

- Shared pass implementation:
  - Refactor the proven scripts into importable pass modules.
  - Make scripts and live hooks call the same pass functions.
- Pass-level parity harness:
  - Compare live Pass 1 output against original Pass 1 output.
  - Compare live Pass 1.5 entity/profile decisions against original entity pipeline output.
  - Compare live Pass 3 thread actions against original thread pipeline output.
  - Compare live Pass 4 identity synthesis against original identity output.
  - Compare live Pass 5 living context/tension-map synthesis against original living-context output.
- Endpoint golden fixtures:
  - Verify representative `startbrief` and `handover` outputs do not regress against proven-good examples.
- Atomic evidence traces:
  - Attach source evidence to each high-signal derived assertion, thread update, identity trait, and living-context statement.
- Lifecycle validation:
  - Enforce valid derived-state transitions.
  - Quarantine invalid lifecycle transitions under `derived_quarantine.reason_code='invalid_lifecycle_transition'`.
- Phase 2 safety:
  - Keep decay, staleness review, and consolidation dormant until rich parity and endpoint golden tests pass.
