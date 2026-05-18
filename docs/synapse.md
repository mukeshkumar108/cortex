# Synapse Architecture Summary (Current Runtime)

Last audited from code: 2026-04-30  
Repo: `/opt/synapse`

## 0) Plain-English System Shape
Synapse is the memory and user-state engine behind Sophie.

Think of it as:

`raw conversation + events -> interpreted continuity memory + factual/evidence memory -> serving packets Sophie consumes`

It currently runs **two complementary memory lanes**:

1. **Derived lane (continuity lane)**
- Purpose: interpret what matters, what changed, what is unresolved, and what should be top-of-mind for conversation continuity.
- Output examples: entity profiles, open threads, living context, identity profile, actionable candidates.

2. **Canonical lane (factual lane)**
- Purpose: deterministic, evidence-backed semantic truth with policy checks and auditable lifecycle.
- Output examples: canonical entities, claims, claim evidence, mutation log.

These are complementary, not identical. Drift can happen and is an active risk.

## 1) End-to-end ingest flow (what happens when a session comes in)

### A) `POST /ingest` (turn-level fast path)
- Appends turn to `session_buffer` and `session_transcript`.
- Evaluates idle/session-close gap logic.
- May trigger janitor/outbox processing conditions.
- Returns quickly (heavy work is async).

### B) `POST /session/ingest` (full transcript path)
- Upserts durable transcript/session records.
- Enqueues `session_raw_episode` into durable `graphiti_outbox`.

### C) Outbox worker executes `session_raw_episode`
- Reads transcript, caps to safe processing limits.
- Writes Graphiti session episode.
- Builds episodic embeddings.
- Enqueues post-ingest hook jobs.

### D) Post-ingest hooks fan out
Primary hooks:
- `session_summary`
- `open_loops`
- `extract_results`
- `pass1_triage` (if derived enabled)

Those hooks branch into the two memory lanes.

## 2) Derived lane (continuity lane) stages

### Pass 1 `pass1_triage`
- Role: routing and gating pass.
- Inputs: transcript window + reference time.
- Output: `session_classifications` + run flags (which downstream passes to run).

### Pass 2a `pass2_actionable`
- Role: infer candidate actions/reminders/commitments.
- Output: `actionable_candidates`.

### Pass 2b `pass2b_session_changes`
- Role: infer meaningful state/fact changes from session.
- Output: `session_changes`.

### Pass 2c `pass2c_entity_candidates`
- Role: infer candidate entities.
- Output: `entity_candidates`, then triggers pass 1.5.

### Pass 1.5 `pass1_5_entities`
- Role: resolve/promote candidates into richer continuity entity view.
- Output: `entity_profiles`, `derived_assertions`, `memory_relationship_links`.

### Pass 3 `pass3_threads`
- Role: open-thread lifecycle and unresolved-state tracking.
- Output: `open_threads`.

### Pass 4 `pass4_identity`
- Role: periodic identity synthesis.
- Trigger: signal threshold or time ceiling.
- Output: `identity_profile`.

### Pass 5 `pass5_living_context`
- Role: periodic current-life-context synthesis.
- Trigger: signal threshold or time ceiling.
- Output: `living_context`.

## 3) Canonical lane (factual lane) stages

### Stage 1: evidence dual-write
- Writes validated session/turn evidence to `sessions_v2` + `turns_v2`.

### Stage 2: extraction persistence
- Writes extractor output to `extract_results`.
- Weak/invalid candidates are quarantined in `claims_quarantine`.

### Stage 3: deterministic entity resolution
- Alias/name resolution with ambiguity fail-closed behavior.
- Writes `entities` + `entity_aliases`.

### Stage 4: deterministic claim resolution
- Applies predicate policy + cardinality/lifecycle rules.
- Writes `claims` + `claim_evidence` + `canonical_mutations`.

## 4) Runtime serving layer (most product-critical)
This is where memory becomes Sophie behavior.

### `GET /session/startbrief`
- Purpose: startup continuity packet.
- Inputs: derived continuity state + loops + selected evidence + freshness indicators.
- Behavior: ranking/selecting top memory ingredients, optional LLM realization path.
- Fallbacks: structured fallback path if richer realization fails.

### `GET /session/handover`
- Purpose: compact continuity handover packet (context, threads, people/projects, episodic recall).
- Main source: derived tables, with explicit packet shaping.

### `GET /signals/pack`
- Purpose: structured recent signal bundle for downstream consumption.
- Behavior: deterministic assembly from stored state.

### `POST /v2/memory/query`
- Purpose: lane-separated retrieval.
- `factual`: canonical claims/evidence.
- `episodic`: recall windows.
- `continuity`: derived continuity text/state.
- Guarantees lane metadata in response.

### `POST /memory/query` (legacy adapter)
- Purpose: compatibility facade that now routes into v2 service.

## 5) Loops vs threads vs commitments (your direct question)
These are still active.

### `loops` table (active)
- Tracks commitments/habits/frictions/threads as operational memory objects for “what user said they’ll do / keep doing / is blocked on”.
- Used by `/memory/loops` and startbrief ranking inputs.

### `open_threads` table (active)
- Tracks unresolved narrative threads (stateful continuity threads).
- Used for continuity/handover context.

### `actionable_candidates` and `action_items` (active)
- `actionable_candidates`: inferred opportunities.
- `action_items`: confirmed operational tasks/reminders/habits.

So no, these did not disappear.

## 6) Inbox/outbox status (your direct question)
- There is **no generic “inbox queue” table** in this repo.
- There is a durable **outbox**: `graphiti_outbox`.
- It is heavily used for:
  - evicted turn processing,
  - `session_raw_episode`,
  - post-ingest hook DAG jobs.

So outbox is not gone; it is core orchestration infrastructure.

## 7) Session summaries and where they live (your direct question)
Session summary data exists in multiple places:

1. `session_buffer.rolling_summary` (Postgres)
- short compression state for active session windowing.

2. Graphiti `SessionSummary` nodes
- written by `session_summary` post-ingest hook.

3. `startbrief_history` (Postgres)
- stores generated startbrief outputs + metadata.

So summaries are **not only** in Graphiti.

## 8) Event/time log and audit trail (your direct question)
You do have durable traces, but spread across surfaces:

- `session_transcript`: raw message timeline.
- `turns_v2` / `sessions_v2`: canonical evidence timeline.
- `graphiti_outbox`: async job history/status attempts.
- `pipeline_runs`: derived pass run records.
- `canonical_mutations`: canonical truth mutation log.
- `claim_evidence`: evidence spans linked to claims.
- `action_audit_log`: action-state mutations.
- `startbrief_history`: generated serving-packet history.

What is missing: one unified cross-surface “global event ledger” that cleanly joins session -> tool/action -> pipeline -> serving.

## 9) LLM usage vs deterministic logic

### LLM-heavy
- derived pass extraction/synthesis (pass1/2/4/5),
- loop extraction hook,
- session summary generation,
- optional startbrief realization,
- daily analysis and some enrichment paths.

### Deterministic-heavy
- ingest validation/idempotency,
- outbox orchestration/retry,
- schema normalization,
- quarantine gates,
- entity/claim policy enforcement,
- invariants + rollout controllers.

## 10) Models configured (operational, not architectural)
From config defaults:
- derived pipeline: `google/gemma-4-26b-a4b-it`
- openrouter generic: `xiaomi/mimo-v2-flash`
- summary/identity: `amazon/nova-micro-v1`
- loops/session-episode: `xiaomi/mimo-v2-flash`
- fallback: `mistral/ministral-3b`
- embeddings: `text-embedding-3-small`

Important warning:
- model routing is **operational config**, not stable architecture.
- each critical task should maintain known-valid fallbacks and tests.

## 11) Action lifecycle (expanded)
Action system is now first-class runtime state.

### Candidate phase
- candidates detected into `actionable_candidates`.
- statuses include: `detected`, `needs_review`, `confirmed`, `dismissed`, `acted_on`, `superseded`, `stale`.

### Promotion phase
- candidates can be promoted into `action_items` via `/actions/candidates/promote` (or auto-promote endpoint).

### Operational action state
- `action_items` lifecycle: `pending -> done|cancelled|dismissed|archived`.
- mutation endpoints: status patch, done, dismiss, cancel, patch details.

### Daily serving lens
- `/actions/daily-agenda` returns due/overdue/today-focused action view.

### Auditability
- `action_audit_log` records action mutations with actor/reason payload.

## 12) Tenant/user isolation and scope
Current controls include:
- tenant_id and user_id included in core table keys/queries,
- session ownership checks in critical write paths,
- canonical mutation/watermark tenant scoping,
- internal/admin token-gated debug/destructive endpoints.

Remaining hardening need:
- explicit, documented isolation test matrix proving no cross-tenant data leakage across all serving and batch loops.

## 13) Freshness and staleness guarantees
Freshness is best-effort via async processing + periodic loops.

Signals in runtime:
- startbrief includes ingest freshness indicators (pending jobs, oldest age, etc).
- loop staleness janitor updates stale/needs_review statuses.
- pass4/pass5 thresholds/ceilings force periodic identity/context refresh.

Risks:
- outbox backlog or failed jobs can delay freshness,
- derived/canonical lag can produce stale or divergent served context.

## 14) Background jobs
Config-gated loops include:
- idle close,
- outbox drain,
- user model updater/enrichment,
- loop staleness janitor,
- derived silence detection,
- derived memory audits,
- proactive shadow candidates,
- daily analysis + habit dedupe,
- v2 invariant checker,
- v2 rollout evaluator.

## 15) Graphiti role (clearer status)
Graphiti is still active for episode/session-summary storage and compatibility-era paths.

But the core runtime continuity strategy is increasingly:
- Postgres-derived projections + canonical v2 factual lane.

So Graphiti is important infrastructure, but not the sole serving authority.

## 16) Deprecated / legacy surfaces
Still present but transitional:
- `POST /memory/query` (legacy adapter to v2),
- `GET /session/brief` compatibility surface,
- some Graphiti-era debug paths,
- rollout/shadow routes supporting migration safety.

Guidance: new product work should target v2/lane-aware and current action/continuity surfaces, not legacy compatibility seams.

## 17) Evaluation and quality gates
Current quality story uses tests + fixtures + rollout controls, but should be treated as evolving.

Recommended explicit eval tracks:
1. startbrief quality eval,
2. handover quality eval,
3. drift eval (derived vs canonical disagreement),
4. action candidate precision/recall,
5. replay parity and shadow-diff regression,
6. freshness/lag SLO compliance.

## 18) What’s strong
- clear stage decomposition,
- durable async orchestration,
- deterministic canonical policy enforcement,
- migration safety controls (shadow, rollout, invariants),
- increasingly rich action + continuity serving surfaces.

## 19) What’s missing / next hardening
1. closed-loop reconciliation from canonical truth into derived continuity projections,  
2. first-class drift metrics + alerting,  
3. unified cross-surface event ledger,  
4. stronger live-quality harnesses beyond fixtures,  
5. isolation/freshness SLOs formalized and monitored,  
6. continued legacy surface retirement.

## 20) Human-readable glossary
- **Entity**: a person/project/place/thing that matters repeatedly.
- **Claim**: a factual assertion about an entity or user state.
- **Evidence**: where claim support came from (session/turn/span).
- **Loop**: an ongoing commitment/habit/friction item.
- **Open thread**: unresolved narrative/context thread.
- **Living context**: current-life-state synthesis for near-term behavior.
- **Identity profile**: slower-changing self/chapter synthesis.
- **Candidate**: inferred suggestion not yet confirmed truth or action.
- **Action item**: confirmed operational todo/reminder/habit.
- **Outbox job**: durable async work unit waiting for background processing.

---

Primary code refs:
- Orchestration: `src/main.py`, `src/session.py`, `src/derived_pipeline.py`
- Canonical resolution: `src/extraction_results.py`, `src/entity_resolution.py`, `src/claim_resolution.py`
- Action state: `src/action_state.py`
- Data model: `schema.sql`, `migrations/`

## 21) Table-by-Table Ownership Map (writer -> trigger -> readers)

### Session ingestion + buffering

#### `session_buffer`
- What it is: active sliding-window session state (`rolling_summary` + recent messages).
- Primary writers: `POST /ingest` path (`src/ingestion.py`, `src/session.py`).
- Trigger: each turn ingest.
- Primary readers: brief/startup assembly helpers.

#### `session_transcript`
- What it is: durable transcript message array per session.
- Primary writers: `POST /ingest`, `POST /session/ingest`, session close enqueue path.
- Trigger: turn write or transcript ingest upsert.
- Primary readers:
  - outbox `session_raw_episode` jobs,
  - post-ingest hooks,
  - serving/debug helpers (`startbrief`, ranking/debug endpoints).

#### `graphiti_outbox`
- What it is: durable async job queue with retries/backoff and dedupe keys.
- Primary writers:
  - ingest/session code enqueueing evicted turns,
  - `session_raw_episode` jobs,
  - post-ingest hook jobs.
- Trigger: ingest/session lifecycle events.
- Primary readers: outbox drain worker (`session.drain_outbox`), internal debug outbox endpoints.

### Canonical v2 evidence + truth lane

#### `sessions_v2`
- What it is: canonical session record for evidence lane.
- Primary writers: v2 dual-write ingest path.
- Trigger: turn/session evidence ingest.
- Primary readers: canonical resolution/retrieval joins and invariants.

#### `turns_v2`
- What it is: canonical turn-level evidence timeline.
- Primary writers: v2 dual-write ingest path (idempotent).
- Trigger: each accepted turn in evidence lane.
- Primary readers: extraction, claim evidence linking, retrieval/invariant checks.

#### `extract_results`
- What it is: persisted extraction payload + run metadata.
- Primary writers: `extract_results` post-ingest hook (`persist_extract_result`).
- Trigger: post-ingest hook execution.
- Primary readers: claim/entity resolution and audits/debug.

#### `claims_quarantine`
- What it is: quarantined low-confidence/weakly-grounded/invalid candidates.
- Primary writers: extraction persistence/quarantine partitioning.
- Trigger: candidate validation/quarantine gate failures.
- Primary readers: review/repair/audit workflows.

#### `entities`
- What it is: canonical resolved entities.
- Primary writers: deterministic entity resolver.
- Trigger: canonical live resolution from extract results.
- Primary readers: canonical factual retrieval and resolver joins.

#### `entity_aliases`
- What it is: alias mapping for canonical entity resolution.
- Primary writers: deterministic entity resolver.
- Trigger: entity create/link/merge operations.
- Primary readers: resolver matching and ambiguity checks.

#### `claims`
- What it is: canonical factual assertions with lifecycle.
- Primary writers: deterministic claim resolver.
- Trigger: candidate application under predicate policy.
- Primary readers: factual lane in `/v2/memory/query`.

#### `claim_evidence`
- What it is: evidence spans backing canonical claims.
- Primary writers: deterministic claim resolver.
- Trigger: accepted claim candidates with evidence spans.
- Primary readers: factual retrieval lane and audit/invariant checks.

#### `canonical_mutations`
- What it is: auditable mutation log for canonical semantic changes.
- Primary writers: entity + claim resolvers via mutation logger.
- Trigger: canonical create/update/supersede/retract operations.
- Primary readers: replay/audit/invariant/rollout tooling.

### Derived continuity lane

#### `pipeline_runs`
- What it is: run ledger for derived pass executions.
- Primary writers: derived pass orchestrators.
- Trigger: each pass run.
- Primary readers: operational diagnostics, QA/audit, debugging.

#### `session_classifications`
- What it is: pass1 triage outputs and routing metadata.
- Primary writers: `pass1_triage`.
- Trigger: post-ingest pass1 execution.
- Primary readers: startbrief/handover continuity assembly; downstream pass gating.

#### `actionable_candidates`
- What it is: inferred operational candidates (task/reminder/commitment/event-like).
- Primary writers: `pass2_actionable`.
- Trigger: pass1 route flag `run_actionable_pass`.
- Primary readers:
  - `/daily-candidates`, `/review-queue`,
  - action promotion flows,
  - agenda/ops lenses.

#### `session_changes`
- What it is: meaningful user-world changes extracted from sessions.
- Primary writers: `pass2b_session_changes`.
- Trigger: pass1 route flag `run_session_changes_pass`.
- Primary readers: `/session-changes`, continuity packet composition.

#### `entity_candidates`
- What it is: durable entity candidates pre-profile resolution.
- Primary writers: `pass2c_entity_candidates`.
- Trigger: pass1 route flag `run_entity_pass`.
- Primary readers: `pass1_5_entities`, `/entity-candidates`.

#### `entity_profiles`
- What it is: continuity-facing entity summaries/attributes.
- Primary writers: `pass1_5_entities`.
- Trigger: post pass2c follow-up.
- Primary readers: `/session/startbrief`, `/session/handover`, `/entities/profile`.

#### `open_threads`
- What it is: unresolved continuity threads with lifecycle metadata.
- Primary writers: `pass3_threads`.
- Trigger: pass1 route flag `run_threads_pass`.
- Primary readers: `/session/handover`, startbrief continuity scoring.

#### `identity_profile`
- What it is: slower-moving identity synthesis.
- Primary writers: `pass4_identity`.
- Trigger: threshold/ceiling gate.
- Primary readers: startbrief/handover packet composition.

#### `living_context`
- What it is: current-life context synthesis.
- Primary writers: `pass5_living_context`.
- Trigger: threshold/ceiling gate.
- Primary readers: `/session/handover`, continuity surfaces.

#### `derived_assertions`
- What it is: atomic derived assertions with lifecycle metadata.
- Primary writers: multiple derived passes (notably entity pipeline and synthesis flows).
- Trigger: pass outputs normalized into assertion form.
- Primary readers: continuity audits, shadow candidate generation, downstream synthesis.

#### `memory_relationship_links`
- What it is: relationship links between memory objects/surfaces.
- Primary writers: derived entity pipeline and synthesis helpers.
- Trigger: entity/relationship extraction and reconciliation.
- Primary readers: continuity linking logic and audits.

#### `memory_contradictions`
- What it is: detected contradiction primitives.
- Primary writers: derived audit/synthesis routines.
- Trigger: contradiction detection rules.
- Primary readers: handover/startbrief guidance and audit views.

#### `memory_events`
- What it is: event primitives for meaningful autobiographical changes.
- Primary writers: derived synthesis/event extraction paths.
- Trigger: pass/audit event extraction.
- Primary readers: continuity/event-aware synthesis.

#### `memory_silence_flags`
- What it is: stale silence markers for high-salience entities/threads.
- Primary writers: `derived_silence_detection_loop`.
- Trigger: scheduled daily detector run.
- Primary readers: proactive candidate generation and continuity quality logic.

#### `always_on_memory_packets`
- What it is: stored always-on packet outputs.
- Primary writers: always-on packet builder routines.
- Trigger: packet generation path.
- Primary readers: internal debug/runtime packet consumers.

### Loops + action-state lane

#### `loops`
- What it is: commitments/habits/frictions/threads memory objects with staleness lifecycle.
- Primary writers: `open_loops` post-ingest hook via loop manager.
- Trigger: post-ingest hook processing.
- Primary readers: `/memory/loops`, startbrief loop ranking.

#### `action_items`
- What it is: confirmed operational actions (todo/reminder/habit).
- Primary writers:
  - direct action endpoints,
  - candidate promotion flows,
  - system updates.
- Trigger: API operations and promotions.
- Primary readers: `/actions/items`, `/actions/daily-agenda`.

#### `action_updates`
- What it is: proposed changes to existing action items.
- Primary writers: action update creation flows.
- Trigger: action update proposal creation.
- Primary readers: action workflows and operator/audit surfaces.

#### `action_audit_log`
- What it is: immutable audit trail for action mutations.
- Primary writers: action state mutation handlers.
- Trigger: create/update/status transitions/promotions/dismissals.
- Primary readers: audits/debug/forensics.

### Serving/audit history tables

#### `startbrief_history`
- What it is: history log of generated startbrief packets + metadata.
- Primary writers: startbrief generation flow.
- Trigger: successful startbrief response assembly.
- Primary readers: `/internal/debug/startbrief/history` and quality diagnostics.

#### `daily_analysis` (and related daily outputs)
- What it is: periodic daily synthesis outputs.
- Primary writers: `daily_analysis_loop`.
- Trigger: scheduled daily analysis jobs.
- Primary readers: `/analysis/daily` and downstream serving helpers.

## 22) Source of Truth by Use Case
Use this to avoid cross-lane confusion.

- **Factual truth** -> canonical lane (`claims` + `claim_evidence` via `/v2/memory/query` factual lane)
- **Conversation continuity** -> derived lane (`session_classifications`, `entity_profiles`, `open_threads`, `identity_profile`, `living_context`)
- **Confirmed executable tasks/reminders/habits** -> `action_items`
- **Candidate possible actions** -> `actionable_candidates`
- **Raw transcript evidence** -> `session_transcript` (operational) and `turns_v2` (canonical evidence lane)
- **Async orchestration state** -> `graphiti_outbox`
- **Serving packet history/debug** -> `startbrief_history`

## 23) Boundary Rules: `loops` vs `open_threads` vs `action_items`

### `action_items`
- Meaning: confirmed, executable operational actions.
- Ownership: action state lane.
- Typical examples:
  - “I need to call Mum tomorrow.”
  - “Remind me Friday 9am to submit taxes.”

### `loops`
- Meaning: ongoing recurring/behavioral/follow-up memory object; not always a single executable task.
- Ownership: loop manager + continuity inputs.
- Typical examples:
  - “I’m trying to fix my sleep schedule.”
  - “I keep forgetting to hydrate.”

### `open_threads`
- Meaning: unresolved narrative context/tension that needs continuity awareness.
- Ownership: derived pass3 thread lifecycle.
- Typical examples:
  - “I’m worried about Ashley’s health results.”
  - “I still haven’t resolved the conflict with my cofounder.”

Practical rule:
- If it is a concrete do-able item with completion semantics -> `action_items`.
- If it is a recurring pattern/commitment signal -> `loops`.
- If it is unresolved context/tension/storyline -> `open_threads`.

## 24) Audit Confidence Levels
This document is code-audited, but confidence varies by area.

- **High confidence**
  - endpoint inventory,
  - major table existence/role,
  - broad ingest and two-lane pipeline shape,
  - outbox orchestration role.

- **Medium confidence**
  - exact trigger interactions between multiple background loops,
  - nuanced runtime selection/ranking effects under mixed backlog conditions,
  - operational precedence under partial failure.

- **Needs ongoing verification**
  - less-used/less-central surfaces (`always_on_memory_packets`, some `memory_events`/`memory_relationship_links` flows),
  - low-volume edge paths only hit in specific cohorts/debug flows.

## 25) Decision Tables (Prevent Surface Sprawl)

### A) When user says X, where should it land?

| User input pattern | Primary write surface | Why |
|---|---|---|
| Explicit fact about person/project/state with evidence | `extract_results` -> canonical `claims`/`claim_evidence` | factual truth requires evidence + policy lifecycle |
| Concrete task/reminder/habit to execute | `actionable_candidates` then promote to `action_items` | separates inference from confirmed execution state |
| Recurring behavior/ongoing personal commitment | `loops` | operational continuity memory for recurring patterns |
| Unresolved worry/tension/context thread | `open_threads` | narrative continuity lifecycle |
| Material “what changed this session” signal | `session_changes` | change-log lens for recent updates |
| Named durable entity mention | `entity_candidates` -> `entity_profiles` (+ canonical entity resolution as applicable) | continuity entity grounding + canonical identity where needed |

### B) When Sophie needs Y, which endpoint should read it?

| Sophie need | Endpoint | Lane/source |
|---|---|---|
| Startup continuity packet | `GET /session/startbrief` | derived continuity + loops + selected evidence |
| Compact handover state | `GET /session/handover` | derived continuity packet |
| Strict factual recall | `POST /v2/memory/query` with `lane=factual` | canonical claims/evidence |
| Episodic recall | `POST /v2/memory/query` with `lane=episodic` | episodic recall lane |
| Continuity recall text/state | `POST /v2/memory/query` with `lane=continuity` | derived lane |
| Active loop view | `GET /memory/loops` | loops table/ranking |
| Task/reminder execution list | `GET /actions/items` | action_items |
| Today-focused execution plan | `GET /actions/daily-agenda` | action lens over action state |
| Candidate triage/review | `GET /daily-candidates`, `GET /review-queue` | actionable_candidates |

### C) Conflict policy: when facts conflict, which lane wins?

| Conflict type | Resolution policy |
|---|---|
| Factual contradiction (who/what/when) | canonical factual lane (`claims` + `claim_evidence`) is authority |
| Continuity tone/priority disagreement | derived lane may differ, but should not overwrite canonical fact authority |
| Candidate vs confirmed action mismatch | `action_items` is execution authority; candidates stay non-authoritative |
| Derived vs canonical drift | flag via audits/shadow/invariants; reconcile explicitly, do not silently merge |

Guardrail:
- Never promote derived narrative text into factual authority without canonical evidence-backed resolution.
