# Synapse v2 Architecture

## 1. Purpose
Synapse is:

> An evidence-grounded context engine that transforms raw conversation history into the minimum useful context an LLM needs to behave with continuity and accuracy.

## 2. Core Principles
- Synapse owns user-state and semantic intelligence.
- Sophie decides what happens next and executes tools.
- External systems own native operational artifacts.
- Sophie-native actions may be canonical in Synapse as `action_items`.
- For external artifacts, Synapse stores references, projections, and interpreted state rather than duplicated operational truth.
- Private sources can influence behaviour, but must not become conversational evidence.
- Raw private data is ephemeral; derived state is durable and minimized.
- Every state object must carry provenance, confidence, timestamps, and freshness.
- Uncertainty, conflict, and staleness must be explicit.
- Retrieval must respect strict lane separation (`factual` / `episodic` / `continuity`).
- System must be deterministic and auditable.

## 3. Canonical vs Derived
### Canonical
- `sessions`
- `turns`
- `entities`
- `entity_aliases`
- `claims`
- `claim_evidence`
- `canonical_mutations`

### Derived
- `user_model`
- `identity_profile`
- `living_context`
- `entity_profiles`
- `open_threads`
- `session_classifications`
- summaries

Derived data must never be used as factual authority.
Derived projections are disposable, versioned, and rebuildable from canonical state.
Derived outputs may guide ranking, continuity, and behavior, but they must never be silently promoted into canonical truth.

## 4. Layered Model
- **Evidence**: transcripts and session records.
- **Signal**: claims and resolved entities derived from evidence.
- **Interpretation**: derived projections for continuity and operator UX.
- **Delivery**: runtime packets returned to assistants and clients.

## 4a. First-Class Synapse Primitives
- `action_items`
- `action_candidates`
- `action_updates`
- `commitments`
- `signals`
- `open_threads`
- `entity_profiles`
- `identity_profile`
- `living_context`

Follow-ups are not first-class primitives. Follow-ups, nudges, check-ins, and review prompts are derived attention moments generated from existing primitives and must reference their source object(s).

## 5. Write Pipeline
1. **Ingest**: persist turns and session boundaries.
2. **Extract**: produce candidate claims from evidence spans.
3. **Resolve entities**: canonicalize entity identity and aliases.
4. **Resolve claims**: compute slot/event keys and apply lifecycle transitions.
5. **Build projections**: generate derived views from canonical state only.
6. **Publish indexes**: update retrieval/search indexes from canonical or derived sources as appropriate.
7. **Log canonical mutations**: emit committed mutation records so projection and retrieval consumers advance by committed watermarks only.

## 6. Claim Model
- **`claim_slot_key`**: identity of a semantic slot (subject + predicate scope).
- **`claim_event_key`**: identity of a specific event/value instance within a slot.
- **Lifecycle**:
  - `active`
  - `superseded`
  - `retracted`
- **Evidence linkage**: every claim must link to one or more evidence spans in `claim_evidence`.
- **Predicate policy**: each predicate defines cardinality and conflict behavior.
- **Confidence split**:
  - extraction confidence: confidence in extraction correctness.
  - truth confidence: confidence in current semantic validity.

## 7. Retrieval Model
Primary contract: `POST /v2/memory/query`

Lanes:
- **factual**: claims only, must include evidence links.
- **episodic**: transcript chunks/windows for recall.
- **continuity**: derived projections only, explicitly marked as derived.

Hard rules:
- No projection text in factual lane.
- No factual result without evidence.

## 8. Operational Guardrails
- **Predicate policy versioning is mandatory**: extraction and claim resolution runs must record `predicate_policy_version`. No unversioned runs.
- **Watermark semantics must be explicit**: Synapse must choose and document one model:
  - global monotonic watermark, or
  - per-tenant monotonic watermark.
  Mixed assumptions are not allowed.
- **Assistant-authored claims are non-authoritative by default**: assistant turns may not create canonical claims unless explicitly confirmed by user evidence.
- **Ambiguous alias resolution fails closed**: if an alias maps to multiple plausible entities, Synapse must not auto-link.
- **Cardinality enforcement is mandatory**: predicates must declare cardinality (`one` / `many`) and conflict rules; resolver must enforce them.

Continuous invariants:
- Factual lane outputs must always include evidence links.
- Derived projection text must never appear in factual lane.
- Tenant-scoped joins are required for all canonical reads/writes.
- Canonical claim lifecycle transitions must be auditable and replayable.

Fail-closed conditions:
- unknown predicate policy
- missing or invalid evidence span
- tenant mismatch
- missing policy version
- unresolved ambiguous entity alias

Repair governance:
- **Human review required** for semantic mutations (claim conflict repair, entity merge repair, assistant-claim gating repair).
- **Auto-repair allowed** for structural/integrity issues (orphan evidence references, projection watermark drift, index rebuilds).

Rollout guardrails:
- define objective rollback thresholds for:
  - factual evidence coverage regressions
  - active-slot conflict detection
  - replay divergence
  - retrieval latency regressions
- Low-confidence or weakly grounded backfill candidates must go to quarantine, not directly into canonical claims.
- Legacy compatibility adapters must route to v2 retrieval only and may not silently invoke legacy mixed-authority retrieval logic.
- During migration, retrieval outputs should carry explicit source metadata so factual, episodic, and derived rows cannot be confused.

## 9. Migration Plan (High Level)
- **T0**: Canonicalization SDK and shared hashing/normalization rules.
- **T1**: Stop mixed-authority retrieval behaviors.
- **T2**: Add v2 additive schema and indexes.
- **T3**: Dual-write evidence ingest to v2 surfaces.
- **T4**: Durable extraction-results pipeline.
- **T4b**: Quarantine pipeline for low-confidence/weakly grounded candidates.
- **T5**: Predicate policy service + versioning.
- **T6**: Entity resolution v2.
- **T7**: Claim resolution v2.
- **T8**: Canonical mutation log + watermarks.
- **T9**: Projection builders v2.
- **T10**: `/v2/memory/query` implementation.
- **T11**: Legacy client compatibility adapter routed to v2 only.
- **T12a**: Offline replay, diffing, and audit harness.
- **T12b**: Live shadow-read diffing and rollout audit dashboard.
- **T13**: Continuous invariants + repair jobs.
- **T14**: Cohort rollout + rollback controls.
- **T15**: Legacy Graphiti-era deprecation and cleanup.

## 10. Current State
Current repository contains mixed-authority memory system. Recent changes partially contain this, but full v2 architecture is not yet implemented.

## 11. Non-goals
- Not a personality dossier.
- Not a summary-as-truth system.
- Not a graph-first system.
- Not a retrieval-everything system.
