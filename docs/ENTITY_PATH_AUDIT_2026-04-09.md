# Entity Path Audit (2026-04-09)

Goal: verify whether Graphiti already has the right entity substrate and whether Synapse runtime `entity_hints` building is distorting it.

Audited user: `tenant_id=default`, `user_id=cmkqxf72t0000lb04axesvlpx`.

## 1) End-to-end path (current)

1. Session close/enqueue writes full transcript to `session_transcript` and enqueues `graphiti_outbox` `job_type=session_raw_episode`.
2. Outbox drain sends raw transcript to Graphiti via `add_session_episode(...)`.
3. Graphiti extracts entities from episodes using:
   - generic entity extraction (names, places, projects)
   - Synapse custom entity types: `MentalState`, `Tension`, `Environment`, `Observation`, `UserFocus`
4. Startbrief runtime builds `entity_hints` via `_build_entity_candidates(...)`:
   - uses recent summary text + top loops + user-model relationship seeds
   - queries Graphiti `search_nodes(...)`
   - mixes graph results with user-model relationship fallback and summary-recurrence fallback

## 2) Ingest reliability audit (raw transcripts -> Graphiti)

### 2.1 For audited user

From `graphiti_outbox`:
- `session_raw_episode sent = 51`
- `session_raw_episode failed = 1`
- pending now = 0

Failure observed:
- session `cmnq7azol0001jo04i2uwe7yq`
- error: `validation error: missing transcript messages for session raw episode`

Interpretation:
- Path is mostly working now, but not perfect. A single failure can drop one session's entity evidence from downstream processing.

### 2.2 Global snapshot

- `session_raw_episode failed = 7` (global)
- `post_ingest_hook failed = 6` (global)
- `turn failed = 9` (global)

Interpretation:
- Reliability is decent but not lossless. Distortion risk increases when failures cluster around key sessions.

## 3) How Graphiti currently creates entities

Code path: `src/graphiti_client.py` `add_episode(...)`.

- Every session raw transcript is sent with:
  - `entity_types = NARRATIVE_ENTITY_TYPES`
  - `edge_types = NARRATIVE_EDGE_TYPES`
  - `custom_extraction_instructions = NARRATIVE_EXTRACTION_INSTRUCTIONS`

Custom entity types include:
- `Tension` (explicit unresolved blocker)
- `MentalState`
- `Environment`
- `Observation`
- `UserFocus`

Important detail:
- Instructions also explicitly say to continue extracting generic entities (names/projects/places).
- So Graphiti substrate is a blend: durable entities + situation/tension entities.

## 4) Direct Graphiti entity check for expected names

Queried Graphiti directly via `/internal/debug/graphiti/query` with: `Ashley`, `Jasmine`, `Sophie`, `Bluum`, `Yoshi`.

Results (top-node exact presence):
- `Ashley`: exact entity present
- `Jasmine`: exact entity present
- `Sophie`: exact entity present
- `Bluum`: not found as exact entity in top results
- `Yoshi`: not found as exact entity in top results

Observed Graphiti node quality:
- Good: durable names exist (`Ashley`, `Jasmine`, `Sophie`, `Mukesh`).
- Noisy: many generic nodes (`quick update`, `messy middle`, `code base`, `file`, `User`).
- Situation nodes present (`Tension`, `Environment`) and can dominate retrieval for certain queries.

## 5) Current startbrief `entity_hints` vs Graphiti direct

Current `/session/startbrief` output:
- `entity_hints = [{ name: "system memory problem", type: "other", ... }]`

Comparison:
- Graphiti already contains stronger person entities (`Ashley`, `Jasmine`, `Sophie`).
- Runtime `entity_hints` surfaced only one low-value tension-like entity.

Conclusion:
- Distortion is happening in runtime hint construction/ranking, not because Graphiti has no relevant entities.

## 6) Where junk entity "system memory problem" enters

Direct Graphiti query confirms node exists:
- `summary = "system memory problem"`
- `type = "Tension"`
- created at `2026-04-08T14:20:00Z`

How it likely got created:
- User said they were trying to fix assistant memory problem.
- Graphiti custom extraction captured that as `Tension` (valid under current extraction rules).

Why it appears in startbrief hints:
- `_build_entity_candidates(...)` seeds query context from recent summary texts.
- Recent summary includes phrase: "trying to fix the assistant's memory problem".
- Graphiti `search_nodes` for that context returns the `Tension` node strongly.
- Current hint builder does not strongly demote/exclude operational/system tensions for startup entities.

## 7) Root causes of entity-hint distortion

1. Runtime query context over-weights recent summaries and transient concerns.
2. Candidate type filtering is too permissive for startup identity entities.
3. Ranking does not sufficiently prioritize durable people/projects over transient `Tension`/generic nodes.
4. Fallback layers (summary recurrence + user model) are mixed into one output without strict class separation.

## 8) Graphiti-first startup entity builder (proposed)

Target output:
- list of simple items: `{name, type, role, importance}`

### 8.1 Principles

- Source of truth: Graphiti nodes/facts first.
- Runtime should not invent entities from recent-chat cleverness.
- Startup list should prefer durable entities (people/projects/products) and suppress transient/system noise.

### 8.2 Candidate selection

1. Query Graphiti node search with node-only config for broad durable intents:
- `important people in user's life`
- `active projects and products`
- `family relationship names`

2. Include only allowed entity classes for startup list:
- allow: person, project/product, company, place (optional)
- suppress by default: `Tension`, `Environment`, generic unlabeled artifacts (`quick update`, `file`, `code base`, `User`, `Assistant`)

3. Merge with relationship entities from `user_model.key_relationships` only as reconciliation layer (not primary source).

### 8.3 Role derivation

Priority order:
1. explicit role from `user_model.key_relationships`
2. role extracted from Graphiti facts mentioning the entity (pattern-based role map)
3. null

### 8.4 Importance scoring

Compute deterministic score in [0,1]:
- recurrence score: mentions/facts across last N days
- recency score
- loop-link score (if entity appears in active loop text/reason)
- relationship boost (if role exists)
- project boost (if repeatedly referenced in session summaries/facts)

Then map to buckets:
- `high` >= 0.75
- `medium` >= 0.5
- `low` otherwise

### 8.5 Output contract

For startup:
```json
[
  {"name":"Ashley","type":"person","role":"girlfriend","importance":"high"},
  {"name":"Jasmine","type":"person","role":"daughter","importance":"high"},
  {"name":"Sophie","type":"project","role":"active_project","importance":"high"},
  {"name":"Bluum","type":"project","role":"active_project","importance":"medium"}
]
```

## 9) Immediate implementation guardrails

1. In startbrief entity hints, hard-filter out `Tension` unless no durable entities exist.
2. Hard-block generic non-entity strings (`quick update`, `major update`, `file`, `code base`, `User`, `Assistant`).
3. Rank `person/project` above all others when generating startup entity list.
4. Add provenance per entity (`graphiti_node`, `graphiti_fact`, `user_model`) for explainability.
5. Add freshness metadata (`last_seen_at`) and minimum recency threshold for startup list.

## 10) Bottom line

Graphiti already contains useful durable entities for this user. The current startup `entity_hints` path is over-indexing transient context and allowing noisy node classes into the final list. A Graphiti-first, type-gated, deterministic ranking builder should fix this without relying on recent-chat inference tricks.

