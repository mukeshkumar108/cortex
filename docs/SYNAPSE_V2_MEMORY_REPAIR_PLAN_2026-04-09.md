# Synapse v2 Memory Repair Plan (2026-04-09)

## 1. Architecture note

### Postgres owns operational memory
- session transcripts
- open loops / habits / explicit workflow state
- lifecycle and ingest/write-back status
- trusted explicit identity fields when directly user-backed

### Graphiti owns semantic memory
- people, projects, goals, preferences, events, and loop-linked semantic entities
- semantic facts and relationship edges
- episode grounding and evidence-backed recall

### Backend-facing products
- startup context: `/session/startbrief`
- targeted recall: `/memory/query`
- open loops: `/memory/loops`
- user picture: `/user/model`
- entity profile: `/entities/profile`
- session write-back: `/session/ingest`

The design rule is simple: do not use summaries-of-summaries as primary truth. Runtime products should consume Postgres operational truth and Graphiti semantic truth directly.

## 2. Ontology v1

### Node types
- `Person`
- `Project`
- `Goal`
- `Loop`
- `Preference`
- `Event`

### Domain facets
- `Relationships`
- `Work`
- `Health`
- `Logistics`
- `Lifestyle`
- `Identity`

Domains are facets on nodes and edges, not standalone node identity.

### Edge types
- `related_to_user_as`
- `working_on`
- `pursuing`
- `about`
- `prefers`
- `involved_in`
- `cares_about`
- `evidence_for`

### Example connected structures

`Jasmine`
- `User:Person --related_to_user_as(role=daughter, importance=high)--> Jasmine:Person`
- `User:Person --pursuing--> repair relationship with Jasmine:Goal`
- `repair relationship with Jasmine:Goal --about--> Jasmine:Person`
- `recent conversation / conflict / outreach:Event --about--> Jasmine:Person`
- `recent conversation / conflict / outreach:Event --evidence_for--> repair relationship with Jasmine:Goal`
- domains: `Relationships`, `Identity`

`Ashley`
- `User:Person --related_to_user_as(role=girlfriend, importance=high)--> Ashley:Person`
- `Ashley-related commitments or tensions:Loop --about--> Ashley:Person`
- `shared plans / notable moments:Event --about--> Ashley:Person`
- domains: `Relationships`, `Lifestyle`

`Sophie`
- `User:Person --working_on--> Sophie:Project`
- `shipping Sophie / improving memory quality:Goal --about--> Sophie:Project`
- `build sessions / releases / incidents:Event --about--> Sophie:Project`
- domains: `Work`, `Identity`

`Bluum`
- `User:Person --working_on--> Bluum:Project`
- `ship Bluum / stabilize Bluum:Goal --about--> Bluum:Project`
- `active work loop:Loop --about--> Bluum:Project`
- `delivery events / design decisions:Event --about--> Bluum:Project`
- domains: `Work`, `Identity`

## 3. Root-cause mapping

### Ownership violations in current Synapse
- `startbrief` still over-consumes summaries, narrative glue, and low-trust hints instead of a strict startup truth packet.
- `entity_hints` currently degrade into summary recurrence fallback instead of Graphiti-first canonical entity selection.
- `memory/query` mixes semantic nodes with `SessionSummary` and internal artifact classes in the same search path.
- user-model hints can surface stale or weakly-evidenced material as if current truth.

### Main drift/noise producers
- broad Graphiti extraction guidance that allowed generic noun phrases
- `SessionSummary` persisted as searchable entity namespace peers
- duplicate autobiographical anchor nodes like `User`
- empty-transcript ingest jobs creating silent graph holes
- startup prose generation and helper layers outranking structured truth

## 4. Phase plan

### Phase 1
- Add ontology constants and runtime node filtering in [memory_ontology.py](/opt/synapse/src/memory_ontology.py).
- Tighten Graphiti extraction and node normalization in [graphiti_client.py](/opt/synapse/src/graphiti_client.py).
- Guard empty transcript ingest and expose explicit ingest diagnostics in [session.py](/opt/synapse/src/session.py) and [main.py](/opt/synapse/src/main.py).
- Stop summary-recurrence fallback from driving startup entity hints in [main.py](/opt/synapse/src/main.py).
- Constrain `/memory/query` node retrieval by ontology type in [main.py](/opt/synapse/src/main.py).

### Phase 2
- Rebuild `/session/startbrief` as a strict structured startup packet from trusted identity basics, top entities, loops, and recent changes.
- Harden `/entities/profile` around canonical Graphiti lookup plus evidence-backed enrichment.
- Add a real graph repair job for canonical anchor merge and duplicate consolidation.

### Phase 3
- Rework `/user/model` surfacing around freshness, provenance, and confidence.
- Deprecate low-trust startup layers and stale narrative fields.
- Clean contracts/docs around canonical products only.

## 5. Phase 1 code changes in this repo

Implemented in this pass:
- ontology module for node types, edge types, domain facets, query-type inference, and runtime node filtering
- Graphiti extraction instructions shifted toward durable autobiographical entities and grounded edges
- runtime node normalization/canonicalization for `User`, `Ashley`, `Jasmine`, `Sophie`, `Bluum`, and `Yoshi`
- empty transcript guardrail: `/session/ingest` returns `skipped_empty_transcript`, does not enqueue `session_raw_episode`
- debug ingest status now reports `graph_episode_status`
- startup `entity_hints` no longer fall back to summary-recurrence extraction
- `/memory/query` now constrains returned entity nodes by ontology type and excludes internal graph classes by default

Not yet implemented in code:
- destructive in-graph merge job for duplicate `User` nodes
- exact canonical graph repair mutations for historical aliases
- full Phase 2 startbrief simplification

## 6. Canonical surfaces

### Keep
- `/session/startbrief`
- `/memory/query`
- `/memory/loops`
- `/user/model`
- `/entities/profile`
- `/session/ingest`

### Demote / internal-only
- `SessionSummary` as a retrieval helper, not a first-class autobiographical entity surface
- verbose handover prose as optional output, not primary truth
- narrative/daily-analysis/user-model helper text as secondary and freshness-gated

### Remove as primary truth sources
- summary-recurrence entity hint generation
- startup dependence on stale psychologizing or summary-of-summary synthesis
