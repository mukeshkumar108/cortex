# Graphiti-First Root Cause Audit (2026-04-09)

Scope: substrate-level audit for real user `tenant=default`, `user=cmkqxf72t0000lb04axesvlpx`.

Goal: determine where Graphiti itself fails to represent clean autobiographical truth, independent of Synapse startbrief wrappers.

## 1) Exact input sent into Graphiti

### 1.1 Ingest path (actual implementation)

- `session_transcript.messages` is the source payload.
- `session_raw_episode` job reads transcript messages and calls `graphiti_client.add_session_episode(...)`.
- `add_session_episode(...)` formats full transcript as plain text:
  - `User: ...`
  - `Assistant: ...`
  - newline-joined.
- Episode written to Graphiti as `Episodic` node with `source=EpisodeType.message` and `source_description=synapse_conversation`.

### 1.2 Verified live episode content samples

For `session_raw_cmnrrndda0001l604f9hnkexv`:
- content preview:
  - `User: Sophie, are you still there?`
  - `Assistant: I'm here, Mukesh. Always here when you need to talk. What's on your mind today?`
- content length: `125` chars.

For `session_raw_cmnrjhd8o0001l404vtcrlw5a`:
- content length: `4763` chars.
- includes full correction conversation (Ashley relationship clarification + memory correction discussion).

Conclusion: Graphiti is receiving full session transcript text for successful ingest jobs.

## 2) Ingest reliability (substate completeness risk)

For this audited user:

- `session_raw_episode` failure exists:
  - session `cmnq7azol0001jo04i2uwe7yq`
  - error: `validation error: missing transcript messages for session raw episode`
  - transcript message_count for that session = `0`

- Recent successful example:
  - session `cmnrrndda0001l604f9hnkexv`
  - transcript message_count = `2`
  - `session_raw_episode` + `post_ingest_hook(session_summary/open_loops)` all `sent`.

Conclusion: ingest is mostly working, but missing transcript payloads directly create substrate holes (Graphiti never sees those episodes).

## 3) Entity/fact extraction quality on real sessions

### 3.1 What works

Direct exact-node checks:
- `Ashley`: exists, rich fact set, linked episodes, strong relationship evidence.
- `Jasmine`: exists, linked episodes, relationship evidence.
- `Sophie`: exists, linked episodes, lighter relationship semantics.

### 3.2 What fails

Exact-node checks:
- `Bluum`: not found as exact entity node.
- `Yoshi`: not found as exact entity node.

Meaning: autobiographically important entities are incompletely extracted/canonicalized.

### 3.3 Fact quality drift examples

Facts include useful statements and noisy/generalized ones together, e.g.:
- good: `Ashley is the User's girlfriend.`
- noisy/general: `Ashley is related to the season of Spring arriving...`

This indicates extraction is not constrained enough for autobiographical memory fidelity.

## 4) Duplicate/canonicalization issues

Live graph counts for this user:

- Node labels:
  - `Entity: 66`
  - `Episodic: 18`
  - `SessionSummary: 18`
  - `Environment: 4`
  - `Tension: 2`
  - `Observation: 1`

- Duplicate entity names:
  - `User` appears as `3` distinct `Entity` nodes (different UUIDs).

- Session summary pollution:
  - `18` nodes with `name` starting `session_summary_*` and labels `Entity, SessionSummary`.

Why this matters:
- duplicate core identity nodes split autobiographical facts.
- session-summary entities inflate retrieval pools with synthetic nodes.

## 5) Node-type pollution in retrieval pools

Broad queries return mixed junk + real entities.

Example query: `important people in this users life`
- top entities include `User`, `User`, `User`, `diaspora`, `grandad`, `quick update`, `session_summary_*`, `major update`.
- actual key people like `Ashley`/`Jasmine` are not consistently top-ranked.

Example query: `key projects in this users life`
- top entities include `User`, `major update`, `quick update`, `session_summary_*`, `cell SDK`.

Example query: `system memory problem`
- top entity is `system memory problem` with labels `Entity,Tension`.
- also returns `session_summary_*`, `major update`, `file`, `code base`.

Conclusion: retrieval pools are polluted by:
- synthetic summary nodes,
- generic noun phrases,
- tension/system-debug artifacts,
- duplicated `User` identity nodes.

## 6) Confidence/salience signal availability in Graphiti

### 6.1 Fact relevance
- `search_facts` returns `relevance` field, but live values are frequently `null` in this dataset.

### 6.2 Node salience
- salience is not consistently present on durable entity nodes.
- only `SessionSummary` nodes currently carry salience in this graph snapshot.

Live stats:
- total `Entity` nodes: `66`
- nodes with salience property: `18`
- those `18` are the `SessionSummary` entities.

Conclusion: Graphiti does not currently provide reliable native confidence/salience for person/project ranking.

## 7) Graph construction model and extraction setup

Runtime config in container:
- `GRAPHITI_LLM_MODEL` unset
- `GRAPHITI_LLM_API_KEY` unset
- `OPENAI_API_KEY` present

So Graphiti falls back to default OpenAI client/model in `graphiti_core`.

Default model path in installed package:
- default OpenAI model: `gpt-4.1-mini` (`graphiti_core/llm_client/openai_base_client.py`).

Current Synapse extraction policy (`src/graphiti_client.py`):
- custom types include `Tension`, `Environment`, `Observation`, `UserFocus`, `MentalState`
- custom instruction explicitly says: `Continue extracting generic entities (names, places, projects)`

Root effect:
- graph construction currently encourages broad noun-phrase extraction, not just durable autobiographical entities.

## 8) Is this query-path-only, or graph-malformed too?

Both.

### 8.1 Query-path problems
- retrieval does not hard-constrain output to autobiographical entity classes.
- session summary entity nodes and tension nodes enter the same retrieval pool as real people.

### 8.2 Graph-malformation problems
- duplicate identity entities (`User` x3).
- missing important entities (`Bluum`, `Yoshi`).
- semantically low-value nodes treated as first-class entities (`major update`, `quick update`, `file`).
- summary artifacts persisted as searchable `Entity` nodes (`session_summary_*`).

Verdict: substrate quality is insufficient for clean autobiographical answers without repair.

## 9) Root causes (Graphiti-first)

1. Extraction objective is too broad for autobiographical memory quality.
2. Canonicalization is weak for low-entropy/common identity names.
3. Session summaries are stored as searchable entities in same namespace as durable entities.
4. Retrieval lacks class-level constraints and artifact suppression.
5. Missing-ingest sessions create blind spots.

## 10) What to fix first (substate before surfaces)

1. Split namespaces in retrieval:
- exclude `SessionSummary` nodes from autobiographical entity queries.
- exclude `Tension`/`Observation`/`Environment` unless query explicitly requests state/context.

2. Add canonical identity binding:
- enforce one canonical user node per group (`User`/known username aliases -> single UUID).

3. Tighten entity extraction policy:
- prioritize durable entities (`person`, `project`, `organization`, `place`).
- suppress generic ephemeral noun phrases unless repeated across episodes.

4. Add post-extraction curation pass:
- merge duplicates (`User` nodes).
- demote or delete low-value artifact entities (`major update`, `quick update`, `file`).

5. Improve entity coverage for known key names:
- explicit reinforcement for product names (`Bluum`) and relationship satellites (`Yoshi`).

6. Keep ingest integrity guardrails:
- block `session_raw_episode` enqueue when transcript message array is empty.

## 11) Bottom line

This is not only a startbrief wrapper issue.

Graphiti currently contains real autobiographical truth (`Ashley`, `Jasmine`, `Sophie`) but also enough structural pollution and canonicalization gaps that broad autobiographical queries become noisy and unreliable. Substrate cleanup is required before adding more entity surfaces.
