# Graphiti Substrate Audit (2026-04-09)

Scope: direct Graphiti audit for real user `default / cmkqxf72t0000lb04axesvlpx`, then comparison to current Synapse `entity_hints` / `entity_profiles`.

## 1) Ingest reliability (raw transcripts -> Graphiti)

For this user:
- `session_raw_episode`: `sent=51`, `failed=1`, `pending=0`
- failed session: `cmnq7azol0001jo04i2uwe7yq`
- failure reason: `validation error: missing transcript messages for session raw episode`

Conclusion:
- Ingest is mostly reliable for this user, but not lossless.
- Missing episodes can still drop entity evidence.

## 2) Direct Graphiti entity checks (requested entities)

### 2.1 Ashley
- Node exists: yes
- Node UUID: `29d9b1f1-e7c3-416b-9d41-5f58e7f17c29`
- Type/labels: `labels=[Entity]` (no explicit richer type)
- Aliases: none
- Attached facts: strong relationship facts present, including long-distance relationship, breakup/reconciliation timeline, girlfriend mention
- Linked episodes: present (multiple recent `session_raw_*`, including 2026-04-09)
- Relationship-to-user evidence: present in facts (`User and Ashley...`) but modeled as generic `RELATES_TO` facts, not explicit role edge

### 2.2 Jasmine
- Node exists: yes
- Node UUID: `3a31b7b6-1ae0-469c-816b-3a4938c78111`
- Type/labels: `labels=[Entity]`
- Aliases: none
- Attached facts: present (reconnecting, text sent)
- Linked episodes: present
- Relationship-to-user evidence: present in facts, again generic `RELATES_TO`

### 2.3 Bluum
- Node exists: no exact node
- Type/aliases: none
- Semantic neighborhood: `cell SDK`, `Vercel AI SDK`, `quick update`, etc. (engineering artifacts)
- Attached fact search: no direct Bluum facts surfaced
- Linked episodes: none for exact Bluum entity
- Relationship-to-user evidence: none

### 2.4 Sophie
- Node exists: yes
- Node UUID: `e379c16b-160c-4494-90fc-f3f9abe5d76f`
- Type/labels: `labels=[Entity]`
- Aliases: none
- Attached facts: minimal (greeting/check-in style fact)
- Linked episodes: present
- Relationship-to-user evidence: weak, generic `RELATES_TO` fact only

### 2.5 Yoshi
- Node exists: no exact node
- Type/aliases: none
- Semantic neighborhood: `Sophie`, `walk`, `Ashley`, `User`, `Mukesh` (no distinct Yoshi node)
- Attached fact search: no direct Yoshi fact
- Linked episodes: none for exact Yoshi entity
- Relationship-to-user evidence: none

## 3) Top person/project entities from Graphiti

### 3.1 Graphiti semantic query: "important people in the user life"
Top results include substantial noise:
- multiple `User` nodes
- `major update`, `diaspora`, `quick update`, `SessionSummary` nodes
- `Jasmine` appears, but buried

### 3.2 Graphiti semantic query: "active projects and products the user is building"
Top results again noisy:
- multiple `User` nodes
- `foundational groundwork`, `quick update`, `cell SDK`, `major update`, `file`
- product entities are not cleanly separated from implementation artifacts

### 3.3 Direct graph ranking (node recency+degree, with lightweight filtering)
Durable entities are present (`Ashley`, `Jasmine`, `Sophie`, `Mukesh`) but mixed with noisy generic summaries and duplicate user-style nodes.

## 4) Synapse runtime comparison (`/session/startbrief`)

Current output:
- `entity_hints`: only `system memory problem`
- `entity_profiles`: empty

Direct Graphiti contradiction:
- Graphiti already has meaningful nodes for `Ashley`, `Jasmine`, `Sophie` with supporting facts + recency.

Interpretation:
- Runtime builder is currently distorting the substrate by selecting transient/system tension over durable personal entities.

## 5) Where junk/drift enters

1. Graphiti extraction intentionally includes `Tension` and generic entities.
2. Recent summary text contains "memory problem" phrasing.
3. `_build_entity_candidates(...)` seeds query context from those summaries.
4. `search_nodes` returns `Tension` and generic entities strongly.
5. Runtime ranking/filtering does not hard-prioritize durable person/project classes.
6. Result: `system memory problem` wins while `Ashley/Jasmine/Sophie` are not surfaced.

## 6) Is Graphiti good enough to be primary source?

Short answer: **yes, with guardrails**.

- For core personal entities (`Ashley`, `Jasmine`, `Sophie`), Graphiti substrate is already good enough to be primary.
- For product and secondary relationship entities (`Bluum`, `Yoshi`), graph coverage is incomplete and needs enrichment.
- The larger immediate issue is selection/filtering at Synapse runtime, not total absence of Graphiti signal.

## 7) What needs fixing first

### P0 (runtime selection)
- Make Graphiti primary for startup entities, but hard-filter classes:
  - allow: durable person/project/company
  - suppress: `Tension`, `Environment`, generic artifacts (`quick update`, `file`, etc.)
- Deduplicate `User`/`Assistant` style nodes.

### P1 (graph quality)
- Improve product/entity extraction for named products (`Bluum`) and relationship satellites (`Yoshi`).
- Add alias capture and canonicalization where possible.

### P2 (relationship semantics)
- Promote relationship role evidence (`girlfriend`, `daughter`, etc.) into explicit structured role edges or normalized facts to avoid fragile role inference.

## 8) Bottom line

Graphiti is already viable as the primary startup entity substrate for major personal entities, but only if Synapse stops using permissive mixed-class runtime hinting. Use Graphiti-first retrieval with strict durable-entity filtering and deterministic ranking; then iterate graph extraction quality for missing entities like Bluum/Yoshi.

