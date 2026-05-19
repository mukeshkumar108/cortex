# Synapse Object Link Specification

**Status:** Draft / Target Specification  
**Date:** 2026-05-19  
**Specification Reference:** `docs/SYNAPSE_OBJECTS_AND_DOMAINS.md` (Doctrine)

---

## 1. Introduction: The Object Link

In the Synapse intelligence layer, **Objects** (Nodes) represent the "what" and "who," while **Object Links** (Edges) represent the "how" and "why." An Object Link is a typed, directional connection between two Synapse Objects that encodes a semantic relationship.

While Objects capture stateful representations of entities (People, Obligations, Workstreams, etc.), Links capture the **logic of involvement**. 

### Object vs. Link: The Distinction
| Attribute | Synapse Object | Synapse Object Link |
| :--- | :--- | :--- |
| **Metaphor** | A Node in the graph. | An Edge in the graph. |
| **Identity** | Durable across sessions; has a lifecycle. | Dependent on the existence of its endpoints. |
| **Domain** | Belongs to exactly one `primary_domain`. | Cross-domain by nature; defined by (Source Domain -> Target Domain). |
| **Purpose** | Stores facts and state about a concept. | Stores the context of a relationship. |

### Why Links lack a `primary_domain`
Unlike Objects, a Link does not "belong" to one of the 9 intelligence domains. Instead, it **mediates** between them. Assigning a Link to a `primary_domain` (e.g., Domain VI: Obligations) would create ambiguity when it connects an Obligation to a Workstream. Links are identified by their `link_type` and the domain-pair they connect.

---

## 2. Target Schema: `object_links`

The following schema defines the target contract for the `object_links` table (currently `memory_relationship_links`).

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID / BIGSERIAL | Unique identifier for the link. |
| `tenant_id` | Text | Workspace isolation key. |
| `user_id` | Text | User ownership key. |
| `source_object_id` | Text / UUID | ID of the origin object. |
| `source_domain` | Enum | Domain of the source (e.g., `workstream`, `person`). |
| `source_type` | Text | Domain-specific type (e.g., `thread`, `entity`). |
| `target_object_id` | Text / UUID | ID of the destination object. |
| `target_domain` | Enum | Domain of the target (e.g., `state`, `goal`). |
| `target_type` | Text | Domain-specific type (e.g., `session`, `goal`). |
| `link_type` | Text | Semantic nature of the link (see Section 4). |
| `status` | Enum | Lifecycle state (e.g., `detected`, `active`). |
| `confidence` | Float (0-1) | Synapse's certainty that this link exists. |
| `strength` | Float (0-1) | The semantic weight or "salience" of the connection. |
| `source_session_ids`| Text[] | Provenance: sessions that corroborated this link. |
| `source_turn_refs` | JSONB | Provenance: specific turns providing evidence. |
| `metadata` | JSONB | Link-specific attributes. |
| `created_at` | Timestamp | Initial detection time. |
| `updated_at` | Timestamp | Last update or corroboration. |
| `valid_from` | Timestamp | Start of temporal validity (e.g., task becomes active). |
| `valid_until` | Timestamp | End of temporal validity (e.g., task completion). |
| `expires_at` | Timestamp | TTL for state-based or high-decay links. |

---

## 3. Link Lifecycle (Statuses)

Links transition through a lifecycle similar to Objects, driven by confidence and corroboration.

*   **`detected` (Candidate):** Inferred by a pipeline (e.g., Pass 3 threads) but not yet confirmed by user behavior or multi-session corroboration.
*   **`confirmed`:** Explicitly acknowledged by the user or validated by high-confidence extraction.
*   **`active`:** The currently relevant and operational state of a link.
*   **`stale`:** A link that has outlived its temporal relevance (e.g., a "mentioned_in" link to a session from 3 months ago).
*   **`dismissed`:** Explicitly rejected by the user.
*   **`archived`:** Retained for historical context but no longer used for runtime reasoning.
*   **`contradicted`:** Retained as evidence of a conflict that requires resolution.

---

## 4. Standard Link Types (v1)

| Link Type | Semantic Description | Domain Example |
| :--- | :--- | :--- |
| `mentioned_in` | Entity was discussed in a session. | People -> State (Session) |
| `involves` | Entity is a participant in a situation. | Workstreams -> People |
| `belongs_to_workstream` | A task or event is part of a larger epic. | Obligations -> Workstreams |
| `supports_goal` | An action contributes to a high-level goal. | Obligations -> Goals |
| `blocks_goal` | A friction or commitment hinders progress. | Workstreams -> Goals |
| `creates_obligation` | A statement or situation generates a task. | State -> Obligations |
| `resolves_obligation` | A behavior fulfills a requirement. | State -> Obligations |
| `relates_to_habit` | An event or action is part of a routine. | Events -> Habits |
| `scheduled_for` | An obligation or workstream has a time slot. | Obligations -> Events |
| `depends_on` | Sequential dependency between objects. | Obligations -> Obligations |
| `waiting_on` | Blocked by an external entity or event. | Obligations -> People |
| `affects_state` | An event or task impacts user wellbeing. | Events -> State |
| `triggered_by_state` | A state change prompts an opportunity. | State -> Opportunities |
| `creates_opportunity` | A situation enables a proactive suggestion. | Workstreams -> Opportunities |
| `suggested_by` | A candidate is derived from specific evidence. | Opportunities -> State |
| `evidence_for` | A session/fact supports an assertion. | State -> Profile |
| `confirms` | New evidence validates a candidate. | State -> Objects (Any) |
| `contradicts` | New evidence disputes an existing fact. | State -> Objects (Any) |
| `updates` | A link representing a mutation over time. | Objects -> Objects |
| `replaces` | A link representing supersession. | Objects -> Objects |

---

## 5. Migration Strategy

To avoid breaking the current `derived_pipeline` while moving toward the target spec, a three-phase approach is required:

### Phase 1: Enhancement (Current Table)
Keep `memory_relationship_links`. 
- Add `status`, `valid_from`, `valid_until`, and `expires_at` columns with sensible defaults.
- Update `src/derived_pipeline.py:_write_relationship_link` to support these new fields.

### Phase 1 Implementation Note
- `memory_relationship_links` remains the physical table in production for backwards compatibility.
- `object_links` remains the conceptual target name for the substrate and future API/schema cleanup.
- Phase 1 now supports `source_domain`, `target_domain`, `status`, `strength`, `valid_from`, `valid_until`, and `expires_at`.
- The table rename to `object_links` is intentionally deferred so current writers and reads do not break.
- For the current `entity -> mentioned_in -> session` writer path, `session` is mapped to `target_domain='evidence'` because the session is acting as provenance/evidence rather than a first-class Synapse Object.

### Phase 2: Unification (Object Links)
Rename `memory_relationship_links` to `object_links` (or create a compatibility view).
- Standardize on `source_object_id` and `target_object_id`.
- Backfill `source_domain` and `target_domain` based on existing `source_type`/`target_type` values.

### Phase 3: Consolidation (Deprecation)
Progressively migrate fragmented linkages:
- Convert `open_threads.related_entities` (Domain IV) into `involves` links in the `object_links` table.
- Convert `action_items.source_ref` (Domain VI) and `calendar_items.source_ref` (Domain VII) into typed links.
- Mark legacy columns as `DEPRECATED`.

---

## 6. Risks & Mitigations

*   **Risk: Pipeline Breakage.** Modifying the write-path helper could cause runtime errors in long-running pipelines.
    *   *Mitigation:* Use optional arguments in `_write_relationship_link` and perform non-breaking schema additions (NULLable columns).
*   **Risk: Over-Normalization.** Turning every session mention into a durable "Object Link" could bloat the graph with noise.
    *   *Mitigation:* Apply the **Corroboration Rule**: only promote `detected` links to `active` if they appear in 3+ sessions or have confidence > 0.8.
*   **Risk: Temporal Decay.** Stale state links (Domain VIII) might be misinterpreted as durable identity links (Domain I).
    *   *Mitigation:* Strict enforcement of `expires_at` for links originating from or targeting the `State` domain.
*   **Risk: Link Explosion.** A high number of links between objects could make retrieval expensive.
    *   *Mitigation:* Implement a `strength` or `salience` field to prune weak edges during RAG/Context construction.

---

## 7. First Implementation Task Recommendation

**Task:** Schema Hardening of `memory_relationship_links`.
Add the lifecycle and temporal columns (`status`, `valid_from`, `valid_until`, `expires_at`, `strength`) to the existing table. This is a non-breaking change that allows pipelines to immediately begin enriching links with temporal metadata without requiring a table rename or code-wide refactor.
