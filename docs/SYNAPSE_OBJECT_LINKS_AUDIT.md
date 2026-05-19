# Synapse Cross-Domain Object Links Audit

**Date:** 2026-05-19  
**Status:** Audit Complete / Implementation Pending  
**Specification Reference:** `docs/SYNAPSE_OBJECTS_AND_DOMAINS.md` (Doctrine)

---

## 1. Current State of `memory_relationship_links`

### What is it today?
`memory_relationship_links` is a relational table designed to store typed, directional edges between disparate system objects. It acts as the "connective tissue" between entities, threads, and sessions.

**Schema Highlights:**
*   `source_type` / `source_id` (Text)
*   `target_type` / `target_id` (Text)
*   `relationship_type` (Text)
*   `confidence` (Double)
*   `source_session_ids` (Text[])
*   `source_turn_refs` (JSONB)
*   `metadata` (JSONB)

### Code & Pipelines
*   **Writer:** `src/derived_pipeline.py` via the `_write_relationship_link` helper.
*   **Active Passes:**
    1.  **PASS1_5_ENTITIES:** Creates `entity` -> `mentioned_in` -> `session` links.
    2.  **PASS3_THREADS:** Creates `thread` -> `involves` -> `entity` links.

### Relationship Types in Use
*   `mentioned_in`: Connects a canonical entity to the session where it was discussed.
*   `involves`: Connects an open thread (Workstream) to the entities participating in it.

---

## 2. Fragmented Linkages (Legacy/Loose Links)

Significant linkage still exists outside the formal `memory_relationship_links` system, stored as arrays or JSONB blobs:

| Table | Field | Link Target | Format |
| :--- | :--- | :--- | :--- |
| `open_threads` | `related_entities` | `entity_profiles` | `TEXT[]` |
| `action_items` | `source_ref` | `candidates` or `external` | `JSONB` |
| `calendar_items`| `source_ref` / `evidence_refs` | `session` / `external` | `JSONB` |
| `action_updates`| `target_action_item_id` | `action_items` | `UUID` |
| `action_updates`| `source_ref` | `session` / `candidates` | `JSONB` |

---

## 3. Doctrine Alignment Gap

| Requirement | `memory_relationship_links` Status | Gap / Missing |
| :--- | :--- | :--- |
| **Typed/Directional** | Yes. | None. |
| **Object Metadata** | Partial. | Missing `primary_domain`, `status` (lifecycle), and `salience`. |
| **Unified Source** | No. | Many domains still use internal arrays (`related_entities`) or JSONB blobs. |
| **Lifecycle Support**| No. | No concept of `detected` vs `confirmed` links; all links are treated as "active" once written. |

---

## 4. Evaluation & Recommendation

### Does it satisfy the Object Link model?
**Partially.** The table structure is sound for basic graph-like connectivity, but it is not yet "Object-Aware" (missing universal fields) and is not the "Universal" edge store it needs to be.

### Strategy: Evolve or Replace?
**Recommendation: Evolve and Rename.**
`memory_relationship_links` should be evolved into `object_links`. The "Memory" prefix is becoming legacy as we shift toward the "Synapse Object" nomenclature.

---

## 5. Implementation Roadmap

### Smallest 3 Tasks for Coherence
1.  **Schema Unification:** Rename `memory_relationship_links` to `object_links`. Add `status` (Enum: `detected`, `confirmed`, `archived`), `salience` (Int), and `primary_domain` (defaulting to 'relationship').
2.  **Pipeline Upgrade:** Update `PASS1_5` and `PASS3` to write to `object_links` with appropriate `status` (e.g., `confirmed` for explicit mentions, `detected` for inferred involvement).
3.  **Legacy Deprecation:** Add a post-processing step to the threads pipeline that populates `object_links` from `related_entities` and marks the `related_entities` column as `DEPRECATED` in documentation (ready for eventual removal).

---

## Appendix: Participating Domains Today
*   **Domain II (People):** `entity_profiles`
*   **Domain IV (Workstreams):** `open_threads`
*   **Domain VII (Events):** `calendar_items` / `memory_events`
*   **Domain VIII (State):** `session_classifications` (as a target for "mentioned_in")
