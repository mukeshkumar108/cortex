# Synapse Table Role Map

**Date:** 2026-05-19  
**Status:** Architectural Reference  
**Specification Reference:** `docs/SYNAPSE_OBJECTS_AND_DOMAINS.md`

---

## 1. Primary Object Tables (Nodes)
These tables represent the "Ground Truth" Synapse Objects. They should eventually implement the full **Synapse Object Contract**.

| Table | Domain | Purpose | Object Contract | Existing Fields | Missing Fields | Recommendation |
| :--- | :--- | :--- | :---: | :--- | :--- | :--- |
| `action_items` | VI: Obligations | Todos, reminders, habits. | **YES** | `id`, `tenant_id`, `user_id`, `status`, `confidence` | `primary_domain`, `salience` | **Migrate:** Add missing fields. |
| `calendar_items` | VII: Events | Appointments/milestones. | **YES** | `id`, `tenant_id`, `user_id`, `status`, `confidence` | `primary_domain`, `salience` | **Migrate:** Add missing fields. |
| `entity_profiles` | II: People | Canonical people/places/orgs. | **YES** | `canonical_name`, `user_id`, `status` | `id` (UUID), `primary_domain`, `confidence` | **Migrate:** Add UUID and domain. |
| `open_threads` | IV: Workstreams | Ongoing situations/projects. | **YES** | `thread_id`, `user_id`, `status`, `priority` | `primary_domain`, `salience`, `confidence` | **Migrate:** Unify priority -> salience. |
| `durable_profile_facts` | I: Profile | Long-lived user traits. | **YES** | `user_id`, `fact_text` | `id`, `status`, `confidence`, `salience` | **Migrate:** Turn into full objects. |
| `loops` | (Various) | Legacy commitments/habits. | **YES** | `id`, `status`, `confidence`, `salience` | `primary_domain` | **Legacy:** Deprecate in favor of domain-specific tables. |

---

## 2. Link Tables (Edges)
The "Connective Tissue" between objects. They do NOT have a `primary_domain` as they bridge domains.

| Table | Purpose | Object Contract | Key Fields | Target Action |
| :--- | :--- | :---: | :--- | :--- |
| `memory_relationship_links`| Typed directional edges. | **Partial** | `source_type`, `target_type`, `link_type`, `confidence` | **Rename:** To `object_links`. |
| `memory_contradictions` | Conflict tracking between facts. | **Partial** | `topic`, `earlier_view`, `recent_view`, `status` | **Leave Alone:** Specialized link type. |

---

## 3. Candidate Staging Tables (Lifecycle Stage 0)
Transient tables for extraction results before they are "promoted" to objects.

| Table | Likely Domain | Purpose | Object Contract | Recommendation |
| :--- | :--- | :--- | :---: | :--- |
| `actionable_candidates` | VI: Obligations | Unconfirmed todos. | **YES** | **Migrate:** Unify with `action_items` via `status='detected'`. |
| `follow_up_candidates` | IX: Opportunities | Suggested check-ins. | **YES** | **Migrate:** Unify with `opportunities` (Domain IX). |
| `clarification_candidates`| IX: Opportunities | Questions for the user. | **YES** | **Migrate:** Unify with `opportunities`. |
| `entity_candidates` | II: People | Potential new entities. | **YES** | **Migrate:** Unify with `entity_profiles` via `status='detected'`. |
| `low_confidence_items` | (Various) | High-noise extractions. | **Partial** | **Keep:** For debugging/quarantine. |

---

## 4. Synthesis Snapshots (Derived/Replaceable)
Highly volatile, synthesized views. They are NOT objects; they are "state summaries" of objects.

| Table | Purpose | Object Contract | Fields to OMIT | Action |
| :--- | :--- | :---: | :--- | :--- |
| `living_context` | 24-48h user state. | **NO** | `salience`, `confidence` | **Leave Alone:** Regenerated frequently. |
| `identity_profile` | Synthesis of Profile facts. | **NO** | `salience`, `status` | **Leave Alone:** Derived from evidence. |
| `always_on_memory_packets`| Runtime LLM payload. | **NO** | (Everything but metadata/ID) | **Leave Alone:** Serving artifact. |

---

## 5. Evidence & Event Log Tables
The immutable foundation. These should NOT have `status`, `salience`, or `updated_at` (unless for sync).

| Table | Role | Purpose | Recommendation |
| :--- | :--- | :--- | :--- |
| `session_transcript` | Evidence | Raw conversation logs. | **Immutable:** Do not touch. |
| `claims` | Evidence | Individual atomic assertions. | **Immutable:** Base evidence layer. |
| `claim_evidence` | Evidence | Mapping claims to turns. | **Immutable:** Base evidence layer. |
| `action_audit_log` | Event Log | Lifecycle history of actions. | **Immutable:** Audit trail. |
| `calendar_audit_log` | Event Log | Lifecycle history of events. | **Immutable:** Audit trail. |
| `session_changes` | Event Log | Incremental state diffs. | **Immutable:** Event sourcing. |

---

## 6. Serving Cache & Infrastructure
Transient or optimized structures for the runtime.

| Table | Role | Purpose | Recommendation |
| :--- | :--- | :--- | :--- |
| `identity_cache` | Serving Cache | Fast user identity lookup. | **Leave Alone:** Managed by Graphiti sync. |
| `episodic_memory_embeddings` | Serving Cache | Vector search for session recall. | **Leave Alone:** Managed by embedding worker. |
| `projection_latest` | Serving Cache | Latest state of Graphiti nodes. | **Leave Alone:** Infrastructure. |
| `pipeline_checkpoints` | Infrastructure | Cursors for workers. | **Leave Alone:** Infrastructure. |

---

## Summary of Metadata Field Application

| Contract Tier | `id` | `tenant_id` | `primary_domain` | `status` | `confidence` | `salience` | `metadata` | `updated_at` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Primary Objects** | REQUIRED | REQUIRED | REQUIRED | REQUIRED | REQUIRED | REQUIRED | REQUIRED | REQUIRED |
| **Links** | REQUIRED | REQUIRED | OMIT | REQUIRED | REQUIRED | STRENGTH | REQUIRED | REQUIRED |
| **Evidence** | ID | REQUIRED | OMIT | OMIT | OMIT | OMIT | REQUIRED | OMIT |
| **Logs** | ID | REQUIRED | OMIT | OMIT | OMIT | OMIT | REQUIRED | OMIT |
| **Snapshots** | OMIT | REQUIRED | OMIT | OMIT | OMIT | OMIT | REQUIRED | REQUIRED |
