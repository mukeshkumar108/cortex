# Synapse Objects and Domains: Codebase Audit

**Status:** Draft Audit  
**Date:** 2026-05-19  
**Specification Reference:** `docs/SYNAPSE_OBJECTS_AND_DOMAINS.md`

---

## 1. Audit Summary
The Synapse codebase has successfully implemented most of the 9 intended domains as discrete tables. However, it fails the **Universal Object Field** requirement across all tables. "Candidate" logic is partially implemented via `status` fields but is often split into separate `_candidates` tables, which violates the "Candidate as a Lifecycle Stage" doctrine.

### Top-Level Missing Universal Fields
| Field | Status across Codebase |
| :--- | :--- |
| `primary_domain` | **Missing everywhere.** No table explicitly stores which domain it belongs to. |
| `salience` | **Partial.** Only exists in `loops` and `open_threads`. Missing in `action_items`, `calendar_items`, and `identity_profile`. |
| `status` | **Present.** But inconsistent enum values (e.g., `active` vs `pending` vs `confirmed`). |
| `confidence` | **Present.** But inconsistent naming (`confidence_score`, `confidence`, `time_confidence`). |

---

## 2. Domain-by-Domain Audit

### I. Profile
*   **Tables:** `identity_profile`, `durable_profile_facts`.
*   **Fields Missing:** `primary_domain`, `salience`.
*   **Lifecycle/Status:** `identity_profile` has no status (assumed always active). `durable_profile_facts` lacks status.
*   **Extraction Pass:** Pass 4 (`run_pass4_identity`).
*   **Serving Endpoint:** `/user/model`, `/profile/truth`, `/signals/pack`.
*   **Smallest Change:** Add `status` and `salience` to `durable_profile_facts`.

### II. People
*   **Tables:** `entity_profiles`, `entity_candidates`.
*   **Fields Missing:** `primary_domain`.
*   **Lifecycle/Status:** `entity_profiles.status` uses `tentative`, `active`, `dormant`, `archived`.
*   **Extraction Pass:** Pass 1.5 (`run_pass1_5_entities`), Pass 2c (`run_pass2c_entity_candidates`).
*   **Serving Endpoint:** `/entities/profile`.
*   **Smallest Change:** Merge `entity_candidates` into `entity_profiles` using the `status` field.

### III. Goals
*   **Tables:** **NONE.**
*   **Legacy/Duplicated:** Goals are currently strings inside `identity_profile.persistent_goals` or `loops.type='decision'`.
*   **Unsafe:** Serving goals as raw strings without status prevents progress tracking.
*   **Smallest Change:** Create a `goals` table with the Universal Object Fields.

### IV. Workstreams
*   **Tables:** `open_threads`.
*   **Fields Missing:** `primary_domain`, `confidence`.
*   **Lifecycle/Status:** `open`, `resolved`, `snoozed`, `archived`.
*   **Extraction Pass:** Pass 3 (`run_pass3_threads`).
*   **Serving Endpoint:** `/signals/pack` (as `active_threads`).
*   **Smallest Change:** Add `confidence` to `open_threads`.

### V. Habits / Routines
*   **Tables:** `action_items` (kind='habit'), `habit_daily_log`.
*   **Fields Missing:** `primary_domain`, `salience`.
*   **Extraction Pass:** Pass 2 (`run_pass2_actionable`).
*   **Serving Endpoint:** `/actions/items`, `/day/brief`.
*   **Smallest Change:** Add `salience` to `action_items`.

### VI. Obligations
*   **Tables:** `action_items` (todo/reminder), `action_updates`, legacy `loops` (commitment).
*   **Fields Missing:** `primary_domain`, `salience` (on `action_items`).
*   **Lifecycle/Status:** `pending`, `done`, `cancelled`, `dismissed`, `archived`.
*   **Extraction Pass:** Pass 2 (`run_pass2_actionable`).
*   **Serving Endpoint:** `/actions/items`, `/day/brief`.
*   **Smallest Change:** Deprecate `loops.type='commitment'` in favor of `action_items`.

### VII. Events
*   **Tables:** `calendar_items`, `memory_events`.
*   **Fields Missing:** `primary_domain`, `salience`.
*   **Lifecycle/Status:** `confirmed`, `cancelled`, `archived`.
*   **Extraction Pass:** Pass 2 (`run_pass2_actionable`) for extraction; manual/sync for calendar.
*   **Serving Endpoint:** `/calendar/items`, `/day/brief`.
*   **Smallest Change:** Add `salience` to `calendar_items` to rank "must-attend" vs "tentative."

### VIII. State
*   **Tables:** `living_context`, `session_classifications`, `session_changes`.
*   **Fields Missing:** `primary_domain`, `salience`, `status` (on `living_context`).
*   **Extraction Pass:** Pass 5 (`run_pass5_living_context`), Pass 1 (`run_pass1_triage`).
*   **Serving Endpoint:** `/signals/pack`, `/internal/debug/always-on-packet`.
*   **Smallest Change:** Add `status` (active/stale) to `living_context` to support the 24-48h decay rule.

### IX. Opportunities
*   **Tables:** `actionable_candidates`, `follow_up_candidates`, `clarification_candidates`, `recent_change_candidates`.
*   **Legacy/Duplicated:** Highly fragmented across 4 tables.
*   **Fields Missing:** `primary_domain`.
*   **Lifecycle/Status:** `detected`, `confirmed`, `dismissed`, `expired`.
*   **Extraction Pass:** `run_proactive_shadow_candidates` worker loop.
*   **Serving Endpoint:** `/daily-candidates`, `/signals/pack`.
*   **Smallest Change:** Consolidate the 4 candidate tables into a single `opportunities` table.

---

## 3. Implementation Priorities
1.  **Schema Hardening:** Add `primary_domain` (enum) and `salience` (int) to all primary tables.
2.  **Status Unification:** Standardize on the lifecycle statuses defined in the Doctrine.
3.  **Startbrief Update:** (High Leverage) Ensure `/session/startbrief` pulls from `living_context` (State) and `open_threads` (Workstreams) instead of legacy `loops`.
4.  **Candidate Consolidation:** Merge the fragmented `_candidates` tables into their respective primary domains with `status='detected'`.
