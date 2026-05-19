# Synapse Objects and Domains: Companion Intelligence Specification

**Status:** Draft / Doctrine  
**Date:** 2026-05-19  
**Vision:** Synapse is the companion intelligence layer. It does not aim for raw retrieval; it aims to answer: "What matters to this person now, for this companion, in this moment?"

---

## 1. The Synapse Object

A **Synapse Object** is a synthesized, stateful representation of a meaningful entity or concept in a user's life. Unlike raw transcript windows or one-off extraction results, an object has a lifecycle, a confidence score, and persistent identity across sessions.

### Structural Philosophy: Nodes and Edges
While the underlying storage may be relational, JSONB, or a graph database, the Synapse mental model is graph-centric:
*   **Objects as Nodes:** Each object represents a discrete point of intelligence (a task, a person, a mood).
*   **Links as Edges:** Objects are connected via typed, directional links (e.g., an Obligation *belongs_to* a Workstream).

### Domains as Classification, Not Silos
Domains are high-level classifications used for discovery and ranking, not isolated storage containers.
*   **Primary Domain:** Every object has exactly one primary domain that defines its core nature.
*   **Cross-Domain Links:** An object can have many typed links to objects in other domains. Links are the "connective tissue" of intelligence.

### Universal Object Fields
Every Synapse object, regardless of domain, must support these core fields:

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID / Text | Unique identifier for the object. |
| `tenant_id` | Text | Workspace/Tenant isolation key. |
| `user_id` | Text | User ownership key. |
| `primary_domain`| Enum | One of the 9 intelligence domains. |
| `status` | Enum | Lifecycle state (e.g., `detected`, `active`, `archived`). |
| `confidence` | Float (0.0-1.0) | Synapse's certainty that this object is accurate. |
| `salience` | Int (1-5) | Importance/Utility of this object for current context. |
| `provenance` | JSONB | Links to source sessions, turns, or external IDs. |
| `metadata` | JSONB | Domain-specific attributes and internal tracking flags. |
| `created_at` | Timestamp | Initial detection/creation time. |
| `updated_at` | Timestamp | Last modification or corroboration. |

---

## 2. Lifecycle & Status Meanings

Objects transition through stages. A "Candidate" is not a separate storage silo, but a **lifecycle status** within any domain.

*   **`detected` (Candidate):** Extracted by a pipeline pass but not yet confirmed. Shown to the companion as a "suggestion" or "shadow" item.
*   **`confirmed` / `active`:** Validated by subsequent turns, explicit user action, or high-confidence synthesis. Part of the "Ground Truth."
*   **`needs_review`:** High-salience but low-confidence; a priority for proactive clarification.
*   **`snoozed` / `stale`:** Temporarily irrelevant based on temporal decay or inactivity.
*   **`resolved` / `completed`:** The object’s purpose is fulfilled (e.g., a task done, a thread closed).
*   **`dismissed` / `archived`:** Explicitly rejected by the user or invalidated by new evidence.

---

## 3. The 9 Intelligence Domains

### I. Profile
Stable facts about the user.
*   **Is:** Values, constraints, communication style, dietary preferences, personality traits, recurring fears.
*   **Is Not:** Current mood, active tasks, or specific people (unless they define the user's identity).
*   **Example:** "Prefers concise communication," "Vegan," "Highly value-driven by autonomy."
*   **Existing Tables:** `identity_profile`, `durable_profile_facts`.

### II. People
The user's social and professional graph.
*   **Is:** Relationships, care teams, family members, key clients, contact context.
*   **Is Not:** Organizations (unless personified) or temporary acquaintances with no salience.
*   **Example:** "Sarah (Partner)," "Dr. Aris (GP)," "Project Team Alpha."
*   **Existing Tables:** `entity_profiles`, `entities`, `entity_aliases`.

### III. Goals
What the user is striving for.
*   **Is:** North-star objectives, desired behavior changes, long-term aspirations.
*   **Is Not:** Single tasks or obligations.
*   **Example:** "Improve cardiovascular health," "Get promoted to Senior Lead," "Write a novel."
*   **Existing Tables:** (Implicitly in `identity_profile.persistent_goals` or legacy `loops`).

### IV. Workstreams
Active projects and ongoing situations.
*   **Is:** Multi-session storylines, active responsibilities, life admin "epics."
*   **Is Not:** One-off appointments or static profile facts.
*   **Example:** "Q2 Product Launch," "Planning the wedding," "Recovering from knee surgery."
*   **Existing Tables:** `open_threads`.

### V. Habits / Routines
Repeated behaviors and accountability.
*   **Is:** Daily/weekly cycles, streak signals, lapse detection.
*   **Is Not:** One-time commitments.
*   **Example:** "Daily meditation," "Tuesday gym sessions," "Weekly budget review."
*   **Existing Tables:** `action_items` (kind='habit'), `habit_daily_log`.

### VI. Obligations
Tasks, commitments, and follow-ups.
*   **Is:** "To-dos," "waiting-on" items, deadlines, open loops.
*   **Is Not:** General project goals or routines.
*   **Example:** "Call the insurance company," "Send the report to Mark by Friday."
*   **Existing Tables:** `action_items` (kind='todo', 'reminder'), legacy `loops`.

### VII. Events
Time-anchored occurrences.
*   **Is:** Calendar items, appointments, milestones, past time-anchors.
*   **Is Not:** Theoretical goals or open-ended workstreams.
*   **Example:** "Flight to Tokyo at 10 AM," "Anniversary dinner," "Dentist appointment."
*   **Existing Tables:** `calendar_items`, `memory_events`.

### VIII. State
The "Right Now" context.
*   **Is:** Current mood, stress levels, health/energy capacity, focus, availability.
*   **Is Not:** Persistent personality traits (Profile).
*   **Example:** "Feeling overwhelmed," "Low energy today," "Currently focused on deep work."
*   **Existing Tables:** `living_context`, `session_classifications`, `session_changes`.

### IX. Opportunities
Proactive suggestions and drafts.
*   **Is:** Suggested prompts, prep reminders, "Should we ask/do this?", drafts.
*   **Is Not:** Confirmed obligations or existing workstreams.
*   **Example:** "Ask how the meeting with Sarah went," "Suggest a meditation session based on high stress."
*   **Existing Tables:** `actionable_candidates`, `follow_up_candidates`, `clarification_candidates`.

---

## 4. Operational Rules

### Candidate Rules
*   **Status, Not Silo:** Candidates should be stored in the same tables as active objects where possible, distinguished by `status='detected'`.
*   **Promotion:** Promotion to `active` occurs via:
    *   Direct User Confirmation (UI/Voice).
    *   Corroboration (3+ high-confidence mentions in different sessions).
    *   Synthesis (LLM synthesis pass identifies high-certainty durable state).

### Expiry & Staleness
*   **State Decay:** State objects (Domain VIII) have the shortest TTL. After 24-48 hours, they move to `stale` unless corroborated.
*   **Obligation Cleanup:** Overdue obligations without recent activity are flagged for `needs_review` after 7 days.
*   **Opportunity Expiry:** Suggestions typically expire within 3-24 hours if not acted upon, as context shifts.

### Surface Policy (Serving)
*   **Reactive (Retrieval):** All domains are searchable.
*   **Proactive (Briefing):**
    *   **Startbrief:** Focus on State, Workstreams, and Obligations.
    *   **Daybrief:** Focus on Events and Obligations.
    *   **Mid-Session Signals:** Focus on Opportunities and Profile (Constraints).

### Cross-Domain Links
Objects are connected via typed links, typically stored in `metadata` or explicit link tables:
*   **Workstream -> People:** "Project X" is linked to "Sarah."
*   **Obligation -> Goal:** "Buy running shoes" linked to "Improve health."
*   **Event -> Workstream:** "Client Meeting" linked to "Q2 Sales Launch."

---

## 5. Open Questions
1.  **Goal Schema:** Should Goals have a dedicated table or remain part of `identity_profile`?
2.  **Conflict Resolution:** How do we handle conflicting State from different sessions (e.g., "Feeling great" vs "Exhausted" within 2 hours)?
3.  **Link Authority:** Is there a canonical "Relationship" table for cross-domain links, or do we use JSONB references?
