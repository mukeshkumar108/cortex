# Synapse Attention and Surfacing

**Status:** Draft / Target Specification  
**Date:** 2026-05-19  
**Specification Reference:** `docs/SYNAPSE_OBJECTS_AND_DOMAINS.md`

---

## 1. What is an Attention Item?

An **Attention Item** is the layer above Synapse Objects and Links. 
*   **It is not an Object:** Objects represent durable state, facts, or entities (e.g., "Ashley," "Event Takedown").
*   **It is not a Link:** Links represent the relationship between Objects (e.g., "Ashley is involved in Event Takedown").
*   **It is a Decision:** An Attention Item is a proposed surfacing or action decision, synthesized dynamically from Objects, Links, Evidence, Temporal Context, and the active Companion Profile.

**Example:**
*   **Object (Person):** Ashley
*   **Object (Workstream):** Event Takedown
*   **Object (State):** Feeling unwell
*   **Link (Involves):** Event Takedown involves Ashley
*   **Link (Affects):** Feeling unwell affects Event Takedown
*   **Attention Item:** "Tomorrow morning, gently ask whether the takedown got sorted."

---

## 2. Attention Item Target Schema

An Attention Item is a runtime or semi-persistent record dictating *when* and *how* to surface intelligence.

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID | Unique identifier for the attention item. |
| `tenant_id` | Text | Workspace isolation. |
| `user_id` | Text | User isolation. |
| `companion_id` / `profile` | Text | Which companion policy generated or owns this item. |
| `source_object_ids` | UUID[] | The foundational Objects triggering this item. |
| `source_link_ids` | UUID[] | The relationships proving relevance. |
| `evidence_refs` | JSONB | Specific session/turn provenance for "Why now?". |
| `title` | Text | Internal summary (e.g., "Check on Ashley takedown"). |
| `reason` | Text | The "Why Now" explanation for the LLM/Companion. |
| `attention_type` | Enum | See Section 3. |
| `surface_mode` | Enum | See Section 4. |
| `action_policy` | Enum | See Section 5. |
| `priority` | Int | Sorting order against other items. |
| `urgency` | Int | Time-sensitivity of the item. |
| `importance` | Int | Consequence of ignoring the item. |
| `confidence` | Float | Likelihood the item is correct and welcome. |
| `sensitivity` | Enum | Risk profile (low/medium/high/critical). |
| `earliest_surface_at` | Timestamp | Do not show before this time. |
| `ideal_surface_at` | Timestamp | The perfect moment to surface. |
| `latest_useful_at` | Timestamp | Point after which surfacing is awkward or useless. |
| `expires_at` / `drop_after` | Timestamp | Hard deletion/archival point. |
| `cooldown_until` | Timestamp | Rate-limiting after previous surface attempt. |
| `surface_count` | Int | How many times it has been shown. |
| `max_surface_count`| Int | Maximum attempts before giving up. |
| `status` | Enum | See Section 6. |
| `created_at` | Timestamp | Time of generation. |
| `updated_at` | Timestamp | Last modification. |
| `resolved_at` | Timestamp | When the user addressed it. |

---

## 3. Attention Types

*   `reminder`: Explicit time-based prompt for an Obligation.
*   `check_in`: Soft prompt regarding State or Workstreams.
*   `follow_up`: Requesting closure on a past Event or Obligation.
*   `prep`: Briefing ahead of an upcoming Event.
*   `review`: Retrospective on a completed Event or Workstream.
*   `clarification`: Asking the user to resolve a Contradiction or low-confidence Object.
*   `opportunity`: A proactive suggestion (e.g., drafting an email, suggesting a habit).
*   `risk_signal`: Alerting to a missed habit, decaying state, or friction.
*   `celebration`: Acknowledging a completed goal or streak.
*   `accountability`: Nudging related to a commitment.
*   `escalation_candidate`: High-urgency item requiring human or caregiver intervention.

---

## 4. Surface Modes

*   `reactive_only`: Only use if the user explicitly asks about the topic.
*   `proactive_allowed`: Can be surfaced during natural conversation lulls.
*   `proactive_recommended`: Should actively steer the conversation toward this.
*   `confirm_first`: Requires user opt-in before executing the underlying action.
*   `quiet_hold`: Monitor, but do not surface yet (waiting for more evidence).
*   `suppress`: Explicitly hide from the companion (e.g., triggered by safety rules).
*   `escalate_if_missed`: Must bypass normal quiet hours if the latest_useful_at approaches.

---

## 5. Action Policies

*   `observe_only`: Just read the state; do nothing.
*   `suggest_only`: Offer the idea to the user.
*   `ask_confirmation`: Ask before taking an external action.
*   `draft_only`: Prepare an artifact (e.g., email) but do not send.
*   `execute_if_preapproved`: Perform the action automatically if the user previously consented.
*   `notify_caregiver_if_rule_matches`: Specialized safety escalation.
*   `never_execute`: Strict boundary preventing external side effects.

---

## 6. Status Lifecycle

*   `candidate`: Generated but pending validation against rules.
*   `pending`: Valid, waiting for the `earliest_surface_at` window.
*   `eligible`: Within the valid time window, ready to be picked up by the Companion.
*   `surfaced`: Currently shown or recently mentioned to the user.
*   `acknowledged`: User saw it but hasn't resolved it (e.g., "I'll do it later").
*   `snoozed`: Explicitly pushed to a later time.
*   `completed`: The underlying reason was fulfilled.
*   `expired`: Past `latest_useful_at` without resolution.
*   `dismissed`: User explicitly rejected the item.
*   `suppressed`: Cancelled by a higher-level safety or anti-creep policy.
*   `escalated`: Sent outside the standard companion loop.

---

## 7. Decay & Staleness Rules

Attention Items must respect the inherent decay rates of their underlying Source Domains:
*   **State (Domain VIII):** Decays rapidly (hours to a day). Do not surface "you felt overwhelmed" 3 days later unless tied to a durable Workstream.
*   **Events (Domain VII):** Strict before/during/after windows. Prep items expire at the event start; Review items expire 24-48 hours after.
*   **Obligations (Domain VI):** Remain active until resolved, stale, or dismissed.
*   **Opportunities (Domain IX):** Expire quickly (hours). If the user doesn't bite, drop it.
*   **Goals/Profile/People:** Rarely trigger surfacing directly; they act as context/modifiers for other items.

---

## 8. Anti-Creep Rules

1.  **Do not over-personalise stale state:** A momentary mood is not a personality trait. 
2.  **Do not nag:** If an Opportunity is ignored twice, suppress it.
3.  **Respect boundaries:** Do not proactively surface highly sensitive Profile facts (e.g., trauma, extreme fears) unless strictly necessary for safety.
4.  **Explain "Why Now":** Internally, every item must justify its existence using the `reason` field to prevent hallucinatory or random prompts.
5.  **Prefer gentle wording:** Attention Items should guide the LLM to use soft invitations ("I remembered you had X, did that get sorted?") rather than robotic demands.

---

## 9. The "Why Now?" Explanation Model

Every surfaced item must trace back to:
*   `source_object_ids` (What are we talking about?)
*   `source_link_ids` (How are they connected?)
*   `evidence_refs` (When did the user mention this?)
*   Timing window (Why is *today* the right day?)
*   Companion Profile (Why am *I* the right persona to ask?)
*   Action Policy (What am I allowed to do about it?)

---

## 10. Relationship to Existing Tables

Currently, Synapse attempts to manage "attention" through fragmented `_candidates` tables and legacy `loops`.

*   **`actionable_candidates`:** Early Attention Items (Action Policy: suggest_only).
*   **`follow_up_candidates`:** Early Attention Items (Attention Type: follow_up).
*   **`clarification_candidates`:** Early Attention Items (Attention Type: clarification).
*   **`open_threads.follow_up_after`:** Implicit Attention Items waiting to be generated.
*   **`daily_analysis` / `living_context`:** Snapshots that currently *imply* attention but lack specific action boundaries.

**Recommendation:** Attention Items should initially be built as a **Read Model / View** or generated dynamically at runtime (e.g., during `/session/startbrief` or a new `/attention/surfaces` endpoint) by querying `object_links` and Objects. They should eventually replace the fragmented `_candidates` tables, serving as a unified queue for the Companion.

---

## 11. "Magic Moment" Examples

### 1. Ashley Unwell + Event Takedown
*   **Source Objects:** "Ashley" (Person), "Event Takedown" (Workstream), "Unwell" (State).
*   **Links:** Event involves Ashley; Unwell affects Event.
*   **Evidence:** "Ashley called in sick, I have to do the takedown myself."
*   **Attention Item:** "Check in on Ashley and the takedown."
*   **Surface Timing:** Next morning.
*   **Action Policy:** `suggest_only` (draft a message).
*   **Expiry:** End of next day.
*   **What Not To Do:** Do not ask 3 days later if the takedown is still happening.

### 2. Unpaid Invoice / Client Chase
*   **Source Objects:** "Acme Corp" (Person/Org), "Invoice #102" (Obligation).
*   **Links:** Acme owes Invoice; Invoice blocks Workstream.
*   **Evidence:** "Still waiting on Acme to pay."
*   **Attention Item:** "Nudge Acme about the invoice."
*   **Surface Timing:** Friday afternoon.
*   **Action Policy:** `draft_only` (prepare the email for review).
*   **Expiry:** Monday morning (needs a new strategy if ignored).
*   **What Not To Do:** Do not automatically send the email without `confirm_first`.

### 3. Elderly Missed Morning Check-in
*   **Source Objects:** "Morning Routine" (Habit), "User State" (State).
*   **Links:** Routine affects State.
*   **Evidence:** No activity on devices by 10 AM.
*   **Attention Item:** "Urgent wellness check."
*   **Surface Timing:** 10:05 AM.
*   **Action Policy:** `notify_caregiver_if_rule_matches`.
*   **Expiry:** N/A (Escalates until resolved).
*   **What Not To Do:** Do not just log it quietly.

### 4. Chronic Health Flare Pattern
*   **Source Objects:** "Knee Pain" (State), "Dr. Smith Appt" (Event).
*   **Links:** State relates to Event.
*   **Evidence:** User complained of knee pain 3 times this week; appt is tomorrow.
*   **Attention Item:** "Prep symptom list for Dr. Smith."
*   **Surface Timing:** Evening before appointment.
*   **Action Policy:** `suggest_only` (compile the list).
*   **Expiry:** Start of appointment.
*   **What Not To Do:** Do not offer medical advice; just organize the user's reported evidence.

### 5. Mukesh Intense Session
*   **Source Objects:** "Coding Synapse" (Workstream), "Intense Focus" (State).
*   **Links:** Workstream causes State.
*   **Evidence:** Continuous typing/querying for 4 hours.
*   **Attention Item:** "Eat/walk/one small ship nudge."
*   **Surface Timing:** End of current pomodoro/lull.
*   **Action Policy:** `suggest_only` (gentle interrupt).
*   **Expiry:** 1 hour.
*   **What Not To Do:** Do not interrupt mid-sentence or demand he stops.

---

## 12. Internal Debug Preview

Current MVP endpoint:
*   `GET /internal/debug/attention`
*   Requires header `X-Internal-Token: <INTERNAL_TOKEN>`
*   Query params:
    *   `tenantId`
    *   `userId`
    *   `companionId` optional, defaults to `sophie`
    *   `limit` optional, defaults to `20`
    *   `includeExpired` optional, defaults to `false`

Example call:

```bash
curl -s "http://localhost:8000/internal/debug/attention?tenantId=default&userId=u1&companionId=sophie&limit=10" \
  -H "X-Internal-Token: <INTERNAL_TOKEN>"
```

Example useful output:

```json
{
  "tenantId": "default",
  "userId": "u1",
  "companionId": "sophie",
  "items": [
    {
      "title": "Check on Ashley and the takedown",
      "attention_type": "follow_up",
      "surface_mode": "proactive_allowed",
      "action_policy": "suggest_only",
      "status": "eligible",
      "source_table": "follow_up_candidates",
      "source_id": "11",
      "latest_useful_at": "2026-05-20T09:00:00+00:00",
      "expires_at": "2026-05-21T09:00:00+00:00",
      "gaps": [],
      "missing_metadata": []
    }
  ],
  "metadata": {
    "totalGenerated": 4,
    "returnedCount": 3,
    "expiredCount": 1,
    "profileApplied": "sophie",
    "readOnly": true
  }
}
```

`includeExpired=true` is for debugging smoke and stale data. Default preview hides `expired`, `completed`, `dismissed`, `suppressed`, and `archived` items so product-like candidates float to the top. When `includeExpired=true`, those items are returned again and still counted in metadata.

Current `gaps` / `missing_metadata` meaning:
*   `source_object_ids_unknown`: legacy source row did not carry normalized object references.
*   `source_link_ids_unknown`: legacy source row did not preserve relationship/link provenance.
*   `evidence_refs_unknown`: the preview could not trace a concrete turn/session reference for "why now".

This endpoint is **preview/debug only**.
*   It does not deliver messages.
*   It does not execute external actions.
*   It does not modify `/session/startbrief`.
*   It is a read model over existing fragmented tables, not delivery automation.
