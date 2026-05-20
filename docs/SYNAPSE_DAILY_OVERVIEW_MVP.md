# Synapse Daily Overview MVP

**Status:** Internal / Debug / Read-Only  
**Date:** 2026-05-19

## 1. What Daily Overview is

Daily Overview is a unified internal read model for the question:

`What should this companion know about this user's day?`

It is exposed at:

- `GET /internal/debug/daily-overview`
- protected by `X-Internal-Token`
- read-only

This is the first product-shaped layer built on top of the Synapse memory substrate. It does not send messages, trigger tools, or perform delivery. It composes existing internal layers into one deterministic payload that is usable for future morning brief, daily planning, reminders, evening reflection, and proactive nudge surfaces.

## 2. Why this is the first product-shaped read layer

Synapse already had the substrate:

- baseline memory objects
- fast handover packets
- attention preview
- attention outcomes
- timeline read model
- calendar and action-item state

What it did not yet have was a single internal answer to "what matters today?" Daily Overview fills that gap without changing serving behavior or adding automation.

## 3. What it uses

Daily Overview composes:

- `calendar_items` for today's schedule
- `action_items` for due-today, overdue, and recently completed obligations
- `build_attention_preview(...)` for companion-filtered worth-attention items
- `get_latest_fast_handover_packet(...)` for short-lived continuity
- `build_timeline_read_model(...)` for recent signals and freshness debugging
- `identity_cache.timezone` when a user timezone is available

## 4. Endpoint contract

Query params:

- `tenantId` required
- `userId` required
- `companionId` optional, default `sophie`
- `date` optional `YYYY-MM-DD`
- `timezone` optional, otherwise uses cached user timezone if available, else `UTC`
- `includeExpired` optional, default `false`
- `limit` optional

Top-level response sections:

- header metadata
- `todaysSchedule`
- `tasksAndObligations`
- `worthAttention`
- `recentContinuity`
- `recentTimelineSignals`
- `suggestedFocus`
- `metadata`

## 5. Guardrails

Daily Overview deliberately avoids:

- treating stale emotional state as current truth
- surfacing expired attention by default
- treating cancelled, archived, dismissed, or done items as active
- turning handover packets into durable identity memory
- over-personalising from volatile living context
- hiding evidence gaps
- silently promoting smoke/demo/test rows into real user-day context

Default anti-noise behavior:

- rows with titles containing `smoke`, `demo`, `test`, or `daybrief-refresh` are excluded by default
- when `includeExpired=true`, those rows can be returned as diagnostic items

## 6. What it does not do yet

Daily Overview v1 does not:

- send any user-facing brief
- execute email, calendar, or reminder actions
- modify `/session/startbrief`
- mutate upstream objects
- integrate web search or external mail tools
- perform LLM copywriting

The summary fields are deterministic and rule-based.

## 7. Example response

```json
{
  "tenantId": "default",
  "userId": "u1",
  "companionId": "sophie",
  "date": "2026-05-19",
  "timezone": "UTC",
  "generatedAt": "2026-05-19T09:00:00+00:00",
  "readOnly": true,
  "todaysSchedule": [
    {
      "title": "Morning meeting",
      "starts_at": "2026-05-19T11:00:00+00:00",
      "ends_at": "2026-05-19T12:00:00+00:00",
      "status": "confirmed",
      "freshness": "current",
      "source_id": "cal-1",
      "source_table": "calendar_items",
      "action_hint": "prepare_soon",
      "time_status": "upcoming",
      "gaps": []
    }
  ],
  "tasksAndObligations": [
    {
      "title": "Submit report",
      "status": "pending",
      "due_at": "2026-05-19T14:00:00+00:00",
      "freshness": "current",
      "source_id": "task-1",
      "source_table": "action_items",
      "action_hint": "due_today",
      "bucket": "due_today",
      "gaps": []
    }
  ],
  "worthAttention": [
    {
      "title": "Check on Ashley and the takedown",
      "attention_type": "follow_up",
      "surface_mode": "proactive_allowed",
      "action_policy": "suggest_only",
      "status": "eligible",
      "source_table": "follow_up_candidates",
      "source_id": "11",
      "gaps": []
    }
  ],
  "recentContinuity": {
    "summary": "Recent session focused on invoice follow-up.",
    "open_questions": ["What should I send?"],
    "gaps": [
      "handover_is_short_lived_and_not_durable_profile_memory",
      "recent_state_note_is_provisional"
    ]
  },
  "suggestedFocus": {
    "primary_focus": "concrete_day_items",
    "suggested_next_action": "compose_from_schedule_tasks_attention",
    "avoid_overdoing": "avoid_over_personalising_from_provisional_context",
    "confidence": 0.8
  },
  "metadata": {
    "readOnly": true
  }
}
```

## 8. How this will be used later

This read model is intended to become the internal basis for:

- morning briefs
- daily planning
- reminder selection
- evening reflections
- proactive nudges

Those future surfaces should consume this read model or a close successor rather than re-deriving day context from scratch.

## 9. Known gaps

- task prioritisation is still shallow and source-dependent
- timeline freshness remains heuristic, not canonical truth
- some rows still lack strong `evidence_refs`
- duplicate concepts across sections are not deeply deduplicated yet
- companion policy is still static/profile-based, not full runtime policy objects
