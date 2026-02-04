# Synapse Orchestrator Contract v1

This document defines the integration contract between an orchestrator and Synapse for Phase 4 (sliding window + outbox). It is written for production use and assumes Graphiti/OpenRouter may be unavailable.

## Endpoints

### POST /brief

Use this at the start of each turn to fetch memory context.

**Request (session_start):**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "now": "2026-02-03T18:35:00Z",
  "mode": "session_start",
  "query": "what did we leave off?"
}
```

**Request (in_session):**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "now": "2026-02-03T18:36:10Z",
  "mode": "in_session",
  "query": "what's the plan for tomorrow?"
}
```

**Response (example):**
```json
{
  "identity": {
    "name": "Kaiser",
    "timezone": "America/Los_Angeles",
    "facts": {}
  },
  "temporalAuthority": {
    "now": "2026-02-03T18:36:10Z",
    "lastInteractionTime": "2026-02-03T18:34:12Z",
    "timeSinceLastInteraction": "PT1M58S"
  },
  "workingMemory": [
    {"role": "user", "text": "Let's plan tomorrow", "timestamp": "2026-02-03T18:34:12Z"},
    {"role": "assistant", "text": "Sure—what should we prioritize?", "timestamp": "2026-02-03T18:34:18Z"}
  ],
  "rollingSummary": "We discussed planning tomorrow and priorities.",
  "activeLoops": [
    {"id": "loop-1", "type": "commitment", "status": "active", "text": "Book the dentist", "salience": 3}
  ],
  "semanticContext": [],
  "entities": [],
  "episodeBridge": "Last session summary: you wanted to book a dentist visit this week."
}
```

Notes:
- Tier 1 fields (identity, temporalAuthority, workingMemory, rollingSummary, activeLoops) are always returned.
- Tier 2 fields (semanticContext, entities, episodeBridge) are best-effort and may be empty.

### POST /ingest

Write both user and assistant turns. Always send both to keep session history coherent.

**Request (user turn):**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "role": "user",
  "text": "Let's plan tomorrow. I need to book the dentist.",
  "timestamp": "2026-02-03T18:34:12Z",
  "metadata": {"sessionId": "session-abc"}
}
```

**Request (assistant turn):**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "role": "assistant",
  "text": "Got it. What time works best?",
  "timestamp": "2026-02-03T18:34:18Z",
  "metadata": {"sessionId": "session-abc"}
}
```

**Response (example):**
```json
{
  "ok": true,
  "sessionId": "session-abc"
}
```

Notes:
- /ingest returns quickly after buffer write; background tasks (loops, summaries, Graphiti) are best-effort.
- Only user turns trigger loop extraction.

## Recommended orchestration loop

1) User speaks → call `/brief` with `mode="session_start"` if new session or gap > 30m; else `mode="in_session"`.
2) Build the LLM prompt using:
   - identity
   - temporalAuthority
   - rollingSummary
   - workingMemory (last 1–6 turns)
   - activeLoops
   - semanticContext/entities (if present)
   - episodeBridge (if present, session_start only)
3) LLM responds to user.
4) Call `/ingest` for the user turn, then `/ingest` for the assistant turn (same sessionId).

## Session rules

**session_start**
- Use when a new sessionId begins or when there is a gap > 30 minutes since last interaction.
- The response may include `episodeBridge` to reconnect context.

**in_session**
- Use for continuous conversation within an active session.

## Recall trigger guidance

When building prompts, prefer injecting memory blocks when:
- A named person/project is referenced.
- Emotional state or preferences appear (“I’m anxious about…”).
- Open commitments or ongoing threads are relevant.
- The user asks for continuity (“remind me”, “where were we”, “what did I say”).

Avoid flooding: use the most relevant 1–3 items.

## Failure behavior

**Graphiti down**
- /brief still returns Tier 1.
- /ingest still writes to Postgres; outbox retries later.

**OpenRouter down**
- /ingest still succeeds; rolling summary and loop extraction may be delayed.

**Any background failure**
- Does not block /ingest or /brief.

## Debug endpoints (internal only)

All debug endpoints require header `X-Internal-Token` = `INTERNAL_TOKEN`.

- `GET /internal/debug/session?tenantId&userId&sessionId`
  - Returns session_buffer row, transcript archive (if any), active loops, session episode blob/name, timestamps.

- `GET /internal/debug/user?tenantId&userId`
  - Returns latest session id, last interaction time, last session summary episode (episodeBridge),
    active loops, and top entities (if available).

- `GET /internal/debug/outbox?tenantId&limit=50`
  - Returns pending/failed outbox rows with next_attempt_at and last_error.

## Orchestrator checklist

- Always pass tenantId/userId/sessionId.
- Use `now` in /brief (UTC ISO).
- Ingest both user and assistant turns.
- Prefer /brief before every assistant response.
- Treat semanticContext/entities as optional.
