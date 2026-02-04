# Sophie Orchestrator ↔ Synapse Integration v1

This doc is the integration contract for the Sophie orchestrator. It explains what Synapse does, what data to send, and how to retrieve memory when needed. It is written for an LLM/agent developer who needs to plug in fast without reading the codebase.

## Narrative: what Synapse is
Synapse is a memory backend. It stores short-term session state (rolling summary + last 6 turns) in Postgres and optional long-term semantic memory in Graphiti. It does **not** generate assistant responses. The orchestrator calls Synapse to read memory before a response, and writes turns after the response.

## Quick model of storage
**Postgres (canonical)**
- `session_buffer`: rolling summary + last 6 turns
- `user_identity`: name/home/timezone/etc (from user statements)
- `loops`: procedural memory (commitments, habits, frictions)
- `graphiti_outbox`: reliable queue for long-term memory delivery

**Graphiti (derived, best‑effort)**
- Episodes (session summary episodes by default)
- Facts/entities (semantic recall)

If Graphiti is down, Synapse still works using Postgres.

## Recommended request flow (the safe default)

1) **Before each assistant response**
- Call `/brief` to get memory context
- Build the LLM prompt using that context

2) **After the assistant response**
- Call `/ingest` for the user turn
- Call `/ingest` for the assistant turn

### Why this order?
- /brief gives you the best available memory before generation.
- /ingest updates memory for the next turn.

## /brief
**Purpose:** read memory context.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "now": "2026-02-04T18:00:00Z",
  "query": "Ashley"
}
```

**Response (shape)**
```json
{
  "identity": {"name": null, "timezone": "UTC", "home": null, "isDefault": true},
  "temporalAuthority": {"currentTime": "06:00 PM", "currentDay": "Tuesday", "timeOfDay": "evening"},
  "workingMemory": [ ...last 6 turns... ],
  "rollingSummary": "...",
  "activeLoops": [ ... ],
  "nudgeCandidates": [ ... ],
  "semanticContext": [ ... ],
  "entities": [ ... ],
  "episodeBridge": "...",
  "metadata": { ... }
}
```

**Notes**
- If you want Graphiti recall, pass a `query` (e.g., person or topic).
- If no `query`, Graphiti is not queried.
- `nudgeCandidates` are suggestions; only send if your UX wants it.

## /ingest
**Purpose:** write memory.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "role": "user",
  "text": "I just had a fight with Ashley.",
  "timestamp": "2026-02-04T18:00:05Z",
  "metadata": {"sessionId": "session-abc"}
}
```

**Rules**
- `sessionId` can be top-level or in metadata.
- If missing, Synapse auto-creates one and returns it.
- Always send **both** user and assistant turns.

## How to retrieve specific memory (example)
User says: “I just had a fight with Ashley.”

**What to do:**
- Call `/brief` with `query: "Ashley"`.
- If Graphiti has facts/entities about Ashley, they appear in `semanticContext` and `entities`.
- If Graphiti is empty, you still get recent context from `workingMemory` + `rollingSummary`.

There is no magic push. The orchestrator must ask.

## Session boundaries
- Synapse auto-closes sessions after inactivity (idle close loop, config).
- New session summary episodes are generated at close, used as `episodeBridge`.
- You can use your own sessionId policy; Synapse will follow it.

## Nudge candidates
- If a completion is **implicit**, Synapse returns `nudgeCandidates` instead of closing the loop.
- If you send a nudge to the user, you should mark it by updating `last_nudged_at` in loop metadata (future endpoint or manual update).

## Failure behavior
- Graphiti down → /brief still returns Postgres data; /ingest still works.
- LLM extraction down → loops/summaries may be delayed, but no data loss.

## Minimal integration checklist
- Call `/brief` before assistant response.
- Call `/ingest` for both turns after response.
- Pass `query` to /brief when you need semantic recall.
- Store/track `sessionId` (or accept auto-generated).

## Orchestrator memory heuristics (practical defaults)
Use these to decide when to call `/brief` and how to choose `query`.

### When to call /brief
- Always on session start.
- Before every assistant response **by default**.
- You may cache for 1–2 turns if:
  - same session
  - no new /ingest since last /brief
  - no explicit memory/recall intent detected

### When to pass `query`
Pass `query` if the user mentions:
- a specific person/place/project (e.g., “Ashley”, “Project Apollo”)
- continuity phrases (“remember”, “what did we decide”, “did I tell you”)
- emotionally charged updates likely tied to prior context (“I’m upset about Ashley”)

### Suggested query construction
- Use a short noun phrase or named entity.
- Keep it under ~5–8 words.
- Examples: `"Ashley"`, `"Project Apollo"`, `"dad health"`, `"trip to LA"`.

### Prompt assembly order (recommended)
1) Identity (if known)
2) TemporalAuthority
3) RollingSummary
4) WorkingMemory (last 6 turns)
5) ActiveLoops + NudgeCandidates
6) Graphiti facts/entities (if any)
7) Current user message

### Avoiding wrong memory
- If `semanticContext` is empty, do not force a memory block into the prompt.
- If `identity.name` is null, avoid using a name in the assistant response.
