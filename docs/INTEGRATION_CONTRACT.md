# INTEGRATION CONTRACT (Synapse API)

This doc is a concise API contract for external clients (e.g., Sophie orchestrator). It is derived from the current implementation and intended to be the source of truth for integration.

## 1) External HTTP Routes

### GET /health
**Auth:** none
**Headers:** none

**Response (example)**
```json
{
  "status": "healthy",
  "service": "synapse",
  "version": "1.0.0"
}
```

---

### POST /brief
**Auth:** none (public endpoint)
**Headers:**
- `Content-Type: application/json`

**Request JSON**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "now": "2026-02-04T18:00:00Z",
  "sessionId": "session-abc",
  "query": "Ashley"
}
```

**Response JSON (example)**
```json
{
  "identity": {
    "name": null,
    "timezone": "UTC",
    "home": null,
    "isDefault": true
  },
  "temporalAuthority": {
    "currentTime": "06:00 PM",
    "currentDay": "Tuesday",
    "timeOfDay": "evening",
    "timeSinceLastInteraction": "10 minutes ago"
  },
  "sessionState": null,
  "workingMemory": [
    {"role": "user", "text": "...", "timestamp": "..."}
  ],
  "rollingSummary": "...",
  "activeLoops": [
    {"id": "...", "type": "commitment", "status": "active", "text": "...", "salience": 3}
  ],
  "nudgeCandidates": [
    {"loopId": "...", "type": "commitment", "text": "...", "question": "Is this done?", "confidence": 0.7, "evidenceText": "..."}
  ],
  "episodeBridge": "...",
  "semanticContext": [
    {"text": "...", "relevance": 0.72, "source": "graphiti"}
  ],
  "entities": [
    {"summary": "Ashley", "type": "person", "uuid": "..."}
  ],
  "instructions": ["..."],
  "metadata": {
    "queryTime": "2026-02-04T18:00:00Z",
    "bufferSize": 2,
    "hasRollingSummary": true,
    "graphitiFacts": 1,
    "graphitiEntities": 1,
    "loopsCount": 1
  }
}
```

**Failure modes**
- `500` on unexpected server errors.

---

### POST /ingest
**Auth:** none (public endpoint)
**Headers:**
- `Content-Type: application/json`

**Request JSON**
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

Notes:
- `sessionId` can be **top-level** or in `metadata.sessionId`.
- If missing, Synapse auto-creates one and returns it.

**Response JSON (example)**
```json
{
  "status": "ingested",
  "sessionId": "session-abc",
  "identityUpdates": null,
  "loopsDetected": null,
  "loopsCompleted": null,
  "graphitiAdded": true
}
```

**Failure modes**
- `status: "skipped"` if message is classified as noise.
- `status: "error"` if ingestion fails.

---

## 2) Internal-Only Routes (not for external clients)
These require header `X-Internal-Token` and are intended for ops/debug only:
- `POST /internal/drain`
- `GET /internal/debug/session`
- `GET /internal/debug/user`
- `GET /internal/debug/outbox`
- `GET /internal/debug/loops`
- `GET /internal/debug/nudges`

## 3) Canonical Identifiers

| Identifier | Required? | Notes |
| --- | --- | --- |
| `tenantId` | required | Always required in /brief and /ingest |
| `userId` | required | Always required |
| `personaId` | required | Used for loop scoping and future persona handling |
| `sessionId` | optional | If omitted, Synapse auto-creates and returns it |

## 4) Memory Concepts + Shapes

### Rolling Summary
- Location: `session_buffer.rolling_summary`
- Shape: plain text summary of older turns

### Working Memory
- Location: `session_buffer.messages`
- Shape: JSON array of last 6 turns `{role,text,timestamp}`

### Loops (procedural memory)
- Stored in Postgres `loops`
- Types: commitment, decision, friction, habit, thread
- Metadata contains evidence + nudge candidates

### Semantic Memory (Graphiti)
- Facts/entities/episodes in Graphiti (best-effort)
- Retrieved only when /brief includes a `query`

## 5) Constraints / Limits
- No explicit payload size limit enforced in code.
- No public rate limits enforced (deployment may impose limits).
- External calls have timeouts (LLM/Graphiti), but /brief and /ingest are designed to return even on timeouts.

## 6) Happy Path (one chat turn)

1) **User sends message** to orchestrator.
2) Orchestrator calls **/brief** with `query` if memory recall is needed.
3) Orchestrator builds prompt and calls the LLM.
4) LLM returns assistant response.
5) Orchestrator calls **/ingest** for user turn.
6) Orchestrator calls **/ingest** for assistant turn.

## 7) Practical query heuristics (memory recall)
Use these to decide when to include `query` in /brief:
- User mentions a specific person/place/project (e.g., “Ashley”, “Project Apollo”).
- User asks for recall/continuity (“remember…”, “what did we decide…”, “did I tell you…”).
- Emotion/relationship update likely ties to prior memory (“I’m upset about Ashley”).

Suggested query values:
- Short noun phrase or named entity.
- Keep it under ~5–8 words.
- Examples: `"Ashley"`, `"Project Apollo"`, `"dad health"`, `"trip to LA"`.

Caching note:
- You can reuse /brief for 1–2 turns if the session is active and no new /ingest happened.

## 8) Known pitfalls (read this)
- Graphiti recall is **not automatic**. Pass `query` when you need semantic memory.
- Identity defaults to null until user states it. Don’t assume a name exists.
- Outbox won’t drain unless `OUTBOX_DRAIN_ENABLED=true` or `/internal/drain` is called.
- Session summaries depend on idle close; enable `IDLE_CLOSE_ENABLED` for reliable episodeBridge.
- Noise filter may return `status: skipped` for very short messages.
- Nudge candidates repeat if `last_nudged_at` isn’t set after you send one.

References:
- `DECISIONS.md` for architectural intent
- `AUDIT_MEMORY_V1.md` for as‑built details
