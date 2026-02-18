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

### GET /session/startbrief
**Auth:** none (public endpoint)
**Headers:** none

**Request (query params)**
```
tenantId, userId, now (optional ISO timestamp), sessionId (optional), personaId (optional), timezone (optional)
```

**Response JSON (example)**
```json
{
  "timeOfDayLabel": "AFTERNOON",
  "timeGapHuman": "8 hours since last spoke",
  "bridgeText": "Last time you spoke, you were focused on the portfolio refresh.",
  "items": [
    {"kind": "loop", "type": "thread", "text": "Finish portfolio site", "timeHorizon": "this_week", "salience": 4, "lastSeenAt": "2026-02-06T10:15:00Z"},
    {"kind": "tension", "text": "Flaky tests in release pipeline"}
  ]
}
```

Notes:
- `bridgeText` is fact‑only, <= 280 chars, and excludes environment/observation by default.
- `bridgeText` is sourced from the latest Graphiti `SessionSummary` node (fallback: legacy episode summary).
- Items come primarily from Postgres loops (salience + recency), with optional unresolved tensions from Graphiti.
- `timeGapHuman` is derived from session/message timestamps when available, otherwise Graphiti episode time.
- `timeOfDayLabel` uses `timezone` when provided (fallback UTC).

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
  "sessionId": "session-abc"
}
```

**Response JSON (example)**
```json
{
  "identity": {"name": null, "isDefault": true},
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
  "activeLoops": [],
  "nudgeCandidates": [],
  "episodeBridge": null,
  "semanticContext": [],
  "entities": [],
  "instructions": [],
  "metadata": {
    "queryTime": "2026-02-04T18:00:00Z",
    "bufferSize": 2,
    "hasRollingSummary": true,
    "graphitiFacts": 0,
    "graphitiEntities": 0,
    "loopsCount": 0
  }
}
```

**Failure modes**
- `500` on unexpected server errors.

Notes:
- `/brief` does not return procedural loops; `activeLoops` is empty in the current implementation.
- Procedural loop extraction happens on `/session/close` and is stored in Postgres.

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
  "graphitiAdded": false
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
- `POST /internal/debug/close_session`
- `POST /internal/debug/close_user_sessions`
- `POST /internal/debug/emit_raw_episode`
- `POST /internal/debug/emit_raw_user_sessions`

Admin-only (requires `X-Admin-Key`):
- `POST /admin/purgeUser` (purge Postgres + Graphiti for a tenant/user)

## 3) Canonical Identifiers

| Identifier | Required? | Notes |
| --- | --- | --- |
| `tenantId` | required | Always required in /brief and /ingest |
| `userId` | required | Always required |
| `personaId` | required | Reserved for future persona handling |
| `sessionId` | optional | If omitted, Synapse auto-creates and returns it |

## 4) Memory Concepts + Shapes

### Rolling Summary
- Location: `session_buffer.rolling_summary`
- Shape: plain text summary of older turns (local only)

### Working Memory
- Location: `session_buffer.messages`
- Shape: JSON array of last 12 messages `{role,text,timestamp}`

### Graphiti Memory (Semantic/Episodic)
- Graphiti is the source of truth for facts/entities and episodic memory.
- Synapse exposes **/memory/query** for on-demand semantic retrieval.

---

### POST /memory/query
**Auth:** none (public endpoint)
**Headers:**
- `Content-Type: application/json`

**Request JSON**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "query": "What happened earlier today?",
  "limit": 10,
  "referenceTime": "2026-02-04T18:00:00Z"
}
```

**Response JSON (example)**
```json
{
  "facts": [
    "User is at the gym",
    "Launch is scheduled for Friday 9 AM"
  ],
  "openLoops": ["blue-widget-glitch"],
  "commitments": ["I will send the demo notes tomorrow"],
  "contextAnchors": {
    "timeOfDayLabel": "AFTERNOON",
    "timeGapDescription": null,
    "lastInteraction": "2026-02-06T10:14:30Z",
    "sessionId": "session-abc"
  },
  "userStatedState": "I feel anxious about the demo",
  "currentFocus": "I'm focused on stabilizing the release pipeline",
  "recallSheet": "FACTS:\n- User is at the gym\n- Launch is scheduled for Friday 9 AM\nOPEN_LOOPS:\n- blue-widget-glitch\nCOMMITMENTS:\n- I will send the demo notes tomorrow\nCONTEXT_ANCHORS:\n- timeOfDayLabel: AFTERNOON\n- lastInteraction: 2026-02-06T10:14:30Z\n- sessionId: session-abc\nUSER_STATED_STATE:\n- I feel anxious about the demo\nCURRENT_FOCUS:\n- I'm focused on stabilizing the release pipeline",
  "supplementalContext": "FACTS:\n- User is at the gym\n- Launch is scheduled for Friday 9 AM\nOPEN_LOOPS:\n- blue-widget-glitch\nCOMMITMENTS:\n- I will send the demo notes tomorrow\nCONTEXT_ANCHORS:\n- timeOfDayLabel: AFTERNOON\n- lastInteraction: 2026-02-06T10:14:30Z\n- sessionId: session-abc\nUSER_STATED_STATE:\n- I feel anxious about the demo\nCURRENT_FOCUS:\n- I'm focused on stabilizing the release pipeline",
  "factItems": [
    {"text": "User is at the gym", "relevance": null, "source": "graphiti"},
    {"text": "Launch is scheduled for Friday 9 AM", "relevance": null, "source": "graphiti"}
  ],
  "entities": [
    {"summary": "gym", "type": "Environment", "uuid": "..."},
    {"summary": "blue-widget-glitch", "type": "Tension", "uuid": "..."},
    {"summary": "I'm focused on stabilizing the release pipeline", "type": "UserFocus", "uuid": "..."}
  ],
  "metadata": {"query": "What is the user stressed about?", "facts": 3, "entities": 3}
}
```

---

### GET /session/brief
**Auth:** none (public endpoint)
**Headers:** none

**Query params**
```
tenantId=tenant_a&userId=user_1&now=2026-02-04T18:00:00Z
```

**Response JSON (example)**
```json
{
  "timeGapDescription": "15 minutes since last spoke",
  "timeOfDayLabel": "AFTERNOON",
  "facts": ["User is at the gym"],
  "openLoops": ["blue-widget-glitch"],
  "commitments": ["I will send the demo notes tomorrow"],
  "contextAnchors": {
    "timeOfDayLabel": "AFTERNOON",
    "timeGapDescription": "15 minutes since last spoke",
    "lastInteraction": "2026-02-06T10:14:30Z",
    "sessionId": "session-abc"
  },
  "userStatedState": "I feel anxious about the demo",
  "currentFocus": "I'm focused on stabilizing the release pipeline",
  "briefContext": "FACTS:\n- User is at the gym\nOPEN_LOOPS:\n- blue-widget-glitch\nCOMMITMENTS:\n- I will send the demo notes tomorrow\nCONTEXT_ANCHORS:\n- timeOfDayLabel: AFTERNOON\n- timeGapDescription: 15 minutes since last spoke\n- lastInteraction: 2026-02-06T10:14:30Z\n- sessionId: session-abc\nUSER_STATED_STATE:\n- I feel anxious about the demo\nCURRENT_FOCUS:\n- I'm focused on stabilizing the release pipeline",
  "narrativeSummary": [
    {"summary": "User is at the gym", "reference_time": "2026-02-06T10:14:30Z"}
  ],
  "activeLoops": [{"description": "Blue-widget-glitch", "status": "unresolved"}],
  "currentVibe": {"timeOfDayLabel": "AFTERNOON"}
}
```

Notes:
- Derived from Graphiti narrative entities: `MentalState`, `Tension`, `Environment`, `UserFocus`.
- Facts are filtered for quality (no single‑token/vague fragments).
- `narrativeSummary` is derived from Graphiti episode summaries and de‑duplicated from facts.
- `briefContext` is a compact, structured sheet (no narrative prose).
- Use at session start if you want a short “start‑brief” without issuing multiple queries.

---

### POST /session/close
**Auth:** none (public endpoint)
**Headers:**
- `Content-Type: application/json`

**Request JSON**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "sessionId": "session-abc",
  "personaId": "persona_1"
}
```

**Response JSON (example)**
```json
{
  "closed": true,
  "sessionId": "session-abc"
}
```

Notes:
- If `sessionId` is omitted, Synapse closes the most recent open session for the user.
- On close, Synapse sends the raw transcript to Graphiti, stores a `SessionSummary` node in Graphiti,
  and performs best‑effort loop extraction (commitments/decisions/frictions/habits/threads) into Postgres
  with provenance metadata.

---

### POST /session/ingest
**Auth:** none (public endpoint)
**Headers:**
- `Content-Type: application/json`

Use this if you keep working memory locally and only send full transcripts.

**Request JSON**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "startedAt": "2026-02-04T18:00:00Z",
  "endedAt": "2026-02-04T18:45:00Z",
  "messages": [
    {"role": "user", "text": "My name is Jordan", "timestamp": "2026-02-04T18:00:01Z"},
    {"role": "assistant", "text": "Nice to meet you", "timestamp": "2026-02-04T18:00:05Z"}
  ]
}
```

**Response JSON (example)**
```json
{
  "status": "ingested",
  "sessionId": "session-abc",
  "graphitiAdded": true
}
```

Notes:
- `/session/ingest` sends the full transcript to Graphiti as one episode.
- Synapse also creates a `SessionSummary` node and precomputes `bridge_text` for startbrief.

### Semantic Memory (Graphiti)
- Facts/entities/episodes in Graphiti (best-effort)
- Retrieved via /memory/query (on-demand)

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
- Episode bridge comes from local session summaries; enable `IDLE_CLOSE_ENABLED` for reliable episodeBridge.
- Noise filter may return `status: skipped` for very short messages.
- Nudge candidates repeat if `last_nudged_at` isn’t set after you send one.

References:
- `DECISIONS.md` for architectural intent
- `AUDIT_MEMORY_V1.md` for as‑built details
