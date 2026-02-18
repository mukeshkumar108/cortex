# Synapse Orchestrator Contract v1 (Graphiti‑native)

This document defines the integration contract between an orchestrator and Synapse. Graphiti is the semantic memory system. Synapse only stores operational state (sessions/transcripts) and provides on‑demand Graphiti queries.

## Endpoints

### POST /brief
Use at **session start** only (optional). This is a minimal seed, not a monolithic memory block.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "now": "2026-02-03T18:35:00Z"
}
```

**Response (example)**
```json
{
  "identity": {"name": null, "isDefault": true},
  "temporalAuthority": {
    "currentTime": "06:35 PM",
    "currentDay": "Tuesday",
    "timeOfDay": "evening",
    "timeSinceLastInteraction": "10 minutes ago"
  },
  "workingMemory": [
    {"role": "user", "text": "Let's plan tomorrow", "timestamp": "2026-02-03T18:34:12Z"}
  ],
  "rollingSummary": "We discussed planning tomorrow and priorities.",
  "semanticContext": [],
  "entities": [],
  "metadata": {
    "bufferSize": 1,
    "hasRollingSummary": true
  }
}
```

Notes:
- This response is intentionally minimal.
- Do **not** rely on /brief for semantic memory. Use /memory/query.

---

### POST /memory/query
On‑demand semantic memory lookup via Graphiti.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "query": "What are the user's current worries?",
  "limit": 10,
  "referenceTime": "2026-02-03T18:35:00Z"
}
```

**Response (example)**
```json
{
  "facts": [
    {"text": "User is stressed at the gym.", "relevance": null, "source": "graphiti"},
    {"text": "User is struggling with the blue-widget-glitch in Sophie and it is unresolved.", "relevance": null, "source": "graphiti"},
    {"text": "User feels burnt out from testing.", "relevance": null, "source": "graphiti"}
  ],
  "entities": [
    {"summary": "stressed", "type": "MentalState", "uuid": "..."},
    {"summary": "gym", "type": "Environment", "uuid": "..."},
    {"summary": "blue-widget-glitch", "type": "Tension", "uuid": "..."}
  ],
  "metadata": {
    "query": "What is the user stressed about?",
    "facts": 3,
    "entities": 3
  }
}
```

---

### GET /session/brief
Narrative start‑brief derived from Graphiti custom entities.

**Query params**
```
tenantId=tenant_a&userId=user_1&now=2026-02-03T18:35:00Z
```

**Response (example)**
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
- Built from Graphiti narrative entities: `MentalState`, `Tension`, `Environment`, `UserFocus`.
- Facts are filtered for quality (no single‑token/vague fragments).
- `narrativeSummary` is derived from Graphiti episode summaries and de‑duplicated from facts.
- Intended for session start only.
- `briefContext` is a compact, structured sheet (no narrative prose).

---

### GET /session/startbrief
Minimal start-brief with a short human bridge and durable items.

**Query params**
```
tenantId=tenant_a&userId=user_1&now=2026-02-03T18:35:00Z
```

**Response (example)**
```json
{
  "timeOfDayLabel": "AFTERNOON",
  "timeGapHuman": "8 hours since last spoke",
  "bridgeText": "Last time you spoke, you were focused on the portfolio refresh.",
  "items": [
    {"kind": "loop", "type": "thread", "text": "Finish portfolio site", "timeHorizon": "this_week", "salience": 4},
    {"kind": "tension", "text": "Flaky tests in release pipeline"}
  ]
}
```

Notes:
- `bridgeText` is fact‑only, <= 280 chars, and excludes environment/observation by default.
- Items come primarily from Postgres loops (salience + recency), with optional unresolved tensions from Graphiti.

---

### POST /ingest
Write both user and assistant turns. This stores the session transcript only.

**Request (user turn)**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "role": "user",
  "text": "Let's plan tomorrow. I need to book the dentist.",
  "timestamp": "2026-02-03T18:34:12Z",
  "sessionId": "session-abc"
}
```

**Request (assistant turn)**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "role": "assistant",
  "text": "Got it. What time works best?",
  "timestamp": "2026-02-03T18:34:18Z",
  "sessionId": "session-abc"
}
```

**Response (example)**
```json
{
  "status": "ingested",
  "sessionId": "session-abc",
  "graphitiAdded": false
}
```

Notes:
- /ingest returns quickly after buffer write; background janitor runs later.
- Session close (idle) sends **raw transcript** to Graphiti as one episode.

---

### POST /session/close
Public close endpoint to flush raw transcript to Graphiti.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "sessionId": "session-abc",
  "personaId": "persona_1"
}
```

**Response**
```json
{
  "closed": true,
  "sessionId": "session-abc"
}
```

Notes:
- If `sessionId` is omitted, Synapse closes the most recent open session for the user.
- On close, Synapse sends the raw transcript to Graphiti and performs best‑effort loop extraction
  (commitments/decisions/frictions/habits/threads) into Postgres with provenance metadata.

---

### POST /session/ingest
Use this if you keep working memory locally and only send full transcripts.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-abc",
  "startedAt": "2026-02-04T18:00:00Z",
  "endedAt": "2026-02-04T18:45:00Z",
  "messages": [
    {"role": "user", "text": "My name is Mukesh", "timestamp": "2026-02-04T18:00:01Z"},
    {"role": "assistant", "text": "Nice to meet you", "timestamp": "2026-02-04T18:00:05Z"}
  ]
}
```

**Response**
```json
{
  "status": "ingested",
  "sessionId": "session-abc",
  "graphitiAdded": true
}
```

---

## Orchestration Loop (Recommended)

1) **Session start** (optional): call `/brief` once to seed time + working memory.
2) **Optional**: call `/session/brief` to get a narrative start‑brief in one call.
2) Build the LLM prompt with:
   - recent working memory (last 1–6 turns)
   - rolling summary (if present)
3) **On demand memory**: call `/memory/query` with targeted questions.
4) LLM responds to user.
5) Call `/ingest` for the user turn and the assistant turn **or** use `/session/ingest` at end of session.
6) If user is inactive for 15 minutes, call `/session/close`.

---

## Debug endpoints (internal only)
All require header `X-Internal-Token` = `INTERNAL_TOKEN`.

- `GET /internal/debug/session?tenantId&userId&sessionId`
- `GET /internal/debug/user?tenantId&userId`
- `GET /internal/debug/outbox?tenantId&limit=50`
- `POST /internal/debug/close_session?tenantId&userId&sessionId`
- `POST /internal/debug/close_user_sessions?tenantId&userId&limit=20`
- `POST /internal/debug/emit_raw_episode?tenantId&userId&sessionId`
- `POST /internal/debug/emit_raw_user_sessions?tenantId&userId&limit=20`

---

## Orchestrator Checklist
- Always pass tenantId/userId/sessionId.
- Use /brief only at session start (optional).
- Use /memory/query for semantic memory.
- Ingest both user and assistant turns.
