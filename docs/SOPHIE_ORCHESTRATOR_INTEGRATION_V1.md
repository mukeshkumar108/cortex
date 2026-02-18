# Sophie Orchestrator Integration v1 (Graphiti‑native)

Synapse is a memory backend. It stores operational session state in Postgres and writes **raw session transcripts** to Graphiti on session close. Graphiti is the semantic memory system.

## What Synapse stores vs Graphiti

**Postgres (operational only)**
- session_buffer: rolling summary + last 12 messages
- session_transcript: raw turns for the session (archival)
- outbox: delivery queue for background work

**Graphiti (semantic memory)**
- episodes (full session transcript)
- extracted facts/entities/relationships
- temporal reasoning over contradictions

## How to use it (recommended)

### 1) Session start
Optionally call `/brief` once to seed time + working memory.

### 2) On‑demand memory
Call `/memory/query` for targeted questions when needed:
- “Who is Ashley?”
- “What is the user worried about?”
- “Any pending tasks?”

### 2b) Start‑brief (optional)
Call `/session/brief` to get a short narrative start‑brief derived from Graphiti’s
custom entities (MentalState, Tension, Environment).
Facts are filtered for quality (no single‑token/vague fragments). `narrativeSummary`
is derived from Graphiti episode summaries and de‑duplicated from facts.
Use `/session/startbrief` if you want a smaller bridgeText + durable items.

### 3) Ingest turns
Send both user and assistant turns to `/ingest`.

## Endpoints

### POST /brief (optional, minimal)
Returns temporal authority + working memory + rolling summary.

### POST /memory/query (Graphiti)
Returns facts/entities for a natural‑language query.
Example response:
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
  "entities": [
    {"summary": "gym", "type": "Environment", "uuid": "..."},
    {"summary": "blue-widget-glitch", "type": "Tension", "uuid": "..."},
    {"summary": "I'm focused on stabilizing the release pipeline", "type": "UserFocus", "uuid": "..."}
  ],
  "metadata": {"query": "What is the user stressed about?", "facts": 3, "entities": 3}
}
```

### GET /session/brief
Returns a structured start‑brief:
- time gap since last episode
- last 3 episode summaries
- unresolved tensions
- most recent mood + environment
Example response:
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

### GET /session/startbrief
Returns a minimal start‑brief (small bridge + durable items).
Example response:
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

### POST /ingest
Stores the turn in the session transcript and buffer.

### POST /session/close
Use after inactivity to flush raw transcript to Graphiti.
Stores a `SessionSummary` node in Graphiti and performs best‑effort loop extraction
(commitments/decisions/frictions/habits/threads) into Postgres.

**Request**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "sessionId": "session-abc",
  "personaId": "persona_1"
}
```

### POST /session/ingest
Use if Sophie stores working memory locally and only sends full transcripts.

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
Notes:
- `/session/ingest` sends the full transcript to Graphiti as one episode.
- Synapse also creates a `SessionSummary` node and precomputes `bridge_text` for startbrief.
## Failure behavior
- Graphiti down → /brief and /ingest still succeed.
- Memory query fails → orchestrator proceeds without semantic memory.

## Practical heuristics
- Call `/brief` only at session start.
- Use `/session/brief` when you want a narrative “start‑brief” in one call.
- Cache memory query results per session to avoid repeated calls.
- Ask Graphiti only when the user mentions a person, project, or asks for recall.
- Close sessions after 15 minutes of user inactivity via `/session/close`.
- Use `/session/ingest` if you want to avoid per‑turn Synapse calls.
