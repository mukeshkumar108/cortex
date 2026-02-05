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

### 3) Ingest turns
Send both user and assistant turns to `/ingest`.

## Endpoints

### POST /brief (optional, minimal)
Returns temporal authority + working memory + rolling summary.

### POST /memory/query (Graphiti)
Returns facts/entities for a natural‑language query.

### POST /ingest
Stores the turn in the session transcript and buffer.

### POST /session/close
Use after inactivity to flush raw transcript to Graphiti.

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
## Failure behavior
- Graphiti down → /brief and /ingest still succeed.
- Memory query fails → orchestrator proceeds without semantic memory.

## Practical heuristics
- Call `/brief` only at session start.
- Cache memory query results per session to avoid repeated calls.
- Ask Graphiti only when the user mentions a person, project, or asks for recall.
- Close sessions after 15 minutes of user inactivity via `/session/close`.
- Use `/session/ingest` if you want to avoid per‑turn Synapse calls.
