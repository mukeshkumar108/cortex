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
  "referenceTime": "2026-02-03T18:35:00Z",
  "includeContext": false
}
```

**Response (default `includeContext=false`)**
```json
{
  "facts": [
    "User felt stressed at the gym.",
    "Launch was scheduled for Friday 9 AM."
  ],
  "factItems": [
    {"text": "User felt stressed at the gym.", "relevance": null, "source": "graphiti"},
    {"text": "Launch was scheduled for Friday 9 AM.", "relevance": null, "source": "graphiti"}
  ],
  "entities": [
    {"summary": "stressed", "type": "MentalState", "uuid": "..."},
    {"summary": "gym", "type": "Environment", "uuid": "..."}
  ],
  "metadata": {
    "query": "What is the user stressed about?",
    "responseMode": "recall",
    "facts": 2,
    "entities": 2,
    "limit": 10
  }
}
```

Compatibility mode:
- Set `includeContext=true` to include: `openLoops`, `commitments`, `contextAnchors`, `userStatedState`, `currentFocus`, `recallSheet`, `supplementalContext`.

---

### GET /memory/loops
Prioritized procedural memory (active loops) from Postgres.

**Query params**
```
tenantId=tenant_a&userId=user_1&limit=10&personaId=<optional>&domain=<optional>
```
Note: loops are user-scoped memory. `personaId` is compatibility-only and ignored for retrieval.

**Response (example)**
```json
{
  "items": [
    {
      "id": "uuid",
      "type": "thread",
      "text": "Complete portfolio refresh and model rollout",
      "status": "active",
      "salience": 5,
      "timeHorizon": "ongoing",
      "dueDate": null,
      "lastSeenAt": "2026-02-18T14:49:19.144334+00:00",
      "domain": "career",
      "importance": 5,
      "urgency": 3,
      "tags": ["portfolio", "rollout"],
      "personaId": null
    }
  ],
  "metadata": {
    "count": 1,
    "limit": 10,
    "sort": "priority_desc",
    "domainFilter": null,
    "personaId": null,
    "scope": "user"
  }
}
```

---

### GET /user/model
Persistent synthesized user model for runtime continuity and discovery overlays.

**Query params**
```
tenantId=tenant_a&userId=user_1
```

**Response (shape highlights)**
```json
{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "model": {
    "north_star": {
      "relationships": {"vision": null, "goal": null, "status": "unknown"},
      "work": {"vision": "...", "goal": "...", "status": "active"},
      "health": {"vision": null, "goal": "...", "status": "active"},
      "spirituality": {"vision": null, "goal": null, "status": "unknown"},
      "general": {"vision": null, "goal": "...", "status": "active"}
    },
    "current_focus": {"text": "...", "source": "inferred|user_stated", "confidence": 0.55},
    "key_relationships": [],
    "work_context": {"text": "...", "source": "inferred|user_stated", "confidence": 0.55},
    "patterns": [],
    "preferences": {"tone": "...", "avoid": [], "notes": []},
    "health": null,
    "spirituality": null
  },
  "completenessScore": {
    "relationships": 0,
    "work": 0,
    "north_star": 0,
    "health": 0,
    "spirituality": 0,
    "general": 0
  },
  "version": 0,
  "exists": false,
  "lastSource": "manual_update|auto_updater|null"
}
```

Updater semantics:
- `north_star.*.goal` may be inferred from loops/sessions (low confidence).
- `north_star.*.vision` only updates from explicit user-stated signals (high confidence).
- Domain status is tri-state: `active|inactive|unknown`.

---

### GET /session/brief
Narrative start‑brief derived from Graphiti custom entities.

**Query params**
```
tenantId=tenant_a&userId=user_1&now=2026-02-03T18:35:00Z&sessionId=session-abc&personaId=persona_1&timezone=America/Los_Angeles
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
tenantId=tenant_a&userId=user_1&now=2026-02-03T18:35:00Z&sessionId=<optional>&personaId=<optional>&timezone=<IANA optional>
```

**Response (exact shape)**
```json
{
  "handover_text": "string",
  "handover_depth": "continuation|yesterday|weekly",
  "time_context": {
    "local_time": "HH:MM",
    "time_of_day": "MORNING|AFTERNOON|EVENING|NIGHT",
    "gap_minutes": 0,
    "sessions_today": 0,
    "first_session_today": true
  },
  "resume": {
    "use_bridge": false,
    "bridge_text": "string|null"
  },
  "ops_context": {
    "top_loops_today": [],
    "waiting_on": [],
    "user_model_hints": [],
    "yesterday_themes": [],
    "steering_note": "string|null"
  },
  "evidence": {
    "session_summary_ids_used": [],
    "session_summary_ids_fetched": [],
    "claim_ranking": [
      {
        "session_id": "string",
        "score": 0.0,
        "recency": 0.0,
        "salience": 0.0,
        "importance": 0.0,
        "confidence": 0.0,
        "contradiction_penalty": 0.0
      }
    ],
    "claim_ranking_defs": {
      "salience": "Immediate intensity/urgency of a session claim (short-horizon prominence).",
      "importance": "Durable relevance inferred from recurrence across recent sessions and alignment with active loops.",
      "confidence": "Trust in claim quality based on summary quality and salience band."
    },
    "loop_ranking": [
      {
        "id": "string|null",
        "text": "string",
        "type": "string|null",
        "score": 0.0,
        "recency": 0.0,
        "salience": 0.0,
        "importance": 0.0,
        "confidence": 0.0,
        "contradiction_penalty": 0.0
      }
    ],
    "loop_ranking_defs": {
      "salience": "Immediate urgency/intensity of the loop signal.",
      "importance": "Durable relevance by loop type/time horizon and commitment durability.",
      "confidence": "Trust in loop signal quality derived from explicit confidence or salience proxy."
    },
    "summary_fetch_count": 0,
    "summary_used_count": 0,
    "summary_content_quality": "ok|none_fetched|empty_after_normalization",
    "fallback_used": false,
    "fallback_success": false,
    "daily_analysis_date_used": "YYYY-MM-DD|null",
    "freshness": {
      "has_pending_session_ingest_jobs": false,
      "pending_raw_episode_jobs": 0,
      "pending_post_ingest_hook_jobs": 0,
      "oldest_pending_age_seconds": 0,
      "latest_pending_session_id": "string|null"
    }
  }
}
```

**Response (example)**
```json
{
  "handover_text": "It is evening, and your most recent thread is still active around the portfolio refresh and one unresolved blocker in test stability.",
  "handover_depth": "continuation",
  "time_context": {"local_time": "18:35", "time_of_day": "EVENING", "gap_minutes": 480, "sessions_today": 1, "first_session_today": false},
  "resume": {"use_bridge": true, "bridge_text": "Continue from the portfolio refresh thread and resolve the remaining test blocker first."},
  "ops_context": {
    "top_loops_today": [{"kind": "loop", "text": "Finish portfolio site"}],
    "waiting_on": [],
    "user_model_hints": [],
    "yesterday_themes": [],
    "steering_note": null
  },
  "evidence": {
    "session_summary_ids_used": ["session-abc"],
    "session_summary_ids_fetched": ["session-abc"],
    "claim_ranking": [
      {
        "session_id": "session-abc",
        "score": 0.92,
        "recency": 1.0,
        "salience": 1.0,
        "importance": 0.9,
        "confidence": 0.85,
        "contradiction_penalty": 0.0
      }
    ],
    "claim_ranking_defs": {
      "salience": "Immediate intensity/urgency of a session claim (short-horizon prominence).",
      "importance": "Durable relevance inferred from recurrence across recent sessions and alignment with active loops.",
      "confidence": "Trust in claim quality based on summary quality and salience band."
    },
    "loop_ranking": [
      {
        "id": null,
        "text": "Repair relationship with Jasmine",
        "type": "thread",
        "score": 0.74,
        "recency": 0.55,
        "salience": 1.0,
        "importance": 0.72,
        "confidence": 0.9,
        "contradiction_penalty": 0.0
      }
    ],
    "loop_ranking_defs": {
      "salience": "Immediate urgency/intensity of the loop signal.",
      "importance": "Durable relevance by loop type/time horizon and commitment durability.",
      "confidence": "Trust in loop signal quality derived from explicit confidence or salience proxy."
    },
    "summary_fetch_count": 1,
    "summary_used_count": 1,
    "summary_content_quality": "ok",
    "fallback_used": false,
    "fallback_success": false,
    "daily_analysis_date_used": "2026-02-05",
    "freshness": {"has_pending_session_ingest_jobs": false, "pending_raw_episode_jobs": 0, "pending_post_ingest_hook_jobs": 0, "oldest_pending_age_seconds": null, "latest_pending_session_id": null}
  }
}
```

Notes:
- `bridgeText` is fact‑only, <= 280 chars, and excludes environment/observation by default.
- Items come primarily from Postgres loops (salience + recency), with optional unresolved tensions from Graphiti.
- `timeGapHuman` is derived from session/message timestamps when available, otherwise Graphiti episode time.
- `timeOfDayLabel` uses `timezone` when provided (fallback UTC).
- `evidence.claim_ranking` and `evidence.loop_ranking` expose why claims/loops were selected.
- Terminology:
  - `salience` = immediate urgency/intensity
  - `importance` = durable relevance across recurrence/persistence

---

### GET /internal/debug/startbrief/ranking (internal)
Debug-only ranking surface for traceability.

**Query params**
```
tenantId=<string>&userId=<string>&now=<ISO-8601 optional>&timezone=<IANA optional>&limit=<3..20 optional>
```

**Headers**
```
x-internal-token: <INTERNAL_TOKEN>
```

**Behavior**
- Returns full summary and loop candidate rankings with score components.
- Includes selected winners and definitions for each scoring field.

Use this endpoint to diagnose stale-memory precedence and ranking drift.

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
- Canonical session finalization is `/session/ingest` (durable transcript upsert + outbox enqueue).
- `/session/close` is legacy/admin-safe and enqueue-only: it may mark buffer closed and enqueue ingest when needed.

---

### POST /session/close
Legacy/admin-safe close endpoint (enqueue-only).

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
- `/session/close` does not execute heavy Graphiti/summary/loop work inline.
- If no existing `session_raw_episode` ingest job exists for the session, it enqueues one via the same durable path as `/session/ingest`.

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
    {"role": "user", "text": "My name is Jordan", "timestamp": "2026-02-04T18:00:01Z"},
    {"role": "assistant", "text": "Nice to meet you", "timestamp": "2026-02-04T18:00:05Z"}
  ]
}
```

**Response**
```json
{
  "status": "ingested",
  "sessionId": "session-abc",
  "graphitiAdded": false
}
```

Notes:
- `/session/ingest` durably upserts `session_transcript` and enqueues outbox jobs.
- Outbox jobs are typed:
  - `session_raw_episode`: writes raw transcript episode to Graphiti.
  - `post_ingest_hook` with `hook=session_summary|open_loops`.
- Graphiti/session-summary work is async via outbox drain, with retries/backoff and dead-letter on permanent errors.
- `/session/startbrief` exposes ingest freshness in `evidence.freshness` so orchestration can explain brief lag.

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
6) Canonical finalization: call `/session/ingest`. `/session/close` remains legacy/admin-safe and enqueue-only.

---

## Debug endpoints (internal only)
All require header `X-Internal-Token` = `INTERNAL_TOKEN`.

- `GET /internal/debug/session?tenantId&userId&sessionId`
- `GET /internal/debug/user?tenantId&userId`
- `GET /internal/debug/outbox?tenantId&limit=50`
- `GET /internal/debug/session_ingest_status?tenant_id&user_id&session_id`
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
