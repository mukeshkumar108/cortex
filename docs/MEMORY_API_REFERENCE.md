# Memory API Reference

Purpose: human-facing contract for the new runtime memory surfaces.

## `GET /session/handover`

Builds a compact startup packet for Sophie from living context, open threads, entities, identity, and episodic recall.

### Query Params
- `user_id` (preferred) or `userId` (compat): required

### Response Shape

```json
{
  "generated_at": "ISO8601",
  "living_context": {
    "current_focus": "...",
    "primary_tension": "...",
    "unspoken_goal": "...",
    "emotional_texture": "...",
    "relationship_pulse": "..."
  },
  "sophie_directives": [],
  "active_contradictions": [],
  "open_threads": [],
  "people": [],
  "projects": [],
  "episodic_recall": [],
  "identity": {
    "current_chapter": "...",
    "core_values": [],
    "persistent_goals": [],
    "what_they_want": "..."
  }
}
```

### Semantics

- `living_context`: current-state synthesis, not lifelong profile.
- `sophie_directives`: behavioral rules derived from explicit user statements.
- `active_contradictions`: earlier vs recent view pairs to avoid silent overwrite.
- `open_threads`: top open items, persistent goals prioritized.
- `people`: active person entities with profile/status.
- `projects`: clustered project workstreams (e.g., Sophie/Synapse cluster).
- `episodic_recall`: semantic nearest sessions based on current tension/focus.

## `POST /memory/search`

Semantic recall over `session_classifications.memory_deltas`.

### Request

```json
{
  "user_id": "cmkqxf72t0000lb04axesvlpx",
  "query": "Jasmine",
  "limit": 5
}
```

### Response

```json
{
  "user_id": "...",
  "query": "...",
  "results": [
    {
      "session_id": "...",
      "session_date": "ISO8601",
      "session_kind": "personal|technical|mixed|transient",
      "emotional_weight": "none|low|medium|high",
      "memory_deltas": [],
      "similarity": 0.52
    }
  ]
}
```

### Error Cases

- `400`: missing `user_id` or `query`
- `500`: embedding generation/query failure

## Compatibility Notes

- Environment in docs may expose API on `:8000`.
- If local orchestration maps to `:3000`, use that base URL accordingly.

