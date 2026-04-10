# Memory Retrieval Contract v1

This contract defines stable semantics for semantic recall from Synapse.

## Endpoint
`POST /memory/query`

## Request
```json
{
  "tenantId": "string",
  "userId": "string",
  "query": "string",
  "limit": 10,
  "memoryIntent": "exact|episodic|hybrid",
  "referenceTime": "ISO-8601 optional",
  "includeContext": false,
  "focusQuery": "string optional"
}
```

## Response (minimum)
```json
{
  "facts": ["..."],
  "factItems": [
    {
      "text": "string",
      "relevance": 0.0,
      "source": "graphiti|graphiti_session_summary|user_model",
      "relevance_tier": "recent|persistent|stale",
      "domain": "health|wellness|work|goals|worries|relationships|routines|finance|learning|spirituality|home|general",
      "sourceTenant": "default"
    }
  ],
  "entities": [],
  "episodes": [
    {
      "episodeId": "string|null",
      "sessionId": "string|null",
      "referenceTime": "ISO-8601|null",
      "score": 0.0,
      "summary": "string",
      "evidence": ["User: ...", "Assistant: ..."],
      "linkedEntities": ["Ashley", "Bluum"],
      "sourceTenant": "default"
    }
  ],
  "metadata": {
    "query": "string",
    "memoryIntent": "exact|episodic|hybrid",
    "responseMode": "recall|context",
    "facts": 0,
    "entities": 0,
    "episodes": 0,
    "limit": 10,
    "tenantCanonical": "default",
    "tenantScope": ["default", "sophie-prod"],
    "provenanceCounts": {"graphiti": 2, "user_model": 1},
    "domainBreakdown": {"health": 2, "work": 1}
  }
}
```

## Semantics
- `facts` is a display-ready deduped list.
- `factItems` is the authoritative retrieval payload with provenance.
- `episodes` is compact episodic recall output ranked for conversational continuation.
- `source` indicates memory surface origin.
- `sourceTenant` indicates which tenant namespace produced the item.
- `domain` is classifier-assigned and used for orchestration/routing.
- `memoryIntent` controls retrieval path:
  - `exact`: fact/entity-centric (default, backward compatible)
  - `episodic`: episode-centric recall with transcript-window embeddings + evidence snippets
  - `hybrid`: combines exact + episodic in one response

## Episodic retrieval unit
- Episodic embeddings are built from transcript windows (`User:/Assistant:` spans) per session episode.
- This preserves conversational nuance better than single session summaries while remaining compact enough to index and rank quickly.

## Episodic ranking (current)
- Hybrid scoring combines:
  - embedding similarity (vector recall over transcript windows)
  - lexical overlap
  - recency
  - linked-entity overlap
  - continuation-intent boost (for prompts like "remember that thread")
- Guardrails:
  - low-signal candidates are suppressed via minimum score/similarity thresholds
  - `metadata.episodicWeakRecall=true` is emitted when episodic confidence is weak

## Tenant behavior
- `tenantId` is canonicalized for writes.
- Read path fans out to canonical + known aliases to prevent historical split loss.

## Relevance behavior
- Graphiti facts are time-filtered using `referenceTime` when available.
- Session summary/user model facts are gated by query-overlap to reduce generic bleed-through.

## Backward compatibility
- Fields are additive; existing clients reading `facts` remain valid.
- Clients should migrate to `factItems` for strict provenance and routing logic.

## Productization note
- Backend routing defaults, weak-recall handling, runtime prompt injection policy, and rollout guardrails are defined in:
  - `docs/BACKEND_MEMORY_INTEGRATION_GUIDE_V1.md`
