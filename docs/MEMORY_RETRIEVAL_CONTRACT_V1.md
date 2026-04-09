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
  "metadata": {
    "query": "string",
    "responseMode": "recall|context",
    "facts": 0,
    "entities": 0,
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
- `source` indicates memory surface origin.
- `sourceTenant` indicates which tenant namespace produced the item.
- `domain` is classifier-assigned and used for orchestration/routing.

## Tenant behavior
- `tenantId` is canonicalized for writes.
- Read path fans out to canonical + known aliases to prevent historical split loss.

## Relevance behavior
- Graphiti facts are time-filtered using `referenceTime` when available.
- Session summary/user model facts are gated by query-overlap to reduce generic bleed-through.

## Backward compatibility
- Fields are additive; existing clients reading `facts` remain valid.
- Clients should migrate to `factItems` for strict provenance and routing logic.
