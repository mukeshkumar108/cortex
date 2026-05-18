# Sophie <-> Synapse Contract

This document defines the operational contract Sophie should use against Synapse after the May 18, 2026 recovery patch.

## Auth

All Sophie-facing internal routes in this contract require:

- Header: `X-Internal-Token: <token>`
- FastAPI parameter name in Synapse: `x_internal_token`
- Validation source: `INTERNAL_TOKEN` / `Settings.internal_token`

Requests without the header, or with the wrong token, return:

```json
{"detail":"Unauthorized"}
```

with HTTP `401`.

## Required Sophie env vars

- `SYNAPSE_BASE_URL`
- `SYNAPSE_INTERNAL_TOKEN`
- `SOPHIE_TENANT_ID`
- `SOPHIE_USER_ID`
- `SOPHIE_SESSION_ID` for session-scoped flows
- `SOPHIE_TIMEZONE` for day/start flows

Optional but useful:

- `SOPHIE_PERSONA_ID`
- `SOPHIE_REFERENCE_TIME` for deterministic replay/testing

## Endpoint list

- `POST /ingest`
- `POST /session/ingest`
- `POST /session/close`
- `GET /session/startbrief`
- `POST /memory/query`
- `POST /v2/memory/query`
- `GET /signals/pack`
- `GET /user/model`
- `GET /day/brief`
- `POST /actions/items`
- `POST /integrations/google/calendar/import`

## Request examples

### `GET /session/startbrief`

```bash
curl -sS "$SYNAPSE_BASE_URL/session/startbrief?tenantId=$SOPHIE_TENANT_ID&userId=$SOPHIE_USER_ID&sessionId=$SOPHIE_SESSION_ID&timezone=$SOPHIE_TIMEZONE" \
  -H "X-Internal-Token: $SYNAPSE_INTERNAL_TOKEN"
```

Response shape summary:

- `handover_text: string`
- `handover_depth: string`
- `time_context: object`
- `resume: object`
- `ops_context: object`
- `evidence: object`
- `entity_hints: array`

### `POST /session/ingest`

```bash
curl -sS "$SYNAPSE_BASE_URL/session/ingest" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Token: $SYNAPSE_INTERNAL_TOKEN" \
  -d '{
    "tenantId": "'"$SOPHIE_TENANT_ID"'",
    "userId": "'"$SOPHIE_USER_ID"'",
    "sessionId": "'"$SOPHIE_SESSION_ID"'",
    "messages": [
      {"role": "user", "text": "hello", "timestamp": "2026-05-18T10:00:00Z"}
    ]
  }'
```

Response shape summary:

- `status: "ingested" | "skipped_empty_transcript"`
- `sessionId: string`
- `graphitiAdded: false`

### `POST /memory/query`

```bash
curl -sS "$SYNAPSE_BASE_URL/memory/query" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Token: $SYNAPSE_INTERNAL_TOKEN" \
  -d '{
    "tenantId": "'"$SOPHIE_TENANT_ID"'",
    "userId": "'"$SOPHIE_USER_ID"'",
    "query": "What matters right now?",
    "limit": 5
  }'
```

Response shape summary:

- `facts: string[]`
- `factItems: object[]`
- `entities: object[]`
- `episodes: object[]`
- `metadata: object`

### `GET /signals/pack`

```bash
curl -sS "$SYNAPSE_BASE_URL/signals/pack?tenantId=$SOPHIE_TENANT_ID&userId=$SOPHIE_USER_ID&sessionId=$SOPHIE_SESSION_ID" \
  -H "X-Internal-Token: $SYNAPSE_INTERNAL_TOKEN"
```

Response shape summary:

- `generated_at: string`
- `session_id: string | null`
- `classes: object`
- `debug: object`

## Flows

### Session start flow

1. Sophie creates or restores `tenantId`, `userId`, `sessionId`.
2. Sophie calls `GET /session/startbrief`.
3. Sophie uses:
   - `handover_text` for startup continuity
   - `ops_context` for tactical context
   - `entity_hints` only as grounding hints
   - `evidence.freshness` to detect lag from pending ingest work

### After-message ingest flow

1. During or after message handling, Sophie may call `POST /ingest` for turn-oriented ingestion.
2. Canonical durable write-back remains `POST /session/ingest` with the full transcript.
3. `POST /session/ingest` is enqueue-oriented and should stay low-latency.

### Session close flow

1. Sophie calls `POST /session/close` when a session is complete or timed out.
2. If `sessionId` is omitted, Synapse resolves the latest open session for that user.
3. This remains a legacy/admin-safe close path. Canonical full transcript persistence is still `POST /session/ingest`.

### Mid-conversation memory query flow

1. Sophie calls `POST /memory/query` for targeted recall.
2. Use specific prompts, not broad autobiographical fishing.
3. Treat `facts` and `factItems` as retrieval output, not guaranteed dialogue text.

### Signals/proactive flow

1. Sophie calls `GET /signals/pack` when she needs a compact proactive steering pack.
2. Treat high-sensitivity signals conservatively.
3. Use `debug.counts` and class distribution for diagnostics, not conversation content.

## Failure handling expectations

- `401`: missing or wrong internal token. Retry only after fixing auth.
- `400`: caller payload or domain validation issue. Do not blind-retry.
- `409` on `POST /v2/memory/query`: rollout gate blocked served v2 path for this scope.
- `500`: server-side failure. Retry with backoff only if the call is idempotent enough for the Sophie workflow.
- `503` on calendar import: feature disabled.

## Notes

- Graphiti/FalkorDB remains active in reads and writes. Clients should not assume a Postgres-only backend.
- Route payloads and scoring contracts were intentionally left unchanged by this patch.
- Protected operator visibility endpoint added: `GET /internal/debug/background_loops`
