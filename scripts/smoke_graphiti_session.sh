#!/usr/bin/env bash
set -euo pipefail

TENANT_ID=${TENANT_ID:-default}
USER_ID=${USER_ID:-user_smoke_dummy}
PERSONA_ID=${PERSONA_ID:-persona_1}
SESSION_ID=${SESSION_ID:-session_smoke_dummy}
NOW=${NOW:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}

post_json() {
  local path="$1"
  local payload="$2"
  docker exec -i synapse-api sh -lc "curl -s --max-time 30 -X POST http://localhost:8000${path} -H 'Content-Type: application/json' -d @-" <<JSON
${payload}
JSON
}

get_url() {
  local path="$1"
  docker exec -i synapse-api sh -lc "curl -s --max-time 30 'http://localhost:8000${path}'"
}

cat <<JSON > /tmp/smoke_session_payload.json
{
  "tenantId": "${TENANT_ID}",
  "userId": "${USER_ID}",
  "personaId": "${PERSONA_ID}",
  "sessionId": "${SESSION_ID}",
  "startedAt": "${NOW}",
  "endedAt": "${NOW}",
  "messages": [
    {"role":"user","text":"Hi, I'm Mukesh and I live in London.","timestamp":"${NOW}"},
    {"role":"assistant","text":"Nice to meet you. What's on your mind?","timestamp":"${NOW}"},
    {"role":"user","text":"I'm working on Sophie and fighting flaky tests.","timestamp":"${NOW}"},
    {"role":"assistant","text":"That sounds stressful. Anything else?","timestamp":"${NOW}"},
    {"role":"user","text":"My girlfriend Ashley is helping me test. I'm tired.","timestamp":"${NOW}"},
    {"role":"assistant","text":"Got it. Maybe take a short break.","timestamp":"${NOW}"}
  ]
}
JSON

INGEST_RESP=$(post_json "/session/ingest" "$(cat /tmp/smoke_session_payload.json)")
echo "[ingest] ${INGEST_RESP}"

echo "[wait] sleeping 10s for Graphiti extraction"
sleep 10

BRIEF_RESP=$(get_url "/session/brief?tenantId=${TENANT_ID}&userId=${USER_ID}&now=${NOW}")
echo "[brief] ${BRIEF_RESP}"

MEM_QUERY=$(post_json "/memory/query" "{\"tenantId\":\"${TENANT_ID}\",\"userId\":\"${USER_ID}\",\"query\":\"Who is Ashley?\",\"limit\":5,\"referenceTime\":\"${NOW}\"}")
echo "[memory] ${MEM_QUERY}"
