#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${SYNAPSE_BASE_URL:-http://localhost:8000}"
INTERNAL_TOKEN="${SYNAPSE_INTERNAL_TOKEN:-}"
TEST_TENANT_ID="${SYNAPSE_TEST_TENANT_ID:-}"
TEST_USER_ID="${SYNAPSE_TEST_USER_ID:-}"
TEST_SESSION_ID="${SYNAPSE_TEST_SESSION_ID:-}"
NOW="${SYNAPSE_TEST_NOW:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
TIMEZONE="${SYNAPSE_TEST_TIMEZONE:-UTC}"
MAX_TIME="${SYNAPSE_MAX_TIME:-20}"

pass() {
  echo "[PASS] $1"
}

fail() {
  echo "[FAIL] $1" >&2
  exit 1
}

require_env() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    fail "Missing required env var: $name"
  fi
}

require_env "SYNAPSE_INTERNAL_TOKEN" "$INTERNAL_TOKEN"
require_env "SYNAPSE_TEST_TENANT_ID" "$TEST_TENANT_ID"
require_env "SYNAPSE_TEST_USER_ID" "$TEST_USER_ID"
require_env "SYNAPSE_TEST_SESSION_ID" "$TEST_SESSION_ID"

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

call_endpoint() {
  local name="$1"
  local method="$2"
  local path="$3"
  local body_file="${4:-}"

  local body_out="$tmpdir/${name}.body"
  local code_out="$tmpdir/${name}.code"

  if [[ -n "$body_file" ]]; then
    curl -sS --max-time "$MAX_TIME" \
      -X "$method" \
      "$BASE_URL$path" \
      -H "Content-Type: application/json" \
      -H "X-Internal-Token: $INTERNAL_TOKEN" \
      --data @"$body_file" \
      -o "$body_out" \
      -w "%{http_code}" > "$code_out"
  else
    local extra_headers=()
    if [[ "$path" != "/health" ]]; then
      extra_headers=(-H "X-Internal-Token: $INTERNAL_TOKEN")
    fi
    curl -sS --max-time "$MAX_TIME" \
      -X "$method" \
      "$BASE_URL$path" \
      "${extra_headers[@]}" \
      -o "$body_out" \
      -w "%{http_code}" > "$code_out"
  fi

  local status
  status="$(cat "$code_out")"
  if [[ "$status" != "200" ]]; then
    echo "[FAIL] $name returned HTTP $status" >&2
    cat "$body_out" >&2 || true
    exit 1
  fi
}

cat > "$tmpdir/session_ingest.json" <<EOF
{
  "tenantId": "$TEST_TENANT_ID",
  "userId": "$TEST_USER_ID",
  "sessionId": "$TEST_SESSION_ID",
  "startedAt": "$NOW",
  "endedAt": "$NOW",
  "messages": [
    {
      "role": "user",
      "text": "smoke contract check",
      "timestamp": "$NOW"
    }
  ]
}
EOF

cat > "$tmpdir/memory_query.json" <<EOF
{
  "tenantId": "$TEST_TENANT_ID",
  "userId": "$TEST_USER_ID",
  "query": "What is active right now?",
  "limit": 3,
  "referenceTime": "$NOW"
}
EOF

echo "[INFO] Base URL: $BASE_URL"
echo "[INFO] Test tenant/user/session: $TEST_TENANT_ID / $TEST_USER_ID / $TEST_SESSION_ID"

call_endpoint "health" "GET" "/health"
pass "GET /health"

call_endpoint "session_ingest" "POST" "/session/ingest" "$tmpdir/session_ingest.json"
pass "POST /session/ingest"

call_endpoint "session_startbrief" "GET" "/session/startbrief?tenantId=$TEST_TENANT_ID&userId=$TEST_USER_ID&sessionId=$TEST_SESSION_ID&now=$NOW&timezone=$TIMEZONE"
pass "GET /session/startbrief"

call_endpoint "memory_query" "POST" "/memory/query" "$tmpdir/memory_query.json"
pass "POST /memory/query"

call_endpoint "signals_pack" "GET" "/signals/pack?tenantId=$TEST_TENANT_ID&userId=$TEST_USER_ID&sessionId=$TEST_SESSION_ID&now=$NOW"
pass "GET /signals/pack"

python3 - "$tmpdir" <<'PY'
import json
import sys
from pathlib import Path

tmpdir = Path(sys.argv[1])

def load(name: str):
    path = tmpdir / f"{name}.body"
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        print(f"[FAIL] {name} returned invalid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

health = load("health")
session_ingest = load("session_ingest")
startbrief = load("session_startbrief")
memory_query = load("memory_query")
signals_pack = load("signals_pack")

if health.get("status") != "healthy":
    print("[FAIL] health payload missing status=healthy", file=sys.stderr)
    sys.exit(1)
if session_ingest.get("status") not in {"ingested", "skipped_empty_transcript"}:
    print("[FAIL] /session/ingest returned unexpected status", file=sys.stderr)
    sys.exit(1)
if "handover_text" not in startbrief or "ops_context" not in startbrief:
    print("[FAIL] /session/startbrief missing handover_text or ops_context", file=sys.stderr)
    sys.exit(1)
if "facts" not in memory_query or "metadata" not in memory_query:
    print("[FAIL] /memory/query missing facts or metadata", file=sys.stderr)
    sys.exit(1)
if "classes" not in signals_pack or "debug" not in signals_pack:
    print("[FAIL] /signals/pack missing classes or debug", file=sys.stderr)
    sys.exit(1)

print("[PASS] Response contracts validated")
print(f"[INFO] /session/ingest status={session_ingest.get('status')}")
print(f"[INFO] /memory/query facts={len(memory_query.get('facts') or [])}")
print(f"[INFO] /signals/pack classes={sorted((signals_pack.get('classes') or {}).keys())}")
PY
