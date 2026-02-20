#!/usr/bin/env bash
set -euo pipefail

HOST="${SYNAPSE_BASE_URL:-${HOST:-http://localhost:8000}}"
TENANT_ID="${TENANT_ID:-default}"
USER_ID="${USER_ID:-cmkqxf72t0000lb04axesvlpx}"
QUERY="${QUERY:-What should the assistant follow up on next?}"
NOW="${NOW:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
TIMEZONE="${TIMEZONE:-UTC}"
MAX_TIME="${MAX_TIME:-30}"

tmp_startbrief="$(mktemp)"
tmp_memory="$(mktemp)"
tmp_loops="$(mktemp)"
cleanup() {
  rm -f "$tmp_startbrief" "$tmp_memory" "$tmp_loops"
}
trap cleanup EXIT

echo "[contract] GET /session/startbrief"
curl -sS --max-time "$MAX_TIME" \
  "$HOST/session/startbrief?tenantId=$TENANT_ID&userId=$USER_ID&now=$NOW&timezone=$TIMEZONE" \
  > "$tmp_startbrief"

echo "[contract] POST /memory/query"
payload="$(cat <<EOF
{
  "tenantId": "$TENANT_ID",
  "userId": "$USER_ID",
  "query": "$QUERY",
  "limit": 5,
  "referenceTime": "$NOW"
}
EOF
)"
curl -sS --max-time "$MAX_TIME" \
  -X POST "$HOST/memory/query" \
  -H "Content-Type: application/json" \
  -d "$payload" \
  > "$tmp_memory"

echo "[contract] GET /memory/loops"
curl -sS --max-time "$MAX_TIME" \
  "$HOST/memory/loops?tenantId=$TENANT_ID&userId=$USER_ID&limit=5" \
  > "$tmp_loops"

python3 - "$tmp_startbrief" "$tmp_memory" "$tmp_loops" <<'PY'
import json
import sys
from pathlib import Path

startbrief_path = Path(sys.argv[1])
memory_path = Path(sys.argv[2])
loops_path = Path(sys.argv[3])

def fail(msg: str) -> None:
    print(f"[contract][FAIL] {msg}")
    sys.exit(1)

def ensure(cond: bool, msg: str) -> None:
    if not cond:
        fail(msg)

try:
    startbrief = json.loads(startbrief_path.read_text())
except Exception as e:
    fail(f"/session/startbrief returned invalid JSON: {e}")

try:
    memory = json.loads(memory_path.read_text())
except Exception as e:
    fail(f"/memory/query returned invalid JSON: {e}")

try:
    loops = json.loads(loops_path.read_text())
except Exception as e:
    fail(f"/memory/loops returned invalid JSON: {e}")

start_required = {"timeOfDayLabel", "timeGapHuman", "bridgeText", "items"}
ensure(start_required.issubset(startbrief.keys()), f"/session/startbrief missing keys: {start_required - set(startbrief.keys())}")
ensure(isinstance(startbrief["items"], list), "/session/startbrief.items must be a list")
for i, item in enumerate(startbrief["items"]):
    ensure(isinstance(item, dict), f"/session/startbrief.items[{i}] must be an object")
    ensure("kind" in item and "text" in item, f"/session/startbrief.items[{i}] missing kind/text")

memory_required = {"facts", "factItems", "entities", "metadata"}
ensure(memory_required.issubset(memory.keys()), f"/memory/query missing keys: {memory_required - set(memory.keys())}")
ensure(isinstance(memory["facts"], list), "/memory/query.facts must be a list")
ensure(isinstance(memory["factItems"], list), "/memory/query.factItems must be a list")
ensure(isinstance(memory["entities"], list), "/memory/query.entities must be a list")
ensure(isinstance(memory["metadata"], dict), "/memory/query.metadata must be an object")
ensure(memory["metadata"].get("responseMode") in {"recall", "context"}, "/memory/query.metadata.responseMode invalid")

loops_required = {"items", "metadata"}
ensure(loops_required.issubset(loops.keys()), f"/memory/loops missing keys: {loops_required - set(loops.keys())}")
ensure(isinstance(loops["items"], list), "/memory/loops.items must be a list")
ensure(isinstance(loops["metadata"], dict), "/memory/loops.metadata must be an object")
for i, item in enumerate(loops["items"]):
    ensure(isinstance(item, dict), f"/memory/loops.items[{i}] must be an object")
    ensure("id" in item and "type" in item and "text" in item, f"/memory/loops.items[{i}] missing id/type/text")

print("[contract][PASS] /session/startbrief, /memory/query, and /memory/loops response shapes are valid")
print("[contract][INFO] startbrief.bridgeText =", repr(startbrief.get("bridgeText")))
print("[contract][INFO] memory facts/entities =", len(memory.get("facts", [])), "/", len(memory.get("entities", [])))
print("[contract][INFO] loops count =", len(loops.get("items", [])))
PY
