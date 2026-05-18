#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

pass() {
  echo "[PASS] $1"
}

fail() {
  echo "[FAIL] $1" >&2
  exit 1
}

warn() {
  echo "[WARN] $1"
}

echo "[check] localhost API health"
curl -fsS http://localhost:8000/health >/dev/null || fail "localhost API health check failed"
pass "localhost API health"

echo "[check] compose exposure config"
grep -Fq '"127.0.0.1:5432:5432"' docker-compose.yml || fail "docker-compose.yml is missing 127.0.0.1 bind for postgres"
grep -Fq '"127.0.0.1:6379:6379"' docker-compose.yml || fail "docker-compose.yml is missing 127.0.0.1 bind for falkordb"
pass "compose exposure config for postgres and falkordb"

echo "[check] container DNS + internal reachability"
docker compose exec -T synapse sh -lc 'python - <<'"'"'PY'"'"'
import socket
for host, port in [("postgres", 5432), ("falkordb", 6379)]:
    s = socket.create_connection((host, port), timeout=3)
    s.close()
    print(f"ok {host}:{port}")
PY' >/dev/null || fail "synapse container cannot reach postgres/falkordb"
pass "synapse container reaches postgres and falkordb"

if [[ -n "${SYNAPSE_INTERNAL_TOKEN:-}" && -n "${SYNAPSE_TEST_TENANT_ID:-}" && -n "${SYNAPSE_TEST_USER_ID:-}" && -n "${SYNAPSE_TEST_SESSION_ID:-}" ]]; then
  echo "[check] smoke_synapse_contract.sh"
  SYNAPSE_BASE_URL="${SYNAPSE_BASE_URL:-http://localhost:8000}" \
    "$ROOT_DIR/scripts/smoke_synapse_contract.sh" >/dev/null || fail "smoke_synapse_contract.sh failed"
  pass "smoke_synapse_contract.sh"
else
  warn "smoke check skipped; set SYNAPSE_INTERNAL_TOKEN, SYNAPSE_TEST_TENANT_ID, SYNAPSE_TEST_USER_ID, and SYNAPSE_TEST_SESSION_ID"
fi

echo "[info] runtime port bindings may still show old values until containers are recreated"
echo "[info] inspect with: docker compose ps"
