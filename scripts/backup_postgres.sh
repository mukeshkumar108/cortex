#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BACKUP_DIR="${BACKUP_DIR:-$ROOT_DIR/backups}"
SERVICE_NAME="${POSTGRES_SERVICE_NAME:-postgres}"
POSTGRES_DB_NAME="${POSTGRES_DB_NAME:-synapse}"
POSTGRES_USER_NAME="${POSTGRES_USER_NAME:-synapse}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$BACKUP_DIR/postgres_${POSTGRES_DB_NAME}_${TIMESTAMP}.sql.gz"
TMP_FILE="$OUT_FILE.tmp"

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

echo "[backup] creating $OUT_FILE"
docker compose exec -T "$SERVICE_NAME" \
  pg_dump -U "$POSTGRES_USER_NAME" "$POSTGRES_DB_NAME" \
  | gzip -c > "$TMP_FILE"

mv "$TMP_FILE" "$OUT_FILE"
chmod 600 "$OUT_FILE"

echo "[backup] done: $OUT_FILE"
