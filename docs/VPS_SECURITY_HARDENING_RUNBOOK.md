# VPS Security Hardening Runbook

This runbook covers the minimum hardening steps for the current Synapse VPS deployment without changing route contracts or removing Graphiti/FalkorDB.

## Current intent

- `postgres` host port should be bound to `127.0.0.1:5432`
- `falkordb` host port should be bound to `127.0.0.1:6379`
- `synapse` API remains published on `0.0.0.0:8000` for now

Why:

- Postgres and FalkorDB do not need public internet exposure.
- Host-local binds preserve operator workflows such as `psql`, `pg_dump`, and localhost diagnostics.
- API exposure was left unchanged because external Sophie/reverse-proxy usage has not been conclusively replaced by a localhost-only path.

## Check what is open

Run these from the VPS:

```bash
ss -tulpn
docker compose ps
ufw status verbose
```

Notes:

- `ufw status verbose` may require `sudo` depending on the shell user.
- Docker-published ports can create exposure even when application auth exists.

## Important Docker firewall warning

Do not assume UFW alone protects Docker-published ports.

Docker manipulates iptables directly. A service published as `0.0.0.0:5432:5432` or `0.0.0.0:6379:6379` is a real exposure risk even if the host firewall rules look reasonable at a glance.

The first control is the Compose bind itself:

- preferred for DBs: `127.0.0.1:HOSTPORT:CONTAINERPORT`
- stronger but less operator-friendly: remove host port publishing entirely

## Recommended UFW posture

Do not apply blindly. Confirm how Sophie or any reverse proxy reaches Synapse first.

Typical baseline:

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
```

If Synapse API must stay directly reachable on port `8000`, restrict by source IP if possible:

```bash
sudo ufw allow from <TRUSTED_IP_OR_CIDR> to any port 8000 proto tcp
```

If Synapse should only be reached through a reverse proxy, do not keep `8000` generally open.

## Apply the compose hardening

This repo now configures:

- `postgres`: `127.0.0.1:5432:5432`
- `falkordb`: `127.0.0.1:6379:6379`

To apply the new bindings:

```bash
docker compose up -d
docker compose ps
ss -tulpn
```

Expected result after recreate:

- `127.0.0.1:5432` only
- `127.0.0.1:6379` only
- `0.0.0.0:8000` remains unless you explicitly change it later

## Verify after recreate

Local checks:

```bash
curl -fsS http://localhost:8000/health
```

Full local verification:

```bash
SYNAPSE_INTERNAL_TOKEN=... \
SYNAPSE_TEST_TENANT_ID=... \
SYNAPSE_TEST_USER_ID=... \
SYNAPSE_TEST_SESSION_ID=... \
scripts/verify_vps_hardening.sh
```

## Backups

Manual Postgres backup:

```bash
scripts/backup_postgres.sh
```

Output:

- writes timestamped dumps under `/opt/synapse/backups`
- format: `postgres_synapse_YYYYMMDDTHHMMSSZ.sql.gz`
- does not delete old backups

Optional cron example:

```bash
0 3 * * * cd /opt/synapse && /bin/bash scripts/backup_postgres.sh >> /opt/synapse/logs/backup_postgres.log 2>&1
```

Do not enable cron until you confirm disk retention policy and monitoring.

## Rollback

If you need to restore old host-wide DB exposure:

1. Revert `docker-compose.yml`:
   - `127.0.0.1:5432:5432` -> `5432:5432`
   - `127.0.0.1:6379:6379` -> `6379:6379`
2. Recreate containers:

```bash
docker compose up -d
```

## Remaining risks

- `synapse` API still publishes on `0.0.0.0:8000`
- token auth protects core Sophie routes, but network exposure remains broader than ideal
- UFW can be misleading if Compose publishes ports publicly
- no automated backup scheduling is enabled yet
