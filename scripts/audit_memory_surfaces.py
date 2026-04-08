#!/usr/bin/env python3
"""Audit where memory facts appear across Synapse surfaces for a user.

Usage example:
  python scripts/audit_memory_surfaces.py \
    --base-url http://localhost:8000 \
    --user-id <user_id> \
    --needle "kidney stones" \
    --tenant default --tenant sophie-prod
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, parse, request


@dataclass
class HttpResult:
    status: int
    data: Any
    error: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _http_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 10.0,
) -> HttpResult:
    payload = None
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    if body is not None:
        payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = request.Request(url=url, method=method.upper(), headers=req_headers, data=payload)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except Exception:
                data = {"raw": raw}
            return HttpResult(status=int(resp.status), data=data)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"raw": raw}
        return HttpResult(status=int(exc.code), data=parsed, error=f"HTTP {exc.code}")
    except Exception as exc:
        return HttpResult(status=0, data=None, error=str(exc))


def _flatten_strings(value: Any) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, str):
        text = " ".join(value.split())
        if text:
            yield text
        return
    if isinstance(value, dict):
        for v in value.values():
            yield from _flatten_strings(v)
        return
    if isinstance(value, list):
        for item in value:
            yield from _flatten_strings(item)
        return


def _find_matches(value: Any, needle: str, max_hits: int = 8) -> List[str]:
    target = needle.casefold().strip()
    if not target:
        return []
    hits: List[str] = []
    seen = set()
    for text in _flatten_strings(value):
        if target in text.casefold():
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            hits.append(text)
            if len(hits) >= max_hits:
                break
    return hits


def _add_query(url: str, params: Dict[str, Any]) -> str:
    pairs: List[Tuple[str, str]] = []
    for key, value in params.items():
        if value is None:
            continue
        pairs.append((key, str(value)))
    return f"{url}?{parse.urlencode(pairs)}" if pairs else url


async def _fetch_db_snapshot(database_url: str, user_id: str) -> Dict[str, Any]:
    import asyncpg

    conn = await asyncpg.connect(database_url)
    try:
        table_counts: Dict[str, List[Dict[str, Any]]] = {}
        for table in ("user_model", "session_transcript", "session_buffer", "graphiti_outbox", "loops"):
            rows = await conn.fetch(
                f"""
                SELECT tenant_id, count(*) AS rows
                FROM {table}
                WHERE user_id = $1
                GROUP BY tenant_id
                ORDER BY count(*) DESC, tenant_id ASC
                """,
                user_id,
            )
            table_counts[table] = [
                {"tenant_id": r["tenant_id"], "rows": int(r["rows"] or 0)}
                for r in rows
            ]

        user_model_rows = await conn.fetch(
            """
            SELECT tenant_id, updated_at, last_source, narrative_stable, narrative_current
            FROM user_model
            WHERE user_id = $1
            ORDER BY updated_at DESC
            """,
            user_id,
        )
        user_models = [
            {
                "tenant_id": r["tenant_id"],
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                "last_source": r["last_source"],
                "narrative_stable": r["narrative_stable"],
                "narrative_current": r["narrative_current"],
            }
            for r in user_model_rows
        ]

        outbox_rows = await conn.fetch(
            """
            SELECT tenant_id,
                   count(*) FILTER (WHERE status = 'pending') AS pending,
                   count(*) FILTER (WHERE status = 'failed') AS failed,
                   count(*) FILTER (WHERE status = 'sent') AS sent
            FROM graphiti_outbox
            WHERE user_id = $1
            GROUP BY tenant_id
            ORDER BY tenant_id ASC
            """,
            user_id,
        )
        outbox = [
            {
                "tenant_id": r["tenant_id"],
                "pending": int(r["pending"] or 0),
                "failed": int(r["failed"] or 0),
                "sent": int(r["sent"] or 0),
            }
            for r in outbox_rows
        ]

        return {
            "table_counts_by_tenant": table_counts,
            "user_model_rows": user_models,
            "outbox_status_by_tenant": outbox,
        }
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit memory recall surfaces for a Synapse user")
    parser.add_argument("--base-url", default=os.getenv("SYNAPSE_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--tenant", action="append", dest="tenants")
    parser.add_argument("--needle", action="append", dest="needles")
    parser.add_argument("--query", action="append", dest="queries")
    parser.add_argument("--internal-token", default=os.getenv("SYNAPSE_INTERNAL_TOKEN"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--timeout", type=float, default=12.0)
    args = parser.parse_args()

    tenants = args.tenants or ["default", "sophie-prod"]
    dedup_tenants: List[str] = []
    seen = set()
    for t in tenants:
        clean = (t or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        dedup_tenants.append(clean)

    now_iso = _now_iso()
    needles = [n for n in (args.needles or []) if (n or "").strip()]
    queries = [q for q in (args.queries or []) if (q or "").strip()]
    if not queries:
        queries = needles[:] if needles else ["hospital kidney stones", "hospital"]

    out: Dict[str, Any] = {
        "generated_at": now_iso,
        "base_url": args.base_url,
        "user_id": args.user_id,
        "tenants_checked": dedup_tenants,
        "queries": queries,
        "needles": needles,
        "tenants": {},
    }

    internal_headers = {"x-internal-token": args.internal_token} if args.internal_token else {}

    for tenant in dedup_tenants:
        tenant_out: Dict[str, Any] = {"public": {}, "internal": {}}

        user_model_url = _add_query(
            f"{args.base_url.rstrip('/')}/user/model",
            {"tenantId": tenant, "userId": args.user_id},
        )
        user_model_res = _http_json("GET", user_model_url, timeout_seconds=args.timeout)
        tenant_out["public"]["user_model"] = {
            "status": user_model_res.status,
            "error": user_model_res.error,
            "exists": bool((user_model_res.data or {}).get("exists")) if isinstance(user_model_res.data, dict) else None,
            "lastSource": (user_model_res.data or {}).get("lastSource") if isinstance(user_model_res.data, dict) else None,
            "updatedAt": (user_model_res.data or {}).get("updatedAt") if isinstance(user_model_res.data, dict) else None,
            "data": user_model_res.data,
        }

        startbrief_url = _add_query(
            f"{args.base_url.rstrip('/')}/session/startbrief",
            {
                "tenantId": tenant,
                "userId": args.user_id,
                "now": now_iso,
                "timezone": "UTC",
            },
        )
        startbrief_res = _http_json("GET", startbrief_url, timeout_seconds=args.timeout)
        tenant_out["public"]["session_startbrief"] = {
            "status": startbrief_res.status,
            "error": startbrief_res.error,
            "data": startbrief_res.data,
        }

        brief_url = _add_query(
            f"{args.base_url.rstrip('/')}/session/brief",
            {"tenantId": tenant, "userId": args.user_id, "now": now_iso},
        )
        brief_res = _http_json("GET", brief_url, timeout_seconds=args.timeout)
        tenant_out["public"]["session_brief"] = {
            "status": brief_res.status,
            "error": brief_res.error,
            "data": brief_res.data,
        }

        memory_query_results: List[Dict[str, Any]] = []
        for q in queries:
            body = {
                "tenantId": tenant,
                "userId": args.user_id,
                "query": q,
                "limit": 12,
                "referenceTime": now_iso,
                "includeContext": True,
            }
            memory_res = _http_json(
                "POST",
                f"{args.base_url.rstrip('/')}/memory/query",
                body=body,
                timeout_seconds=args.timeout,
            )
            memory_query_results.append(
                {
                    "query": q,
                    "status": memory_res.status,
                    "error": memory_res.error,
                    "facts": (memory_res.data or {}).get("facts") if isinstance(memory_res.data, dict) else None,
                    "entities": (memory_res.data or {}).get("entities") if isinstance(memory_res.data, dict) else None,
                    "data": memory_res.data,
                }
            )
        tenant_out["public"]["memory_query"] = memory_query_results

        loops_url = _add_query(
            f"{args.base_url.rstrip('/')}/memory/loops",
            {"tenantId": tenant, "userId": args.user_id, "limit": 10},
        )
        loops_res = _http_json("GET", loops_url, timeout_seconds=args.timeout)
        tenant_out["public"]["memory_loops"] = {
            "status": loops_res.status,
            "error": loops_res.error,
            "count": len((loops_res.data or {}).get("items") or []) if isinstance(loops_res.data, dict) else None,
            "data": loops_res.data,
        }

        if args.internal_token:
            internal_query_rows: List[Dict[str, Any]] = []
            for q in queries:
                debug_res = _http_json(
                    "POST",
                    f"{args.base_url.rstrip('/')}/internal/debug/graphiti/query",
                    headers=internal_headers,
                    body={
                        "tenantId": tenant,
                        "userId": args.user_id,
                        "query": q,
                        "limit": 20,
                        "referenceTime": now_iso,
                    },
                    timeout_seconds=args.timeout,
                )
                internal_query_rows.append(
                    {
                        "query": q,
                        "status": debug_res.status,
                        "error": debug_res.error,
                        "data": debug_res.data,
                    }
                )
            tenant_out["internal"]["graphiti_query"] = internal_query_rows

            episodes_url = _add_query(
                f"{args.base_url.rstrip('/')}/internal/debug/graphiti/episodes",
                {"tenantId": tenant, "userId": args.user_id, "limit": 20},
            )
            episodes_res = _http_json("GET", episodes_url, headers=internal_headers, timeout_seconds=args.timeout)
            tenant_out["internal"]["graphiti_episodes"] = {
                "status": episodes_res.status,
                "error": episodes_res.error,
                "count": len((episodes_res.data or {}).get("episodes") or []) if isinstance(episodes_res.data, dict) else None,
                "data": episodes_res.data,
            }

            summaries_url = _add_query(
                f"{args.base_url.rstrip('/')}/internal/debug/graphiti/session_summaries_view",
                {"tenantId": tenant, "userId": args.user_id, "limit": 20},
            )
            summaries_res = _http_json("GET", summaries_url, headers=internal_headers, timeout_seconds=args.timeout)
            tenant_out["internal"]["session_summaries_view"] = {
                "status": summaries_res.status,
                "error": summaries_res.error,
                "count": len((summaries_res.data or {}).get("summaries") or []) if isinstance(summaries_res.data, dict) else None,
                "data": summaries_res.data,
            }

            outbox_url = _add_query(
                f"{args.base_url.rstrip('/')}/internal/debug/outbox",
                {"tenantId": tenant, "limit": 200},
            )
            outbox_res = _http_json("GET", outbox_url, headers=internal_headers, timeout_seconds=args.timeout)
            filtered_rows: List[Dict[str, Any]] = []
            if isinstance(outbox_res.data, dict):
                for row in (outbox_res.data.get("rows") or []):
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("user_id") or "") != args.user_id:
                        continue
                    filtered_rows.append(row)
            tenant_out["internal"]["outbox_for_user"] = {
                "status": outbox_res.status,
                "error": outbox_res.error,
                "count": len(filtered_rows),
                "rows": filtered_rows[:50],
            }

        if needles:
            needle_hits: Dict[str, Any] = {}
            for needle in needles:
                surface_hits: Dict[str, Any] = {}
                for surface_name, surface_payload in tenant_out.get("public", {}).items():
                    hits = _find_matches(surface_payload, needle)
                    surface_hits[f"public.{surface_name}"] = hits
                for surface_name, surface_payload in tenant_out.get("internal", {}).items():
                    hits = _find_matches(surface_payload, needle)
                    surface_hits[f"internal.{surface_name}"] = hits
                needle_hits[needle] = {
                    "found_on_surfaces": [k for k, v in surface_hits.items() if v],
                    "snippets": {k: v for k, v in surface_hits.items() if v},
                }
            tenant_out["needle_hits"] = needle_hits

        out["tenants"][tenant] = tenant_out

    if args.database_url:
        try:
            out["database_snapshot"] = asyncio.run(_fetch_db_snapshot(args.database_url, args.user_id))
        except Exception as exc:
            out["database_snapshot_error"] = str(exc)

    json.dump(out, sys.stdout, indent=2, ensure_ascii=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
