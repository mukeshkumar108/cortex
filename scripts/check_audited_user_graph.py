#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from urllib import request as urllib_request

from src.graphiti_client import GraphitiClient


TENANT_ID = "default"
USER_ID = "cmkqxf72t0000lb04axesvlpx"
API_BASE = "http://127.0.0.1:8000"

TARGETS = [
    {"name": "User", "type": "person", "candidate_names": ["user", "the user", "Mukesh"]},
    {"name": "Ashley", "type": "person", "candidate_names": ["ashley"]},
    {"name": "Jasmine", "type": "person", "candidate_names": ["jasmine"]},
    {"name": "Sophie", "type": "project", "candidate_names": ["sophie"]},
    {"name": "Bluum", "type": "project", "candidate_names": ["bluum"]},
    {"name": "Yoshi", "type": "person", "candidate_names": ["yoshi", "ashleys daughter"]},
]


def _post_json(path: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        f"{API_BASE}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib_request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_json(path: str, params: dict) -> dict:
    import urllib.parse

    url = f"{API_BASE}{path}?{urllib.parse.urlencode(params)}"
    with urllib_request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def main() -> None:
    client = GraphitiClient()
    await client.initialize()

    cluster_reports = {}
    role_reports = {}
    for target in TARGETS:
        cluster_reports[target["name"]] = await client.inspect_exact_entity_cluster(
            tenant_id=TENANT_ID,
            user_id=USER_ID,
            canonical_name=target["name"],
            candidate_names=target["candidate_names"],
            allowed_types=[target["type"]],
        )
        role_reports[target["name"]] = await client.inspect_exact_role_edges(
            tenant_id=TENANT_ID,
            user_id=USER_ID,
            entity_name=target["name"],
        )

    now = datetime.now(timezone.utc).isoformat()
    startbrief = _get_json(
        "/session/startbrief",
        {
            "tenantId": TENANT_ID,
            "userId": USER_ID,
            "now": now,
            "timezone": "Europe/Berlin",
        },
    )
    profiles = {
        target["name"]: _post_json(
            "/entities/profile",
            {
                "tenantId": TENANT_ID,
                "userId": USER_ID,
                "name": target["name"],
                "referenceTime": now,
                "includeOpenLoops": True,
                "factsLimit": 6,
                "loopsLimit": 3,
            },
        )
        for target in TARGETS[1:]
    }

    print(
        json.dumps(
            {
                "clusters": cluster_reports,
                "role_edges": role_reports,
                "startbrief": startbrief,
                "profiles": profiles,
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
