#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.graphiti_client import GraphitiClient


TENANT_ID = "default"
USER_ID = "cmkqxf72t0000lb04axesvlpx"


async def main() -> None:
    client = GraphitiClient()
    await client.initialize()
    now = datetime.now(timezone.utc)

    merges = [
        {
            "canonical_name": "User",
            "node_type": "person",
            "candidate_names": ["user", "the user", "Mukesh"],
            "summary": "Canonical autobiographical anchor for the user.",
            "attributes": {
                "importance": "high",
                "confidence": 1.0,
                "domains": ["identity"],
            },
        },
        {
            "canonical_name": "Ashley",
            "node_type": "person",
            "candidate_names": ["ashley"],
            "summary": "Ashley is the user's girlfriend and an important relationship in the user's life.",
            "attributes": {
                "importance": "high",
                "confidence": 0.95,
                "domains": ["relationships", "identity"],
                "relationship_role": "girlfriend",
            },
        },
        {
            "canonical_name": "Jasmine",
            "node_type": "person",
            "candidate_names": ["jasmine"],
            "summary": "Jasmine is the user's daughter and an important family relationship.",
            "attributes": {
                "importance": "high",
                "confidence": 0.95,
                "domains": ["relationships", "identity"],
                "relationship_role": "daughter",
            },
        },
        {
            "canonical_name": "Sophie",
            "node_type": "project",
            "candidate_names": ["sophie"],
            "summary": "Sophie is an active assistant product and core project the user is working on.",
            "attributes": {
                "importance": "high",
                "confidence": 0.9,
                "domains": ["work", "identity"],
                "project_role": "active_project",
                "profile_summary": "Sophie is an active assistant product and core project the user is working on.",
            },
        },
        {
            "canonical_name": "Bluum",
            "node_type": "project",
            "candidate_names": ["bluum"],
            "summary": "Bluum is an active product and project the user is working on.",
            "attributes": {
                "importance": "high",
                "confidence": 0.9,
                "domains": ["work"],
                "project_role": "active_project",
                "profile_summary": "Bluum is an active product and project the user is working on.",
            },
        },
        {
            "canonical_name": "Yoshi",
            "node_type": "person",
            "candidate_names": ["yoshi", "ashleys daughter"],
            "summary": "Yoshi is Ashley's daughter and an important person in the user's relationship context.",
            "attributes": {
                "importance": "medium",
                "confidence": 0.85,
                "domains": ["relationships"],
                "aliases": ["Ashley's daughter"],
                "relationship_role": "Ashley's daughter",
                "profile_summary": "Yoshi is Ashley's daughter and an important person in the user's relationship context.",
            },
        },
    ]

    for merge in merges:
        node = await client.merge_canonical_entity_cluster(
            tenant_id=TENANT_ID,
            user_id=USER_ID,
            canonical_name=merge["canonical_name"],
            node_type=merge["node_type"],
            candidate_names=merge["candidate_names"],
            summary=merge["summary"],
            attributes=merge["attributes"],
        )
        print(f"merge {merge['canonical_name']} -> {bool(node)}")

    repairs = [
        {
            "source_name": "User",
            "source_type": "person",
            "source_summary": "Canonical autobiographical anchor for the user.",
            "source_attributes": {
                "importance": "high",
                "confidence": 1.0,
                "domains": ["identity"],
            },
            "target_name": "Ashley",
            "target_type": "person",
            "target_summary": "Ashley is the user's girlfriend and an important relationship in the user's life.",
            "target_attributes": {
                "importance": "high",
                "confidence": 0.95,
                "domains": ["relationships", "identity"],
                "relationship_role": "girlfriend",
            },
            "edge_name": "RELATED_TO_USER_AS",
            "fact": "Ashley is the user's girlfriend.",
            "edge_attributes": {
                "role": "girlfriend",
                "role_display": "girlfriend",
                "importance": "high",
                "confidence": 0.95,
                "provenance": "targeted_graph_repair_2026-04-09",
            },
        },
        {
            "source_name": "User",
            "source_type": "person",
            "source_summary": "Canonical autobiographical anchor for the user.",
            "source_attributes": {
                "importance": "high",
                "confidence": 1.0,
                "domains": ["identity"],
            },
            "target_name": "Jasmine",
            "target_type": "person",
            "target_summary": "Jasmine is the user's daughter and an important family relationship.",
            "target_attributes": {
                "importance": "high",
                "confidence": 0.95,
                "domains": ["relationships", "identity"],
                "relationship_role": "daughter",
            },
            "edge_name": "RELATED_TO_USER_AS",
            "fact": "Jasmine is the user's daughter.",
            "edge_attributes": {
                "role": "daughter",
                "role_display": "daughter",
                "importance": "high",
                "confidence": 0.95,
                "provenance": "targeted_graph_repair_2026-04-09",
            },
        },
        {
            "source_name": "User",
            "source_type": "person",
            "source_summary": "Canonical autobiographical anchor for the user.",
            "source_attributes": {
                "importance": "high",
                "confidence": 1.0,
                "domains": ["identity"],
            },
            "target_name": "Sophie",
            "target_type": "project",
            "target_summary": "Sophie is an active assistant product and core project the user is working on.",
            "target_attributes": {
                "importance": "high",
                "confidence": 0.9,
                "domains": ["work", "identity"],
                "profile_summary": "Sophie is an active assistant product and core project the user is working on.",
            },
            "edge_name": "WORKING_ON",
            "fact": "The user is actively working on Sophie.",
            "edge_attributes": {
                "role": "active_project",
                "role_display": "active_project",
                "importance": "high",
                "confidence": 0.9,
                "provenance": "targeted_graph_repair_2026-04-09",
            },
        },
        {
            "source_name": "User",
            "source_type": "person",
            "source_summary": "Canonical autobiographical anchor for the user.",
            "source_attributes": {
                "importance": "high",
                "confidence": 1.0,
                "domains": ["identity"],
            },
            "target_name": "Bluum",
            "target_type": "project",
            "target_summary": "Bluum is an active product and project the user is working on.",
            "target_attributes": {
                "importance": "high",
                "confidence": 0.9,
                "domains": ["work"],
                "profile_summary": "Bluum is an active product and project the user is working on.",
            },
            "edge_name": "WORKING_ON",
            "fact": "The user is actively working on Bluum.",
            "edge_attributes": {
                "role": "active_project",
                "role_display": "active_project",
                "importance": "high",
                "confidence": 0.9,
                "provenance": "targeted_graph_repair_2026-04-09",
            },
        },
        {
            "source_name": "Ashley",
            "source_type": "person",
            "source_summary": "Ashley is the user's girlfriend and an important relationship in the user's life.",
            "source_attributes": {
                "importance": "high",
                "confidence": 0.95,
                "domains": ["relationships", "identity"],
            },
            "target_name": "Yoshi",
            "target_type": "person",
            "target_summary": "Yoshi is Ashley's daughter and an important person in the user's relationship context.",
            "target_attributes": {
                "importance": "medium",
                "confidence": 0.85,
                "domains": ["relationships"],
                "aliases": ["Ashley's daughter"],
                "profile_summary": "Yoshi is Ashley's daughter and an important person in the user's relationship context.",
            },
            "edge_name": "ABOUT",
            "fact": "Yoshi is Ashley's daughter.",
            "edge_attributes": {
                "role": "Ashley's daughter",
                "role_display": "Ashley's daughter",
                "importance": "medium",
                "confidence": 0.85,
                "provenance": "targeted_graph_repair_2026-04-09",
            },
        },
    ]

    for repair in repairs:
        ok = await client.upsert_canonical_fact_edge(
            tenant_id=TENANT_ID,
            user_id=USER_ID,
            source_name=repair["source_name"],
            source_type=repair["source_type"],
            target_name=repair["target_name"],
            target_type=repair["target_type"],
            edge_name=repair["edge_name"],
            fact=repair["fact"],
            reference_time=now,
            source_summary=repair["source_summary"],
            target_summary=repair["target_summary"],
            source_attributes=repair["source_attributes"],
            target_attributes=repair["target_attributes"],
            edge_attributes=repair["edge_attributes"],
        )
        print(f"{repair['source_name']} -> {repair['target_name']} [{repair['edge_name']}] ok={ok}")


if __name__ == "__main__":
    asyncio.run(main())
