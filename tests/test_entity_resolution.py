from datetime import datetime
import os
from uuid import uuid4

import asyncpg
import pytest

from src.db import Database
from src.entity_resolution import (
    ENTITY_RESOLVER_VERSION,
    EntityResolver,
    RESOLUTION_STATUS_AMBIGUOUS,
    RESOLUTION_STATUS_CREATED,
    RESOLUTION_STATUS_RESOLVED,
)
from src.main import app


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_t6_known_entity_resolves_deterministically():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    now = datetime.utcnow()
    alias_text = "Ashley"
    alias_normalized = "ashley"

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        entity_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, 'person', 'active', '{}'::jsonb, $5, $5)
            RETURNING entity_id
            """,
            tenant,
            user,
            alias_text,
            alias_normalized,
            now,
        )
        await db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id, user_id, entity_id, alias_text, alias_normalized, is_primary, confidence, metadata, first_seen_at, last_seen_at
            )
            VALUES ($1, $2, $3::uuid, $4, $5, true, 1.0, '{}'::jsonb, $6, $6)
            """,
            tenant,
            user,
            str(entity_id),
            alias_text,
            alias_normalized,
            now,
        )

        first = await resolver.resolve_entity_mention(
            tenant_id=tenant,
            user_id=user,
            mention_text="Ashley",
            entity_type="person",
        )
        second = await resolver.resolve_entity_mention(
            tenant_id=tenant,
            user_id=user,
            mention_text="  Ashley  ",
            entity_type="person",
        )
        await db.close()

    assert first.resolution_status == RESOLUTION_STATUS_RESOLVED
    assert second.resolution_status == RESOLUTION_STATUS_RESOLVED
    assert first.resolved_entity_id == str(entity_id)
    assert second.resolved_entity_id == str(entity_id)
    assert first.resolver_version == ENTITY_RESOLVER_VERSION


@pytest.mark.asyncio
async def test_t6_new_entity_created_with_primary_alias():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        result = await resolver.resolve_entity_mention(
            tenant_id=tenant,
            user_id=user,
            mention_text="Project Helios",
            entity_type="project",
        )
        row = await db.fetchone(
            """
            SELECT canonical_name, canonical_name_normalized, entity_type, status
            FROM entities
            WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
            """,
            tenant,
            user,
            result.resolved_entity_id,
        )
        alias_row = await db.fetchone(
            """
            SELECT alias_text, alias_normalized, is_primary
            FROM entity_aliases
            WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
            LIMIT 1
            """,
            tenant,
            user,
            result.resolved_entity_id,
        )
        await db.close()

    assert result.resolution_status == RESOLUTION_STATUS_CREATED
    assert result.resolved_entity_id is not None
    assert row is not None
    assert row["canonical_name"] == "Project Helios"
    assert row["canonical_name_normalized"] == "project helios"
    assert row["entity_type"] == "project"
    assert row["status"] == "active"
    assert alias_row is not None
    assert alias_row["alias_normalized"] == "project helios"
    assert alias_row["is_primary"] is True


@pytest.mark.asyncio
async def test_t6_ambiguous_alias_fails_closed_and_records_mutation():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    alias = "Alex"
    alias_norm = "alex"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        e1 = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, 'Alex Kim', 'alex kim', 'person', 'active', '{}'::jsonb, $3, $3)
            RETURNING entity_id
            """,
            tenant,
            user,
            now,
        )
        e2 = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, 'Alex Chen', 'alex chen', 'person', 'active', '{}'::jsonb, $3, $3)
            RETURNING entity_id
            """,
            tenant,
            user,
            now,
        )
        await db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id, user_id, entity_id, alias_text, alias_normalized, is_primary, confidence, metadata, first_seen_at, last_seen_at
            )
            VALUES ($1, $2, $3::uuid, $4, $5, false, 0.9, '{}'::jsonb, $6, $6)
            """,
            tenant,
            user,
            str(e1),
            alias,
            alias_norm,
            now,
        )
        await db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id, user_id, entity_id, alias_text, alias_normalized, is_primary, confidence, metadata, first_seen_at, last_seen_at
            )
            VALUES ($1, $2, $3::uuid, $4, $5, false, 0.9, '{}'::jsonb, $6, $6)
            """,
            tenant,
            user,
            str(e2),
            alias,
            alias_norm,
            now,
        )

        result = await resolver.resolve_entity_mention(
            tenant_id=tenant,
            user_id=user,
            mention_text=alias,
            entity_type="person",
        )
        mutation = await db.fetchone(
            """
            SELECT mutation_type, payload
            FROM canonical_mutations
            WHERE tenant_id = $1 AND mutation_type = 'entity_resolution_ambiguous'
            ORDER BY mutation_id DESC
            LIMIT 1
            """,
            tenant,
        )
        await db.close()

    assert result.resolution_status == RESOLUTION_STATUS_AMBIGUOUS
    assert result.resolved_entity_id is None
    assert sorted(result.candidate_entity_ids) == sorted([str(e1), str(e2)])
    assert mutation is not None
    payload = mutation["payload"] or {}
    assert payload.get("alias_normalized") == alias_norm


@pytest.mark.asyncio
async def test_t6_merge_lineage_preserved_and_aliases_copied():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        survivor_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, 'Ashley', 'ashley', 'person', 'active', '{}'::jsonb, $3, $3)
            RETURNING entity_id
            """,
            tenant,
            user,
            now,
        )
        source_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, 'Ash', 'ash', 'person', 'active', '{}'::jsonb, $3, $3)
            RETURNING entity_id
            """,
            tenant,
            user,
            now,
        )
        await db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id, user_id, entity_id, alias_text, alias_normalized, is_primary, confidence, metadata, first_seen_at, last_seen_at
            )
            VALUES ($1, $2, $3::uuid, 'Ash', 'ash', true, 1.0, '{}'::jsonb, $4, $4)
            """,
            tenant,
            user,
            str(source_id),
            now,
        )

        merge = await resolver.merge_entity_into_survivor(
            tenant_id=tenant,
            user_id=user,
            source_entity_id=str(source_id),
            survivor_entity_id=str(survivor_id),
            committed_by="tester",
            reason="manual duplicate cleanup",
        )
        source_row = await db.fetchone(
            """
            SELECT status, merged_into_entity_id::text AS merged_into_entity_id
            FROM entities
            WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
            """,
            tenant,
            user,
            str(source_id),
        )
        copied_alias = await db.fetchone(
            """
            SELECT alias_normalized
            FROM entity_aliases
            WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
              AND alias_normalized = 'ash'
            """,
            tenant,
            user,
            str(survivor_id),
        )
        mutation = await db.fetchone(
            """
            SELECT mutation_type, payload
            FROM canonical_mutations
            WHERE tenant_id = $1 AND entity_id = $2::uuid AND mutation_type = 'entity_merge'
            ORDER BY mutation_id DESC
            LIMIT 1
            """,
            tenant,
            str(source_id),
        )
        await db.close()

    assert merge.resolution_status == "merged"
    assert merge.source_entity_id == str(source_id)
    assert merge.survivor_entity_id == str(survivor_id)
    assert source_row is not None
    assert source_row["status"] == "merged"
    assert source_row["merged_into_entity_id"] == str(survivor_id)
    assert copied_alias is not None
    assert mutation is not None
    payload = mutation["payload"] or {}
    assert payload.get("source_entity_id") == str(source_id)
    assert payload.get("survivor_entity_id") == str(survivor_id)


@pytest.mark.asyncio
async def test_t6_resolution_respects_tenant_isolation():
    tenant_a = f"tenant-{uuid4().hex}"
    tenant_b = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        entity_a = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata, created_at, updated_at
            )
            VALUES ($1, $2, 'Bluum', 'bluum', 'project', 'active', '{}'::jsonb, $3, $3)
            RETURNING entity_id
            """,
            tenant_a,
            user,
            now,
        )
        await db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id, user_id, entity_id, alias_text, alias_normalized, is_primary, confidence, metadata, first_seen_at, last_seen_at
            )
            VALUES ($1, $2, $3::uuid, 'Bluum', 'bluum', true, 1.0, '{}'::jsonb, $4, $4)
            """,
            tenant_a,
            user,
            str(entity_a),
            now,
        )

        result_b = await resolver.resolve_entity_mention(
            tenant_id=tenant_b,
            user_id=user,
            mention_text="Bluum",
            entity_type="project",
        )
        await db.close()

    assert result_b.resolution_status == RESOLUTION_STATUS_CREATED
    assert result_b.resolved_entity_id is not None
    assert result_b.resolved_entity_id != str(entity_a)
