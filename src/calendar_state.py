from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import HTTPException

from .db import Database

CALENDAR_ITEM_STATUSES = {"confirmed", "cancelled", "archived"}
CALENDAR_SOURCE_KINDS = {
    "manual",
    "chat",
    "email",
    "whatsapp",
    "instagram",
    "google_calendar",
    "system",
    "candidate_promotion",
}
CALENDAR_SYNC_STATUSES = {
    "not_synced",
    "synced",
    "pending_create",
    "pending_update",
    "pending_delete",
    "conflict",
    "imported",
}
CALENDAR_RSVP_STATUSES = {"unknown", "needs_action", "accepted", "declined", "tentative"}
CALENDAR_ACTORS = {"user", "sophie", "synapse", "system", "external_provider"}

CALENDAR_ALLOWED_TRANSITIONS = {
    "confirmed": {"cancelled", "archived"},
    "cancelled": {"archived"},
    "archived": set(),
}


class CalendarStateError(HTTPException):
    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _json_safe(value: Optional[Any]) -> Optional[Any]:
    if value is None:
        return None
    return json.loads(json.dumps(value, default=_json_default))


def _ensure_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, (float, int, Decimal)):
        return float(value)
    return None


def _row_to_calendar_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row.get("id")),
        "tenantId": row.get("tenant_id"),
        "userId": row.get("user_id"),
        "title": row.get("title"),
        "description": row.get("description"),
        "notes": row.get("notes"),
        "startsAt": row.get("starts_at").isoformat() if row.get("starts_at") else None,
        "endsAt": row.get("ends_at").isoformat() if row.get("ends_at") else None,
        "timezone": row.get("timezone"),
        "allDay": bool(row.get("all_day")),
        "location": row.get("location"),
        "participants": row.get("participants") or [],
        "organizer": row.get("organizer"),
        "rsvpStatus": row.get("rsvp_status"),
        "status": row.get("status"),
        "sourceKind": row.get("source_kind"),
        "sourceRef": row.get("source_ref"),
        "evidenceRefs": row.get("evidence_refs") or [],
        "provenanceSummary": row.get("provenance_summary"),
        "confidence": _as_float(row.get("confidence")),
        "externalProvider": row.get("external_provider"),
        "externalId": row.get("external_id"),
        "externalCalendarId": row.get("external_calendar_id"),
        "externalEtag": row.get("external_etag"),
        "externalUpdatedAt": row.get("external_updated_at").isoformat() if row.get("external_updated_at") else None,
        "syncStatus": row.get("sync_status"),
        "recurrenceRule": row.get("recurrence_rule"),
        "recurrenceParentId": str(row.get("recurrence_parent_id")) if row.get("recurrence_parent_id") else None,
        "dedupeKey": row.get("dedupe_key"),
        "metadata": row.get("metadata") or {},
        "createdAt": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updatedAt": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        "cancelledAt": row.get("cancelled_at").isoformat() if row.get("cancelled_at") else None,
        "archivedAt": row.get("archived_at").isoformat() if row.get("archived_at") else None,
    }


def _validate_common(input_data: Dict[str, Any], *, partial: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    if not partial or "title" in input_data:
        title = (input_data.get("title") or "").strip()
        if not title:
            raise CalendarStateError(400, "title is required")
        out["title"] = title

    if not partial or "startsAt" in input_data:
        starts_at = _ensure_tz(input_data.get("startsAt"))
        if starts_at is None:
            raise CalendarStateError(400, "startsAt is required")
        out["starts_at"] = starts_at

    if "endsAt" in input_data:
        out["ends_at"] = _ensure_tz(input_data.get("endsAt"))

    starts_at_value = out.get("starts_at", input_data.get("_current_starts_at"))
    ends_at_value = out.get("ends_at", input_data.get("_current_ends_at"))
    if starts_at_value and ends_at_value and ends_at_value <= starts_at_value:
        raise CalendarStateError(400, "endsAt must be after startsAt")

    if "status" in input_data and input_data.get("status") is not None:
        status = str(input_data.get("status")).strip().lower()
        if status not in CALENDAR_ITEM_STATUSES:
            raise CalendarStateError(400, f"Invalid calendar item status: {status}")
        out["status"] = status

    if "sourceKind" in input_data and input_data.get("sourceKind") is not None:
        source_kind = str(input_data.get("sourceKind")).strip().lower()
        if source_kind not in CALENDAR_SOURCE_KINDS:
            raise CalendarStateError(400, f"Invalid sourceKind: {source_kind}")
        out["source_kind"] = source_kind

    if "syncStatus" in input_data and input_data.get("syncStatus") is not None:
        sync_status = str(input_data.get("syncStatus")).strip().lower()
        if sync_status not in CALENDAR_SYNC_STATUSES:
            raise CalendarStateError(400, f"Invalid syncStatus: {sync_status}")
        out["sync_status"] = sync_status

    if "rsvpStatus" in input_data and input_data.get("rsvpStatus") is not None:
        rsvp_status = str(input_data.get("rsvpStatus")).strip().lower()
        if rsvp_status not in CALENDAR_RSVP_STATUSES:
            raise CalendarStateError(400, f"Invalid rsvpStatus: {rsvp_status}")
        out["rsvp_status"] = rsvp_status

    return out


async def _write_audit(
    conn,
    *,
    tenant_id: str,
    user_id: str,
    object_id: str,
    action: str,
    old_value: Optional[Dict[str, Any]],
    new_value: Optional[Dict[str, Any]],
    actor: str,
    reason: Optional[str] = None,
    provenance_summary: Optional[str] = None,
) -> None:
    if actor not in CALENDAR_ACTORS:
        raise CalendarStateError(400, f"Invalid actor: {actor}")
    await conn.execute(
        """
        INSERT INTO calendar_audit_log (
          tenant_id, user_id, object_type, object_id, action, old_value, new_value, actor, reason, provenance_summary
        ) VALUES ($1, $2, 'calendar_item', $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)
        """,
        tenant_id,
        user_id,
        object_id,
        action,
        _json_safe(old_value),
        _json_safe(new_value),
        actor,
        reason,
        provenance_summary,
    )


async def createCalendarItem(db: Database, input_data: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = input_data.get("tenantId") or "default"
    user_id = input_data.get("userId")
    if not user_id:
        raise CalendarStateError(400, "userId is required")

    validated = _validate_common(input_data)
    status = validated.get("status") or "confirmed"
    source_kind = validated.get("source_kind") or "manual"
    sync_status = validated.get("sync_status") or "not_synced"
    rsvp_status = validated.get("rsvp_status") or "unknown"
    timezone_name = (input_data.get("timezone") or "UTC").strip() or "UTC"

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO calendar_items (
                  tenant_id, user_id, title, description, notes, starts_at, ends_at, timezone,
                  all_day, location, participants, organizer, rsvp_status, status, source_kind,
                  source_ref, evidence_refs, provenance_summary, confidence, external_provider,
                  external_id, external_calendar_id, external_etag, external_updated_at, sync_status,
                  recurrence_rule, recurrence_parent_id, dedupe_key, metadata
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13,$14,$15,
                  $16::jsonb,$17::jsonb,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27::uuid,$28,$29::jsonb
                )
                RETURNING *
                """,
                tenant_id,
                user_id,
                validated["title"],
                input_data.get("description"),
                input_data.get("notes"),
                validated["starts_at"],
                validated.get("ends_at"),
                timezone_name,
                bool(input_data.get("allDay")),
                input_data.get("location"),
                input_data.get("participants") or [],
                input_data.get("organizer"),
                rsvp_status,
                status,
                source_kind,
                input_data.get("sourceRef"),
                input_data.get("evidenceRefs") or [],
                input_data.get("provenanceSummary"),
                input_data.get("confidence"),
                input_data.get("externalProvider"),
                input_data.get("externalId"),
                input_data.get("externalCalendarId"),
                input_data.get("externalEtag"),
                _ensure_tz(input_data.get("externalUpdatedAt")),
                sync_status,
                input_data.get("recurrenceRule"),
                input_data.get("recurrenceParentId"),
                input_data.get("dedupeKey"),
                input_data.get("metadata") or {},
            )
            if not row:
                raise CalendarStateError(500, "Failed to create calendar item")
            item = dict(row)
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_id=str(item["id"]),
                action="create",
                old_value=None,
                new_value=item,
                actor="user" if source_kind == "manual" else "sophie",
                provenance_summary=item.get("provenance_summary"),
            )
    return _row_to_calendar_item(item)


async def listCalendarItems(db: Database, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    tenant_id = filters.get("tenantId") or "default"
    user_id = filters.get("userId")
    if not user_id:
        raise CalendarStateError(400, "userId is required")

    where = ["tenant_id=$1", "user_id=$2"]
    params: List[Any] = [tenant_id, user_id]

    status = (filters.get("status") or "").strip().lower() or None
    if status:
        if status not in CALENDAR_ITEM_STATUSES:
            raise CalendarStateError(400, f"Invalid calendar item status: {status}")
        where.append(f"status=${len(params) + 1}")
        params.append(status)
    else:
        if not bool(filters.get("include_cancelled")):
            where.append("status <> 'cancelled'")
        if not bool(filters.get("include_archived")):
            where.append("status <> 'archived'")

    source_kind = (filters.get("source_kind") or "").strip().lower() or None
    if source_kind:
        if source_kind not in CALENDAR_SOURCE_KINDS:
            raise CalendarStateError(400, f"Invalid source_kind: {source_kind}")
        where.append(f"source_kind=${len(params) + 1}")
        params.append(source_kind)

    external_provider = (filters.get("external_provider") or "").strip() or None
    if external_provider:
        where.append(f"external_provider=${len(params) + 1}")
        params.append(external_provider)

    participant = (filters.get("participant") or "").strip() or None
    if participant:
        where.append(f"participants::text ILIKE ${len(params) + 1}")
        params.append(f"%{participant}%")

    date_from = _ensure_tz(filters.get("date_from"))
    date_to = _ensure_tz(filters.get("date_to"))
    if date_from:
        where.append(f"COALESCE(ends_at, starts_at) >= ${len(params) + 1}")
        params.append(date_from)
    if date_to:
        where.append(f"starts_at <= ${len(params) + 1}")
        params.append(date_to)

    limit = max(1, min(int(filters.get("limit") or 100), 500))
    offset = max(0, int(filters.get("offset") or 0))

    rows = await db.fetch(
        f"""
        SELECT *
        FROM calendar_items
        WHERE {' AND '.join(where)}
        ORDER BY starts_at ASC, created_at DESC
        LIMIT ${len(params) + 1}
        OFFSET ${len(params) + 2}
        """,
        *params,
        limit,
        offset,
    )
    return [_row_to_calendar_item(dict(r)) for r in rows]


async def updateCalendarItem(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    calendar_item_id: str,
    patch: Dict[str, Any],
    actor: str = "user",
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    if actor not in CALENDAR_ACTORS:
        raise CalendarStateError(400, f"Invalid actor: {actor}")

    editable_fields = {
        "title",
        "description",
        "notes",
        "startsAt",
        "endsAt",
        "timezone",
        "allDay",
        "location",
        "participants",
        "organizer",
        "rsvpStatus",
        "status",
        "sourceKind",
        "sourceRef",
        "evidenceRefs",
        "provenanceSummary",
        "confidence",
        "externalProvider",
        "externalId",
        "externalCalendarId",
        "externalEtag",
        "externalUpdatedAt",
        "syncStatus",
        "recurrenceRule",
        "recurrenceParentId",
        "dedupeKey",
        "metadata",
    }
    incoming = {k: v for k, v in (patch or {}).items() if k in editable_fields}
    if not incoming:
        raise CalendarStateError(400, "No editable fields provided")

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            current = await conn.fetchrow(
                "SELECT * FROM calendar_items WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid",
                tenant_id,
                user_id,
                calendar_item_id,
            )
            if not current:
                raise CalendarStateError(404, "Calendar item not found")
            current_row = dict(current)
            next_row = dict(current_row)

            validation_input = dict(incoming)
            validation_input["_current_starts_at"] = current_row.get("starts_at")
            validation_input["_current_ends_at"] = current_row.get("ends_at")
            validated = _validate_common(validation_input, partial=True)

            for key, value in validated.items():
                next_row[key] = value
            simple_map = {
                "description": "description",
                "notes": "notes",
                "timezone": "timezone",
                "allDay": "all_day",
                "location": "location",
                "participants": "participants",
                "organizer": "organizer",
                "sourceRef": "source_ref",
                "evidenceRefs": "evidence_refs",
                "provenanceSummary": "provenance_summary",
                "confidence": "confidence",
                "externalProvider": "external_provider",
                "externalId": "external_id",
                "externalCalendarId": "external_calendar_id",
                "externalEtag": "external_etag",
                "externalUpdatedAt": "external_updated_at",
                "recurrenceRule": "recurrence_rule",
                "recurrenceParentId": "recurrence_parent_id",
                "dedupeKey": "dedupe_key",
                "metadata": "metadata",
            }
            for input_key, db_key in simple_map.items():
                if input_key in incoming:
                    next_row[db_key] = _ensure_tz(incoming[input_key]) if input_key == "externalUpdatedAt" else incoming[input_key]

            current_status = str(current_row.get("status") or "").strip().lower()
            next_status = str(next_row.get("status") or current_status).strip().lower()
            if next_status != current_status:
                allowed = CALENDAR_ALLOWED_TRANSITIONS.get(current_status, set())
                if next_status not in allowed:
                    raise CalendarStateError(409, f"Invalid calendar item status transition: {current_status} -> {next_status}")

            cancelled_at = current_row.get("cancelled_at")
            archived_at = current_row.get("archived_at")
            now = datetime.now(timezone.utc)
            if next_status != current_status:
                if next_status == "cancelled":
                    cancelled_at = now
                elif next_status == "archived":
                    archived_at = now

            updated = await conn.fetchrow(
                """
                UPDATE calendar_items
                SET title=$4,
                    description=$5,
                    notes=$6,
                    starts_at=$7,
                    ends_at=$8,
                    timezone=$9,
                    all_day=$10,
                    location=$11,
                    participants=$12::jsonb,
                    organizer=$13::jsonb,
                    rsvp_status=$14,
                    status=$15,
                    source_kind=$16,
                    source_ref=$17::jsonb,
                    evidence_refs=$18::jsonb,
                    provenance_summary=$19,
                    confidence=$20,
                    external_provider=$21,
                    external_id=$22,
                    external_calendar_id=$23,
                    external_etag=$24,
                    external_updated_at=$25,
                    sync_status=$26,
                    recurrence_rule=$27,
                    recurrence_parent_id=$28::uuid,
                    dedupe_key=$29,
                    metadata=$30::jsonb,
                    cancelled_at=$31,
                    archived_at=$32,
                    updated_at=NOW()
                WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid
                RETURNING *
                """,
                tenant_id,
                user_id,
                calendar_item_id,
                next_row.get("title"),
                next_row.get("description"),
                next_row.get("notes"),
                next_row.get("starts_at"),
                next_row.get("ends_at"),
                next_row.get("timezone"),
                bool(next_row.get("all_day")),
                next_row.get("location"),
                next_row.get("participants") or [],
                next_row.get("organizer"),
                next_row.get("rsvp_status"),
                next_status,
                next_row.get("source_kind"),
                next_row.get("source_ref"),
                next_row.get("evidence_refs") or [],
                next_row.get("provenance_summary"),
                next_row.get("confidence"),
                next_row.get("external_provider"),
                next_row.get("external_id"),
                next_row.get("external_calendar_id"),
                next_row.get("external_etag"),
                next_row.get("external_updated_at"),
                next_row.get("sync_status"),
                next_row.get("recurrence_rule"),
                next_row.get("recurrence_parent_id"),
                next_row.get("dedupe_key"),
                next_row.get("metadata") or {},
                cancelled_at,
                archived_at,
            )
            updated_row = dict(updated)
            action_name = "update"
            if next_status != current_status:
                action_name = f"status_change:{current_status}->{next_status}"
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_id=str(calendar_item_id),
                action=action_name,
                old_value=current_row,
                new_value=updated_row,
                actor=actor,
                reason=reason,
                provenance_summary=updated_row.get("provenance_summary"),
            )
    return _row_to_calendar_item(updated_row)


async def updateCalendarItemStatus(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    calendar_item_id: str,
    status: str,
    reason: Optional[str] = None,
    actor: str = "sophie",
) -> Dict[str, Any]:
    return await updateCalendarItem(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        calendar_item_id=calendar_item_id,
        patch={"status": status},
        actor=actor,
        reason=reason,
    )


async def cancelCalendarItem(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    calendar_item_id: str,
    reason: Optional[str] = None,
    actor: str = "sophie",
) -> Dict[str, Any]:
    return await updateCalendarItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        calendar_item_id=calendar_item_id,
        status="cancelled",
        reason=reason,
        actor=actor,
    )


async def archiveCalendarItem(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    calendar_item_id: str,
    reason: Optional[str] = None,
    actor: str = "sophie",
) -> Dict[str, Any]:
    return await updateCalendarItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        calendar_item_id=calendar_item_id,
        status="archived",
        reason=reason,
        actor=actor,
    )
