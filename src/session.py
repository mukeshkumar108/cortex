"""
Session Manager - Sliding Window Architecture

The session_buffer is a sliding window, not a growing log:
- Always contains: 1 rolling_summary + last 12 messages (6 user+assistant turns)
- When a 13th message arrives, oldest message gets folded into rolling_summary and sent to Graphiti
- Buffer size stays constant

Rolling summary is a compressor, not a memory:
- Exists only to keep context window small
- Is lossy and temporary - that's fine
- Graphiti receives raw turns for full-fidelity long-term storage
"""

from typing import Dict, Any, List, Optional, Set, Callable, Awaitable, Tuple
from datetime import datetime, timedelta, date
import inspect
import json
import logging
import asyncio
import re
from datetime import timezone as dt_timezone
from .db import Database
from .models import Message
from .config import get_settings
from .openrouter_client import get_llm_client
from .episodic_memory import upsert_session_episode_embeddings
from .canonicalization import stable_short_hash
from . import loops

logger = logging.getLogger(__name__)

# Constants
MAX_BUFFER_SIZE = 12
MAX_OUTBOX_ATTEMPTS = 5
OUTBOX_BASE_BACKOFF_SECONDS = 5
OUTBOX_MAX_BACKOFF_SECONDS = 300
OUTBOX_CLAIM_HOLD_SECONDS = 30
DEFAULT_LOOP_PERSONA_ID = "default"
JOB_TYPE_TURN = "turn"
JOB_TYPE_SESSION_RAW_EPISODE = "session_raw_episode"
JOB_TYPE_POST_INGEST_HOOK = "post_ingest_hook"
POST_INGEST_HOOK_SESSION_SUMMARY = "session_summary"
POST_INGEST_HOOK_OPEN_LOOPS = "open_loops"
POST_INGEST_HOOK_EXTRACT_RESULTS = "extract_results"
POST_INGEST_HOOK_USER_MODEL_DELTA = "user_model_delta"
POST_INGEST_HOOK_DAILY_ANALYSIS = "daily_analysis"
POST_INGEST_HOOK_PASS1_TRIAGE = "pass1_triage"
POST_INGEST_HOOK_PASS1_5_ENTITIES = "pass1_5_entities"
POST_INGEST_HOOK_PASS3_THREADS = "pass3_threads"
POST_INGEST_HOOK_PASS4_IDENTITY = "pass4_identity"
POST_INGEST_HOOK_PASS5_LIVING_CONTEXT = "pass5_living_context"
SESSION_INGEST_GRAPHITI_MAX_MESSAGES = 400
SESSION_INGEST_GRAPHITI_MAX_CHARS = 120000
SESSION_INGEST_HOOK_MAX_MESSAGES = 200
SESSION_INGEST_HOOK_MAX_CHARS = 60000


class EvidenceContractError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class SessionManager:
    _SUMMARY_MAX_WORDS = 35
    _TONE_MAX_WORDS = 12
    _MOMENT_MAX_WORDS = 20
    _SUMMARY_MAX_SENTENCES = 2
    _FILLER_VERBS = ("reported", "stated", "mentioned", "noted", "said")
    _INDEX_TEXT_MAX_CHARS = 800

    def __init__(self, db: Database):
        self.db = db
        self.settings = get_settings()
        self.llm_client = get_llm_client()

    @staticmethod
    def _normalize_v2_timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            raw = str(value or "").strip()
            if not raw:
                parsed = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
            else:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt_timezone.utc)
        return parsed.astimezone(dt_timezone.utc)

    @classmethod
    def _validate_turn_contract(
        cls,
        *,
        role: Any,
        text: Any,
        timestamp: Any,
    ) -> Tuple[str, str, datetime]:
        normalized_role = str(role or "").strip().lower()
        if normalized_role not in {"system", "user", "assistant", "tool"}:
            raise EvidenceContractError(
                "EVIDENCE_INVALID_ROLE",
                f"invalid role '{role}'",
            )
        normalized_text = str(text or "")
        if not normalized_text.strip():
            raise EvidenceContractError(
                "EVIDENCE_EMPTY_TEXT",
                "turn text must be non-empty",
            )
        try:
            normalized_ts = cls._normalize_v2_timestamp(timestamp)
        except Exception:
            raise EvidenceContractError(
                "EVIDENCE_INVALID_TIMESTAMP",
                f"invalid timestamp '{timestamp}'",
            )
        return normalized_role, normalized_text, normalized_ts

    @staticmethod
    def _build_turn_idempotency_key(
        *,
        tenant_id: str,
        user_id: str,
        session_id: str,
        role: str,
        text: str,
        timestamp: str,
        source: str,
        source_turn_id: Optional[str] = None,
    ) -> str:
        source_turn_normalized = str(source_turn_id or "").strip()
        if source_turn_normalized:
            return f"{source}:source_turn:{source_turn_normalized}"
        digest = stable_short_hash(
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "session_id": session_id,
                "role": role,
                "text": text,
                "timestamp": timestamp,
                "source": source,
            },
            version="v2-turn-idempotency-v1",
            length=40,
        )
        return f"{source}:payload:{digest}"

    async def _dual_write_turn_v2(
        self,
        *,
        conn: Any,
        tenant_id: str,
        user_id: str,
        session_id: str,
        role: str,
        text: str,
        timestamp: str,
        source: str,
        source_turn_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        started_at: Optional[Any] = None,
        ended_at: Optional[Any] = None,
    ) -> Optional[int]:
        occurred_at = self._normalize_v2_timestamp(timestamp)
        normalized_role, normalized_text, occurred_at = self._validate_turn_contract(
            role=role,
            text=text,
            timestamp=occurred_at,
        )
        started_ts = self._normalize_v2_timestamp(started_at or occurred_at)
        ended_ts = self._normalize_v2_timestamp(ended_at or occurred_at)
        payload = dict(metadata) if isinstance(metadata, dict) else {}
        idempotency_key = self._build_turn_idempotency_key(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            role=normalized_role,
            text=normalized_text,
            timestamp=occurred_at.isoformat(),
            source=source,
            source_turn_id=source_turn_id,
        )

        await conn.execute(
            """
            INSERT INTO sessions_v2 (
                tenant_id, session_id, user_id, started_at, ended_at, status, source, metadata, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, 'open', $6, $7::jsonb, NOW())
            ON CONFLICT (tenant_id, session_id)
            DO UPDATE SET
                started_at = CASE
                    WHEN sessions_v2.started_at IS NULL THEN EXCLUDED.started_at
                    WHEN EXCLUDED.started_at IS NULL THEN sessions_v2.started_at
                    ELSE LEAST(sessions_v2.started_at, EXCLUDED.started_at)
                END,
                ended_at = CASE
                    WHEN sessions_v2.ended_at IS NULL THEN EXCLUDED.ended_at
                    WHEN EXCLUDED.ended_at IS NULL THEN sessions_v2.ended_at
                    ELSE GREATEST(sessions_v2.ended_at, EXCLUDED.ended_at)
                END,
                status = CASE
                    WHEN sessions_v2.status = 'archived' THEN sessions_v2.status
                    ELSE 'open'
                END,
                source = COALESCE(sessions_v2.source, EXCLUDED.source),
                metadata = COALESCE(sessions_v2.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                updated_at = NOW()
            WHERE sessions_v2.user_id = EXCLUDED.user_id
            """,
            tenant_id,
            session_id,
            user_id,
            started_ts,
            ended_ts,
            source,
            payload,
        )

        session_row = await conn.fetchrow(
            """
            SELECT user_id
            FROM sessions_v2
            WHERE tenant_id = $1 AND session_id = $2
            FOR UPDATE
            """,
            tenant_id,
            session_id,
        )
        if not session_row or str(session_row.get("user_id") or "") != user_id:
            raise TypeError(
                f"v2 dual-write session owner mismatch tenant={tenant_id} session={session_id} user={user_id}"
            )

        existing_turn_id = await conn.fetchval(
            """
            SELECT turn_id
            FROM turn_ingest_idempotency
            WHERE tenant_id = $1
              AND user_id = $2
              AND session_id = $3
              AND idempotency_key = $4
            LIMIT 1
            """,
            tenant_id,
            user_id,
            session_id,
            idempotency_key,
        )
        if existing_turn_id is not None:
            return int(existing_turn_id)

        latest_turn = await conn.fetchrow(
            """
            SELECT turn_index, occurred_at
            FROM turns_v2
            WHERE tenant_id = $1
              AND session_id = $2
            ORDER BY turn_index DESC
            LIMIT 1
            """,
            tenant_id,
            session_id,
        )
        last_turn_index = (
            int(latest_turn["turn_index"])
            if latest_turn is not None and latest_turn["turn_index"] is not None
            else -1
        )
        next_turn_index = last_turn_index + 1
        last_occurred_at = latest_turn["occurred_at"] if latest_turn is not None else None
        if isinstance(last_occurred_at, datetime):
            if last_occurred_at.tzinfo is None:
                last_occurred_at = last_occurred_at.replace(tzinfo=dt_timezone.utc)
            if occurred_at <= last_occurred_at:
                occurred_at = last_occurred_at + timedelta(microseconds=1)
                payload["timestamp_normalized_from_out_of_order"] = True

        inserted_turn_id = await conn.fetchval(
            """
            INSERT INTO turns_v2 (
                tenant_id, session_id, user_id, turn_index, role, content, occurred_at, source_turn_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
            RETURNING turn_id
            """,
            tenant_id,
            session_id,
            user_id,
            int(next_turn_index or 0),
            normalized_role,
            normalized_text,
            occurred_at,
            source_turn_id,
            payload,
        )
        await conn.execute(
            """
            INSERT INTO turn_ingest_idempotency (
                tenant_id, user_id, session_id, idempotency_key, turn_id, source, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            tenant_id,
            user_id,
            session_id,
            idempotency_key,
            inserted_turn_id,
            source,
            payload,
        )
        return int(inserted_turn_id)

    async def get_or_create_buffer(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Get existing buffer or create new one"""
        try:
            query = """
                SELECT rolling_summary, messages, created_at, updated_at, closed_at
                FROM session_buffer
                WHERE tenant_id = $1 AND session_id = $2
            """
            result = await self.db.fetchone(query, tenant_id, session_id)

            if result:
                messages = result.get('messages', [])
                if not isinstance(messages, list):
                    messages = []

                return {
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "rolling_summary": result.get('rolling_summary') or '',
                    "messages": messages,
                    "created_at": result.get('created_at'),
                    "updated_at": result.get('updated_at'),
                    "closed_at": result.get('closed_at')
                }
            else:
                # Create new buffer
                query = """
                    INSERT INTO session_buffer (tenant_id, session_id, user_id, rolling_summary, messages)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING created_at, updated_at
                """
                result = await self.db.fetchone(query, tenant_id, session_id, user_id, '', [])

                return {
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "rolling_summary": '',
                    "messages": [],
                    "created_at": result.get('created_at') if result else None,
                    "updated_at": result.get('updated_at') if result else None,
                    "closed_at": None
                }

        except Exception as e:
            logger.error(f"Failed to get or create buffer: {e}")
            raise

    async def add_turn(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        role: str,
        text: str,
        timestamp: str,
        source_turn_id: Optional[str] = None,
        v2_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Add a turn to the buffer. Returns oldest_turn if buffer exceeds MAX_BUFFER_SIZE.

        The caller should trigger the janitor process with the returned oldest_turn.
        """
        try:
            pool = await self.db.get_pool()
            normalized_role, normalized_text, normalized_ts = self._validate_turn_contract(
                role=role,
                text=text,
                timestamp=timestamp,
            )
            new_turn = {
                "role": normalized_role,
                "text": normalized_text,
                "timestamp": normalized_ts.isoformat().replace("+00:00", "Z"),
            }

            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        SELECT tenant_id, session_id, user_id, rolling_summary, messages, session_state
                        FROM session_buffer
                        WHERE tenant_id = $1 AND session_id = $2
                        FOR UPDATE
                        """,
                        tenant_id,
                        session_id
                    )

                    if row is None:
                        await conn.execute(
                            """
                            INSERT INTO session_buffer (
                                tenant_id, session_id, user_id, messages, session_state
                            )
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (tenant_id, session_id) DO NOTHING
                            """,
                            tenant_id,
                            session_id,
                            user_id,
                            [],
                            {}
                        )
                        row = await conn.fetchrow(
                            """
                            SELECT tenant_id, session_id, user_id, rolling_summary, messages, session_state
                            FROM session_buffer
                            WHERE tenant_id = $1 AND session_id = $2
                            FOR UPDATE
                            """,
                            tenant_id,
                            session_id
                        )
                    elif str(row.get("user_id") or "") != user_id:
                        raise EvidenceContractError(
                            "EVIDENCE_SESSION_USER_MISMATCH",
                            f"session user mismatch for session_id={session_id}",
                        )

                    messages = row["messages"] or []
                    if isinstance(messages, str):
                        try:
                            messages = json.loads(messages)
                        except Exception:
                            messages = []
                    if not isinstance(messages, list):
                        messages = []

                    messages.append(new_turn)

                    oldest_turn = None
                    if len(messages) > MAX_BUFFER_SIZE:
                        oldest_turn = messages[0]
                        messages = messages[1:]
                        ts = datetime.fromisoformat(oldest_turn["timestamp"].replace("Z", "+00:00"))
                        await conn.execute(
                            """
                            INSERT INTO graphiti_outbox (
                                tenant_id, user_id, session_id, role, text, ts, status, attempts
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0)
                            """,
                            tenant_id,
                            user_id,
                            session_id,
                            oldest_turn["role"],
                            oldest_turn["text"],
                            ts
                        )
                        logger.info(
                            f"Buffer exceeded {MAX_BUFFER_SIZE} turns, "
                            f"evicted oldest turn to outbox"
                        )

                    last_user_ts = (
                        normalized_ts.isoformat().replace("+00:00", "Z")
                        if normalized_role == "user"
                        else None
                    )
                    await conn.execute(
                        """
                        UPDATE session_buffer
                        SET
                            rolling_summary = $1,
                            messages = $2,
                            session_state = CASE
                                WHEN $5::text IS NULL THEN session_state
                                ELSE jsonb_set(
                                    COALESCE(session_state, '{}'::jsonb),
                                    '{last_user_message_ts}',
                                    to_jsonb($5::text),
                                    true
                                )
                            END,
                            updated_at = NOW()
                        WHERE tenant_id = $3 AND session_id = $4
                        """,
                        row["rolling_summary"] or "",
                        messages,
                        tenant_id,
                        session_id,
                        last_user_ts
                    )

                    if bool(self.settings.v2_dual_write_enabled):
                        try:
                            await self._dual_write_turn_v2(
                                conn=conn,
                                tenant_id=tenant_id,
                                user_id=user_id,
                                session_id=session_id,
                                role=normalized_role,
                                text=normalized_text,
                                timestamp=normalized_ts.isoformat(),
                                source="ingest_v1",
                                source_turn_id=source_turn_id,
                                metadata=v2_metadata,
                            )
                        except Exception as e:
                            if bool(self.settings.v2_dual_write_fail_open):
                                logger.error(
                                    "v2 dual-write failed open during add_turn tenant=%s user=%s session=%s err=%s",
                                    tenant_id,
                                    user_id,
                                    session_id,
                                    e,
                                )
                            else:
                                raise

                    return oldest_turn

        except Exception as e:
            logger.error(f"Failed to add turn: {e}")
            raise

    async def _insert_outbox(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        turn: Dict[str, Any]
    ) -> None:
        """Insert a pending outbox row for Graphiti processing."""
        query = """
            INSERT INTO graphiti_outbox (
                tenant_id, user_id, session_id, role, text, ts, status, attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0)
        """
        ts = datetime.fromisoformat(turn["timestamp"].replace("Z", "+00:00"))
        await self.db.execute(
            query,
            tenant_id,
            user_id,
            session_id,
            turn["role"],
            turn["text"],
            ts
        )

    async def _enqueue_outbox_job(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        *,
        job_type: str,
        payload: Dict[str, Any],
        ts: datetime,
        text: str,
        dedupe_key: Optional[str] = None
    ) -> None:
        await self.db.execute(
            """
            INSERT INTO graphiti_outbox (
                tenant_id, user_id, session_id, role, text, ts, status, attempts,
                job_type, payload, dedupe_key
            )
            VALUES ($1, $2, $3, 'system', $4, $5, 'pending', 0, $6, $7::jsonb, $8)
            ON CONFLICT (dedupe_key)
            DO UPDATE
            SET payload = EXCLUDED.payload,
                status = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.status ELSE 'pending' END,
                attempts = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.attempts ELSE 0 END,
                last_error = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.last_error ELSE NULL END,
                next_attempt_at = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.next_attempt_at ELSE NULL END,
                sent_at = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.sent_at ELSE NULL END
            """,
            tenant_id,
            user_id,
            session_id,
            text[:2000],
            ts,
            job_type,
            payload or {},
            dedupe_key
        )

    async def enqueue_session_ingest(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        messages: List[Dict[str, Any]],
        started_at: Optional[str],
        ended_at: Optional[str],
    ) -> None:
        messages_payload = messages or []
        normalized_messages: List[Dict[str, Any]] = []
        for idx, msg in enumerate(messages_payload):
            if not isinstance(msg, dict):
                raise EvidenceContractError(
                    "EVIDENCE_MALFORMED_TURN",
                    f"message[{idx}] is not an object",
                )
            role, text, ts = self._validate_turn_contract(
                role=msg.get("role"),
                text=msg.get("text"),
                timestamp=msg.get("timestamp"),
            )
            normalized_messages.append(
                {
                    **msg,
                    "role": role,
                    "text": text,
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                }
            )
        messages_payload = normalized_messages
        valid_messages = [
            m for m in messages_payload
            if isinstance(m, dict)
            and str((m.get("text") or "")).strip()
            and str((m.get("role") or "")).strip()
        ]
        reference_time = self._parse_ts(ended_at)
        if reference_time is None:
            ts_values = [self._parse_ts((m or {}).get("timestamp")) for m in messages_payload if isinstance(m, dict)]
            ts_values = [t for t in ts_values if t is not None]
            reference_time = max(ts_values) if ts_values else datetime.utcnow()

        payload = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "started_at": started_at,
            "ended_at": ended_at,
            "reference_time": reference_time.isoformat(),
        }
        dedupe_key = f"session_ingest_raw:{tenant_id}:{user_id}:{session_id}"

        pool = await self.db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                existing_session = await conn.fetchrow(
                    """
                    SELECT user_id
                    FROM session_transcript
                    WHERE tenant_id = $1 AND session_id = $2
                    FOR UPDATE
                    """,
                    tenant_id,
                    session_id,
                )
                if existing_session and str(existing_session.get("user_id") or "") != user_id:
                    raise EvidenceContractError(
                        "EVIDENCE_SESSION_USER_MISMATCH",
                        f"session transcript user mismatch for session_id={session_id}",
                    )
                await conn.execute(
                    """
                    INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, updated_at)
                    VALUES ($1, $2, $3, $4::jsonb, NOW())
                    ON CONFLICT (tenant_id, session_id)
                    DO UPDATE SET messages = $4::jsonb, updated_at = NOW()
                    """,
                    tenant_id,
                    session_id,
                    user_id,
                    messages_payload
                )
                if bool(self.settings.v2_dual_write_enabled):
                    started_ts = self._parse_ts(started_at)
                    ended_ts = self._parse_ts(ended_at)
                    for idx, msg in enumerate(messages_payload):
                        if not isinstance(msg, dict):
                            continue
                        msg_role = str((msg.get("role") or "")).strip()
                        msg_text = str((msg.get("text") or "")).strip()
                        msg_ts_raw = msg.get("timestamp")
                        msg_ts = (
                            msg_ts_raw
                            if str(msg_ts_raw or "").strip()
                            else ((started_ts or ended_ts or reference_time).isoformat())
                        )
                        if not msg_role or not msg_text:
                            continue
                        msg_source_turn_id = str((msg.get("id") or msg.get("sourceTurnId") or "")).strip() or None
                        dual_metadata = {"path": "session_ingest", "message_index": idx}
                        try:
                            await self._dual_write_turn_v2(
                                conn=conn,
                                tenant_id=tenant_id,
                                user_id=user_id,
                                session_id=session_id,
                                role=msg_role,
                                text=msg_text,
                                timestamp=msg_ts,
                                source="session_ingest_v1",
                                source_turn_id=msg_source_turn_id,
                                metadata=dual_metadata,
                                started_at=started_ts,
                                ended_at=ended_ts or reference_time,
                            )
                        except Exception as e:
                            if bool(self.settings.v2_dual_write_fail_open):
                                logger.error(
                                    "v2 dual-write failed open during session_ingest tenant=%s user=%s session=%s idx=%s err=%s",
                                    tenant_id,
                                    user_id,
                                    session_id,
                                    idx,
                                    e,
                                )
                            else:
                                raise
                if not valid_messages:
                    logger.warning(
                        "Skipping session_raw_episode enqueue for empty transcript tenant=%s user=%s session=%s",
                        tenant_id,
                        user_id,
                        session_id,
                    )
                    return
                await conn.execute(
                    """
                    INSERT INTO graphiti_outbox (
                        tenant_id, user_id, session_id, role, text, ts, status, attempts,
                        job_type, payload, dedupe_key
                    )
                    VALUES ($1, $2, $3, 'system', $4, $5, 'pending', 0, $6, $7::jsonb, $8)
                    ON CONFLICT (dedupe_key)
                    DO UPDATE
                    SET payload = EXCLUDED.payload,
                        status = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.status ELSE 'pending' END,
                        attempts = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.attempts ELSE 0 END,
                        last_error = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.last_error ELSE NULL END,
                        next_attempt_at = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.next_attempt_at ELSE NULL END,
                        sent_at = CASE WHEN graphiti_outbox.status = 'sent' THEN graphiti_outbox.sent_at ELSE NULL END
                    """,
                    tenant_id,
                    user_id,
                    session_id,
                    f"session_raw_episode:{session_id}",
                    reference_time,
                    JOB_TYPE_SESSION_RAW_EPISODE,
                    payload,
                    dedupe_key
                )

    async def _has_session_ingest_job(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str
    ) -> bool:
        dedupe_key = f"session_ingest_raw:{tenant_id}:{user_id}:{session_id}"
        row = await self.db.fetchone(
            """
            SELECT 1
            FROM graphiti_outbox
            WHERE tenant_id = $1
              AND user_id = $2
              AND session_id = $3
              AND (
                dedupe_key = $4
                OR (
                  job_type = $5
                  AND status IN ('pending', 'sent')
                )
              )
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant_id,
            user_id,
            session_id,
            dedupe_key,
            JOB_TYPE_SESSION_RAW_EPISODE
        )
        return bool(row)

    @staticmethod
    def _error_status_code(value: Any) -> Optional[int]:
        raw = None
        if isinstance(value, dict):
            raw = value.get("status_code") if value.get("status_code") is not None else value.get("status")
        else:
            raw = getattr(value, "status_code", None)
            if raw is None:
                raw = getattr(value, "status", None)
        try:
            if raw is None:
                return None
            return int(raw)
        except Exception:
            return None

    @staticmethod
    def _looks_permanent_error_text(error_text: str) -> bool:
        lower = (error_text or "").lower()
        if not lower:
            return False
        permanent_markers = (
            "validation",
            "schema",
            "invalid",
            "malformed",
            "bad request",
            "missing required",
            "unsupported",
        )
        return any(marker in lower for marker in permanent_markers)

    @classmethod
    def _raise_for_graphiti_response_failure(
        cls,
        response: Any,
        context: str
    ) -> None:
        status = cls._error_status_code(response)
        error_text = ""
        if isinstance(response, dict):
            error_text = str(response.get("error") or response.get("detail") or "")
        if status is not None and 400 <= status < 500:
            raise TypeError(f"{context} failed permanently status={status} error={error_text}")
        if cls._looks_permanent_error_text(error_text):
            raise TypeError(f"{context} failed permanently error={error_text}")
        raise RuntimeError(f"{context} failed transiently status={status} error={error_text}")

    @staticmethod
    def _cap_messages_for_processing(
        messages: List[Dict[str, Any]],
        max_messages: int,
        max_chars: int
    ) -> List[Dict[str, Any]]:
        if not isinstance(messages, list):
            return []
        capped_by_count = messages[-max_messages:] if max_messages > 0 else messages[:]
        out: List[Dict[str, Any]] = []
        chars = 0
        for msg in capped_by_count:
            if not isinstance(msg, dict):
                continue
            text = str(msg.get("text") or "")
            if not text:
                continue
            projected = chars + len(text)
            if projected > max_chars:
                break
            out.append(msg)
            chars = projected
        return out

    async def _load_session_transcript_messages(
        self,
        tenant_id: str,
        session_id: str
    ) -> List[Dict[str, Any]]:
        row = await self.db.fetchone(
            """
            SELECT messages
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant_id,
            session_id
        )
        messages = row.get("messages") if row else []
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            return []
        return [m for m in messages if isinstance(m, dict)]

    @staticmethod
    def _parse_ts(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
        else:
            return None
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    async def _get_full_transcript_messages(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        buffer: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        transcript = await self.db.fetchone(
            """
            SELECT messages
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant_id,
            session_id
        )
        transcript_messages = transcript.get("messages") if transcript else []
        if isinstance(transcript_messages, str):
            try:
                transcript_messages = json.loads(transcript_messages)
            except Exception:
                transcript_messages = []

        if buffer is None:
            buffer = await self.get_or_create_buffer(tenant_id, session_id, user_id)

        full_messages: List[Dict[str, Any]] = []
        if isinstance(transcript_messages, list):
            full_messages.extend(transcript_messages)
        if isinstance(buffer.get("messages"), list):
            full_messages.extend(buffer["messages"])
        return full_messages

    async def get_outbox_count(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str
    ) -> int:
        query = """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
              AND status = 'pending'
              AND job_type = $4
        """
        return int(await self.db.fetchval(query, tenant_id, session_id, user_id, JOB_TYPE_TURN) or 0)

    async def get_last_janitor_run_at(
        self,
        tenant_id: str,
        session_id: str
    ) -> Optional[datetime]:
        query = """
            SELECT session_state->>'last_janitor_run_at' AS last_run
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
        """
        row = await self.db.fetchone(query, tenant_id, session_id)
        if not row or not row.get("last_run"):
            return None
        try:
            return datetime.fromisoformat(row["last_run"].replace("Z", "+00:00"))
        except Exception:
            return None

    async def set_last_janitor_run_at(
        self,
        tenant_id: str,
        session_id: str,
        ts: datetime
    ) -> None:
        query = """
            UPDATE session_buffer
            SET session_state = jsonb_set(
                COALESCE(session_state, '{}'::jsonb),
                '{last_janitor_run_at}',
                to_jsonb($3::text),
                true
            ),
                updated_at = NOW()
            WHERE tenant_id = $1 AND session_id = $2
        """
        await self.db.execute(query, tenant_id, session_id, ts.isoformat())

    async def _save_buffer(self, buffer: Dict[str, Any]) -> None:
        """Save buffer to database"""
        try:
            query = """
                UPDATE session_buffer
                SET
                    rolling_summary = $1,
                    messages = $2,
                    updated_at = NOW()
                WHERE tenant_id = $3 AND session_id = $4
            """
            await self.db.execute(
                query,
                buffer["rolling_summary"],
                buffer["messages"],
                buffer["tenant_id"],
                buffer["session_id"]
            )
        except Exception as e:
            logger.error(f"Failed to save buffer: {e}")
            raise

    @staticmethod
    def _is_permanent_outbox_error(error: Exception) -> bool:
        msg = str(error).lower()
        if isinstance(error, TypeError) and "unexpected keyword argument" in msg:
            return True
        if "validation" in msg or "schema" in msg:
            return True
        status = getattr(error, "status_code", None)
        if status is None:
            status = getattr(error, "status", None)
        if isinstance(status, int):
            if 400 <= status < 500:
                return True
            if status >= 500:
                return False
        return False

    @staticmethod
    def _filter_kwargs(func, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        try:
            params = inspect.signature(func).parameters
        except (TypeError, ValueError):
            return kwargs
        for param in params.values():
            if param.kind == param.VAR_KEYWORD:
                return kwargs
        return {key: value for key, value in kwargs.items() if key in params}

    async def _call_add_episode(self, graphiti_client, **kwargs) -> Any:
        fn = graphiti_client.add_episode
        filtered = self._filter_kwargs(fn, kwargs)
        result = fn(**filtered)
        if inspect.isawaitable(result):
            return await result
        return result

    async def janitor_process(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        graphiti_client
    ) -> None:
        """
        Janitor process: fold pending outbox turns into rolling summary and send to Graphiti.

        This runs in the background and doesn't block the user response.
        """
        try:
            pending = await self._fetch_pending_outbox(tenant_id, session_id, user_id, limit=20)
            if not pending:
                return

            buffer = await self.get_or_create_buffer(tenant_id, session_id, user_id)
            current_summary = buffer["rolling_summary"]

            for row in pending:
                turn = {
                    "role": row["role"],
                    "text": row["text"],
                    "timestamp": row["ts"].isoformat()
                }

                if row.get("folded_at") is None:
                    new_summary = await self._fold_into_summary(current_summary, turn)
                    await self._update_rolling_summary(tenant_id, session_id, new_summary)
                    await self._append_transcript_turn(tenant_id, session_id, user_id, turn)
                    await self._mark_outbox_folded(row["id"])
                    current_summary = new_summary

                if self.settings.graphiti_per_turn:
                    try:
                        episode_name = self._build_episode_name(
                            tenant_id=tenant_id,
                            user_id=user_id,
                            session_id=session_id,
                            role=row["role"],
                            ts=row["ts"],
                            text=row["text"]
                        )
                        await self._call_add_episode(
                            graphiti_client,
                            tenant_id=tenant_id,
                            user_id=user_id,
                            text=row["text"],
                            timestamp=row["ts"],
                            role=row["role"],
                            metadata={
                                "session_id": session_id,
                                "evicted_from_buffer": True
                            },
                            episode_name=episode_name
                        )
                        await self._mark_outbox_sent(row["id"])
                        logger.info(f"Janitor sent outbox {row['id']} to Graphiti")
                    except Exception as e:
                        if self._is_permanent_outbox_error(e):
                            await self._mark_outbox_dead_letter(row["id"], str(e))
                            logger.error(
                                f"Janitor permanently failed outbox {row['id']}: {e}"
                            )
                        else:
                            await self._mark_outbox_failed(row["id"], str(e))
                            logger.warning(
                                f"Janitor transient failure outbox {row['id']}: {e}"
                            )
                else:
                    await self._mark_outbox_sent(row["id"])
                    logger.info(
                        f"Janitor marked outbox {row['id']} sent (per-turn disabled)"
                    )

        except Exception as e:
            logger.error(f"Janitor process failed: {e}")

    async def _fetch_pending_outbox(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT id, role, text, ts, attempts, folded_at, next_attempt_at
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
              AND status = 'pending'
              AND job_type = $5
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ORDER BY id ASC
            LIMIT $4
        """
        return await self.db.fetch(query, tenant_id, session_id, user_id, limit, JOB_TYPE_TURN)

    async def _claim_pending_outbox(
        self,
        limit: int,
        tenant_id: Optional[str] = None,
        job_types: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        normalized_job_types = [
            str(value).strip().lower()
            for value in (job_types or [])
            if str(value).strip()
        ]
        pool = await self.db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                if tenant_id:
                    if normalized_job_types:
                        rows = await conn.fetch(
                            """
                            SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at, job_type, payload, dedupe_key
                            FROM graphiti_outbox
                            WHERE status = 'pending'
                              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                              AND tenant_id = $1
                              AND LOWER(COALESCE(job_type, '')) = ANY($2::text[])
                            ORDER BY id ASC
                            LIMIT $3
                            FOR UPDATE SKIP LOCKED
                            """,
                            tenant_id,
                            normalized_job_types,
                            limit
                        )
                    else:
                        rows = await conn.fetch(
                            """
                            SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at, job_type, payload, dedupe_key
                            FROM graphiti_outbox
                            WHERE status = 'pending'
                              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                              AND tenant_id = $1
                            ORDER BY id ASC
                            LIMIT $2
                            FOR UPDATE SKIP LOCKED
                            """,
                            tenant_id,
                            limit
                        )
                else:
                    if normalized_job_types:
                        rows = await conn.fetch(
                            """
                            SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at, job_type, payload, dedupe_key
                            FROM graphiti_outbox
                            WHERE status = 'pending'
                              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                              AND LOWER(COALESCE(job_type, '')) = ANY($1::text[])
                            ORDER BY id ASC
                            LIMIT $2
                            FOR UPDATE SKIP LOCKED
                            """,
                            normalized_job_types,
                            limit
                        )
                    else:
                        rows = await conn.fetch(
                            """
                            SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at, job_type, payload, dedupe_key
                            FROM graphiti_outbox
                            WHERE status = 'pending'
                              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                            ORDER BY id ASC
                            LIMIT $1
                            FOR UPDATE SKIP LOCKED
                            """,
                            limit
                        )

                if not rows:
                    return []

                ids = [row["id"] for row in rows]
                await conn.execute(
                    """
                    UPDATE graphiti_outbox
                    SET next_attempt_at = NOW() + ($2 * INTERVAL '1 second')
                    WHERE id = ANY($1::bigint[])
                    """,
                    ids,
                    OUTBOX_CLAIM_HOLD_SECONDS
                )

                return [dict(row) for row in rows]

    async def _update_rolling_summary(
        self,
        tenant_id: str,
        session_id: str,
        rolling_summary: str
    ) -> None:
        query = """
            UPDATE session_buffer
            SET rolling_summary = $1, updated_at = NOW()
            WHERE tenant_id = $2 AND session_id = $3
        """
        await self.db.execute(query, rolling_summary, tenant_id, session_id)

    async def _append_transcript_turn(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        turn: Dict[str, Any]
    ) -> None:
        query = """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (tenant_id, session_id)
            DO UPDATE SET
                messages = session_transcript.messages || EXCLUDED.messages,
                updated_at = NOW()
        """
        await self.db.execute(query, tenant_id, session_id, user_id, [turn])

    async def _mark_outbox_sent(self, outbox_id: int) -> None:
        query = """
            UPDATE graphiti_outbox
            SET status = 'sent',
                attempts = attempts + 1,
                sent_at = NOW(),
                last_error = NULL
            WHERE id = $1
        """
        await self.db.execute(query, outbox_id)

    async def _mark_outbox_failed(self, outbox_id: int, error: str) -> None:
        delay = OUTBOX_BASE_BACKOFF_SECONDS
        try:
            current_attempts = await self.db.fetchval(
                "SELECT attempts FROM graphiti_outbox WHERE id = $1",
                outbox_id
            )
            attempt_num = int(current_attempts or 0) + 1
            delay = min(
                OUTBOX_BASE_BACKOFF_SECONDS * (2 ** (attempt_num - 1)),
                OUTBOX_MAX_BACKOFF_SECONDS
            )
        except Exception:
            pass
        query = """
            UPDATE graphiti_outbox
            SET attempts = attempts + 1,
                last_error = $2,
                next_attempt_at = NOW() + ($3 * INTERVAL '1 second'),
                status = CASE
                    WHEN attempts + 1 >= $4 THEN 'failed'
                    ELSE 'pending'
                END
            WHERE id = $1
        """
        await self.db.execute(query, outbox_id, error[:500], delay, MAX_OUTBOX_ATTEMPTS)

    async def _mark_outbox_dead_letter(self, outbox_id: int, error: str) -> None:
        query = """
            UPDATE graphiti_outbox
            SET status = 'failed',
                attempts = attempts + 1,
                last_error = $2,
                next_attempt_at = NULL,
                sent_at = NULL
            WHERE id = $1
        """
        await self.db.execute(query, outbox_id, error[:500])

    @staticmethod
    def _parse_payload_reference_time(payload: Dict[str, Any]) -> datetime:
        raw = payload.get("reference_time") if isinstance(payload, dict) else None
        parsed = SessionManager._parse_ts(raw)
        return parsed or datetime.utcnow()

    @staticmethod
    def _is_success_response(response: Any) -> bool:
        if isinstance(response, dict):
            if response.get("success") is False:
                return False
            status = response.get("status_code")
            try:
                if status is not None and int(status) >= 400:
                    return False
            except Exception:
                pass
        return True

    async def _enqueue_post_ingest_hook_jobs(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        reference_time: datetime,
        episode_uuid: Optional[str]
    ) -> None:
        base_payload = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "reference_time": reference_time.isoformat(),
            "episode_uuid": episode_uuid,
        }

        await self._enqueue_outbox_job(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            job_type=JOB_TYPE_POST_INGEST_HOOK,
            payload={**base_payload, "hook": POST_INGEST_HOOK_SESSION_SUMMARY},
            ts=reference_time,
            text=f"{JOB_TYPE_POST_INGEST_HOOK}:{POST_INGEST_HOOK_SESSION_SUMMARY}",
            dedupe_key=f"session_hook_summary:{tenant_id}:{user_id}:{session_id}"
        )
        await self._enqueue_outbox_job(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            job_type=JOB_TYPE_POST_INGEST_HOOK,
            payload={**base_payload, "hook": POST_INGEST_HOOK_OPEN_LOOPS},
            ts=reference_time,
            text=f"{JOB_TYPE_POST_INGEST_HOOK}:{POST_INGEST_HOOK_OPEN_LOOPS}",
            dedupe_key=f"session_hook_loops:{tenant_id}:{user_id}:{session_id}"
        )
        await self._enqueue_outbox_job(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            job_type=JOB_TYPE_POST_INGEST_HOOK,
            payload={**base_payload, "hook": POST_INGEST_HOOK_EXTRACT_RESULTS},
            ts=reference_time,
            text=f"{JOB_TYPE_POST_INGEST_HOOK}:{POST_INGEST_HOOK_EXTRACT_RESULTS}",
            dedupe_key=f"session_hook_extract_results:{tenant_id}:{user_id}:{session_id}"
        )
        if bool(getattr(self.settings, "derived_pipeline_enabled", False)):
            await self._enqueue_outbox_job(
                tenant_id=tenant_id,
                user_id=user_id,
                session_id=session_id,
                job_type=JOB_TYPE_POST_INGEST_HOOK,
                payload={**base_payload, "hook": POST_INGEST_HOOK_PASS1_TRIAGE},
                ts=reference_time,
                text=f"{JOB_TYPE_POST_INGEST_HOOK}:{POST_INGEST_HOOK_PASS1_TRIAGE}",
                dedupe_key=f"session_hook_pass1_triage:{tenant_id}:{user_id}:{session_id}"
            )

    async def _handle_session_raw_episode_job(self, row: Dict[str, Any], graphiti_client) -> None:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        messages_full = await self._load_session_transcript_messages(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"]
        )
        messages = self._cap_messages_for_processing(
            messages_full,
            max_messages=SESSION_INGEST_GRAPHITI_MAX_MESSAGES,
            max_chars=SESSION_INGEST_GRAPHITI_MAX_CHARS
        )
        if not messages:
            raise TypeError("validation error: missing transcript messages for session raw episode")
        if len(messages) < len(messages_full):
            logger.warning(
                "session_raw_episode payload capped tenant=%s user=%s session=%s full_messages=%s capped_messages=%s",
                row["tenant_id"],
                row["user_id"],
                row["session_id"],
                len(messages_full),
                len(messages),
            )
        reference_time = self._parse_payload_reference_time(payload)
        episode_name = f"session_raw_{row['session_id']}"
        response = await graphiti_client.add_session_episode(
            tenant_id=row["tenant_id"],
            user_id=row["user_id"],
            messages=messages,
            reference_time=reference_time,
            episode_name=episode_name,
            metadata={
                "session_id": row["session_id"],
                "started_at": payload.get("started_at"),
                "ended_at": payload.get("ended_at"),
                "episode_type": "session_raw",
            }
        )
        if not self._is_success_response(response):
            self._raise_for_graphiti_response_failure(response, context="session_raw_episode")
        try:
            await self._infer_habit_daily_log_from_episode(
                tenant_id=row["tenant_id"],
                user_id=row["user_id"],
                messages=messages,
            )
        except Exception as e:
            logger.warning(
                "habit daily log inference soft-failed tenant=%s user=%s session=%s err=%s",
                row["tenant_id"],
                row["user_id"],
                row["session_id"],
                e,
            )
        episode_uuid = response.get("episode_uuid") if isinstance(response, dict) else None
        try:
            if bool(self.settings.episodic_embedding_enabled):
                embedding_result = await upsert_session_episode_embeddings(
                    db=self.db,
                    tenant_id=row["tenant_id"],
                    user_id=row["user_id"],
                    session_id=row["session_id"],
                    episode_uuid=episode_uuid,
                    reference_time=reference_time,
                    messages=messages,
                    model=self.settings.episodic_embedding_model,
                    window_size=int(self.settings.episodic_embedding_window_size),
                    stride=int(self.settings.episodic_embedding_stride),
                    max_windows=int(self.settings.episodic_embedding_max_windows),
                    max_chars=int(self.settings.episodic_embedding_max_chars),
                    user_turn_centric=bool(self.settings.episodic_embedding_user_turn_centric),
                    include_assistant_turns=bool(self.settings.episodic_embedding_include_assistant_turns),
                    assistant_char_weight=float(self.settings.episodic_embedding_assistant_char_weight),
                )
                logger.info(
                    "episodic embedding index tenant=%s user=%s session=%s indexed=%s attempted=%s model=%s",
                    row["tenant_id"],
                    row["user_id"],
                    row["session_id"],
                    embedding_result.get("indexed"),
                    embedding_result.get("attempted"),
                    embedding_result.get("embedding_model"),
                )
        except Exception as e:
            logger.warning(
                "episodic embedding index soft-failed tenant=%s user=%s session=%s err=%s",
                row["tenant_id"],
                row["user_id"],
                row["session_id"],
                e,
            )
        await self._enqueue_post_ingest_hook_jobs(
            tenant_id=row["tenant_id"],
            user_id=row["user_id"],
            session_id=row["session_id"],
            reference_time=reference_time,
            episode_uuid=episode_uuid
        )

    async def _handle_post_ingest_hook_job(self, row: Dict[str, Any]) -> None:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        hook_name = str(payload.get("hook") or "").strip()
        if not hook_name:
            raise TypeError("validation error: missing hook name")
        if _post_ingest_hook_executor is None:
            raise RuntimeError("post_ingest_hook_executor_unavailable")
        ok = await _post_ingest_hook_executor(hook_name, payload)
        if ok is False:
            raise RuntimeError(f"post_ingest_hook_failed:{hook_name}")

    @staticmethod
    def _parse_json_object_response(raw: Any) -> Optional[Dict[str, Any]]:
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str):
            raw = str(raw)
        text = raw.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            pass
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            candidate = text[start:end + 1]
            try:
                return json.loads(candidate)
            except Exception:
                return None
        return None

    async def _upsert_habit_daily_log_row(
        self,
        *,
        user_id: str,
        habit_id: str,
        log_date: date,
        completed: bool,
        nudged: bool,
        acknowledged: bool,
        user_response: Optional[str],
        inferred_from: Optional[str],
    ) -> None:
        await self.db.execute(
            """
            INSERT INTO habit_daily_log (
                user_id, habit_id, date, completed, nudged, acknowledged, user_response, inferred_from, created_at
            )
            VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (habit_id, date)
            DO UPDATE SET
                completed = (habit_daily_log.completed OR EXCLUDED.completed),
                nudged = (habit_daily_log.nudged OR EXCLUDED.nudged),
                acknowledged = (habit_daily_log.acknowledged OR EXCLUDED.acknowledged),
                user_response = COALESCE(EXCLUDED.user_response, habit_daily_log.user_response),
                inferred_from = COALESCE(EXCLUDED.inferred_from, habit_daily_log.inferred_from)
            """,
            user_id,
            habit_id,
            log_date,
            completed,
            nudged,
            acknowledged,
            user_response,
            inferred_from,
        )

    async def _infer_habit_daily_log_from_episode(
        self,
        *,
        tenant_id: str,
        user_id: str,
        messages: List[Dict[str, Any]],
    ) -> None:
        if not messages:
            return
        active_habits = await self.db.fetch(
            """
            SELECT id::text AS id, text, COALESCE(hint, '') AS hint
            FROM loops
            WHERE tenant_id = $1
              AND user_id = $2
              AND type = 'habit'
              AND status = 'active'
            ORDER BY CASE WHEN time_horizon = 'ongoing' THEN 0 ELSE 1 END, COALESCE(last_seen_at, updated_at) DESC
            """,
            tenant_id,
            user_id,
        )
        if not active_habits:
            return

        transcript = self._messages_to_transcript(messages)
        if not transcript:
            return
        habits_payload = [
            {
                "habit_id": str(h.get("id") or ""),
                "habit_text": str(h.get("text") or ""),
                "hint": str(h.get("hint") or ""),
            }
            for h in active_habits
            if str(h.get("id") or "").strip() and str(h.get("text") or "").strip()
        ]
        if not habits_payload:
            return

        prompt = (
            "You are classifying habit progress from a conversation transcript.\n"
            "For each habit, decide status as one of: completed, later, dismissed, unclear.\n"
            "Also decide nudged=true if the assistant explicitly nudged/reminded/discussed that habit in the transcript; else false.\n"
            "Also decide acknowledged=true only when the assistant explicitly acknowledges or praises the user's completed habit in the transcript; else false.\n"
            "Return JSON only in this schema:\n"
            "{\"habits\":[{\"habit_id\":\"...\",\"status\":\"completed|later|dismissed|unclear\",\"nudged\":true|false,\"acknowledged\":true|false}]}\n\n"
            f"HABITS_JSON:\n{json.dumps(habits_payload, ensure_ascii=True)}\n\n"
            f"TRANSCRIPT:\n{transcript}\n"
        )
        raw = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=600,
            temperature=0.0,
            task="loops",
        )
        parsed = self._parse_json_object_response(raw)
        if not isinstance(parsed, dict):
            return
        rows = parsed.get("habits")
        if not isinstance(rows, list):
            return

        active_habit_ids = {str(h.get("id") or "").strip() for h in active_habits}
        log_date = datetime.utcnow().date()
        for item in rows:
            if not isinstance(item, dict):
                continue
            habit_id = str(item.get("habit_id") or "").strip()
            if not habit_id or habit_id not in active_habit_ids:
                continue
            status = str(item.get("status") or "").strip().lower()
            nudged = bool(item.get("nudged"))
            completed = status == "completed"
            acknowledged = bool(item.get("acknowledged")) and completed
            user_response: Optional[str] = None
            if status == "later":
                user_response = "later"
            elif status == "dismissed":
                user_response = "dismissed"
            inferred_from = "transcript mention" if completed else None
            if not completed and not user_response and not nudged and not acknowledged:
                continue
            await self._upsert_habit_daily_log_row(
                user_id=user_id,
                habit_id=habit_id,
                log_date=log_date,
                completed=completed,
                nudged=nudged,
                acknowledged=acknowledged,
                user_response=user_response,
                inferred_from=inferred_from,
            )

    async def drain_outbox(
        self,
        graphiti_client,
        limit: int = 200,
        tenant_id: Optional[str] = None,
        job_types: Optional[List[str]] = None,
        budget_seconds: float = 2.0,
        per_row_timeout_seconds: float = 8.0
    ) -> Dict[str, int]:
        start = datetime.utcnow()
        claimed = await self._claim_pending_outbox(
            limit=limit,
            tenant_id=tenant_id,
            job_types=job_types
        )
        counts = {"claimed": len(claimed), "sent": 0, "failed": 0, "pending": 0}
        if not claimed:
            return counts

        summary_cache: Dict[tuple, str] = {}

        for row in claimed:
            elapsed = (datetime.utcnow() - start).total_seconds()
            if elapsed >= budget_seconds:
                break
            job_type = (row.get("job_type") or JOB_TYPE_TURN).strip().lower()

            if job_type == JOB_TYPE_TURN:
                key = (row["tenant_id"], row["session_id"], row["user_id"])
                if key not in summary_cache:
                    buffer = await self.get_or_create_buffer(
                        row["tenant_id"],
                        row["session_id"],
                        row["user_id"]
                    )
                    summary_cache[key] = buffer.get("rolling_summary", "")

                if row.get("folded_at") is None:
                    turn = {
                        "role": row["role"],
                        "text": row["text"],
                        "timestamp": row["ts"].isoformat()
                    }
                    try:
                        new_summary = await asyncio.wait_for(
                            self._fold_into_summary(summary_cache[key], turn),
                            timeout=per_row_timeout_seconds
                        )
                        await self._update_rolling_summary(
                            row["tenant_id"],
                            row["session_id"],
                            new_summary
                        )
                        await self._append_transcript_turn(
                            row["tenant_id"],
                            row["session_id"],
                            row["user_id"],
                            turn
                        )
                        await self._mark_outbox_folded(row["id"])
                        summary_cache[key] = new_summary
                    except asyncio.TimeoutError as e:
                        await self._mark_outbox_failed(row["id"], f"fold_timeout: {e}")
                        counts["pending"] += 1
                        continue
                    except Exception as e:
                        await self._mark_outbox_failed(row["id"], str(e))
                        counts["pending"] += 1
                        continue

                try:
                    if self.settings.graphiti_per_turn:
                        episode_name = self._build_episode_name(
                            tenant_id=row["tenant_id"],
                            user_id=row["user_id"],
                            session_id=row["session_id"],
                            role=row["role"],
                            ts=row["ts"],
                            text=row["text"]
                        )
                        response = await asyncio.wait_for(
                            self._call_add_episode(
                                graphiti_client,
                                tenant_id=row["tenant_id"],
                                user_id=row["user_id"],
                                text=row["text"],
                                timestamp=row["ts"],
                                role=row["role"],
                                metadata={
                                    "session_id": row["session_id"],
                                    "evicted_from_buffer": True
                                },
                                episode_name=episode_name
                            ),
                            timeout=per_row_timeout_seconds
                        )
                        if not self._is_success_response(response):
                            self._raise_for_graphiti_response_failure(response, context="per_turn_episode")
                    await self._mark_outbox_sent(row["id"])
                    counts["sent"] += 1
                except asyncio.TimeoutError as e:
                    await self._mark_outbox_failed(row["id"], f"send_timeout: {e}")
                    counts["pending"] += 1
                except Exception as e:
                    if self._is_permanent_outbox_error(e):
                        await self._mark_outbox_dead_letter(row["id"], str(e))
                        counts["failed"] += 1
                    else:
                        await self._mark_outbox_failed(row["id"], str(e))
                        counts["pending"] += 1
                continue

            try:
                if job_type == JOB_TYPE_SESSION_RAW_EPISODE:
                    await asyncio.wait_for(
                        self._handle_session_raw_episode_job(row, graphiti_client),
                        timeout=per_row_timeout_seconds
                    )
                elif job_type == JOB_TYPE_POST_INGEST_HOOK:
                    await asyncio.wait_for(
                        self._handle_post_ingest_hook_job(row),
                        timeout=per_row_timeout_seconds
                    )
                else:
                    raise TypeError(f"validation error: unknown outbox job_type {job_type}")
                await self._mark_outbox_sent(row["id"])
                counts["sent"] += 1
            except asyncio.TimeoutError as e:
                await self._mark_outbox_failed(row["id"], f"job_timeout: {e}")
                counts["pending"] += 1
            except Exception as e:
                if self._is_permanent_outbox_error(e):
                    await self._mark_outbox_dead_letter(row["id"], str(e))
                    counts["failed"] += 1
                else:
                    await self._mark_outbox_failed(row["id"], str(e))
                    counts["pending"] += 1

        return counts

    async def _mark_outbox_folded(self, outbox_id: int) -> None:
        query = """
            UPDATE graphiti_outbox
            SET folded_at = NOW()
            WHERE id = $1 AND folded_at IS NULL
        """
        await self.db.execute(query, outbox_id)

    def _build_episode_name(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        role: str,
        ts: datetime,
        text: str
    ) -> str:
        payload = {
            "kind": "episode",
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "role": role,
            "timestamp": ts,
            "text": text,
        }
        digest = stable_short_hash(payload, length=16)
        return f"episode_{digest}"

    def _build_session_summary_name(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        ts: datetime,
        summary: str
    ) -> str:
        payload = {
            "kind": "session_summary",
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": ts,
            "summary": summary,
        }
        digest = stable_short_hash(payload, length=16)
        return f"session_summary_{digest}"

    async def _fold_into_summary(
        self,
        current_summary: str,
        new_turn: Dict[str, Any]
    ) -> str:
        """
        Fold a turn into the rolling summary using LLM.

        This is a lossy compression - that's fine, Graphiti has full fidelity.
        """
        try:
            prompt = f"""You are a conversation compressor.

Current summary of earlier conversation:
{current_summary or "No previous summary."}

New turn to fold in:
{new_turn['role']}: {new_turn['text']}

Write an updated summary that incorporates this turn.
Keep it concise (2-4 sentences max).
Preserve key facts, commitments, emotional state, and context.
Do not include filler or meta-commentary."""

            response = await self.llm_client._call_llm(
                prompt=prompt,
                max_tokens=200,
                temperature=0.3,
                task="summary"
            )

            if response:
                return response.strip()
            else:
                # If LLM fails, append to summary manually
                logger.warning("LLM failed to fold summary, using fallback")
                fallback = current_summary + f" {new_turn['role']}: {new_turn['text'][:50]}..."
                return fallback[:500]  # Limit length

        except Exception as e:
            logger.error(f"Failed to fold into summary: {e}")
            # Fallback: append manually
            return current_summary + f" {new_turn['role']}: {new_turn['text'][:50]}..."

    def _build_session_summary_input(
        self,
        rolling_summary: str,
        recent_turns: List[Dict[str, Any]]
    ) -> str:
        recent = "\n".join([f"{t['role']}: {t['text']}" for t in recent_turns])
        if rolling_summary:
            return f"Earlier summary:\n{rolling_summary}\n\nRecent turns:\n{recent}"
        return f"Recent turns:\n{recent}"

    async def _summarize_session_close(
        self,
        transcript: str,
        reference_time: datetime
    ) -> Dict[str, Any]:
        date_str = reference_time.strftime("%A, %b %d, %Y")
        prompt = (
            f"Current Date: {date_str}\n\n"
            "You are a memory processor. Your job is to extract two distinct texts from the transcript below.\n\n"
            "TASK 1: HISTORICAL SUMMARY\n"
            "Rules:\n"
            "- Write in PAST TENSE.\n"
            "- No speaker labels, no line-by-line quoting.\n"
            f"- Convert relative dates (like \"tomorrow\") into absolute dates based on the current date ({date_str}).\n"
            "- Use this exact structure:\n"
            "  SUMMARY: 1-2 sentences (what happened, session arc, factual)\n"
            "  TONE: 1 sentence — what happened in this conversation and how it resolved. Facts only. No mood inference or emotional labelling.\n"
            "  DECISIONS: bullet list only if explicit decisions/commitments were made; otherwise empty\n"
            "  UNRESOLVED: bullet list only if something significant was left open; otherwise empty\n"
            "  MOMENT: 1 sentence for the single most significant thing said/happened, if anything; omit if routine session\n"
            "- SUMMARY must be <= 2 sentences and <= 35 words total.\n"
            "- TONE must be <= 12 words.\n"
            "- MOMENT (if present) must be <= 20 words.\n"
            "- Capture only facts, commitments, decisions, unresolved items, and observed emotional tone.\n"
            "- Be concise and neutral in all sections.\n"
            "- If a detail is uncertain, omit it.\n"
            "- Do NOT add coaching language, advice, or prescriptive framing in any section.\n"
            "- Do not infer or invent; include only what is explicitly present in transcript.\n"
            "- Graphiti handles entity extraction separately; do not list people/projects unless needed for significance.\n"
            "- Avoid filler verbs (reported, stated, mentioned, noted, said); use at most once total.\n"
            "- DECISIONS: include only durable, future-impacting decisions/commitments (deploy/ship/commit/schedule/stop/start/change plan).\n"
            "- DECISIONS: do NOT include trivial actions already in progress.\n"
            "- UNRESOLVED: include only actionable items likely to recur next session.\n"
            "- Prevent repetition: do not repeat the same fact across SUMMARY and MOMENT.\n"
            "- Do NOT use user name or assistant name; use “the user” or “the assistant” if needed.\n\n"
            "TASK 2: BRIDGING TEXT\n"
            "Rules:\n"
            "- 1-2 sentences max.\n"
            "- Write in PRESENT or FUTURE tense.\n"
            "- This text is to bridge the gap to the next session.\n"
            "- Focus on pending facts, unresolved items, and the user's stated plans.\n"
            "- Use factual narrative only. Do NOT give instructions.\n"
            "- Do NOT use wording like \"the assistant should\" or imperative advice.\n"
            "- Do NOT include speaker labels or verbatim transcript.\n"
            "- Do NOT use user name or assistant name; use “the user” or “the assistant” if needed.\n\n"
            "- BRIDGE must be <= 25 words.\n"
            f"Session Transcript:\n{transcript}\n\n"
            "Response format:\n"
            "SUMMARY: ...\n"
            "TONE: ...\n"
            "DECISIONS:\n"
            "- ...\n"
            "UNRESOLVED:\n"
            "- ...\n"
            "MOMENT: ...\n"
            "BRIDGE: ...\n"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=220,
            temperature=0.1,
            task="session_episode"
        )
        if not response:
            return {
                "summary": "",
                "bridge": "",
                "summary_facts": "",
                "tone": "",
                "moment": "",
                "decisions": [],
                "unresolved": [],
            }
        parsed = self._parse_summary_bridge(response)
        parsed = await self._enforce_structured_recap_rules(transcript, parsed)
        summary = parsed.get("summary", "")
        bridge = parsed.get("bridge", "")
        if not summary and not bridge:
            logger.warning(
                "Session recap parse returned empty output. Raw response (truncated): %s",
                response[:500]
            )
        if self._looks_like_transcript(summary):
            summary = ""
            parsed["summary_facts"] = ""
            parsed["tone"] = ""
            parsed["moment"] = ""
        if self._looks_like_transcript(bridge):
            bridge = ""
        parsed["summary"] = summary
        parsed["bridge"] = bridge
        return parsed

    @staticmethod
    def _clean_message_text(value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).strip().split())

    @staticmethod
    def _messages_to_transcript(messages: List[Dict[str, Any]]) -> str:
        lines: List[str] = []
        for msg in messages or []:
            text = SessionManager._clean_message_text(msg.get("text"))
            if not text:
                continue
            role = (msg.get("role") or "user").strip().lower()
            lines.append(f"{role}: {text}")
        return "\n".join(lines)

    @staticmethod
    def _select_reference_time(messages: List[Dict[str, Any]]) -> datetime:
        reference_time = datetime.utcnow()
        for msg in reversed(messages or []):
            ts = msg.get("timestamp")
            if not ts:
                continue
            try:
                reference_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                break
            except Exception:
                continue
        return reference_time

    def _deterministic_session_recap(
        self,
        messages: List[Dict[str, Any]],
        reference_time: datetime
    ) -> Dict[str, str]:
        cleaned_messages = []
        for msg in messages or []:
            text = self._clean_message_text(msg.get("text"))
            if not text:
                continue
            cleaned_messages.append({
                "role": (msg.get("role") or "user").lower(),
                "text": text
            })

        date_str = reference_time.strftime("%A, %B %d, %Y")
        user_messages = [m for m in cleaned_messages if m["role"] == "user"]
        assistant_messages = [m for m in cleaned_messages if m["role"] == "assistant"]

        commitment = None
        commitment_markers = (" i will ", " i'm going to ", " i am going to ", " my plan ", " i need to ")
        for m in user_messages:
            padded = f" {m['text'].lower()} "
            if any(marker in padded for marker in commitment_markers):
                commitment = m["text"]
                break

        if not cleaned_messages:
            summary = (
                f"The session on {date_str} contained no substantive message content. "
                "No concrete commitments or decisions were recorded."
            )
            bridge = "At the next session, the user's current priority can be clarified before continuing."
            return {"summary": summary, "bridge": bridge}

        if user_messages and not assistant_messages:
            summary = (
                f"On {date_str}, the user initiated a brief check-in without receiving a completed response. "
                "No confirmed decision was recorded in the session."
            )
            bridge = "At the next session, the pending user message remains unresolved and can be addressed first."
            return {"summary": summary, "bridge": bridge}

        if commitment:
            summary = (
                f"On {date_str}, the user stated a concrete plan: {commitment}. "
                "The conversation set expectations for the immediate next step."
            )
            bridge = "At the next session, this plan can be revisited to confirm progress and what remains pending."
            return {"summary": summary, "bridge": bridge}

        summary = (
            f"On {date_str}, the user and assistant had a brief exchange with limited actionable detail. "
            "No explicit commitments or unresolved blockers were captured."
        )
        bridge = "At the next session, it can be clarified whether to continue this thread or begin a new one."
        return {"summary": summary, "bridge": bridge}

    async def summarize_session_messages_with_quality(
        self,
        messages: List[Dict[str, Any]],
        reference_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        transcript = self._messages_to_transcript(messages or [])
        ref_time = reference_time or self._select_reference_time(messages or [])

        recaps = await self._summarize_session_close(transcript=transcript, reference_time=ref_time)
        summary_text = self._compose_display_summary(
            recaps.get("summary_facts") or "",
            recaps.get("tone") or "",
            recaps.get("moment") or ""
        )
        bridge_text = (recaps.get("bridge") or "").strip()
        summary_facts = (recaps.get("summary_facts") or "").strip()
        tone = (recaps.get("tone") or "").strip()
        moment = (recaps.get("moment") or "").strip()
        decisions = recaps.get("decisions") if isinstance(recaps.get("decisions"), list) else []
        unresolved = recaps.get("unresolved") if isinstance(recaps.get("unresolved"), list) else []
        quality_tier = "llm_primary"

        if not summary_text or self._looks_like_transcript(summary_text):
            repaired = await self._rewrite_summary_from_text(transcript)
            repaired = (repaired or "").strip()
            if repaired and not self._looks_like_transcript(repaired):
                summary_text = repaired
                summary_facts = repaired
                tone = ""
                moment = ""
                quality_tier = "llm_repair"

        if not bridge_text or self._looks_like_transcript(bridge_text):
            candidate_source = summary_text if summary_text else transcript
            repaired_bridge = await self._summarize_session_bridge(candidate_source)
            repaired_bridge = (repaired_bridge or "").strip()
            if repaired_bridge and not self._looks_like_transcript(repaired_bridge):
                bridge_text = repaired_bridge
                quality_tier = "llm_repair"

        used_deterministic = False
        if not summary_text or self._looks_like_transcript(summary_text):
            used_deterministic = True
        if not bridge_text or self._looks_like_transcript(bridge_text):
            used_deterministic = True
        if used_deterministic:
            deterministic = self._deterministic_session_recap(messages or [], ref_time)
            if not summary_text or self._looks_like_transcript(summary_text):
                summary_text = deterministic.get("summary", "").strip()
                summary_facts = summary_text
                tone = ""
                moment = ""
            if not bridge_text or self._looks_like_transcript(bridge_text):
                bridge_text = deterministic.get("bridge", "").strip()
            quality_tier = "deterministic_fallback"

        # Final hard guarantee
        if not summary_text:
            summary_text = "The session completed with minimal actionable detail and no confirmed commitments."
            summary_facts = summary_text
            tone = ""
            moment = ""
            quality_tier = "deterministic_fallback"
        if not bridge_text:
            bridge_text = "At the next session, the user's current priority and next action can be clarified."
            quality_tier = "deterministic_fallback"

        return {
            "summary_text": summary_text,
            "bridge_text": bridge_text,
            "summary_facts": summary_facts,
            "tone": tone,
            "moment": moment,
            "decisions": decisions,
            "unresolved": unresolved,
            "index_text": self._compose_index_text(
                summary_facts=summary_facts,
                moment=moment,
                decisions=decisions,
                unresolved=unresolved,
            ),
            "salience": self._classify_summary_salience(
                tone=tone,
                moment=moment,
                decisions=decisions,
                unresolved=unresolved,
            ),
            "summary_quality_tier": quality_tier,
            "summary_source": "full_transcript"
        }

    async def _summarize_session_bridge(self, summary_text: str) -> str:
        if self._looks_like_transcript(summary_text):
            return ""
        prompt = (
            "Rewrite the session summary into a 1-2 sentence bridge for the next session. "
            "Be factual, concise, and in present/future tense. "
            "Use factual narrative only, not instructions. "
            "Do not write phrases like 'the assistant should'. "
            "Do NOT include speaker labels or verbatim transcript. "
            "Do NOT use user name or assistant name; use “the user” or “the assistant” if needed. "
            "Prefer concrete plans, commitments, and state of ongoing work. "
            "Exclude environment unless it was explicitly stated.\n\n"
            f"Summary:\n{summary_text}\n\nBridge:"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=120,
            temperature=0.2,
            task="session_bridge"
        )
        if response:
            text = response.strip()
            if self._looks_like_transcript(text):
                strict = await self._summarize_session_bridge_strict(summary_text)
                if strict:
                    text = strict
            if self._looks_like_transcript(text):
                return ""
            return text
        return ""

    @staticmethod
    def _normalize_sentence_tokens(value: str) -> Set[str]:
        return set(re.findall(r"[a-z0-9]+", (value or "").lower()))

    @classmethod
    def _dedupe_summary_sentences(cls, summary_text: str) -> str:
        if not summary_text:
            return ""
        raw_sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", summary_text) if s.strip()]
        kept: List[str] = []
        kept_tokens: List[Set[str]] = []
        for sentence in raw_sentences:
            tokens = cls._normalize_sentence_tokens(sentence)
            if not tokens:
                continue
            is_duplicate = False
            for prev_tokens in kept_tokens:
                overlap = len(tokens & prev_tokens)
                base = max(1, min(len(tokens), len(prev_tokens)))
                if (overlap / base) > 0.7:
                    is_duplicate = True
                    break
            if is_duplicate:
                continue
            kept.append(sentence)
            kept_tokens.append(tokens)
        return " ".join(kept).strip()

    @classmethod
    def _compose_display_summary(cls, summary_facts: str, tone: str, moment: str) -> str:
        parts = []
        for value in (summary_facts, tone, moment):
            cleaned = " ".join((value or "").split()).strip()
            if cleaned:
                parts.append(cleaned)
        return " ".join(parts).strip()

    @classmethod
    def _compose_index_text(
        cls,
        summary_facts: str,
        moment: str,
        decisions: List[str],
        unresolved: List[str],
    ) -> str:
        def _clean_items(items: List[str]) -> List[str]:
            return [" ".join((i or "").split()).strip() for i in (items or []) if str(i or "").strip()]

        parts: List[str] = []
        remaining = cls._INDEX_TEXT_MAX_CHARS

        def _append_text(value: str) -> None:
            nonlocal remaining
            if remaining <= 0:
                return
            text = " ".join((value or "").split()).strip()
            if not text:
                return
            prefix = " " if parts else ""
            room = remaining - len(prefix)
            if room <= 0:
                remaining = 0
                return
            if len(text) > room:
                text = text[:room].rstrip(" ,;")
            if not text:
                remaining = 0
                return
            parts.append(text)
            remaining -= len(prefix) + len(text)

        facts = " ".join((summary_facts or "").split()).strip()
        moment_text = " ".join((moment or "").split()).strip()
        if facts and moment_text:
            # Reserve room so moment is always retained (possibly clipped).
            reserve_for_moment = min(len(moment_text) + 1, max(40, cls._INDEX_TEXT_MAX_CHARS // 5))
            facts_room = max(0, remaining - reserve_for_moment)
            if facts_room > 0:
                clipped_facts = facts[:facts_room].rstrip(" ,;")
                _append_text(clipped_facts)
            _append_text(moment_text)
        else:
            _append_text(facts)
            _append_text(moment_text)

        def _append_labeled_items(label: str, items: List[str]) -> None:
            nonlocal remaining
            cleaned = _clean_items(items)
            if not cleaned or remaining <= 0:
                return
            prefix = " " if parts else ""
            section_prefix = f"{label}: "
            # Must be able to fit " Label: x" at minimum.
            if remaining <= len(prefix) + len(section_prefix):
                return
            available = remaining - len(prefix)
            section = section_prefix
            for idx, item in enumerate(cleaned):
                sep = "" if idx == 0 else "; "
                candidate = f"{sep}{item}"
                if len(section) + len(candidate) <= available:
                    section += candidate
                    continue
                spare = available - len(section) - len(sep)
                if spare > 0:
                    clipped = item[:spare].rstrip(" ,;")
                    if clipped:
                        section += f"{sep}{clipped}"
                break
            if section == section_prefix:
                return
            parts.append(section)
            remaining -= len(prefix) + len(section)

        _append_labeled_items("Decisions", decisions)
        _append_labeled_items("Open loops", unresolved)

        return " ".join(parts).strip()

    @classmethod
    def _classify_summary_salience(
        cls,
        tone: str,
        moment: str,
        decisions: List[str],
        unresolved: List[str],
    ) -> str:
        if decisions or unresolved:
            return "high"
        if moment:
            return "medium"
        tone_lower = (tone or "").lower()
        affect_markers = (
            "frustrat",
            "anxious",
            "overwhelm",
            "angry",
            "upset",
            "relieved",
            "excited",
            "panic",
            "scared",
            "afraid",
            "devastat",
            "proud",
        )
        if any(marker in tone_lower for marker in affect_markers):
            return "medium"
        return "low"

    @staticmethod
    def _word_count(value: str) -> int:
        return len(re.findall(r"\b\w+\b", value or ""))

    @classmethod
    def _truncate_words(cls, value: str, max_words: int) -> str:
        if not value:
            return ""
        words = re.findall(r"\S+", value.strip())
        if len(words) <= max_words:
            return value.strip()
        return " ".join(words[:max_words]).strip().rstrip(",;")

    @classmethod
    def _truncate_sentences(cls, value: str, max_sentences: int) -> str:
        if not value:
            return ""
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", value.strip()) if s.strip()]
        if not sentences:
            return value.strip()
        return " ".join(sentences[:max_sentences]).strip()

    @classmethod
    def _normalize_for_overlap(cls, value: str) -> Set[str]:
        return cls._normalize_sentence_tokens(value)

    @classmethod
    def _overlap_ratio(cls, left: str, right: str) -> float:
        left_tokens = cls._normalize_for_overlap(left)
        right_tokens = cls._normalize_for_overlap(right)
        if not left_tokens or not right_tokens:
            return 0.0
        base = max(1, min(len(left_tokens), len(right_tokens)))
        return len(left_tokens & right_tokens) / base

    @classmethod
    def _count_filler_verbs(cls, *values: str) -> int:
        text = " ".join([v for v in values if v]).lower()
        count = 0
        for verb in cls._FILLER_VERBS:
            count += len(re.findall(rf"\b{re.escape(verb)}\b", text))
        return count

    @classmethod
    def _has_cross_field_repetition(cls, summary_facts: str, tone: str, moment: str) -> bool:
        checks = []
        if summary_facts and tone:
            checks.append((summary_facts, tone))
        if summary_facts and moment:
            checks.append((summary_facts, moment))
        if tone and moment:
            checks.append((tone, moment))
        for left, right in checks:
            if cls._overlap_ratio(left, right) > 0.7:
                return True
        return False

    @classmethod
    def _structured_recap_is_valid(
        cls,
        summary_facts: str,
        tone: str,
        moment: str
    ) -> bool:
        if cls._word_count(summary_facts) > cls._SUMMARY_MAX_WORDS:
            return False
        if cls._word_count(tone) > cls._TONE_MAX_WORDS:
            return False
        if cls._word_count(moment) > cls._MOMENT_MAX_WORDS:
            return False
        if cls._word_count(summary_facts) and len(
            [s for s in re.split(r"(?<=[.!?])\s+", summary_facts.strip()) if s.strip()]
        ) > cls._SUMMARY_MAX_SENTENCES:
            return False
        if cls._has_cross_field_repetition(summary_facts, tone, moment):
            return False
        if cls._count_filler_verbs(summary_facts, tone, moment) > 1:
            return False
        return True

    async def _rewrite_structured_recap(
        self,
        transcript: str,
        parsed: Dict[str, Any]
    ) -> Dict[str, Any]:
        prompt = (
            "Rewrite to satisfy rules exactly; remove repetition; remove filler verbs; keep meaning.\n"
            "Return EXACTLY this format:\n"
            "SUMMARY: <=2 sentences, <=35 words total\n"
            "TONE: <=12 words\n"
            "DECISIONS:\n"
            "- durable future-impacting decisions only\n"
            "UNRESOLVED:\n"
            "- actionable recurring unresolved only\n"
            "MOMENT: <=20 words (omit if routine)\n"
            "BRIDGE: <=25 words\n\n"
            "Current structured output:\n"
            f"SUMMARY: {parsed.get('summary_facts','')}\n"
            f"TONE: {parsed.get('tone','')}\n"
            "DECISIONS:\n"
            + "\n".join([f"- {d}" for d in (parsed.get("decisions") or [])])
            + ("\n" if parsed.get("decisions") else "")
            + "UNRESOLVED:\n"
            + "\n".join([f"- {u}" for u in (parsed.get("unresolved") or [])])
            + ("\n" if parsed.get("unresolved") else "")
            + f"MOMENT: {parsed.get('moment','')}\n"
            + f"BRIDGE: {parsed.get('bridge','')}\n\n"
            + f"Transcript:\n{transcript}\n"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=220,
            temperature=0.1,
            task="session_episode_rewrite"
        )
        if not response:
            return parsed
        return self._parse_summary_bridge(response)

    @classmethod
    def _truncate_structured_recap_fields(cls, parsed: Dict[str, Any]) -> Dict[str, Any]:
        summary_facts = cls._truncate_sentences(parsed.get("summary_facts") or "", cls._SUMMARY_MAX_SENTENCES)
        summary_facts = cls._truncate_words(summary_facts, cls._SUMMARY_MAX_WORDS)
        tone = cls._truncate_words(parsed.get("tone") or "", cls._TONE_MAX_WORDS)
        moment = cls._truncate_words(parsed.get("moment") or "", cls._MOMENT_MAX_WORDS)
        if cls._has_cross_field_repetition(summary_facts, tone, moment):
            if cls._overlap_ratio(summary_facts, tone) > 0.7:
                tone = ""
            if cls._overlap_ratio(summary_facts, moment) > 0.7:
                moment = ""
            if tone and moment and cls._overlap_ratio(tone, moment) > 0.7:
                moment = ""
        if cls._count_filler_verbs(summary_facts, tone, moment) > 1:
            for field in ("summary_facts", "tone", "moment"):
                value = {"summary_facts": summary_facts, "tone": tone, "moment": moment}[field]
                cleaned = re.sub(
                    r"\b(reported|stated|mentioned|noted|said)\b",
                    "",
                    value,
                    count=10,
                    flags=re.IGNORECASE
                )
                cleaned = " ".join(cleaned.split()).strip()
                if field == "summary_facts":
                    summary_facts = cleaned
                elif field == "tone":
                    tone = cleaned
                else:
                    moment = cleaned
                if cls._count_filler_verbs(summary_facts, tone, moment) <= 1:
                    break
        return {
            **parsed,
            "summary_facts": summary_facts,
            "tone": tone,
            "moment": moment,
            "summary": cls._compose_display_summary(summary_facts, tone, moment),
        }

    async def _enforce_structured_recap_rules(
        self,
        transcript: str,
        parsed: Dict[str, Any]
    ) -> Dict[str, Any]:
        summary_facts = (parsed.get("summary_facts") or "").strip()
        tone = (parsed.get("tone") or "").strip()
        moment = (parsed.get("moment") or "").strip()
        if self._structured_recap_is_valid(summary_facts, tone, moment):
            parsed["summary"] = self._compose_display_summary(summary_facts, tone, moment)
            return parsed

        rewritten = await self._rewrite_structured_recap(transcript, parsed)
        rewritten_summary = (rewritten.get("summary_facts") or "").strip()
        rewritten_tone = (rewritten.get("tone") or "").strip()
        rewritten_moment = (rewritten.get("moment") or "").strip()
        if self._structured_recap_is_valid(rewritten_summary, rewritten_tone, rewritten_moment):
            rewritten["summary"] = self._compose_display_summary(
                rewritten_summary, rewritten_tone, rewritten_moment
            )
            return rewritten

        return self._truncate_structured_recap_fields(rewritten)

    @classmethod
    def _parse_summary_bridge(cls, text: str) -> Dict[str, Any]:
        sections: Dict[str, List[str]] = {
            "summary": [],
            "tone": [],
            "decisions": [],
            "unresolved": [],
            "moment": [],
            "bridge": [],
        }
        mode: Optional[str] = None
        for raw in (text or "").splitlines():
            line = raw.strip()
            if not line:
                continue
            upper = line.upper()
            if upper.startswith("SUMMARY:"):
                mode = "summary"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["summary"].append(content)
                continue
            if upper.startswith("TONE:"):
                mode = "tone"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["tone"].append(content)
                continue
            if upper.startswith("DECISIONS:"):
                mode = "decisions"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["decisions"].append(content)
                continue
            if upper.startswith("UNRESOLVED:"):
                mode = "unresolved"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["unresolved"].append(content)
                continue
            if upper.startswith("MOMENT:"):
                mode = "moment"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["moment"].append(content)
                continue
            if upper.startswith("BRIDGE:"):
                mode = "bridge"
                content = line.split(":", 1)[1].strip()
                if content:
                    sections["bridge"].append(content)
                continue

            if mode in {"decisions", "unresolved"}:
                cleaned = re.sub(r"^\s*(?:[-*•]+|\d+[.)])\s*", "", line).strip()
                if cleaned:
                    sections[mode].append(cleaned)
            elif mode in sections:
                sections[mode].append(line)

        def _clean_list(items: List[str]) -> List[str]:
            out: List[str] = []
            for item in items:
                value = " ".join((item or "").split()).strip()
                if not value:
                    continue
                if value.lower() in {"none", "empty", "n/a", "(none)"}:
                    continue
                out.append(value)
            return out

        summary_text = " ".join(_clean_list(sections["summary"]))
        summary_text = cls._dedupe_summary_sentences(summary_text)
        tone_text = " ".join(_clean_list(sections["tone"]))
        moment_text = " ".join(_clean_list(sections["moment"]))
        decisions = _clean_list(sections["decisions"])
        unresolved = _clean_list(sections["unresolved"])
        bridge_text = " ".join(_clean_list(sections["bridge"]))

        display_summary = cls._compose_display_summary(summary_text, tone_text, moment_text)

        return {
            "summary_facts": summary_text,
            "tone": tone_text,
            "moment": moment_text,
            "decisions": decisions,
            "unresolved": unresolved,
            "summary": display_summary,
            "bridge": bridge_text,
        }

    async def _summarize_session_bridge_strict(self, summary_text: str) -> str:
        prompt = (
            "Rewrite into a 1–2 sentence bridge recap. "
            "ABSOLUTELY NO speaker labels. NO transcript. Present/future tense only.\n\n"
            "Factual narrative only. No instructions and no 'the assistant should'.\n\n"
            f"Summary:\n{summary_text}\n\nBridge:"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=120,
            temperature=0.1,
            task="session_bridge_strict"
        )
        return response.strip() if response else ""

    @staticmethod
    def _contains_speaker_labels(text: str) -> bool:
        if not text:
            return False
        lower = text.lower()
        return "user:" in lower or "assistant:" in lower

    @staticmethod
    def _looks_like_transcript(text: str) -> bool:
        if not text:
            return False
        lower = text.lower()
        if "user:" in lower or "assistant:" in lower:
            return True
        if "recent turns:" in lower or "earlier summary:" in lower:
            return True
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return False
        labelish = 0
        for line in lines:
            l = line.lower()
            if l.startswith(("user:", "assistant:")):
                labelish += 1
            elif l.startswith("user ") or l.startswith("assistant "):
                labelish += 1
        return labelish >= 2

    async def _rewrite_summary_from_text(self, text: str) -> str:
        prompt = (
            "Rewrite this into a 2–4 sentence narrative recap. "
            "No speaker labels. No transcript. Past tense. "
            "Focus on key facts, commitments, and ongoing threads.\n\n"
            f"Text:\n{text}\n\nSummary:"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=200,
            temperature=0.2,
            task="session_rewrite"
        )
        if response:
            cleaned = response.strip()
            if self._looks_like_transcript(cleaned):
                return ""
            return cleaned
        return ""

    @staticmethod
    def _build_session_episode_text(
        summary_text: str,
        rolling_summary: str,
        recent_turns: List[Dict[str, Any]],
        active_loops: List[Any]
    ) -> str:
        loop_lines = []
        for loop in active_loops or []:
            loop_lines.append(f"- [{loop.type}] {loop.text}")
        loops_block = "\n".join(loop_lines) if loop_lines else "- (none)"

        decisions_block = "- (not detected)"
        entities_block = "- (not available)"

        return (
            "Session Summary:\n"
            f"{summary_text}\n\n"
            "Decisions:\n"
            f"{decisions_block}\n\n"
            "Open Loops:\n"
            f"{loops_block}\n\n"
            "Entities:\n"
            f"{entities_block}\n"
        )

    async def get_working_memory(
        self,
        tenant_id: str,
        session_id: str
    ) -> List[Message]:
        """Get working memory (last 12 messages) from buffer"""
        try:
            query = """
                SELECT messages FROM session_buffer
                WHERE tenant_id = $1 AND session_id = $2
            """
            result = await self.db.fetchone(query, tenant_id, session_id)

            if not result:
                return []

            messages = result.get('messages', [])
            if not isinstance(messages, list):
                return []

            # Convert to Message objects
            return [
                Message(
                    role=msg['role'],
                    text=msg['text'],
                    timestamp=msg['timestamp']
                )
                for msg in messages
            ]

        except Exception as e:
            logger.error(f"Failed to get working memory: {e}")
            return []

    async def get_rolling_summary(
        self,
        tenant_id: str,
        session_id: str
    ) -> str:
        """Get rolling summary from buffer"""
        try:
            query = """
                SELECT rolling_summary FROM session_buffer
                WHERE tenant_id = $1 AND session_id = $2
            """
            result = await self.db.fetchone(query, tenant_id, session_id)

            if result:
                return result.get('rolling_summary') or ''
            return ''

        except Exception as e:
            logger.error(f"Failed to get rolling summary: {e}")
            return ''

    async def get_last_interaction_time(
        self,
        tenant_id: str,
        session_id: str
    ) -> Optional[datetime]:
        """Get timestamp of last user message in buffer"""
        try:
            query = """
                SELECT messages FROM session_buffer
                WHERE tenant_id = $1 AND session_id = $2
            """
            result = await self.db.fetchone(query, tenant_id, session_id)

            if not result:
                return None

            messages = result.get('messages', [])
            if not isinstance(messages, list) or not messages:
                return None

            for message in reversed(messages):
                if message.get("role") == "user":
                    timestamp_str = message.get('timestamp')
                    if timestamp_str:
                        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

            return None

        except Exception as e:
            logger.error(f"Failed to get last interaction time: {e}")
            return None

    async def get_latest_session_episode_text(
        self,
        tenant_id: str,
        user_id: str
    ) -> Optional[str]:
        """Return latest closed session summary text for episode bridge."""
        try:
            row = await self.db.fetchone(
                """
                SELECT session_state->>'session_episode_text' AS episode_text
                FROM session_buffer
                WHERE tenant_id = $1 AND user_id = $2 AND closed_at IS NOT NULL
                ORDER BY closed_at DESC
                LIMIT 1
                """,
                tenant_id,
                user_id
            )
            if row:
                return row.get("episode_text")
            return None
        except Exception as e:
            logger.error(f"Failed to get latest session episode text: {e}")
            return None

    async def close_session(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        graphiti_client,
        persona_id: Optional[str] = None
    ) -> bool:
        """
        Close session in enqueue-only mode.
        Canonical extraction path is /session/ingest -> outbox drain.
        """
        try:
            buffer_row = await self.db.fetchone(
                """
                SELECT messages, created_at
                FROM session_buffer
                WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
                """,
                tenant_id,
                session_id,
                user_id
            )
            buffer_messages = (buffer_row or {}).get("messages") or []
            if not isinstance(buffer_messages, list):
                buffer_messages = []

            transcript_row = await self.db.fetchone(
                """
                SELECT messages
                FROM session_transcript
                WHERE tenant_id = $1 AND session_id = $2
                """,
                tenant_id,
                session_id
            )
            transcript_messages = (transcript_row or {}).get("messages") or []
            if isinstance(transcript_messages, str):
                try:
                    transcript_messages = json.loads(transcript_messages)
                except Exception:
                    transcript_messages = []
            if not isinstance(transcript_messages, list):
                transcript_messages = []

            full_messages: List[Dict[str, Any]] = []
            full_messages.extend(transcript_messages)
            full_messages.extend(buffer_messages)

            has_ingest_job = await self._has_session_ingest_job(
                tenant_id=tenant_id,
                user_id=user_id,
                session_id=session_id
            )

            if not has_ingest_job and full_messages:
                ts_values = [
                    self._parse_ts((m or {}).get("timestamp"))
                    for m in full_messages
                    if isinstance(m, dict)
                ]
                ts_values = [t for t in ts_values if t is not None]
                started_dt = min(ts_values) if ts_values else (buffer_row or {}).get("created_at")
                ended_dt = max(ts_values) if ts_values else datetime.utcnow()
                await self.enqueue_session_ingest(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    messages=full_messages,
                    started_at=started_dt.isoformat() if started_dt else None,
                    ended_at=ended_dt.isoformat() if ended_dt else None,
                )
                logger.info(
                    "session close enqueued ingest tenant=%s user=%s session=%s messages=%s",
                    tenant_id,
                    user_id,
                    session_id,
                    len(full_messages),
                )
            elif has_ingest_job:
                logger.info(
                    "session close detected existing ingest job tenant=%s user=%s session=%s",
                    tenant_id,
                    user_id,
                    session_id,
                )
            else:
                logger.info(
                    "session close found no transcript content tenant=%s user=%s session=%s",
                    tenant_id,
                    user_id,
                    session_id,
                )

            query = """
                UPDATE session_buffer
                SET closed_at = COALESCE(closed_at, NOW()),
                    messages = $1,
                    updated_at = NOW()
                WHERE tenant_id = $2 AND session_id = $3 AND user_id = $4
            """
            updated = await self.db.execute(
                query,
                [],
                tenant_id,
                session_id,
                user_id
            )

            if updated == "UPDATE 0":
                logger.info(
                    "session close no session_buffer row to mark closed tenant=%s user=%s session=%s",
                    tenant_id,
                    user_id,
                    session_id,
                )
            else:
                logger.info(f"Closed session {session_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to close session: {e}")
            return False

    async def _claim_idle_sessions(
        self,
        idle_minutes: int,
        limit: int
    ) -> List[Dict[str, Any]]:
        pool = await self.db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    WITH candidates AS (
                        SELECT ctid, tenant_id, session_id, user_id
                        FROM session_buffer
                        WHERE closed_at IS NULL
                          AND (session_state->>'closing') IS NULL
                          AND (
                            CASE
                              WHEN session_state ? 'last_user_message_ts'
                                THEN (session_state->>'last_user_message_ts')::timestamptz
                              ELSE updated_at
                            END
                          ) < NOW() - ($1::int * INTERVAL '1 minute')
                        ORDER BY updated_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT $2
                    )
                    UPDATE session_buffer
                    SET session_state = jsonb_set(
                        COALESCE(session_state, '{}'::jsonb),
                        '{closing}',
                        'true'::jsonb,
                        true
                    ),
                        updated_at = NOW()
                    WHERE ctid IN (SELECT ctid FROM candidates)
                    RETURNING tenant_id, session_id, user_id
                    """,
                    idle_minutes,
                    limit
                )
                return [dict(r) for r in rows]

    async def _send_raw_transcript_episode(
        self,
        tenant_id: str,
        session_id: str,
        user_id: str,
        graphiti_client,
        started_at: Optional[datetime] = None,
        ended_at: Optional[datetime] = None
    ) -> Optional[str]:
        try:
            buffer = await self.get_or_create_buffer(tenant_id, session_id, user_id)
            full_messages = await self._get_full_transcript_messages(
                tenant_id=tenant_id,
                session_id=session_id,
                user_id=user_id,
                buffer=buffer
            )

            if not full_messages:
                return None

            ended_at = ended_at or datetime.utcnow()
            raw_text = "\n".join(
                [f"{m.get('role')}: {m.get('text')}" for m in full_messages]
            )
            raw_episode_name = f"session_raw_{session_id}_{int(ended_at.timestamp())}"
            result = await graphiti_client.add_episode(
                tenant_id=tenant_id,
                user_id=user_id,
                text=raw_text,
                timestamp=ended_at,
                role=None,
                episode_name=raw_episode_name,
                metadata={
                    "session_id": session_id,
                    "started_at": started_at.isoformat() if started_at else None,
                    "ended_at": ended_at.isoformat(),
                    "episode_type": "session_raw"
                }
            )
            if isinstance(result, dict):
                return result.get("episode_uuid")
            return None
        except Exception as e:
            logger.error(f"Failed to send raw transcript episode: {e}")
            return None

    async def close_idle_sessions_once(
        self,
        graphiti_client,
        idle_minutes: int,
        limit: int
    ) -> int:
        claimed = await self._claim_idle_sessions(idle_minutes, limit)
        if not claimed:
            return 0

        closed = 0
        for row in claimed:
            ok = await self.close_session(
                tenant_id=row["tenant_id"],
                session_id=row["session_id"],
                user_id=row["user_id"],
                graphiti_client=graphiti_client,
                persona_id=None
            )
            if ok:
                closed += 1
        return closed

    async def should_close_session(
        self,
        tenant_id: str,
        session_id: str,
        current_time: datetime
    ) -> bool:
        """Check if session should be closed based on 30 min gap"""
        try:
            last_interaction = await self.get_last_interaction_time(tenant_id, session_id)

            if not last_interaction:
                return False

            gap_minutes = (current_time - last_interaction).total_seconds() / 60
            threshold = self.settings.session_close_gap_minutes

            should_close = gap_minutes > threshold

            if should_close:
                logger.info(
                    f"Session {session_id} should close: "
                    f"gap={gap_minutes:.1f}min > threshold={threshold}min"
                )

            return should_close

        except Exception as e:
            logger.error(f"Failed to check if session should close: {e}")
            return False


# Module-level singleton
_manager: Optional[SessionManager] = None
_post_ingest_hook_executor: Optional[Callable[[str, Dict[str, Any]], Awaitable[bool]]] = None


def init_session_manager(db: Database):
    """Initialize the session manager"""
    global _manager
    _manager = SessionManager(db)


def set_post_ingest_hook_executor(
    executor: Optional[Callable[[str, Dict[str, Any]], Awaitable[bool]]]
) -> None:
    global _post_ingest_hook_executor
    _post_ingest_hook_executor = executor


async def idle_close_loop(
    graphiti_client,
    interval_seconds: int,
    idle_minutes: int,
    batch_size: int
) -> None:
    """Periodic idle session closer. Best-effort and bounded per run."""
    while True:
        try:
            if _manager is None:
                await asyncio.sleep(interval_seconds)
                continue
            await _manager.close_idle_sessions_once(
                graphiti_client=graphiti_client,
                idle_minutes=idle_minutes,
                limit=batch_size
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Idle close loop error: {e}")
        await asyncio.sleep(interval_seconds)


async def drain_loop(
    graphiti_client,
    interval_seconds: int,
    limit: int,
    budget_seconds: float,
    per_row_timeout_seconds: float
) -> None:
    """Periodic outbox drain. Best-effort and bounded per run."""
    while True:
        try:
            if _manager is None:
                await asyncio.sleep(interval_seconds)
                continue
            await _manager.drain_outbox(
                graphiti_client=graphiti_client,
                limit=limit,
                tenant_id=None,
                budget_seconds=budget_seconds,
                per_row_timeout_seconds=per_row_timeout_seconds
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Outbox drain loop error: {e}")
        await asyncio.sleep(interval_seconds)


async def get_or_create_buffer(
    tenant_id: str,
    session_id: str,
    user_id: str
) -> Dict[str, Any]:
    """Get or create session buffer"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_or_create_buffer(tenant_id, session_id, user_id)


async def add_turn(
    tenant_id: str,
    session_id: str,
    user_id: str,
    role: str,
    text: str,
    timestamp: str,
    source_turn_id: Optional[str] = None,
    v2_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Add turn to buffer, returns oldest_turn if evicted"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.add_turn(
        tenant_id,
        session_id,
        user_id,
        role,
        text,
        timestamp,
        source_turn_id=source_turn_id,
        v2_metadata=v2_metadata,
    )


async def enqueue_session_ingest(
    tenant_id: str,
    session_id: str,
    user_id: str,
    messages: List[Dict[str, Any]],
    started_at: Optional[str],
    ended_at: Optional[str]
) -> None:
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    await _manager.enqueue_session_ingest(
        tenant_id=tenant_id,
        session_id=session_id,
        user_id=user_id,
        messages=messages,
        started_at=started_at,
        ended_at=ended_at,
    )


async def janitor_process(
    tenant_id: str,
    session_id: str,
    user_id: str,
    graphiti_client
) -> None:
    """Run janitor process in background"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    await _manager.janitor_process(tenant_id, session_id, user_id, graphiti_client)


async def get_outbox_count(
    tenant_id: str,
    session_id: str,
    user_id: str
) -> int:
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_outbox_count(tenant_id, session_id, user_id)


async def get_last_janitor_run_at(
    tenant_id: str,
    session_id: str
) -> Optional[datetime]:
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_last_janitor_run_at(tenant_id, session_id)


async def set_last_janitor_run_at(
    tenant_id: str,
    session_id: str,
    ts: datetime
) -> None:
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    await _manager.set_last_janitor_run_at(tenant_id, session_id, ts)


async def drain_outbox(
    graphiti_client,
    limit: int = 200,
    tenant_id: Optional[str] = None,
    job_types: Optional[List[str]] = None,
    budget_seconds: float = 2.0,
    per_row_timeout_seconds: float = 8.0
) -> Dict[str, int]:
    """Drain pending outbox rows independently of /ingest."""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.drain_outbox(
        graphiti_client,
        limit=limit,
        tenant_id=tenant_id,
        job_types=job_types,
        budget_seconds=budget_seconds,
        per_row_timeout_seconds=per_row_timeout_seconds
    )


async def get_working_memory(tenant_id: str, session_id: str) -> List[Message]:
    """Get working memory"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_working_memory(tenant_id, session_id)


async def get_rolling_summary(tenant_id: str, session_id: str) -> str:
    """Get rolling summary"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_rolling_summary(tenant_id, session_id)


async def get_last_interaction_time(tenant_id: str, session_id: str) -> Optional[datetime]:
    """Get last interaction time"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_last_interaction_time(tenant_id, session_id)


async def get_latest_session_episode_text(tenant_id: str, user_id: str) -> Optional[str]:
    """Get latest closed session summary text"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.get_latest_session_episode_text(tenant_id, user_id)


async def send_raw_transcript_episode(
    tenant_id: str,
    session_id: str,
    user_id: str,
    graphiti_client
) -> None:
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    await _manager._send_raw_transcript_episode(
        tenant_id=tenant_id,
        session_id=session_id,
        user_id=user_id,
        graphiti_client=graphiti_client
    )


async def should_close_session(
    tenant_id: str,
    session_id: str,
    current_time: datetime
) -> bool:
    """Check if session should close"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.should_close_session(tenant_id, session_id, current_time)


async def close_session(
    tenant_id: str,
    session_id: str,
    user_id: str,
    graphiti_client,
    persona_id: Optional[str] = None
) -> bool:
    """Close session"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.close_session(
        tenant_id,
        session_id,
        user_id,
        graphiti_client,
        persona_id=persona_id
    )


async def summarize_session_messages(
    messages: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Summarize a full session transcript into summary + bridge."""
    mgr = _manager
    if mgr is None:
        mgr = SessionManager(Database())

    recaps = await mgr.summarize_session_messages_with_quality(messages or [])
    summary_text = (recaps.get("summary_text") or "").strip()
    bridge_text = (recaps.get("bridge_text") or "").strip()
    if not summary_text:
        logger.warning("Session recap returned empty summary (bridge_len=%s)", len(bridge_text))
    return {
        "summary_text": summary_text,
        "bridge_text": bridge_text,
        "summary_facts": recaps.get("summary_facts"),
        "tone": recaps.get("tone"),
        "moment": recaps.get("moment"),
        "decisions": recaps.get("decisions") or [],
        "unresolved": recaps.get("unresolved") or [],
        "index_text": recaps.get("index_text") or "",
        "salience": recaps.get("salience") or "low",
        "summary_quality_tier": recaps.get("summary_quality_tier", "deterministic_fallback"),
        "summary_source": recaps.get("summary_source", "full_transcript")
    }
