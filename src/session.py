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

from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import hashlib
import inspect
import json
import logging
import asyncio
import re
from .db import Database
from .models import Message
from .config import get_settings
from .openrouter_client import get_llm_client
from . import loops

logger = logging.getLogger(__name__)

# Constants
MAX_BUFFER_SIZE = 12
MAX_OUTBOX_ATTEMPTS = 5
OUTBOX_BASE_BACKOFF_SECONDS = 5
OUTBOX_MAX_BACKOFF_SECONDS = 300
OUTBOX_CLAIM_HOLD_SECONDS = 30
DEFAULT_LOOP_PERSONA_ID = "default"


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
        timestamp: str
    ) -> Optional[Dict[str, Any]]:
        """
        Add a turn to the buffer. Returns oldest_turn if buffer exceeds MAX_BUFFER_SIZE.

        The caller should trigger the janitor process with the returned oldest_turn.
        """
        try:
            pool = await self.db.get_pool()
            new_turn = {
                "role": role,
                "text": text,
                "timestamp": timestamp
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

                    last_user_ts = timestamp if role == "user" else None
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
        """
        return int(await self.db.fetchval(query, tenant_id, session_id, user_id) or 0)

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
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ORDER BY id ASC
            LIMIT $4
        """
        return await self.db.fetch(query, tenant_id, session_id, user_id, limit)

    async def _claim_pending_outbox(
        self,
        limit: int,
        tenant_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        pool = await self.db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                if tenant_id:
                    rows = await conn.fetch(
                        """
                        SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at
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
                    rows = await conn.fetch(
                        """
                        SELECT id, tenant_id, user_id, session_id, role, text, ts, attempts, folded_at
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

    async def drain_outbox(
        self,
        graphiti_client,
        limit: int = 200,
        tenant_id: Optional[str] = None,
        budget_seconds: float = 2.0,
        per_row_timeout_seconds: float = 8.0
    ) -> Dict[str, int]:
        start = datetime.utcnow()
        claimed = await self._claim_pending_outbox(limit=limit, tenant_id=tenant_id)
        counts = {"claimed": len(claimed), "sent": 0, "failed": 0, "pending": 0}
        if not claimed:
            return counts

        summary_cache: Dict[tuple, str] = {}

        for row in claimed:
            elapsed = (datetime.utcnow() - start).total_seconds()
            if elapsed >= budget_seconds:
                break
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
                    await asyncio.wait_for(
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
        payload = f"{tenant_id}|{user_id}|{session_id}|{role}|{ts.isoformat()}|{text}"
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return f"episode_{digest}"

    def _build_session_summary_name(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        ts: datetime,
        summary: str
    ) -> str:
        payload = f"{tenant_id}|{user_id}|{session_id}|session_summary|{ts.isoformat()}|{summary}"
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
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
            "  TONE: 1 sentence (emotional state of the user during this session)\n"
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
        Close session: enqueue remaining raw turns to outbox and mark closed.
        Graphiti-native: send raw transcript as a single episode; no local semantic summary.
        """
        try:
            buffer = await self.get_or_create_buffer(tenant_id, session_id, user_id)

            # 1. Enqueue remaining raw turns to outbox
            for turn in buffer["messages"]:
                await self._insert_outbox(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    turn=turn
                )

            logger.info(f"Enqueued {len(buffer['messages'])} remaining turns on session close")

            # 2. Send raw transcript episode to Graphiti (best-effort)
            started_at = buffer.get("created_at")
            ended_at = datetime.utcnow()
            episode_uuid = await self._send_raw_transcript_episode(
                tenant_id=tenant_id,
                session_id=session_id,
                user_id=user_id,
                graphiti_client=graphiti_client,
                started_at=started_at,
                ended_at=ended_at
            )

            # 2b. Extract loops from full session transcript (best-effort)
            try:
                # Loops are user-scoped; store under canonical default persona key.
                effective_persona_id = DEFAULT_LOOP_PERSONA_ID
                full_messages = await self._get_full_transcript_messages(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    user_id=user_id,
                    buffer=buffer
                )
                if full_messages:
                    last_user_text = next(
                        (m.get("text") for m in reversed(full_messages) if m.get("role") == "user" and m.get("text")),
                        None
                    )
                    if not last_user_text:
                        last_user_text = full_messages[-1].get("text") if full_messages[-1].get("text") else ""

                    ts_values = [self._parse_ts(m.get("timestamp")) for m in full_messages]
                    ts_values = [t for t in ts_values if t]
                    start_ts = min(ts_values) if ts_values else None
                    end_ts = max(ts_values) if ts_values else ended_at

                    provenance = {
                        "session_id": session_id,
                        "start_ts": start_ts.isoformat() if start_ts else None,
                        "end_ts": end_ts.isoformat() if end_ts else None
                    }

                    if last_user_text:
                        await loops.extract_and_create_loops(
                            tenant_id=tenant_id,
                            user_id=user_id,
                            persona_id=effective_persona_id,
                            user_text=last_user_text,
                            recent_turns=full_messages,
                            source_turn_ts=end_ts or datetime.utcnow(),
                            session_id=session_id,
                            provenance=provenance
                        )
            except Exception as e:
                logger.error(f"Loop extraction on session close failed: {e}")

            # 2c. Create SessionSummary node in Graphiti (best-effort)
            try:
                full_messages = await self._get_full_transcript_messages(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    user_id=user_id,
                    buffer=buffer
                )
                recaps = await self.summarize_session_messages_with_quality(
                    messages=full_messages or [],
                    reference_time=ended_at or datetime.utcnow()
                )
                summary_text = (recaps.get("summary_text") or "").strip()
                bridge_text = (recaps.get("bridge_text") or "").strip()
                if summary_text:
                    await graphiti_client.add_session_summary(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        session_id=session_id,
                        summary_text=summary_text,
                        bridge_text=bridge_text,
                        reference_time=ended_at or datetime.utcnow(),
                        episode_uuid=episode_uuid,
                        extra_attributes={
                            "summary_quality_tier": recaps.get("summary_quality_tier"),
                            "summary_source": recaps.get("summary_source"),
                            "summary_facts": recaps.get("summary_facts"),
                            "tone": recaps.get("tone"),
                            "moment": recaps.get("moment"),
                            "decisions": recaps.get("decisions") or [],
                            "unresolved": recaps.get("unresolved") or [],
                            "index_text": recaps.get("index_text") or "",
                            "salience": recaps.get("salience") or "low",
                        },
                        replace_existing_session=True
                    )
            except Exception as e:
                logger.error(f"Session summary creation failed: {e}")

            # 3. Mark session closed and clear messages
            query = """
                UPDATE session_buffer
                SET closed_at = NOW(),
                    messages = $1,
                    updated_at = NOW()
                WHERE tenant_id = $2 AND session_id = $3
            """
            await self.db.execute(
                query,
                [],
                tenant_id,
                session_id
            )

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


def init_session_manager(db: Database):
    """Initialize the session manager"""
    global _manager
    _manager = SessionManager(db)


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
    timestamp: str
) -> Optional[Dict[str, Any]]:
    """Add turn to buffer, returns oldest_turn if evicted"""
    if _manager is None:
        raise RuntimeError("SessionManager not initialized")
    return await _manager.add_turn(tenant_id, session_id, user_id, role, text, timestamp)


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
