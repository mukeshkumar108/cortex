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

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import hashlib
import inspect
import json
import logging
import asyncio
from .db import Database
from .models import Message
from .config import get_settings
from .openrouter_client import get_llm_client

logger = logging.getLogger(__name__)

# Constants
MAX_BUFFER_SIZE = 12
MAX_OUTBOX_ATTEMPTS = 5
OUTBOX_BASE_BACKOFF_SECONDS = 5
OUTBOX_MAX_BACKOFF_SECONDS = 300
OUTBOX_CLAIM_HOLD_SECONDS = 30


class SessionManager:
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

    async def _summarize_session_close(self, summary_input: str) -> str:
        prompt = (
            "Write a short session summary (2-4 sentences). "
            "Preserve key facts, commitments, and narrative continuity.\n\n"
            f"{summary_input}\n\nSummary:"
        )
        response = await self.llm_client._call_llm(
            prompt=prompt,
            max_tokens=200,
            temperature=0.2,
            task="session_episode"
        )
        if response:
            return response.strip()
        return summary_input[:500]

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
            await self._send_raw_transcript_episode(
                tenant_id=tenant_id,
                session_id=session_id,
                user_id=user_id,
                graphiti_client=graphiti_client,
                started_at=started_at,
                ended_at=ended_at
            )

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
    ) -> None:
        try:
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

            buffer = await self.get_or_create_buffer(tenant_id, session_id, user_id)
            full_messages = []
            if isinstance(transcript_messages, list):
                full_messages.extend(transcript_messages)
            if isinstance(buffer.get("messages"), list):
                full_messages.extend(buffer["messages"])

            if not full_messages:
                return

            ended_at = ended_at or datetime.utcnow()
            raw_text = "\n".join(
                [f"{m.get('role')}: {m.get('text')}" for m in full_messages]
            )
            raw_episode_name = f"session_raw_{session_id}_{int(ended_at.timestamp())}"
            await graphiti_client.add_episode(
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
        except Exception as e:
            logger.error(f"Failed to send raw transcript episode: {e}")

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
