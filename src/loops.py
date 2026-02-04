"""
Loops Manager - Pure LLM Extraction

Loops are commitments, habits, threads, and frictions extracted from conversation.

No regex gates - pure LLM understanding of context.
"""

from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime
import logging
import re
import json
import openai
from .db import Database
from .models import Loop
from .config import get_settings
from .openrouter_client import get_llm_client

logger = logging.getLogger(__name__)

LOOP_TYPES = {"commitment", "decision", "friction", "habit", "thread"}
LOOP_STATUSES = {"active", "completed", "dropped", "snoozed"}
TIME_HORIZONS = {"today", "this_week", "ongoing"}


class LoopManager:
    def __init__(self, db: Database):
        self.db = db
        self.settings = get_settings()
        self.openai_client = openai.OpenAI(api_key=self.settings.openai_api_key)
        self.llm_client = get_llm_client()

    async def create_loop(
        self,
        tenant_id: str,
        user_id: str,
        persona_id: str,
        loop_type: str,
        text: str,
        confidence: float,
        salience: int,
        time_horizon: str,
        source_turn_ts: datetime,
        due_date: Optional[str] = None,
        entity_refs: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Create a new loop"""
        try:
            loop_id = uuid4()

            # Generate embedding for the loop text
            embedding = await self._generate_embedding(text)
            embedding_value = self._format_embedding(embedding)
            parsed_due_date = None
            if isinstance(due_date, str) and due_date:
                try:
                    parsed_due_date = datetime.fromisoformat(due_date).date()
                except ValueError:
                    parsed_due_date = None
            elif due_date:
                parsed_due_date = due_date

            query = """
                INSERT INTO loops (
                    id, tenant_id, user_id, persona_id, type, status, text,
                    confidence, salience, time_horizon, source_turn_ts,
                    due_date, entity_refs, tags, embedding, metadata,
                    created_at, updated_at, last_seen_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, 'active', $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14, $15,
                    NOW(), NOW(), NOW()
                )
                RETURNING id
            """

            result = await self.db.fetchval(
                query,
                loop_id,
                tenant_id,
                user_id,
                persona_id,
                loop_type,
                text,
                confidence,
                salience,
                time_horizon,
                source_turn_ts,
                parsed_due_date,
                entity_refs or [],
                tags or [],
                embedding_value,
                metadata or {}
            )

            logger.info(f"Created loop {loop_id} of type {loop_type}")
            return UUID(str(result))

        except Exception as e:
            logger.error(f"Failed to create loop: {e}")
            raise

    async def get_active_loops(
        self,
        tenant_id: str,
        user_id: str,
        persona_id: str,
        limit: int = 5
    ) -> List[Loop]:
        """Get active loops"""
        try:
            query = """
                SELECT id, type, status, text, confidence, salience, time_horizon,
                       source_turn_ts, due_date, entity_refs, tags,
                       created_at, updated_at, last_seen_at, metadata
                FROM loops
                WHERE tenant_id = $1
                    AND user_id = $2
                    AND persona_id = $3
                    AND status = 'active'
                ORDER BY last_seen_at DESC
                LIMIT $4
            """

            rows = await self.db.fetch(query, tenant_id, user_id, persona_id, limit)

            loops = []
            for row in rows:
                loops.append(Loop(
                    id=row['id'],
                    type=row['type'],
                    status=row['status'],
                    text=row['text'],
                    confidence=row.get('confidence'),
                    salience=row.get('salience'),
                    timeHorizon=row.get('time_horizon'),
                    sourceTurnTs=row['source_turn_ts'].isoformat() if row.get('source_turn_ts') else None,
                    dueDate=row['due_date'].isoformat() if row.get('due_date') else None,
                    entityRefs=row.get('entity_refs') or [],
                    tags=row.get('tags') or [],
                    createdAt=row['created_at'].isoformat() if row['created_at'] else None,
                    updatedAt=row['updated_at'].isoformat() if row.get('updated_at') else None,
                    lastSeenAt=row['last_seen_at'].isoformat() if row.get('last_seen_at') else None,
                    metadata=row['metadata']
                ))

            return loops

        except Exception as e:
            logger.error(f"Failed to get active loops: {e}")
            return []

    async def get_active_loops_any(
        self,
        tenant_id: str,
        user_id: str,
        limit: int = 5
    ) -> List[Loop]:
        """Get active loops without persona filter"""
        try:
            query = """
                SELECT id, type, status, text, confidence, salience, time_horizon,
                       source_turn_ts, due_date, entity_refs, tags,
                       created_at, updated_at, last_seen_at, metadata
                FROM loops
                WHERE tenant_id = $1
                    AND user_id = $2
                    AND status = 'active'
                ORDER BY last_seen_at DESC
                LIMIT $3
            """
            rows = await self.db.fetch(query, tenant_id, user_id, limit)
            loops = []
            for row in rows:
                loops.append(Loop(
                    id=row['id'],
                    type=row['type'],
                    status=row['status'],
                    text=row['text'],
                    confidence=row.get('confidence'),
                    salience=row.get('salience'),
                    timeHorizon=row.get('time_horizon'),
                    sourceTurnTs=row['source_turn_ts'].isoformat() if row.get('source_turn_ts') else None,
                    dueDate=row['due_date'].isoformat() if row.get('due_date') else None,
                    entityRefs=row.get('entity_refs') or [],
                    tags=row.get('tags') or [],
                    createdAt=row['created_at'].isoformat() if row['created_at'] else None,
                    updatedAt=row['updated_at'].isoformat() if row.get('updated_at') else None,
                    lastSeenAt=row['last_seen_at'].isoformat() if row.get('last_seen_at') else None,
                    metadata=row['metadata']
                ))
            return loops
        except Exception as e:
            logger.error(f"Failed to get active loops (any persona): {e}")
            return []

    async def get_active_loops_debug(
        self,
        tenant_id: str,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """Get active loops with full metadata for debugging"""
        try:
            query = """
                SELECT id, tenant_id, user_id, persona_id, type, status, text,
                       confidence, salience, time_horizon, source_turn_ts,
                       due_date, entity_refs, tags, metadata,
                       created_at, updated_at, last_seen_at
                FROM loops
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND status = 'active'
                ORDER BY last_seen_at DESC
            """
            rows = await self.db.fetch(query, tenant_id, user_id)
            return rows
        except Exception as e:
            logger.error(f"Failed to get loops debug: {e}")
            return []

    async def mark_completed(self, tenant_id: str, loop_id: UUID, completed_at: datetime) -> None:
        """Mark a loop as completed"""
        try:
            query = """
                UPDATE loops
                SET status = 'completed',
                    completed_at = $1,
                    updated_at = NOW(),
                    last_seen_at = NOW()
                WHERE id = $2 AND tenant_id = $3
            """

            await self.db.execute(query, completed_at, loop_id, tenant_id)
            logger.info(f"Marked loop {loop_id} as completed")

        except Exception as e:
            logger.error(f"Failed to mark loop completed: {e}")
            raise

    async def mark_dropped(self, tenant_id: str, loop_id: UUID) -> None:
        """Mark a loop as dropped"""
        try:
            query = """
                UPDATE loops
                SET status = 'dropped',
                    completed_at = NOW(),
                    updated_at = NOW(),
                    last_seen_at = NOW()
                WHERE id = $1 AND tenant_id = $2
            """
            await self.db.execute(query, loop_id, tenant_id)
            logger.info(f"Marked loop {loop_id} as dropped")
        except Exception as e:
            logger.error(f"Failed to mark loop dropped: {e}")
            raise

    async def check_completion(
        self,
        tenant_id: str,
        user_id: str,
        persona_id: str,
        user_text: str
    ) -> List[UUID]:
        """Deprecated: LLM-first handling is performed in extract_and_create_loops."""
        logger.warning("check_completion is deprecated; use extract_and_create_loops")
        return []

    async def extract_and_create_loops(
        self,
        tenant_id: str,
        user_id: str,
        persona_id: str,
        user_text: str,
        recent_turns: List[Dict[str, Any]],
        source_turn_ts: datetime
    ) -> Dict[str, Any]:
        """
        Extract loops from conversation context using LLM.

        Returns dict with counts of new loops and completions.
        """
        try:
            active_loops = await self.get_active_loops(tenant_id, user_id, persona_id, limit=20)
            recent_context = "\n".join(
                [f"{t['role']}: {t['text']}" for t in recent_turns[-2:]]
            )
            loops_block = [
                {
                    "id": str(loop.id),
                    "type": loop.type,
                    "text": loop.text,
                    "salience": loop.salience
                }
                for loop in active_loops
            ]

            prompt = (
                "You are Synapse Loops v1. Use semantic meaning; be language-agnostic.\n"
                "Primary goal: procedural memory with low noise.\n\n"
                "INPUT:\n"
                f"- current_user_turn: {user_text}\n"
                f"- recent_context (last 1-2 turns):\n{recent_context}\n"
                f"- active_loops (id/type/text/salience): {json.dumps(loops_block)}\n\n"
                "OUTPUT: strict JSON only, no prose.\n"
                "{\n"
                '  "new_loops": [\n'
                '    {"type": "commitment|decision|friction|habit|thread", "text": "...", "confidence": 0-1, '
                '"salience": 1-5, "time_horizon": "today|this_week|ongoing", '
                '"due_date": "YYYY-MM-DD" | null, "entity_refs": ["..."], "tags": ["..."], "reason": "..."}\n'
                "  ],\n"
                '  "reinforced_loops": [ {"loop_id": "...", "confidence": 0-1, "reason": "..."} ],\n'
                '  "completed_loops": [ {"loop_id": "...", "confidence": 0-1, "reason": "...", '
                '"evidence_text": "...", "evidence_type": "explicit|implicit"} ],\n'
                '  "dropped_loops":   [ {"loop_id": "...", "confidence": 0-1, "reason": "..."} ]\n'
                "}\n"
            )

            response = await self.llm_client._call_llm(
                prompt=prompt,
                max_tokens=300,
                temperature=0.1,
                task="loops"
            )

            if not response or not str(response).strip():
                logger.warning("LLM returned empty response for loop extraction")
                return {"new_loops": 0, "completions": 0}

            raw = str(response).strip()
            if raw.startswith("```"):
                raw = raw.strip("`").strip()
            raw = raw.replace("\r", "")
            raw = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
            if not raw.startswith("{"):
                start = raw.find("{")
                end = raw.rfind("}")
                if start != -1 and end != -1 and end > start:
                    raw = raw[start:end + 1]
            try:
                extracted = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse loop extraction JSON: {e}")
                return {"new_loops": 0, "completions": 0}

            if not isinstance(extracted, dict):
                logger.warning("Loop extraction response was not a JSON object")
                return {"new_loops": 0, "completions": 0}

            new_loops = extracted.get("new_loops") or []
            reinforced = extracted.get("reinforced_loops") or []
            completed = extracted.get("completed_loops") or []
            dropped = extracted.get("dropped_loops") or []

            new_loop_count = 0
            seen = set()

            for loop in new_loops:
                if new_loop_count >= 3:
                    break
                if not isinstance(loop, dict):
                    continue

                loop_type = str(loop.get("type", "")).lower().strip()
                text = str(loop.get("text", "")).strip()
                confidence = float(loop.get("confidence", 0))
                salience = int(loop.get("salience", 1))
                time_horizon = str(loop.get("time_horizon", "ongoing")).lower().strip()
                due_date = loop.get("due_date")
                entity_refs = loop.get("entity_refs") or []
                tags = loop.get("tags") or []
                reason = loop.get("reason")

                if loop_type not in LOOP_TYPES:
                    continue
                if not text:
                    continue

                normalized_text = self._normalize_loop_text(text)
                dedupe_key = f"{loop_type}:{normalized_text}"
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                if time_horizon not in TIME_HORIZONS:
                    time_horizon = "ongoing"
                salience = max(1, min(5, salience))
                if loop_type != "commitment":
                    due_date = None

                existing = await self._find_active_loop_by_text(
                    tenant_id, user_id, loop_type, normalized_text
                )
                if existing:
                    await self._bump_loop_salience(UUID(str(existing["id"])))
                    continue

                await self.create_loop(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    persona_id=persona_id,
                    loop_type=loop_type,
                    text=text,
                    confidence=confidence,
                    salience=salience,
                    time_horizon=time_horizon,
                    source_turn_ts=source_turn_ts,
                    due_date=due_date,
                    entity_refs=entity_refs,
                    tags=tags,
                    metadata={"dedupe_key": dedupe_key, "reason": reason}
                )
                new_loop_count += 1

            def _normalize_id(item: Dict[str, Any]) -> Optional[UUID]:
                loop_id = item.get("loop_id")
                if not loop_id:
                    return None
                try:
                    return UUID(str(loop_id))
                except Exception:
                    return None

            for item in reinforced:
                if not isinstance(item, dict):
                    continue
                loop_id = _normalize_id(item)
                if not loop_id:
                    continue
                await self._bump_loop_salience(loop_id)
                await self._update_loop_metadata(loop_id, "reinforced", item.get("reason"), item.get("confidence"))

            loop_map = {str(loop.id): loop for loop in active_loops}

            for item in completed:
                if not isinstance(item, dict):
                    continue
                loop_id = _normalize_id(item)
                if not loop_id:
                    continue
                confidence = float(item.get("confidence", 0.0))
                evidence_type = str(item.get("evidence_type", "implicit")).lower().strip()
                if evidence_type not in {"explicit", "implicit"}:
                    evidence_type = "implicit"
                evidence_text = str(item.get("evidence_text", "")).strip()[:140]

                if evidence_type == "explicit" and confidence >= 0.85:
                    await self.mark_completed(tenant_id, loop_id, datetime.utcnow())
                    await self._update_loop_metadata(
                        loop_id,
                        "completed",
                        item.get("reason"),
                        confidence,
                        evidence_text=evidence_text,
                        evidence_type=evidence_type
                    )
                    continue

                if evidence_type == "implicit" and confidence >= 0.60:
                    loop = loop_map.get(str(loop_id))
                    if loop:
                        question = f"Is this done: {loop.text}?"
                        await self._set_pending_nudge(
                            loop_id=loop_id,
                            question=question,
                            confidence=confidence,
                            evidence_text=evidence_text,
                            evidence_type=evidence_type,
                            reason=item.get("reason")
                        )

            for item in dropped:
                if not isinstance(item, dict):
                    continue
                loop_id = _normalize_id(item)
                if not loop_id:
                    continue
                await self.mark_dropped(tenant_id, loop_id)
                await self._update_loop_metadata(loop_id, "dropped", item.get("reason"), item.get("confidence"))

            return {"new_loops": new_loop_count, "completions": len(completed)}

        except Exception as e:
            logger.error(f"Failed to extract and create loops: {e}")
            return {"new_loops": 0, "completions": 0}

    async def _loop_exists(self, tenant_id: str, user_id: str, dedupe_key: str) -> bool:
        """Check if a loop with this dedupe_key already exists"""
        try:
            query = """
                SELECT 1 FROM loops
                WHERE tenant_id = $1
                    AND user_id = $2
                    AND metadata->>'dedupe_key' = $3
                    AND status = 'active'
                LIMIT 1
            """
            result = await self.db.fetchone(query, tenant_id, user_id, dedupe_key)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to check loop existence: {e}")
            return False

    async def _find_loop_by_dedupe_key(
        self,
        tenant_id: str,
        user_id: str,
        dedupe_key: str
    ) -> Optional[UUID]:
        """Find loop ID by dedupe_key"""
        try:
            query = """
                SELECT id FROM loops
                WHERE tenant_id = $1
                    AND user_id = $2
                    AND metadata->>'dedupe_key' = $3
                    AND status = 'active'
                LIMIT 1
            """
            result = await self.db.fetchval(query, tenant_id, user_id, dedupe_key)
            return UUID(str(result)) if result else None
        except Exception as e:
            logger.error(f"Failed to find loop by dedupe_key: {e}")
            return None

    @staticmethod
    def _normalize_loop_text(text: str) -> str:
        return " ".join(text.strip().lower().split())

    async def _find_active_loop_by_text(
        self,
        tenant_id: str,
        user_id: str,
        loop_type: str,
        normalized_text: str
    ) -> Optional[Dict[str, Any]]:
        try:
            query = """
                SELECT id, salience
                FROM loops
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND type = $3
                  AND status = 'active'
                  AND lower(text) = $4
                LIMIT 1
            """
            row = await self.db.fetchone(query, tenant_id, user_id, loop_type, normalized_text)
            return row
        except Exception as e:
            logger.error(f"Failed to find loop by text: {e}")
            return None

    async def _bump_loop_salience(self, loop_id: UUID) -> None:
        query = """
            UPDATE loops
            SET salience = LEAST(5, COALESCE(salience, 1) + 1),
                last_seen_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
        """
        await self.db.execute(query, loop_id)

    async def _update_loop_metadata(
        self,
        loop_id: UUID,
        action: str,
        reason: Optional[str],
        confidence: Optional[float],
        evidence_text: Optional[str] = None,
        evidence_type: Optional[str] = None
    ) -> None:
        query = """
            UPDATE loops
            SET metadata = jsonb_set(
                jsonb_set(
                    jsonb_set(COALESCE(metadata, '{}'::jsonb), '{last_action}', to_jsonb($2::text), true),
                    '{last_action_reason}', to_jsonb($3::text), true
                ),
                '{last_action_confidence}', to_jsonb($4::float), true
            ),
                updated_at = NOW(),
                last_seen_at = NOW()
            WHERE id = $1
        """
        await self.db.execute(query, loop_id, action, reason or "", confidence or 0.0)
        if evidence_text is not None or evidence_type is not None:
            evidence_query = """
                UPDATE loops
                SET metadata = jsonb_set(
                    jsonb_set(COALESCE(metadata, '{}'::jsonb), '{last_action_evidence_text}', to_jsonb($2::text), true),
                    '{last_action_evidence_type}', to_jsonb($3::text), true
                ),
                    updated_at = NOW(),
                    last_seen_at = NOW()
                WHERE id = $1
            """
            await self.db.execute(
                evidence_query,
                loop_id,
                (evidence_text or "")[:140],
                evidence_type or ""
            )

    async def _set_pending_nudge(
        self,
        loop_id: UUID,
        question: str,
        confidence: float,
        evidence_text: str,
        evidence_type: str,
        reason: Optional[str]
    ) -> None:
        pending = {
            "question": question,
            "confidence": confidence,
            "evidence_text": evidence_text[:140],
            "evidence_type": evidence_type,
            "created_at": datetime.utcnow().isoformat()
        }
        query = """
            UPDATE loops
            SET metadata = jsonb_set(
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(COALESCE(metadata, '{}'::jsonb), '{pending_nudge}', $2::jsonb, true),
                            '{last_action}', to_jsonb('nudge_candidate'::text), true
                        ),
                        '{last_action_reason}', to_jsonb($3::text), true
                    ),
                    '{last_action_confidence}', to_jsonb($4::float), true
                ),
                '{last_action_evidence_text}', to_jsonb($5::text), true
            ),
                updated_at = NOW(),
                last_seen_at = NOW()
            WHERE id = $1
        """
        await self.db.execute(
            query,
            loop_id,
            json.dumps(pending),
            reason or "",
            confidence,
            evidence_text[:140]
        )
        evidence_type_query = """
            UPDATE loops
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{last_action_evidence_type}',
                to_jsonb($2::text),
                true
            ),
                updated_at = NOW(),
                last_seen_at = NOW()
            WHERE id = $1
        """
        await self.db.execute(
            evidence_type_query,
            loop_id,
            evidence_type
        )
    async def _mark_abandoned(self, tenant_id: str, loop_id: UUID) -> None:
        """Mark a loop as abandoned"""
        try:
            query = """
                UPDATE loops
                SET status = 'dropped', completed_at = NOW(),
                    updated_at = NOW(), last_seen_at = NOW()
                WHERE id = $1 AND tenant_id = $2
            """
            await self.db.execute(query, loop_id, tenant_id)
            logger.info(f"Marked loop {loop_id} as abandoned")
        except Exception as e:
            logger.error(f"Failed to mark loop abandoned: {e}")

    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using OpenAI"""
        try:
            response = self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=text
            )
            return response.data[0].embedding

        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    @staticmethod
    def _format_embedding(embedding: Any) -> Any:
        if isinstance(embedding, list):
            return "[" + ",".join(str(x) for x in embedding) + "]"
        return embedding

    async def _check_similarity(
        self,
        tenant_id: str,
        loop_id: UUID,
        user_embedding: List[float]
    ) -> Optional[float]:
        """Check cosine similarity between user text and loop"""
        try:
            query = """
                SELECT 1 - (embedding <=> $1::vector) as similarity
                FROM loops
                WHERE id = $2 AND tenant_id = $3
            """

            result = await self.db.fetchval(query, user_embedding, loop_id, tenant_id)
            return result

        except Exception as e:
            logger.error(f"Failed to check similarity: {e}")
            return None


# Module-level singleton
_manager: Optional[LoopManager] = None


def init_loop_manager(db: Database):
    """Initialize the loop manager"""
    global _manager
    _manager = LoopManager(db)


async def create_loop(
    tenant_id: str,
    user_id: str,
    persona_id: str,
    loop_type: str,
    text: str,
    confidence: float,
    salience: int,
    time_horizon: str,
    source_turn_ts: datetime,
    due_date: Optional[str] = None,
    entity_refs: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> UUID:
    """Create a loop"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.create_loop(
        tenant_id,
        user_id,
        persona_id,
        loop_type,
        text,
        confidence,
        salience,
        time_horizon,
        source_turn_ts,
        due_date,
        entity_refs,
        tags,
        metadata
    )


async def get_active_loops(
    tenant_id: str,
    user_id: str,
    persona_id: str,
    limit: int = 5
) -> List[Loop]:
    """Get active loops"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.get_active_loops(tenant_id, user_id, persona_id, limit)


async def get_active_loops_any(
    tenant_id: str,
    user_id: str,
    limit: int = 5
) -> List[Loop]:
    """Get active loops without persona filter"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.get_active_loops_any(tenant_id, user_id, limit)


async def get_active_loops_debug(
    tenant_id: str,
    user_id: str
) -> List[Dict[str, Any]]:
    """Get active loops with full metadata for debugging"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.get_active_loops_debug(tenant_id, user_id)


async def mark_completed(tenant_id: str, loop_id: UUID, completed_at: datetime) -> None:
    """Mark loop as completed"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    await _manager.mark_completed(tenant_id, loop_id, completed_at)


async def check_completion(
    tenant_id: str,
    user_id: str,
    persona_id: str,
    user_text: str
) -> List[UUID]:
    """Check loop completion"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.check_completion(tenant_id, user_id, persona_id, user_text)


async def extract_and_create_loops(
    tenant_id: str,
    user_id: str,
    persona_id: str,
    user_text: str,
    recent_turns: List[Dict[str, Any]],
    source_turn_ts: datetime
) -> Dict[str, Any]:
    """Extract and create loops from conversation context"""
    if _manager is None:
        raise RuntimeError("LoopManager not initialized")
    return await _manager.extract_and_create_loops(
        tenant_id, user_id, persona_id, user_text, recent_turns, source_turn_ts
    )
