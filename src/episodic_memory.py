from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional

try:
    import openai
except Exception:  # pragma: no cover - optional dependency fallback
    openai = None

from .config import get_settings
from .canonicalization import normalize_text as canonicalize_text
from .db import Database


def _normalize_text(value: Any) -> str:
    return canonicalize_text(value, casefold=False)


def _shorten_text(value: str, max_chars: int) -> str:
    text = _normalize_text(value)
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _to_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = _normalize_text(value)
        if not raw:
            dt = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        else:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except Exception:
                dt = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc)


def _format_embedding(vector: List[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vector) + "]"


def build_transcript_windows(
    messages: List[Dict[str, Any]],
    *,
    window_size: int,
    stride: int,
    max_windows: int,
    max_chars: int,
    user_turn_centric: bool,
    include_assistant_turns: bool,
    assistant_char_weight: float,
) -> List[Dict[str, Any]]:
    if not isinstance(messages, list) or not messages:
        return []
    rows: List[Dict[str, Any]] = []

    clean_messages: List[Dict[str, Any]] = []
    for idx, row in enumerate(messages):
        if not isinstance(row, dict):
            continue
        role = _normalize_text(row.get("role")).lower()
        text = _normalize_text(row.get("text"))
        if role not in {"user", "assistant"} or not text:
            continue
        if role == "assistant" and not include_assistant_turns:
            continue
        ts = _to_utc(row.get("timestamp"))
        clean_messages.append(
            {
                "idx": idx,
                "role": role,
                "text": text,
                "timestamp": ts,
            }
        )

    if not clean_messages:
        return []

    safe_window = max(2, int(window_size or 8))
    safe_stride = max(1, int(stride or 4))
    safe_max_windows = max(1, min(int(max_windows or 8), 24))
    safe_max_chars = max(200, int(max_chars or 1400))
    safe_assistant_weight = min(1.0, max(0.15, float(assistant_char_weight or 1.0)))

    if user_turn_centric:
        user_message_positions = [idx for idx, item in enumerate(clean_messages) if item.get("role") == "user"]
        starts = [user_message_positions[i] for i in range(0, len(user_message_positions), safe_stride)]
        if not starts:
            starts = list(range(0, len(clean_messages), safe_stride))
    else:
        starts = list(range(0, len(clean_messages), safe_stride))

    for start in starts:
        if len(rows) >= safe_max_windows:
            break
        chunk = clean_messages[start : start + safe_window]
        if not chunk:
            continue
        if user_turn_centric and not any(item.get("role") == "user" for item in chunk):
            continue
        lines = []
        user_turn_count = 0
        assistant_turn_count = 0
        user_char_count = 0
        assistant_char_count = 0
        for item in chunk:
            prefix = "User" if item["role"] == "user" else "Assistant"
            text = _normalize_text(item["text"])
            if item["role"] == "assistant" and safe_assistant_weight < 0.999:
                max_len = max(60, int(len(text) * safe_assistant_weight))
                text = _shorten_text(text, max_len)
            lines.append(f"{prefix}: {text}")
            if item["role"] == "user":
                user_turn_count += 1
                user_char_count += len(text)
            else:
                assistant_turn_count += 1
                assistant_char_count += len(text)
        text = "\n".join(lines).strip()
        if not text:
            continue
        if len(text) > safe_max_chars:
            text = text[: max(0, safe_max_chars - 3)].rstrip() + "..."
        rows.append(
            {
                "window_index": len(rows),
                "unit_text": text,
                "start_idx": chunk[0]["idx"],
                "end_idx": chunk[-1]["idx"],
                "start_ts": chunk[0]["timestamp"],
                "end_ts": chunk[-1]["timestamp"],
                "user_turn_count": user_turn_count,
                "assistant_turn_count": assistant_turn_count,
                "user_char_ratio": round(
                    float(user_char_count) / float(max(1, user_char_count + assistant_char_count)),
                    4,
                ),
            }
        )
    return rows


async def embed_texts(texts: List[str], model: str) -> Optional[List[List[float]]]:
    clean = [_normalize_text(t) for t in (texts or []) if _normalize_text(t)]
    if not clean:
        return []
    if openai is None:
        return None

    def _blocking() -> List[List[float]]:
        settings = get_settings()
        client = openai.OpenAI(api_key=settings.openai_api_key, timeout=10.0)
        response = client.embeddings.create(model=model, input=clean)
        return [list(row.embedding) for row in response.data]

    try:
        return await asyncio.to_thread(_blocking)
    except Exception:
        return None


async def upsert_session_episode_embeddings(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    episode_uuid: Optional[str],
    reference_time: datetime,
    messages: List[Dict[str, Any]],
    model: str,
    window_size: int,
    stride: int,
    max_windows: int,
    max_chars: int,
    user_turn_centric: bool,
    include_assistant_turns: bool,
    assistant_char_weight: float,
) -> Dict[str, Any]:
    windows = build_transcript_windows(
        messages=messages,
        window_size=window_size,
        stride=stride,
        max_windows=max_windows,
        max_chars=max_chars,
        user_turn_centric=user_turn_centric,
        include_assistant_turns=include_assistant_turns,
        assistant_char_weight=assistant_char_weight,
    )
    if not windows:
        return {"indexed": 0, "attempted": 0, "embedding_model": model}
    vectors = await embed_texts([row["unit_text"] for row in windows], model=model)
    if not vectors or len(vectors) != len(windows):
        return {"indexed": 0, "attempted": len(windows), "embedding_model": model, "error": "embedding_unavailable"}

    ref = _to_utc(reference_time)
    indexed = 0
    for row, vector in zip(windows, vectors):
        metadata = {
            "start_idx": row.get("start_idx"),
            "end_idx": row.get("end_idx"),
            "start_ts": row.get("start_ts").isoformat() if isinstance(row.get("start_ts"), datetime) else None,
            "end_ts": row.get("end_ts").isoformat() if isinstance(row.get("end_ts"), datetime) else None,
            "user_turn_count": int(row.get("user_turn_count") or 0),
            "assistant_turn_count": int(row.get("assistant_turn_count") or 0),
            "user_char_ratio": float(row.get("user_char_ratio") or 0.0),
        }
        await db.execute(
            """
            INSERT INTO episodic_memory_embeddings (
                tenant_id, user_id, session_id, episode_uuid, unit_kind,
                window_index, unit_text, reference_time, embedding_model,
                embedding, metadata, updated_at
            )
            VALUES ($1, $2, $3, $4, 'transcript_window', $5, $6, $7, $8, $9::vector, $10::jsonb, NOW())
            ON CONFLICT (tenant_id, user_id, session_id, window_index)
            DO UPDATE SET
                episode_uuid = EXCLUDED.episode_uuid,
                unit_kind = EXCLUDED.unit_kind,
                unit_text = EXCLUDED.unit_text,
                reference_time = EXCLUDED.reference_time,
                embedding_model = EXCLUDED.embedding_model,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
            """,
            tenant_id,
            user_id,
            session_id,
            _normalize_text(episode_uuid) or None,
            int(row.get("window_index") or 0),
            _normalize_text(row.get("unit_text")),
            ref,
            _normalize_text(model) or "text-embedding-3-small",
            _format_embedding(vector),
            metadata,
        )
        indexed += 1

    return {"indexed": indexed, "attempted": len(windows), "embedding_model": model}


async def search_episode_embedding_candidates(
    *,
    db: Database,
    tenant_scope: List[str],
    user_id: str,
    query: str,
    model: str,
    limit: int,
    reference_time: Optional[datetime],
) -> List[Dict[str, Any]]:
    scope = [_normalize_text(t) for t in (tenant_scope or []) if _normalize_text(t)]
    if not scope or not _normalize_text(query):
        return []

    vectors = await embed_texts([query], model=model)
    if not vectors:
        return []
    query_vec = _format_embedding(vectors[0])
    safe_limit = max(1, min(int(limit or 20), 100))
    ref = _to_utc(reference_time) if reference_time else None
    min_ref = (ref - timedelta(days=365)) if ref is not None else None

    rows = await db.fetch(
        """
        SELECT
            tenant_id,
            user_id,
            session_id,
            episode_uuid,
            window_index,
            unit_text,
            reference_time,
            embedding_model,
            metadata,
            1 - (embedding <=> $3::vector) AS embedding_similarity
        FROM episodic_memory_embeddings
        WHERE tenant_id = ANY($1::text[])
          AND user_id = $2
          AND ($4::timestamptz IS NULL OR reference_time <= $4::timestamptz)
          AND ($5::timestamptz IS NULL OR reference_time >= $5::timestamptz)
        ORDER BY embedding <=> $3::vector ASC, reference_time DESC
        LIMIT $6
        """,
        scope,
        user_id,
        query_vec,
        ref,
        min_ref,
        safe_limit,
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "tenant_id": _normalize_text(row.get("tenant_id")),
                "session_id": _normalize_text(row.get("session_id")),
                "episode_uuid": _normalize_text(row.get("episode_uuid")),
                "window_index": int(row.get("window_index") or 0),
                "unit_text": _normalize_text(row.get("unit_text")),
                "reference_time": row.get("reference_time"),
                "embedding_model": _normalize_text(row.get("embedding_model")),
                "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
                "embedding_similarity": float(row.get("embedding_similarity") or 0.0),
            }
        )
    return out


async def get_user_episodic_embedding_stats(
    *,
    db: Database,
    tenant_scope: List[str],
    user_id: str,
    reference_time: Optional[datetime],
) -> Dict[str, int]:
    scope = [_normalize_text(t) for t in (tenant_scope or []) if _normalize_text(t)]
    if not scope or not _normalize_text(user_id):
        return {"rows": 0, "sessions": 0}
    ref = _to_utc(reference_time) if reference_time else None
    min_ref = (ref - timedelta(days=365)) if ref is not None else None
    row = await db.fetchone(
        """
        SELECT
            COUNT(*)::int AS rows,
            COUNT(DISTINCT session_id)::int AS sessions
        FROM episodic_memory_embeddings
        WHERE tenant_id = ANY($1::text[])
          AND user_id = $2
          AND ($3::timestamptz IS NULL OR reference_time <= $3::timestamptz)
          AND ($4::timestamptz IS NULL OR reference_time >= $4::timestamptz)
        """,
        scope,
        user_id,
        ref,
        min_ref,
    )
    if not isinstance(row, dict):
        return {"rows": 0, "sessions": 0}
    return {
        "rows": int(row.get("rows") or 0),
        "sessions": int(row.get("sessions") or 0),
    }
