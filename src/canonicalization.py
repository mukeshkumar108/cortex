"""
Synapse v2 canonicalization SDK.

Dependency-light, deterministic normalization and key generation utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import unicodedata
from typing import Any, Dict, Mapping, Optional


CANONICALIZATION_VERSION = "v1"
SLOT_KEY_PREFIX = "csk"
EVENT_KEY_PREFIX = "cek"
TIMESTAMP_STATE_VALID = "valid"
TIMESTAMP_STATE_MISSING = "missing"
TIMESTAMP_STATE_INVALID = "invalid"


@dataclass(frozen=True)
class CanonicalizationConfig:
    version: str = CANONICALIZATION_VERSION


DEFAULT_CONFIG = CanonicalizationConfig()


def normalize_text(value: Any, *, casefold: bool = True) -> str:
    """Normalize user/system text into a deterministic canonical string."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = " ".join(text.strip().split())
    if casefold:
        text = text.casefold()
    return text


def normalize_timestamp(value: Any) -> Optional[str]:
    """
    Normalize a timestamp-like value into UTC ISO-8601 with second precision.
    Returns None when value is empty/invalid.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp_field(value: Any) -> Dict[str, Any]:
    """
    Canonical timestamp field encoding for hashing.
    Distinguishes missing vs invalid vs valid timestamp inputs.
    """
    if value is None:
        return {"state": TIMESTAMP_STATE_MISSING, "value": None}
    if isinstance(value, str) and not value.strip():
        return {"state": TIMESTAMP_STATE_MISSING, "value": None}

    normalized = normalize_timestamp(value)
    if normalized is None:
        return {
            "state": TIMESTAMP_STATE_INVALID,
            "value": normalize_text(value, casefold=False),
        }
    return {"state": TIMESTAMP_STATE_VALID, "value": normalized}


def normalize_subject(subject: Any) -> str:
    """Normalize claim subject identity string."""
    return normalize_text(subject, casefold=True)


def normalize_object(
    obj: Any,
    *,
    predicate: Optional[str] = None,
    expected_type: Optional[str] = None,
) -> Any:
    """
    Normalize object values into deterministic canonical representation.
    Policy hooks are accepted but not enforced in T0.
    """
    del predicate, expected_type
    return _normalize_value(obj)


def generate_claim_slot_key(
    *,
    tenant_id: str,
    subject: Any,
    predicate: str,
    slot_scope: Optional[Mapping[str, Any]] = None,
    config: CanonicalizationConfig = DEFAULT_CONFIG,
) -> str:
    """Generate deterministic claim slot key."""
    payload = {
        "tenant_id": normalize_text(tenant_id, casefold=True),
        "subject": normalize_subject(subject),
        "predicate": normalize_text(predicate, casefold=True),
        "slot_scope": _normalize_mapping(slot_scope or {}),
    }
    digest = _stable_hash(payload, version=config.version)
    return f"{SLOT_KEY_PREFIX}_{config.version}_{digest}"


def generate_claim_event_key(
    *,
    tenant_id: str,
    subject: Any,
    predicate: str,
    obj: Any,
    occurred_at: Any = None,
    event_scope: Optional[Mapping[str, Any]] = None,
    config: CanonicalizationConfig = DEFAULT_CONFIG,
) -> str:
    """Generate deterministic claim event key."""
    payload = {
        "tenant_id": normalize_text(tenant_id, casefold=True),
        "subject": normalize_subject(subject),
        "predicate": normalize_text(predicate, casefold=True),
        "object": normalize_object(obj, predicate=predicate),
        "occurred_at": normalize_timestamp_field(occurred_at),
        "event_scope": _normalize_mapping(event_scope or {}),
    }
    digest = _stable_hash(payload, version=config.version)
    return f"{EVENT_KEY_PREFIX}_{config.version}_{digest}"


def stable_short_hash(value: Any, *, version: str = CANONICALIZATION_VERSION, length: int = 16) -> str:
    """Small deterministic hash for non-claim identifiers (e.g. dedupe names)."""
    digest = _stable_hash(value, version=version)
    return digest[: max(8, int(length or 16))]


def _normalize_mapping(value: Mapping[str, Any]) -> Dict[str, Any]:
    return {normalize_text(k, casefold=True): _normalize_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        return normalize_timestamp(value)
    if isinstance(value, Mapping):
        return _normalize_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_normalize_value(v) for v in value]
    return normalize_text(value, casefold=True)


def _stable_hash(payload: Any, *, version: str) -> str:
    normalized = _normalize_value(payload)
    encoded = json.dumps(
        {"version": version, "payload": normalized},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
