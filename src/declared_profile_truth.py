from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional


SCALAR_FIELDS = (
    "preferred_name",
    "location",
    "timezone",
    "age",
    "faith",
    "notes_for_sophie",
    "user_about",
)
LIST_FIELDS = (
    "roles",
    "projects",
    "writing_or_public_work",
    "health_considerations",
)
PEOPLE_FIELD = "important_people"
ALLOWED_FIELDS = (*SCALAR_FIELDS, *LIST_FIELDS, PEOPLE_FIELD)


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    clean = " ".join(value.strip().split())
    return clean or None


def _clean_text_list(value: Any, *, limit: int) -> List[str]:
    if not isinstance(value, list):
        return []
    seen = set()
    items: List[str] = []
    for item in value:
        clean = _clean_text(item)
        if not clean:
            continue
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(clean)
        if len(items) >= limit:
            break
    return items


def _clean_people(value: Any, *, limit: int) -> List[Dict[str, str]]:
    if not isinstance(value, list):
        return []
    seen = set()
    people: List[Dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name"))
        relationship = _clean_text(item.get("relationship") or item.get("role"))
        note = _clean_text(item.get("note") or item.get("status"))
        situation = _clean_text(item.get("situation"))
        contact = _clean_text(item.get("contact"))
        context = _clean_text(item.get("context"))
        directive = _clean_text(item.get("directive"))
        location = _clean_text(item.get("location"))
        faith = _clean_text(item.get("faith"))
        if not name:
            continue
        key = (name.casefold(), (relationship or "").casefold())
        if key in seen:
            continue
        seen.add(key)
        payload: Dict[str, str] = {"name": name}
        if relationship:
            payload["relationship"] = relationship
        if note:
            payload["note"] = note
        if situation:
            payload["situation"] = situation
        if contact:
            payload["contact"] = contact
        if context:
            payload["context"] = context
        if directive:
            payload["directive"] = directive
        if location:
            payload["location"] = location
        if faith:
            payload["faith"] = faith
        people.append(payload)
        if len(people) >= limit:
            break
    return people


def _normalize_field_value(field: str, value: Any) -> Any:
    if field in SCALAR_FIELDS:
        return _clean_text(value)
    if field in LIST_FIELDS:
        return _clean_text_list(value, limit=12)
    if field == PEOPLE_FIELD:
        return _clean_people(value, limit=12)
    return None


def normalize_declared_profile_truth(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    truth: Dict[str, Any] = {}
    for field in SCALAR_FIELDS:
        clean = _normalize_field_value(field, raw.get(field))
        if clean:
            truth[field] = clean
    for field in LIST_FIELDS:
        clean = _normalize_field_value(field, raw.get(field))
        if clean:
            truth[field] = clean
    people = _normalize_field_value(PEOPLE_FIELD, raw.get(PEOPLE_FIELD))
    if people:
        truth[PEOPLE_FIELD] = people
    return truth


def extract_declared_profile_truth(
    identity_data: Any,
    identity_cache: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    data = identity_data if isinstance(identity_data, dict) else {}
    cache = identity_cache if isinstance(identity_cache, dict) else {}
    nested = data.get("declared_profile_truth") if isinstance(data.get("declared_profile_truth"), dict) else {}
    truth = normalize_declared_profile_truth(nested)

    profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
    cache_facts = cache.get("facts") if isinstance(cache.get("facts"), dict) else {}

    def _setdefault(field: str, value: Any) -> None:
        if field in truth:
            return
        clean = _normalize_field_value(field, value)
        if clean:
            truth[field] = clean

    _setdefault("preferred_name", data.get("preferred_name") or data.get("name") or cache.get("preferred_name"))
    _setdefault("timezone", data.get("timezone") or cache.get("timezone"))
    _setdefault("location", data.get("location") or data.get("home") or profile.get("location") or cache_facts.get("location"))
    _setdefault("age", data.get("age") or profile.get("age"))
    _setdefault("faith", data.get("faith") or data.get("religion") or profile.get("faith") or cache_facts.get("faith"))
    _setdefault("roles", data.get("roles") or profile.get("roles"))
    _setdefault("projects", data.get("projects") or data.get("products") or data.get("work") or profile.get("work"))
    _setdefault("writing_or_public_work", data.get("writing") or data.get("public_work") or profile.get("writing"))
    _setdefault("health_considerations", data.get("health") or profile.get("health"))
    _setdefault("notes_for_sophie", data.get("notes_for_sophie") or profile.get("notes_for_sophie"))
    _setdefault("user_about", data.get("user_about") or data.get("about") or profile.get("about"))
    _setdefault("important_people", data.get("important_people") or data.get("relationships") or profile.get("relationships"))

    return normalize_declared_profile_truth(truth)


def merge_declared_profile_truth(
    current: Any,
    patch: Any,
    *,
    replace: bool = False,
) -> Dict[str, Any]:
    merged = {} if replace else normalize_declared_profile_truth(current)
    raw_patch = patch if isinstance(patch, dict) else {}
    for field in ALLOWED_FIELDS:
        if field not in raw_patch:
            continue
        clean = _normalize_field_value(field, raw_patch.get(field))
        if clean:
            merged[field] = clean
        else:
            merged.pop(field, None)
    return normalize_declared_profile_truth(merged)


def build_declared_profile_truth_change_summary(
    previous: Any,
    current: Any,
) -> Dict[str, List[str]]:
    previous_truth = normalize_declared_profile_truth(previous)
    current_truth = normalize_declared_profile_truth(current)
    added: List[str] = []
    updated: List[str] = []
    removed: List[str] = []
    for field in ALLOWED_FIELDS:
        prev_value = previous_truth.get(field)
        curr_value = current_truth.get(field)
        if prev_value == curr_value:
            continue
        if prev_value is None and curr_value is not None:
            added.append(field)
        elif prev_value is not None and curr_value is None:
            removed.append(field)
        else:
            updated.append(field)
    return {
        "added": added,
        "updated": updated,
        "removed": removed,
    }


def copy_declared_profile_truth(raw: Any) -> Dict[str, Any]:
    return deepcopy(normalize_declared_profile_truth(raw))
