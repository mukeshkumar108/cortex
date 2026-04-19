from datetime import datetime, timedelta, timezone
import random
import unicodedata

from src.canonicalization import (
    CANONICALIZATION_VERSION,
    generate_claim_event_key,
    generate_claim_slot_key,
    normalize_object,
    normalize_subject,
    normalize_text,
    normalize_timestamp,
    normalize_timestamp_field,
    stable_short_hash,
)


def test_normalize_text_case_spacing_and_unicode():
    raw = "  Café\u00A0  AU   LAIT  "
    assert normalize_text(raw) == "café au lait"


def test_normalize_subject_uses_text_canonicalization():
    assert normalize_subject("  Alice  ") == "alice"


def test_normalize_timestamp_to_utc_iso_z():
    assert normalize_timestamp("2026-04-19T12:30:45+02:00") == "2026-04-19T10:30:45Z"
    assert normalize_timestamp("2026-04-19T10:30:45Z") == "2026-04-19T10:30:45Z"


def test_normalize_timestamp_from_datetime():
    dt = datetime(2026, 4, 19, 10, 30, 45, tzinfo=timezone.utc)
    assert normalize_timestamp(dt) == "2026-04-19T10:30:45Z"


def test_normalize_timestamp_variants_equivalent():
    expected = "2026-04-19T10:30:45Z"
    variants = [
        "2026-04-19T10:30:45Z",
        "2026-04-19T10:30:45+00:00",
        "2026-04-19T12:30:45+02:00",
        datetime(2026, 4, 19, 12, 30, 45, tzinfo=timezone(timedelta(hours=2))),
    ]
    for variant in variants:
        assert normalize_timestamp(variant) == expected


def test_timestamp_field_distinguishes_missing_and_invalid():
    missing = normalize_timestamp_field(None)
    blank = normalize_timestamp_field("   ")
    invalid = normalize_timestamp_field("not-a-date")
    valid = normalize_timestamp_field("2026-04-19T10:30:45Z")

    assert missing["state"] == "missing"
    assert blank["state"] == "missing"
    assert invalid["state"] == "invalid"
    assert valid["state"] == "valid"
    assert missing != invalid
    assert invalid != valid


def test_slot_key_deterministic_for_same_input():
    k1 = generate_claim_slot_key(
        tenant_id="Default",
        subject="Alice",
        predicate="relationship.status",
        slot_scope={"other": "Bob", "kind": "romantic"},
    )
    k2 = generate_claim_slot_key(
        tenant_id=" default ",
        subject="  alice  ",
        predicate="RELATIONSHIP.STATUS",
        slot_scope={"kind": "romantic", "other": " bob "},
    )
    assert k1 == k2
    assert k1.startswith(f"csk_{CANONICALIZATION_VERSION}_")


def test_event_key_changes_when_object_changes():
    k1 = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="relationship.status",
        obj="together",
        occurred_at="2026-04-19T10:00:00Z",
    )
    k2 = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="relationship.status",
        obj="separated",
        occurred_at="2026-04-19T10:00:00Z",
    )
    assert k1 != k2


def test_event_key_changes_when_timestamp_changes():
    k1 = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="event.happened",
        obj={"name": "hospital visit"},
        occurred_at="2026-04-19T10:00:00Z",
    )
    k2 = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="event.happened",
        obj={"name": "hospital visit"},
        occurred_at="2026-04-19T11:00:00Z",
    )
    assert k1 != k2


def test_event_key_distinguishes_missing_vs_invalid_timestamp():
    missing_key = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="event.happened",
        obj={"name": "visit"},
        occurred_at=None,
    )
    invalid_key = generate_claim_event_key(
        tenant_id="default",
        subject="alice",
        predicate="event.happened",
        obj={"name": "visit"},
        occurred_at="not-a-date",
    )
    assert missing_key != invalid_key


def test_normalize_object_dict_is_deterministic_key_order():
    o1 = normalize_object({"b": "X", "a": "Y"})
    o2 = normalize_object({"a": "y", "b": "x"})
    assert o1 == o2


def test_stable_short_hash_deterministic():
    h1 = stable_short_hash("tenant|user|session|payload", length=16)
    h2 = stable_short_hash("tenant|user|session|payload", length=16)
    assert h1 == h2
    assert len(h1) == 16


def _random_word(rng: random.Random, min_len: int = 3, max_len: int = 10) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    size = rng.randint(min_len, max_len)
    return "".join(rng.choice(alphabet) for _ in range(size))


def _noisy_equivalent(text: str, rng: random.Random) -> str:
    # Add casing, irregular spacing, and occasionally decomposed unicode form.
    parts = text.split(" ")
    spaced = (" " * rng.randint(1, 3)).join(parts)
    padded = f"{' ' * rng.randint(0, 2)}{spaced}{' ' * rng.randint(0, 2)}"
    if rng.random() < 0.5:
        padded = padded.upper()
    if rng.random() < 0.2:
        padded = unicodedata.normalize("NFD", padded)
    return padded


def test_slot_key_determinism_stress_1000_randomized_fixtures():
    rng = random.Random(1337)
    fixtures = 1200
    for _ in range(fixtures):
        tenant = _random_word(rng)
        subject = f"{_random_word(rng)} {_random_word(rng)}"
        predicate = f"{_random_word(rng)}.{_random_word(rng)}"
        scope = {"a": _random_word(rng), "b": _random_word(rng)}
        k1 = generate_claim_slot_key(
            tenant_id=tenant,
            subject=subject,
            predicate=predicate,
            slot_scope=scope,
        )
        k2 = generate_claim_slot_key(
            tenant_id=_noisy_equivalent(tenant, rng),
            subject=_noisy_equivalent(subject, rng),
            predicate=_noisy_equivalent(predicate, rng),
            slot_scope={"b": _noisy_equivalent(scope["b"], rng), "a": _noisy_equivalent(scope["a"], rng)},
        )
        assert k1 == k2


def test_event_key_variation_sensitivity_stress():
    rng = random.Random(2026)
    fixtures = 1000
    changes = 0
    for _ in range(fixtures):
        tenant = _random_word(rng)
        subject = f"{_random_word(rng)} {_random_word(rng)}"
        predicate = f"{_random_word(rng)}.{_random_word(rng)}"
        obj1 = {"value": _random_word(rng)}
        obj2 = {"value": f"{obj1['value']}_changed"}
        ts = "2026-04-19T10:30:45Z"
        k1 = generate_claim_event_key(
            tenant_id=tenant,
            subject=subject,
            predicate=predicate,
            obj=obj1,
            occurred_at=ts,
        )
        k2 = generate_claim_event_key(
            tenant_id=tenant,
            subject=subject,
            predicate=predicate,
            obj=obj2,
            occurred_at=ts,
        )
        if k1 != k2:
            changes += 1
    assert changes == fixtures
