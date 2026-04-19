# Synapse v2 Migration Changelog

Purpose: factual, append-only progress log for Synapse v2 migration execution.

Scope: tickets defined in [SYNAPSE_V2_ROADMAP.md](/opt/synapse/docs/SYNAPSE_V2_ROADMAP.md).

## Entry Rules
- Append entries in reverse-chronological order (newest first).
- Record only implemented changes observed in code, migrations, tests, or config.
- Keep each field concise and specific.
- Do not include forward-looking plans or speculation.
- Ticket status must be exactly one of: `done`, `partially done`, `blocked`.

## Entry Template
```md
### YYYY-MM-DD HH:MM UTC — <TICKET_ID>
- Summary of what changed:
  - <bullet>
- Files changed:
  - `<path>`
- Tests added/updated:
  - `<test path>`
- Acceptance criteria satisfied:
  - <criterion now met>
- Known remaining gaps:
  - <gap still open>
- Status: <done|partially done|blocked>
```

## Entries

### 2026-04-19 15:09 UTC — T0
- Summary of what changed:
  - Replaced remaining `uuid5`-based local hashing in `main.py` with canonicalization SDK hashing.
  - Updated session digest payload construction to hash canonical structured payloads (timestamp passed as native value, normalized in SDK), removing preformatted timestamp hashing.
  - Added timestamp field semantics in canonicalization (`valid` / `missing` / `invalid`) so invalid and missing timestamps hash differently.
  - Refactored local text-normalization helpers in `main.py`, `episodic_memory.py`, and `memory_ontology.py` to delegate to canonicalization SDK.
  - Added enforcement test that blocks `hashlib`/`uuid5` usage outside `src/canonicalization.py`.
  - Added 1000+ fixture deterministic/variation stress tests.
- Files changed:
  - `src/canonicalization.py`
  - `src/main.py`
  - `src/session.py`
  - `src/episodic_memory.py`
  - `src/memory_ontology.py`
  - `tests/test_canonicalization.py`
  - `tests/test_canonicalization_enforcement.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_canonicalization.py`:
    - timestamp equivalence variants
    - missing vs invalid timestamp semantic separation
    - event-key missing-vs-invalid differentiation
    - 1200-fixture slot-key determinism stress test
    - 1000-fixture event-key variation sensitivity stress test
  - `tests/test_canonicalization_enforcement.py`:
    - bans `hashlib`/`uuid5` usage outside canonicalization module in `src/`
- Acceptance criteria satisfied:
  - Canonicalization SDK is the single hashing/key authority for current write/replay hashing paths.
  - Determinism stress coverage exceeds 1,000 randomized fixtures.
  - Local hashing primitives (`hashlib`/`uuid5`) are enforced out of non-canonicalization modules.
- Known remaining gaps:
  - None for T0 scope.
- Status: done

### 2026-04-19 15:03 UTC — T0
- Summary of what changed:
  - Added canonicalization SDK module with deterministic normalization and key generation primitives.
  - Implemented `generate_claim_slot_key(...)`, `generate_claim_event_key(...)`, timestamp normalization, subject/object normalization, and versioned deterministic hashing.
  - Migrated local session hash generation for episode/summary names to use shared canonicalization hash utility.
- Files changed:
  - `src/canonicalization.py`
  - `src/session.py`
  - `tests/test_canonicalization.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_canonicalization.py`:
    - deterministic key output tests
    - variation tests for object/timestamp changes
    - normalization tests for case/spacing/unicode and UTC timestamp formatting
- Acceptance criteria satisfied:
  - Shared canonicalization module exists with deterministic slot/event key generation and normalization primitives.
  - Deterministic and variation test coverage added for canonicalization behavior.
- Known remaining gaps:
  - Full adoption across all write/replay claim/entity paths is pending (resolver/extraction paths not yet implemented).
  - 1,000-fixture determinism stress test is not yet added.
- Status: partially done

### 2026-04-19 14:54 UTC — T1
- Summary of what changed:
  - Removed factual candidate injection from session-summary prose and `user_model` prose in `/memory/query`.
  - Hardened factual helper paths to fail closed unless explicitly called as derived (`include_derived=True`).
  - Disabled legacy `/memory/search` endpoint with HTTP 410.
- Files changed:
  - `src/main.py`
  - `tests/test_graphiti_native.py`
  - `docs/SYNAPSE_V2_ROADMAP.md`
- Tests added/updated:
  - `tests/test_graphiti_native.py`:
    - `test_memory_query_uses_evidence_backed_factual_rows`
    - `test_memory_query_blocks_session_summary_and_user_model_fact_injection`
    - `test_memory_query_factual_output_requires_evidence_backed_rows`
    - `test_memory_search_endpoint_is_disabled`
- Acceptance criteria satisfied:
  - Factual candidate construction no longer injects `graphiti_session_summary` or `user_model` prose.
  - Legacy `/memory/search` mixed-authority retrieval is disabled.
  - Regression coverage added for evidence-backed-only factual outputs.
- Known remaining gaps:
  - `/v2/memory/query` lane contract and legacy compatibility adapter remain unimplemented (T10/T11).
  - Canonical claim store is not implemented (T2/T7).
- Status: done

### 2026-04-19 00:00 UTC — CHANGELOG_BOOTSTRAP
- Summary of what changed:
  - Created Synapse v2 migration changelog file and standardized entry format.
- Files changed:
  - `docs/SYNAPSE_V2_CHANGELOG.md`
- Tests added/updated:
  - None.
- Acceptance criteria satisfied:
  - Changelog format established for ticket-by-ticket execution tracking.
- Known remaining gaps:
  - No ticket implementation progress entries logged yet.
- Status: partially done
