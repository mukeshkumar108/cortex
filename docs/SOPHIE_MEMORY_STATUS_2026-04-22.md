# Sophie Memory Status - 2026-04-22

## Current State

The derived memory pipeline is materially healthier than the original regression state.

The main improvements completed in this work period:

- Entity hygiene blocks assistant/system/project items from rendering as people.
- Durable human roles survive weak recent contradictory labels.
- Empty entities are excluded from serving and synthesis surfaces.
- Project entities can be tracked without appearing in `handover.people`.
- Pass 4 and Pass 5 now receive curated packets instead of flat database dumps.
- Pass 4 and Pass 5 prompts are constrained against dramatic, therapeutic, or motive-inferential prose.
- A lightweight last-mile synthesis validator removes unsupported dramatic phrasing while preserving output shape.
- Seeded live-window review copies durable derived state before replay, so reviews test update-on-memory behavior instead of rediscovery from scratch.
- Pass 3 thread creation now rejects weak one-off threads, requires evidence, and can update existing semantic threads instead of creating duplicates.
- Pass 3 now reactivates matching resolved threads instead of creating sibling duplicates when the same topic returns.
- Resolved relationship-state writes can now close older conflict threads instead of leaving stale relational tension active.
- Thread audit now snoozes low-salience zombie threads rather than leaving static unresolved items active forever.
- Thread title/detail entity conflicts are detected, quarantined, or conservatively repaired when the conflicting name is unsupported.
- Conservative entity audit now sanitizes strongly supported legacy profile prose that still contains interpretive or motive-inferential language.
- Endpoint safety-gate tests now verify `startbrief` and `handover` stay derived-only without canonical or Graphiti fallback helpers in the serving path.

## Verified Health

Latest deterministic regression run:

```text
PYTHONPYCACHEPREFIX=/tmp/synapse-pycache python3 -m py_compile ...
passed

.venv/bin/pytest -q \
  tests/test_derived_pipeline_hardening.py \
  tests/test_derived_endpoint_golden.py \
  tests/test_script_live_parity.py \
  tests/test_schema_migration.py

52 passed, 2 warnings
```

The warnings are existing dependency deprecation warnings and are not failures.

## Review Artifacts

The latest seeded live review artifacts are local-only and must not be committed:

```text
/tmp/sophie_seeded_review_thread_hygiene.json
/tmp/sophie_seeded_review_thread_hygiene.md
```

These files may contain private memory content. Keep them out of git.

## What Is Fixed Enough

These areas are now good enough to stop treating as the main blocker:

- Entity/project/person separation.
- Durable role preservation.
- Empty entity suppression.
- Pass 4/5 packet curation.
- Basic dramatic-prose suppression.
- `unspoken_goal` suppression when weak.
- Project entities staying out of `handover.people`.
- Hydration-style duplicate thread creation reduced by semantic topic matching.
- Explicit thread title/detail entity mismatch caught before packet serving.

## Remaining High-Leverage Work

### 1. Conservative Profile Regeneration / Audit

Stored legacy profile text can still contain older interpretive phrasing outside the newly audited high-support cases.

The remaining work is:

- run the conservative audit against real stored profiles,
- clear or regenerate only clearly contaminated high-signal rows,
- keep evidence-backed facts,
- avoid mutating ambiguous low-support cases automatically.

### 2. Commit Hygiene Before Push

Before pushing to GitHub:

- Review all staged files for private memory content.
- Keep `/tmp` review artifacts out of git.
- Scrub real-user IDs from docs or examples where possible.
- Prefer synthetic examples in tests instead of private memory details.

## Lower-Priority Optimization

These are useful but not urgent:

- Broader semantic topic clustering for all thread classes.
- More nuanced stale/frequency scoring.
- Frontend low-confidence review queue.
- User-facing memory correction UI.
- More review report formatting.

These should come after the profile audit pass on real stored data.

## Commit Recommendation

Do not push blindly.

Recommended sequence:

1. Scrub sensitive examples from tests/docs.
2. Run the deterministic regression suite again.
3. Stage only intended source, migration, test, and sanitized doc files.
4. Confirm no `/tmp`, live review JSON/Markdown, or private artifacts are staged.
5. Commit and push once the staged diff is clean.
