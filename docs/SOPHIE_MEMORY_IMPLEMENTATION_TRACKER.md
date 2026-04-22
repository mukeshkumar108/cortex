# Sophie Memory Implementation Tracker

Version: 2026-04-22  
Scope: Phase 1 hardening and Phase 2 packet quality only  
Status: Working tracker

This document is the implementation checklist for restoring premium, meaning-first memory quality without redesigning the system.

It exists so future work can resume safely across chats, commits, or operators.

## North-Star Constraints

- Meaning synthesis remains the serving authority.
- `startbrief` and `handover` remain sourced from derived Postgres tables only.
- Canonical v2 remains governance/audit/anchor-fact infrastructure only.
- Graphiti is not memory authority.
- Do not introduce a new framework.
- Do not change serving response shapes.
- Do not rely on keyword dictionaries or regex as core meaning filters.
- LLMs judge semantic meaning; deterministic code enforces evidence, hygiene, and invariants.
- Phase 2 decay/staleness/consolidation remains dormant unless explicitly activated later.

## Global Invariants

These rules apply across all passes.

### G0.1 Evidence Or It Does Not Exist

Nothing may influence Pass 4, Pass 5, `startbrief`, or `handover` unless it has evidence references.

Applies to:
- entities
- entity profile facts
- threads
- thread updates
- contradictions
- transitions
- low-confidence items
- identity signals
- living-context signals

Acceptance:
- Items without evidence refs are excluded from synthesis packets.
- Items without evidence refs may be quarantined, flagged, or kept internal, but not served as confident memory.

### G0.2 No Upgrade Without Reinforcement

Weak signals cannot become important or durable from one mention.

Promotion requires one of:
- multiple distinct sessions
- cross-context reinforcement
- one explicitly high-impact event

Acceptance:
- One mention does not create an important person.
- One emotional moment does not create an identity pattern.
- One task does not become a persistent goal.

### G0.3 No Narrative Stitching

Profiles and synthesis cannot causally connect facts unless the connection is explicitly stated, repeated, or represented as a contradiction/transition.

Blocked failure modes:
- invented causal arcs
- relationship “rollercoaster” language from thin evidence
- “this helped them heal/reconnect” unless evidenced
- merging facts from different entities

Acceptance:
- Profiles say what is known, what changed, and what remains uncertain.
- Profiles do not invent connective tissue.

### G0.4 Thread Directionality

A thread must move over time.

Allowed movement:
- update
- snooze
- resolve
- supersede
- reactivate
- degrade salience
- flag for review

Acceptance:
- No silent immortal threads.
- No zombie threads in Pass 4/5 packets.

### G0.5 Packet Inclusion Is Explainable

Every item included in Pass 4/5 packets must include:

```json
{
  "why_included": "...",
  "evidence_strength": "weak | medium | strong",
  "importance_score": 0.0,
  "evidence_refs": []
}
```

Acceptance:
- Review harness shows included and dropped packet items with reasons.

### G0.6 Durable Roles Are Anchor Truths

Strong, well-supported roles are durable anchors, not disposable recent-window labels.

Examples:
- daughter / son / child
- partner / spouse / girlfriend / boyfriend
- parent
- close family

Rules:
- Durable roles cannot be downgraded by weaker or narrower recent evidence.
- Durable roles must survive packet pruning and recent-window bias.
- Durable roles may change only when explicitly corrected by strong evidence.

Acceptance:
- A durable role remains available to Pass 4/5 packets even if the entity was not recently mentioned.
- Weak recent labels like `friend`, `other`, or unclear relationship do not overwrite strong prior roles.

### G0.7 Empty Entities Never Surface

Entities with no usable memory content must not appear in serving or synthesis surfaces.

An entity is empty for serving purposes if it has:
- null or empty profile text
- no key facts
- no behaviorally relevant uncertainty
- no active thread/event/contradiction linkage

Acceptance:
- Empty entities do not appear in `people`.
- Empty entities do not appear in `entity_hints`.
- Empty entities do not appear in Pass 4/5 packets.

## Execution Sequence

Do not reorder without explicitly updating this document.

## Step 1: Entity Layer

Goal: stop entity pollution before anything reaches synthesis.

Files expected:
- `src/derived_passes/pass15_entities.py`
- `src/derived_pipeline.py`
- `src/main.py`
- tests under `tests/`

Implementation tasks:
- [x] Extend Pass 1.5 semantic decision contract.
- [x] Support decisions: `MATCH`, `NEW`, `TENTATIVE`, `SKIP`.
- [x] Add `evidence_strength`.
- [x] Add `memory_relevance`.
- [x] Add `relationship_confidence`.
- [x] Add `why_this_matters`.
- [x] Require evidence refs for entity decisions.
- [x] Add profile gate before rich profile generation.
- [x] Preserve durable roles from weak downgrades.
- [x] Inject durable roles as anchors into later packets.
- [x] Prevent assistant/system entities from becoming human relationships.
- [x] Prevent empty/null profile entities from surfacing.
- [x] Prevent cross-entity contamination.
- [x] Block narrative stitching in entity profiles.

Tests required:
- [x] Weak new entity becomes tentative or skipped.
- [x] Tentative entity does not receive rich profile.
- [x] Empty/null profile entity does not surface in `people`.
- [x] Empty/null profile entity does not surface in `entity_hints`.
- [x] Assistant/system entity cannot become person/friend.
- [x] Durable role cannot downgrade from weak evidence.
- [x] Durable role is injected into Pass 4 packet.
- [x] Durable role is injected into Pass 5 packet.
- [x] Profile cannot stitch causal narrative from unrelated facts.
- [x] Cross-entity contamination is rejected or flagged.

Exit criteria:
- No assistant/system entity appears as a real person.
- No weak entity gets a confident warm profile.
- No empty/null entity surfaces in startbrief/handover.
- Durable roles survive recency pruning and weak contradictory mentions.

Do not proceed to Step 2 if entity pollution is still present.

## Step 2: Thread Layer

Goal: stop thread explosion and weak unresolved-state writes.

Files expected:
- `src/derived_passes/pass3_threads.py`
- `src/derived_pipeline.py`
- tests under `tests/`

Implementation tasks:
- [x] Remove “lean toward creating” behavior.
- [x] Extend Pass 3 action contract.
- [x] Add `unresolvedness`.
- [x] Add `follow_up_value`.
- [x] Add `evidence_strength`.
- [x] Add `why_this_matters_later`.
- [x] Require evidence refs for thread actions.
- [x] Reject weak one-off thread creates.
- [x] Restrict commitment threads to actual future relevance.
- [x] Enforce lifecycle validity.
- [x] Enforce thread directionality.
- [x] Prevent duplicate thread explosion.

Tests required:
- [x] Weak one-off plan is skipped.
- [x] Real unresolved relationship tension creates or updates a thread.
- [ ] Resolved relationship state resolves or supersedes old conflict thread.
- [ ] Reactivation updates existing thread instead of creating duplicate.
- [x] Invalid lifecycle transition writes quarantine.
- [x] Thread without evidence is excluded from packet.
- [x] Contaminated title/detail entity mismatch is excluded from Pass 5 packet and flagged in review.
- [ ] Static zombie thread is degraded, snoozed, or flagged.

Exit criteria:
- Threads are fewer, evidence-backed, and future-relevant.
- Resolved states do not remain active unless unresolved tension remains.
- Duplicate topic threads are merged, superseded, or flagged.

Do not proceed to Step 3 if thread explosion remains.

## Step 3: Lightweight Temporal Reinforcement

Goal: stabilize meaning across time without building a full pattern framework yet.

Files expected:
- migration under `migrations/`
- `schema.sql`
- `src/derived_pipeline.py`
- tests under `tests/`

Implementation tasks:
- [x] Add or verify `first_seen_at` for entities/threads/signals where needed.
- [x] Add or verify `last_seen_at` for entities/threads/signals where needed.
- [x] Add or verify `distinct_session_count` for entities/threads/signals where needed.
- [x] Enforce promotion rules using evidence depth.
- [ ] Preserve high-impact single-event anchors.
- [ ] Prevent recency-only promotion.
- [ ] Prevent frequency-only stale dominance.

Tests required:
- [x] One weak mention cannot become important entity.
- [x] One emotional moment cannot become identity pattern.
- [ ] Repeated multi-session signal can promote.
- [ ] High-impact single event can become anchor.
- [ ] Old durable anchor survives recent-window pruning.
- [ ] Recent weak item cannot outrank durable high-importance item.

Exit criteria:
- Synthesis no longer overfits to the latest window.
- Durable anchors survive if semantically important.
- Weak recent noise cannot dominate packets.

Do not proceed to Step 4 if promotion is still one-mention driven.

## Step 4: Conservative Audits

Goal: repair drift safely without rewriting meaning aggressively.

Files expected:
- `src/derived_passes/pass3_threads.py`
- `src/derived_pipeline.py`
- worker/outbox scheduling code
- tests under `tests/`

Implementation tasks:
- [x] Activate scheduled thread audit.
- [x] Add conservative entity audit.
- [x] Apply only safe high-confidence actions automatically.
- [x] Flag ambiguous actions for review.
- [x] Quarantine invalid lifecycle transitions.
- [x] Prevent audits from inventing new meaning without evidence.

Thread audit actions:
- `KEEP`
- `MERGE`
- `RESOLVE`
- `SNOOZE`
- `REACTIVATE`
- `FLAG_REVIEW`

Entity audit actions:
- `KEEP`
- `MERGE`
- `DEMOTE_TO_TENTATIVE`
- `CLEAR_PROFILE`
- `FLAG_REVIEW`

Tests required:
- [x] Thread audit safe merge applies.
- [x] Ambiguous thread merge flags review.
- [x] Entity audit clears contaminated profile.
- [ ] Role conflict flags review.
- [ ] Unsafe audit action quarantines.
- [x] Audit does not rewrite meaning without evidence.

Exit criteria:
- Drift is visible and repairable.
- Ambiguous cases are not silently mutated.
- Audits improve hygiene without flattening meaning.

Do not proceed to Step 5 if audits mutate ambiguous state automatically.

## Step 5: Curated Pass 4/5 Packet Compilers

Goal: give Pass 4/5 the strongest things that matter, not a flat database dump.

Files expected:
- `src/derived_pipeline.py`
- `src/derived_passes/pass4_identity.py`
- `src/derived_passes/pass5_living_context.py`
- `scripts/inspect_derived_pipeline_fixture.py`
- tests under `tests/`

Implementation tasks:
- [x] Add `build_pass4_identity_packet(...)`.
- [x] Add `build_pass5_living_packet(...)`.
- [x] Group packet sections by semantic class.
- [x] Inject durable anchors.
- [x] Apply attention budgets per class.
- [x] Include `why_included`, `evidence_strength`, `importance_score`, and evidence refs.
- [x] Keep contradictions separate from transitions.
- [x] Exclude items without evidence refs.
- [x] Exclude empty/null entities.
- [x] Exclude behaviorally irrelevant low-confidence items.
- [x] Show dropped items and reasons in inspection output.

Packet sections:
- durable anchors
- key entities
- active high-signal threads
- recent meaningful deltas
- contradictions
- transitions
- low-confidence items
- silence/reactivation signals
- anchor events

Tests required:
- [x] Every included item has evidence refs.
- [x] Every included item has `why_included`.
- [x] Contradictions and transitions are separate.
- [x] Behaviorally irrelevant low-confidence is excluded.
- [x] Behaviorally relevant low-confidence is included lightly.
- [x] Empty/null entities are excluded.
- [x] Packet size budgets are enforced.
- [x] Durable anchors are injected.
- [x] High-priority older anchor beats weak recent filler.

Exit criteria:
- Pass 4/5 packets are compact, inspectable, and high-signal.
- Stored memory remains intact while synthesis input is curated.
- Quality failures can be traced to packet contents or model output.

Do not proceed to Step 6 if packets are still flat dumps.

## Step 6: Real 30-Session Review

Goal: evaluate actual model behavior only after upstream quality is fixed.

Required review:
- [x] Run real 30-session live review.
- [x] Seed isolated real-user review runs with durable derived state before replay.
- [x] Keep project entities tracked without letting them render as people.
- [x] Inspect entity decisions.
- [x] Inspect skipped/tentative entities.
- [x] Inspect thread actions.
- [x] Inspect audit actions.
- [x] Inspect Pass 4 packet.
- [x] Inspect Pass 5 packet.
- [x] Inspect dropped items.
- [x] Inspect identity output.
- [x] Inspect living context output.
- [x] Inspect startbrief.
- [x] Inspect handover.
- [x] Compare against old good manual handover.
- [x] Compare against current bad output.

Latest review artifact:
- `/tmp/sophie_seeded_review_after_tracker.json`
- `/tmp/sophie_seeded_review_after_tracker.md`
- `/tmp/sophie_seeded_review_step7.json`
- `/tmp/sophie_seeded_review_step7.md`

Review harness invariant:
- Real-user `--run-live-window` reviews copy durable derived state from the source user into the isolated review user before replaying the selected recent window. This keeps review focused on update-on-top-of-memory behavior, not rediscovery from scratch.
- Project entities may surface as project/entity hints, but they must not render inside `handover.people`.

Exit criteria:
- People/entities are correct.
- Threads are fewer and sharper.
- Durable anchors survive.
- No assistant/person contamination.
- Living context is concrete and useful.
- Identity is operationally useful, not a character study.

Do not proceed to Step 7 if bad output is explained by bad packet inputs.

## Step 7: Light Pass 4/5 Constraints

Goal: remove residual generic or psychologizing prose without changing serving shape.

Only start this step after Step 6 review.

Implementation tasks:
- [x] Keep output fields unchanged.
- [x] Add small constraints only if packet cleanup is insufficient.
- [x] Block generic literary phrases.
- [x] Block unsupported inner motives.
- [x] Block causal stitching.
- [x] Block identity claims from one session.
- [x] Prefer concrete behavior and current context over interpretation.

Tests required:
- [x] Generic prose constraints are pinned in deterministic prompt tests.
- [x] Output preserves field shape.
- [x] Identity prompt requires useful, evidence-backed constraints.
- [x] Living-context prompt requires concrete, non-therapeutic context.

Exit criteria:
- Same contract, better content.
- Synthesis is meaning-rich but not dramatic.
- Sophie has useful awareness, not a psych profile.

## Endpoint Safety Gates

These must remain true after every step.

- [ ] `startbrief` remains derived-only.
- [ ] `handover` remains derived-only.
- [ ] No canonical factual lane leaks into serving authority.
- [ ] No Graphiti authority path reappears.
- [ ] No empty/null entity surfaces.
- [ ] No assistant/system entity appears as a person.
- [ ] No duplicate contradictions render.
- [ ] No filler threads dominate packets.
- [ ] Serving response shape is unchanged.

## Regression Commands

Use these after each implementation step unless the step explicitly requires a broader suite.

```bash
PYTHONPYCACHEPREFIX=/tmp/synapse-pycache python3 -m py_compile \
  src/derived_pipeline.py \
  src/main.py \
  src/derived_passes/pass15_entities.py \
  src/derived_passes/pass3_threads.py \
  src/derived_passes/pass4_identity.py \
  src/derived_passes/pass5_living_context.py
```

```bash
.venv/bin/pytest -q \
  tests/test_derived_pipeline_hardening.py \
  tests/test_derived_endpoint_golden.py \
  tests/test_script_live_parity.py \
  tests/test_schema_migration.py
```

## Review Commands

Run after Step 5 and Step 6 work.

```bash
source .venv/bin/activate
python scripts/inspect_derived_pipeline_fixture.py \
  --user-id <REAL_USER_ID> \
  --last-sessions 30 \
  --run-live-window \
  --live-model \
  --output /tmp/sophie_seeded_review.json \
  --markdown-output /tmp/sophie_seeded_review.md
```

## Definition Of Done

The work is not done until all are true:

- [ ] Entity pollution is blocked.
- [ ] Thread explosion is blocked.
- [ ] Temporal reinforcement prevents one-off overfitting.
- [ ] Conservative audits are active and safe.
- [ ] Pass 4/5 packets are curated and inspectable.
- [ ] Real 30-session review is materially better than the bad regression output.
- [ ] Output is closer to the old good manual handover in specificity and groundedness.
- [ ] Deterministic tests pass.
- [ ] Serving shape is unchanged.
- [ ] No sensitive review output is committed.
