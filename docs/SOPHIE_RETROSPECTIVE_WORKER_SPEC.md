# Sophie Retrospective Worker Spec

Version: 2026-04-23  
Status: V1 contract with deterministic slice implemented

This document defines the first cross-time retrospective worker for Synapse.

It exists because the ingest pipeline operates per session, while some memory judgments only become safe after multiple sessions have accumulated.

The worker is not a replacement for Pass 1 / Pass 1.5 / Pass 3 / Pass 4 / Pass 5.

Its job is narrower:
- revisit weak or ambiguous state after more evidence exists
- consolidate overlapping state across time
- reinterpret earlier weak or incorrect assumptions when later evidence changes the picture
- demote stale weak state that should stop influencing synthesis
- prune weak state that never mattered enough to keep
- strengthen durable anchors when reinforcement is real
- resolve contradictions or transitions only when later evidence is clear

## Current Implementation Status

Implemented now:
- retrospective run/checkpoint metadata
- candidate selection after `3` new memory-worthy sessions
- candidate selection from stale low-confidence items
- candidate selection from zombie open threads
- candidate selection from active contradictions
- candidate selection from tentative entities / reinforcement-needed anchors
- deterministic stale low-confidence close
- deterministic low-confidence prune
- deterministic zombie-thread cleanup through thread audit reuse
- conservative low-confidence `REINTERPRET` from explicit durable anchors
- conservative contradiction `REINTERPRET` from explicit durable anchors
- tentative-entity promotion / prune review
- durable-anchor reinforcement metadata writes
- explicit priority-ordered processing
- anti-false-certainty blocking when a strong anchor exists but does not explicitly resolve the uncertainty
- integration into the existing conservative memory audit loop

Designed here but not yet implemented:
- thread-level `REINTERPRET` actions
- broader contradiction / transition resolution beyond anchor-backed cases
- tentative-entity reinterpretation beyond conservative promote / prune
- richer review-flag output for ambiguous cascades

This matters because the worker now exists as a real bounded V1, but this document still describes the full conservative contract for the next slice.

## 1. Core Purpose

The ingest pipeline should stay optimized for:
- fast local writes
- evidence capture
- conservative per-session updates

The retrospective worker should handle:
- slow judgments
- cross-session consolidation
- stale-state cleanup
- cautious promotion or demotion

This keeps the system meaning-first without forcing every session-sized pass to solve a timeline-sized problem.

## 2. Scope Boundary

This worker is allowed to:
- update existing rows
- merge overlapping rows
- promote weak state when reinforcement is sufficient
- demote weak stale state
- reinterpret earlier weak state when later evidence shows the earlier framing was wrong
- prune weak low-value state that never became meaningful
- resolve contradictions when later evidence is strong and explicit
- write audit metadata and review flags

This worker is not allowed to:
- invent new meaning from thin evidence
- create rich new synthesis prose
- rewrite `identity_profile` or `living_context` directly as its primary job
- change serving response shape
- use canonical claims as direct serving authority

The worker may improve the state that later synthesis reads. It does not bypass synthesis.

Important principle:
- the worker is allowed to say “we were wrong earlier” only when later evidence is materially better than the earlier evidence

## 3. Recommended Tempo

Use two tempos eventually, but build only the first one now.

### Tempo A: Short Retrospective Worker

Cadence:
- every 72 hours, or
- when a user accumulates 3 new memory-worthy sessions since the last retrospective run

Purpose:
- clean weak or ambiguous state before it accumulates into drift

### Tempo B: Slow Consolidation Worker

Cadence:
- weekly or biweekly

Purpose:
- consolidate repeated patterns
- strengthen long-term anchors
- evaluate silence/staleness shifts

Do not build Tempo B yet.

Build Tempo A first.

## 4. V1 Surfaces

The first worker should operate only on these surfaces:

### 4.1 Tentative entities

Allowed actions:
- `KEEP`
- `PROMOTE_ACTIVE`
- `DEMOTE_TENTATIVE`
- `REINTERPRET`
- `PRUNE`
- `FLAG_REVIEW`

Promotion requires:
- multiple distinct sessions, or
- strong explicit evidence in a later session

Do not promote from one weak mention.

`REINTERPRET` means:
- the entity itself may stay the same
- but its type, role, or confidence framing may need correction
- weak downstream state that depended on the earlier wrong framing may also need review

### 4.2 Low-confidence items

Allowed actions:
- `KEEP_OPEN`
- `CLOSE_CONFIRMED`
- `CLOSE_STALE`
- `REINTERPRET`
- `PRUNE`
- `FLAG_REVIEW`

Rules:
- confirm only when later evidence answers the uncertainty clearly
- close stale low-confidence items that went unreinforced for long enough
- do not keep ambiguous low-confidence items alive forever
- prune low-confidence items that were weak, low-impact, and never reinforced
- reinterpret low-confidence items when the uncertainty came from an earlier wrong assumption

### 4.3 Open threads

Allowed actions:
- `KEEP`
- `MERGE`
- `SUPERSEDE`
- `RESOLVE`
- `SNOOZE`
- `REACTIVATE`
- `REINTERPRET`
- `FLAG_REVIEW`

Rules:
- merge only when semantic overlap is high and evidence supports sameness
- resolve only when later evidence clearly indicates closure or transition
- snooze static zombie threads instead of keeping them active forever
- reactivate older matching threads instead of creating sibling copies
- reinterpret threads when an earlier weak premise was wrong, for example wrong relationship role or wrong entity framing

### 4.4 Contradictions and transitions

Allowed actions:
- `KEEP`
- `RESOLVE_CONTRADICTION`
- `WRITE_TRANSITION`
- `REINTERPRET`
- `FLAG_REVIEW`

Rules:
- resolve only when the later view is explicit and better supported
- keep tension visible when evidence remains mixed
- prefer preserving uncertainty over flattening too early
- reinterpret contradiction state when the apparent contradiction was caused by earlier weak classification rather than real user inconsistency

### 4.5 Durable anchors

Allowed actions:
- `REINFORCE`
- `KEEP`
- `FLAG_REVIEW`

Rules:
- anchors may strengthen through repeated reinforcement
- anchors should not be weakened by recent low-quality noise
- strong relationship roles remain sticky unless explicitly corrected

Anchor reinforcement metadata must be explicit:
- `reinforcement_count`
- `last_reinforced_at`
- `reinforcement_source_sessions`

The worker should not treat anchors as static facts that merely exist.
It should track whether they are being actively reinforced across time.

## 5. What V1 Should Not Touch

Keep V1 small.

Do not let the first worker:
- generate new identity traits
- generate new living-context prose
- create broad psychological summaries
- run web research
- mutate user-facing queue delivery logic
- change schema shape unnecessarily
- emit behavioral implications for product actions yet
- implement continuous confidence decay yet

If the worker finds something important but ambiguous, it should flag it, not solve it creatively.

## 6. Decision Rules

### 6.1 Promotion rules

Promote only when at least one is true:
- `distinct_session_count >= 2` with semantically aligned reinforcement
- one explicitly high-impact event with strong evidence

Do not promote based on:
- frequency without quality
- recency without reinforcement
- one dramatic mention

Promotion and uncertainty reduction must be separated unless evidence is explicit.

The worker must not:
- raise confidence
- and close uncertainty
- in the same move

unless the later evidence is explicit, strong, and directly resolves the earlier ambiguity.

### 6.2 Demotion rules

Demote when all are true:
- evidence is weak
- reinforcement is absent
- the item is old enough that keeping it active now adds more noise than value

Do not demote:
- durable family/partner anchors
- health anchors with persistent relevance
- persistent goals with clear continued relevance

### 6.3 Reinterpretation rules

Reinterpret only when:
- the later evidence is materially stronger than the earlier evidence
- the earlier state was weak, tentative, inferred, or visibly ambiguous
- the reinterpretation improves coherence without inventing new meaning

Examples:
- earlier role was `other`, later role is clearly `daughter`
- low-confidence item assumed one relationship framing, later evidence makes that framing wrong
- thread was created under a weak mistaken premise and should be reclassified or superseded

Reinterpretation should cascade only into weak downstream artifacts.

Do not rewrite strong well-supported history casually.

### 6.4 Merge rules

Merge only when:
- topic match is high
- entity match is compatible
- lifecycle combination is valid
- evidence does not indicate genuinely separate threads

If uncertain:
- `FLAG_REVIEW`

### 6.5 Prune rules

Prune only when all are true:
- the item is weak
- it remained unreinforced
- it is low-impact
- keeping it adds more noise than value

Good V1 prune targets:
- low-confidence items that never mattered
- tentative entities with no reinforcement
- weak stale derived debris that should not continue to influence later synthesis

Do not prune:
- durable anchors
- active high-signal threads
- real contradictions still awaiting resolution

### 6.6 Contradiction resolution rules

Resolve only when:
- later evidence is explicit
- the later evidence is stronger than the earlier contradiction
- the contradiction is genuinely resolved rather than simply quiet

If the system cannot tell whether the change is real or merely temporary:
- keep the contradiction active

## 7. Triggers

V1 should support these triggers:

1. Scheduled trigger
- run every 72 hours

2. Session-count trigger
- run for a user after 3 new memory-worthy sessions since the last retrospective run

3. Optional operator trigger
- allow manual run for one user during reviews

Do not trigger on every session.

## 8. Priority Order

Retrospective work is not flat.

V1 should process in this order:

1. contradictions and transitions
2. relationship entities and relationship threads
3. active/open threads
4. low-confidence items
5. tentative entities
6. stale weak cleanup / prune candidates

This is important because:
- relationship drift is high-cost
- contradictions directly affect meaning quality
- active threads poison Pass 5 faster than weak entity debris
- low-value cleanup should not consume the best budget first

## 9. Inputs

V1 should read:
- `entity_profiles`
- `open_threads`
- `low_confidence_items`
- `memory_contradictions`
- `memory_silence_flags`
- `derived_assertions`
- `session_classifications`
- `pipeline_checkpoints`

It may use:
- evidence refs
- distinct session counts
- salience / importance
- last seen timestamps
- lifecycle state

## 10. Outputs

V1 should write:
- conservative updates to existing tables
- audit summaries
- review flags / quarantine when needed
- retrospective run/checkpoint metadata
- anchor reinforcement metadata where applicable

It should not write:
- direct serving payloads
- final user-facing text as a primary output

## 11. Safety Constraints

These are non-negotiable.

- Evidence or it does not exist.
- No one-mention promotion.
- No silent overwrite of meaningful contradiction.
- No mutation of ambiguous state without review path.
- No reinterpretation of strong state from weak later evidence.
- No simultaneous confidence increase and uncertainty closure unless explicit evidence clearly allows it.
- No serving authority leak from canonical lane.
- No change to `startbrief` / `handover` response shape.

## 12. Suggested Implementation Shape

Keep the first version simple:

1. Select candidate users
- users with recent memory-worthy activity or stale weak state

2. Build a retrospective packet
- tentative entities
- low-confidence items
- active/open/snoozed/resolved thread candidates
- contradictions with later evidence
- durable anchors needing reinforcement check
- weak stale items eligible for prune

3. Apply deterministic rules first
- stale low-confidence close
- zombie thread snooze
- obvious duplicate merge/supersede
- obvious durable-anchor reinforcement
- obvious low-value prune
- obvious weak-state reinterpretation when later evidence is explicit

4. Use LLM judgment only for bounded ambiguous comparisons
- duplicate-thread similarity
- contradiction-vs-transition judgment
- tentative entity promotion review
- reinterpretation review where the cascade is not obvious

5. Apply only high-confidence allowed actions

6. Flag the rest

## 13. V1 Test Matrix

Before implementation, require tests for:

- tentative entity promotes only after repeated reinforcement
- weak tentative entity stays tentative
- tentative entity can be reinterpreted when later evidence clearly corrects earlier framing
- stale low-confidence item closes without surfacing
- repeated low-confidence item confirms when later evidence is explicit
- low-impact stale low-confidence item prunes instead of staying open forever
- overlapping threads merge only when truly same
- resolved conflict thread is superseded by later clear state
- weak thread can be reinterpreted when its earlier premise was wrong
- contradiction stays active when later evidence is mixed
- contradiction resolves when later evidence is explicit
- durable anchor strengthens without being rewritten dramatically
- durable anchor reinforcement metadata updates correctly
- stale weak signal demotes without affecting durable anchor ordering
- confidence does not increase while uncertainty is closed unless evidence is explicit

## 14. Recommended Build Order

Build in this order:

1. retrospective checkpoint/run metadata
2. candidate selection
3. deterministic low-confidence stale close
4. deterministic zombie-thread cleanup
5. deterministic thread merge/supersede pass
6. tentative-entity re-evaluation and reinterpretation
7. contradiction/transition resolution
8. durable-anchor reinforcement metadata
9. bounded prune rules

Do not build weekly consolidation before V1 is stable.

## 15. Product Interpretation

This worker is not polish.

It is the missing bridge between:
- session-level ingestion
- time-level understanding

Without it, the system keeps collecting good local signals but remains too dependent on immediate session context.

With it, the system can become more correct over time without becoming more aggressive.
