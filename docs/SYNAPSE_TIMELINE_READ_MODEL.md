# Synapse Timeline Read Model

**Status:** Internal / Debug / Read-Only  
**Date:** 2026-05-20

---

## 1. Why timeline matters

Synapse already stores useful truth in objects, links, evidence, snapshots, and attention outcomes. What it still needed was a simple way to inspect that truth over time.

Timeline matters because companion intelligence is not just about "what was ever mentioned." It is about:
- when something first appeared
- when it was last reinforced
- whether it has gone stale
- whether it was resolved
- whether it should still influence current context

Without a timeline layer, short-lived state can linger too long. A throwaway emotional comment from weeks ago can be mistaken for current truth if there is no clean read path showing that it was old, unreinforced, and never seen again.

---

## 2. What this is

The Timeline Read Model is a lightweight internal aggregation endpoint:

- `GET /internal/debug/timeline`
- requires `X-Internal-Token`
- read-only
- built from existing tables only

It does not rewrite the pipeline. It does not replace objects, links, or attention. It simply normalizes existing memory-bearing rows into timeline events so developers can inspect what Synapse currently knows across time.

---

## 3. How it differs from other layers

### Objects
Objects are the durable things Synapse tracks, like obligations, events, people, and workstreams.

Timeline is not a new object system. It is a chronological read model over those objects and related event tables.

### Links
Links explain how objects connect.

Timeline can include link activity as events, but it does not try to become the graph itself.

### Attention
Attention answers: "What deserves surfacing right now?"

Timeline answers: "What happened, when, and how fresh is it?"

Attention is a decision layer. Timeline is an inspection layer.

### Evidence
Evidence is the ground-truth support for claims or changes.

Timeline points at evidence where available, but it does not replace the evidence layer.

---

## 4. Current source tables

The read model now pulls timeline events from:

- canonical `timeline_events`
- existing domain tables below

Existing domain sources:

- `action_items`
- `action_updates`
- `calendar_items`
- `session_changes`
- `session_handover_packets`
- `attention_outcomes`
- `memory_relationship_links`

`timeline_events` is the new canonical write path for:

- user corrections
- user suppressions
- state decay/suppression markers
- lightweight system-level timeline events

Initial domain mapping:

- `action_items` -> `obligations` or `habits`
- `action_updates` -> `obligations`
- `calendar_items` -> `events`
- `session_changes` -> `state` by default, `workstreams` for decision/focus-oriented changes
- `session_handover_packets` -> `handover`
- `attention_outcomes` -> `attention_feedback`
- `memory_relationship_links` -> `relationship`

This is intentionally incomplete. It is a minimal first read model, not a final ontology.

---

## 5. Endpoint contract

### Request

`GET /internal/debug/timeline`

Query params:

- `tenantId` required
- `userId` required
- `limit` optional, default `50`
- `since` optional ISO timestamp
- `includeExpired` optional, default `false`
- `domain` optional
- `sourceTable` optional
- `timelineType` optional

### Response

The endpoint returns normalized timeline events with:

- `id`
- `tenant_id`
- `user_id`
- `event_type`
- `domain`
- `title`
- `summary`
- `source_table`
- `source_id`
- `occurred_at`
- `first_seen_at`
- `last_seen_at`
- `expires_at`
- `status`
- `confidence`
- `salience`
- `evidence_refs`
- `related_object_ids`
- `related_link_ids`
- `freshness`
- `gaps`
- `missing_metadata`

It also returns summary metadata:

- `returnedCount`
- `bySourceTable`
- `byDomain`
- `byFreshness`
- `byTimelineType`
- `oldestOccurredAt`
- `newestOccurredAt`
- `readOnly=true`

---

## 6. What freshness means

Freshness is a simple derived field. It is not intended to be perfect.

- `expired`: `expires_at < now`
- `current`: active and very recent
- `recent`: within roughly the last 48 hours
- `stale`: old and not clearly reinforced
- `historical`: old but still useful as past context
- `unknown`: not enough timing metadata

The goal is not perfect truth inference. The goal is to make temporal blind spots obvious.

---

## 7. Why this helps state decay

State decay problems usually come from one of two failures:

1. Synapse remembers a statement but not whether it was reinforced.
2. Synapse stores timing in scattered places that are hard to inspect together.

The timeline read model helps by exposing:

- first seen
- last seen
- status
- expiry
- freshness

That makes it much easier to tell the difference between:

- a current live state signal
- a recent but fading signal
- a stale unresolved row
- a historical context marker

This is especially important for Sophie-style companions, where emotional or situational context should decay unless it keeps being reinforced.

Trust Layer V1 now makes user corrections visible here as first-class timeline rows, so stale inferred memory can be inspected alongside the correction that overrode it.

---

## 8. Why this helps daily overview

Daily overview, continuity packets, and future start-of-day surfaces all benefit from an explicit timeline layer.

It gives Synapse a compact way to answer:

- what changed recently
- what is still active
- what has already been handled
- what has gone cold

That makes it easier to build better summaries without over-indexing on stale context.

The current `GET /internal/debug/daily-overview` MVP now uses timeline as one of its internal ingredients. Timeline remains the debugging and freshness-inspection layer; Daily Overview is the product-shaped composition layer.

---

## 9. Current limitations

- It is internal/debug only.
- It is read-only.
- It does not replace `/session/startbrief`.
- It does not change serving behavior on its own.
- It only covers a subset of existing tables.
- `timeline_events` exists now as the canonical event substrate, but semantic propagation from correction events is still conservative.
- Freshness is heuristic, not canonical truth.
- Some older tables do not expose enough timestamps or evidence to avoid `gaps` and `missing_metadata`.
- It does not yet perform cross-table deduplication of conceptually similar events.

That is acceptable for v1. The job of this layer is visibility, not perfection.

---

## 10. Design intent

This read model exists to expose what Synapse currently knows over time without:

- rewriting `derived_pipeline.py`
- migrating every table
- introducing a new graph store
- changing delivery behavior

It is a minimal inspection surface for debugging relevance, staleness, and reinforcement across the current system.
