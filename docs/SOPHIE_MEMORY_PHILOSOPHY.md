# Sophie Memory Philosophy
## A Meaning-First Memory System

Version: 1.0
Date: April 2026

This is a meaning-first memory system. All technical decisions must preserve that property.

## 1. Core Belief

Memory is not storage. It is understanding.

Sophie does not primarily remember isolated facts about a user. Sophie holds a synthesized understanding of who the user is, what they are carrying, what matters, what is unresolved, and what is changing.

Facts serve that understanding. They do not replace it.

## 2. The Problem With Filing-Cabinet Memory

Most AI memory systems extract facts, store them, and retrieve them:

- name
- job
- preferences
- goals
- relationships
- dates

This is insufficient because people are not collections of facts.

A person is a living narrative: contradictory, evolving, shaped by loss, hope, pressure, unfinished business, and repeated patterns. The most important things about someone are often not stated directly. They emerge through:

- what they return to repeatedly
- what they avoid
- what changes emotional tone
- what goes silent
- what they say indirectly
- how declared intentions differ from repeated behavior

Raw fact extraction misses much of this.

## 3. Dual Ingestion Model

Sophie processes memory through two complementary paths.

### Path 1: Meaning Synthesis

This is the primary path.

The multi-pass synthesis pipeline builds:

- patterns
- relevance
- identity signals
- emotional texture
- unresolved threads
- living context
- contradictions
- inferred but evidence-grounded meaning

This is where understanding is built.

### Path 2: Explicit Fact Capture

This is the secondary path.

Clear, unambiguous facts are extracted directly when stated without ambiguity:

- events
- dates
- relationships
- locations
- health facts
- time-bound commitments
- concrete states

These become anchor facts.

Anchor facts stabilize synthesized understanding across time. They do not replace it.

Without anchors, synthesis can drift. Without synthesis, anchors are just a list.

## 4. Architectural Consequences

Meaning comes first. Facts come second.

The 6-pass pipeline reads conversation like a thoughtful person would: looking for patterns, tensions, identity signals, unresolved threads, and current context.

Canonical structures exist to store explicit facts, provide evidence traceability, enforce lifecycle discipline, and support audit/replay. They do not define the user.

Serving surfaces are driven by synthesized meaning:

- `startbrief`
- `handover`
- continuity

Canonical facts may anchor, audit, or challenge synthesis, but they must not bypass the synthesis layer into user-facing understanding.

## 5. Evidence Discipline

Evidence always matters, but evidence serves meaning.

Every meaningful assertion should be traceable to source conversation:

- source session ids
- source turn references
- source spans where available
- producing pass/run/version
- confidence and lifecycle state

Traceability exists to keep Sophie honest. If understanding drifts from evidence, the system must be able to inspect and repair it.

## 6. Facts Update Meaning Through Synthesis

Facts do not blindly overwrite understanding.

When a new fact arrives, it is integrated through synthesis. For example:

- "We broke up" should update relationship understanding through synthesis.
- It should not silently overwrite all prior relationship context.
- The contradiction or transition is itself meaningful.

Contradictions are information. They should be preserved until resolved through evidence and context.

## 7. Silence Is A Signal

Absence matters.

A habit that stops appearing, a relationship that fades from conversation, or a goal that goes unmentioned can indicate a real change.

Memory that never notices silence eventually becomes confidently wrong.

## 8. Behavioral Patterns Are Real Signals

Declared intent and inferred pattern are both valid but different.

Someone who mentions a goal with guilt repeatedly has provided a meaningful signal, even if they never explicitly say "this is important to me."

Inferred understanding is valid when it is:

- grounded in evidence
- marked as inferred
- assigned appropriate confidence
- open to revision

## 9. Memory Evolves At Different Speeds

Not all memory should decay equally.

Long-term memory layer:

- relationships
- core identity
- persistent goals
- health facts
- family context

These decay slowly and retain a persistence floor.

Short-term memory layer:

- session observations
- passing mentions
- transient context
- current-week pressures

These decay faster.

The goal is not a perfect record of everything said. The goal is an accurate understanding of who this person is now and what they are carrying.

## 10. Governing Principles

1. Memory is synthesized understanding, not a collection of facts.
2. Raw transcripts are ambiguous, emotional, and incomplete.
3. Meaning synthesis is the primary memory path.
4. Explicit facts are anchor inputs and audit artifacts.
5. Serving surfaces are driven by synthesized meaning.
6. Canonical structures audit understanding; they do not define the user.
7. Every meaningful assertion must be evidence-traceable.
8. Facts update meaning through synthesis, not direct overwrite.
9. Contradictions are preserved until resolved.
10. Silence is a signal.
11. Behavioral patterns are valid evidence-grounded memory signals.
12. Declared intent and inferred pattern must be tracked separately.
13. Memory evolves; different memory classes decay at different rates.
14. Technical elegance is secondary to preserving meaning-first behavior.

## 11. Decision Tests

Use this document as the test for memory decisions.

A change is wrong if it:

- sources `startbrief`, `handover`, or continuity from raw facts instead of synthesized meaning
- treats canonical truth as the authority over narrative synthesis
- overwrites understanding without preserving conflicting signal
- discards inferred patterns because they were not explicitly declared
- cannot trace a meaningful assertion back to source evidence
- flattens rich synthesis into structured-only output
- removes emotional texture, unresolved tension, or narrative continuity for convenience
- optimizes storage correctness at the expense of human understanding

## 12. Current Synapse Interpretation

In the current Synapse architecture:

- The Gemma/Postgres 6-pass pipeline is the synthesis and serving layer.
- Canonical v2 is governance, audit, factual retrieval safety, and anchor-fact infrastructure.
- `startbrief` and `handover` must remain synthesized-meaning surfaces.
- Rich synthesis quality must be preserved before hardening, scoring, or lifecycle changes are considered complete.

The implementation standard is not "does it store memory?" The standard is: does it help Sophie understand the user better while staying grounded in evidence?
