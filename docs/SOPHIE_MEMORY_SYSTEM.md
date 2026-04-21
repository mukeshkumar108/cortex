# Sophie Memory System — What We Are Actually Building

*A reference document for humans, agents, and LLMs working on Synapse. Read this before touching anything. The philosophy document says what we believe. This document says what we are building and why every piece exists.*

---

## The Thing We Are Actually Building

Sophie is not a chatbot with memory. She is not a fact retrieval system. She is not a journal search engine.

Sophie is the first AI that genuinely holds a person across time — their patterns, their weight, their contradictions, their growth, their silences. The goal is that a user feels *held*, not *tracked*.

Every technical decision should serve that feeling. If it doesn't, it's the wrong decision.

---

## The Authority of Layers

Before anything else, understand this:

**Meaning is the authoritative layer for interpretation. Facts are the authoritative layer for verification. Neither replaces the other. Disagreements between them are signals, not errors.**

This resolves every future argument about canonical vs synthesis authority. When a fact contradicts a synthesized understanding, the system does not pick a winner. It preserves the tension and surfaces it. That tension is information. Flattening it is a loss.

---

## The Memory Primitives

These are the building blocks of Sophie's understanding. They are not equal.

**Primary primitives** — where understanding lives:
- Threads
- Identity traits
- Living context

**Supporting primitives** — what anchors and connects understanding:
- Entities
- Events

**Input primitives** — the raw material synthesis works from:
- Memory deltas

Understanding is built from primary primitives. Supporting primitives anchor and connect them. Input primitives feed the synthesis passes. Future systems that treat entities and events as the core of memory have already degraded. Don't let that happen here.

---

### Entities
Named people, places, and things that carry relational or emotional weight. Not every noun. Only what matters. Jasmine. Ashley. The father. The job. The gym.

Entities have:
- A relationship to the user
- Sophie's current understanding of that relationship
- A salience score — how present this entity is in the user's life right now
- A last-seen timestamp — when this entity last appeared in conversation
- A memory layer — LML (long-term, slow decay) or SML (short-term, fast decay)

Entities are supporting primitives. They anchor understanding. They are not the understanding itself.

---

### Events
Things that happened or are going to happen. Anchor facts. Datable, verifiable, concrete.

Past: "lost his father to cancer." "Relationship with Ashley ended."
Upcoming: "Jasmine's birthday next month." "Committed to starting X by end of month."

Events are temporal anchors. They ground understanding in time and reality. They do two jobs simultaneously — anchoring time and anchoring truth. Without events, synthesis drifts. Without synthesis, events are disconnected facts with no meaning.

Events are supporting primitives. They stabilize. They do not interpret.

---

### Threads
Unresolved life motion. Things in progress, things avoided, things that keep returning.

A thread is not a task. It is a living tension. "Reconnecting with Jasmine." "The grief that hasn't finished." "The habit that keeps not starting."

Threads have lifecycle: `active`, `snoozed`, `resolved`, `superseded`. They can go quiet and reactivate. Resolution requires evidence, not just time passing.

Threads are primary primitives. They are the closest thing to the living narrative of a person's current story.

---

### Identity Traits
Slow-moving, durable understanding of who this person is. Values. Patterns. How they respond under pressure. What they return to. What they avoid.

These are the hardest to extract and the most valuable. They emerge from synthesis across many sessions, not from single statements. A single session cannot produce a reliable identity trait. Patterns across time can.

Identity traits are primary primitives. They are the most durable layer of understanding in the system.

---

### Living Context
The current chapter. What the user is carrying right now. The primary tension. The emotional texture of this period of their life. Where they are in their own story.

This changes on the scale of weeks and months, not sessions. It should feel like the answer to "how are they really doing right now."

Living context is a primary primitive. It is the synthesis of everything else into a current, coherent understanding of the person.

---

### Memory Deltas
What changed in this session. Not a summary — a signal. What was new, what shifted, what was said for the first time.

Memory deltas are input primitives. They are the raw material the synthesis passes work from. They do not persist as serving-layer outputs. They feed the system.

---

## The Six Passes and What Each One Does

**Pass 1 — Triage**
Reads the new session transcript. Detects what matters. Flags identity signals, context deltas, entity mentions, emotional weight, tension signals. Writes memory deltas. This is the entry point for everything. Every subsequent pass depends on what Pass 1 finds.

**Pass 1.5 — Entity resolution**
Updates entity profiles based on what Pass 1 found. Matches mentions to known entities. Creates new entities when needed. Updates relationship understanding. Every update is evidence-backed.

**Pass 3 — Threads**
Applies lifecycle transitions to open threads. Creates new threads when unresolved tension is detected. Updates existing threads with new evidence. Resolves or snoozes threads when signals indicate closure.

**Pass 4 — Identity synthesis**
Runs on cadence, not every session. Triggers when Pass 1 has flagged 3 identity-signal sessions since last run, or 14 days have elapsed — whichever comes first. Re-synthesizes identity traits from accumulated evidence. Looks for patterns across many sessions. Writes rich prose fields: `who_they_are`, `core_values`, `current_chapter`.

**Pass 5 — Living context synthesis**
Runs on cadence. Triggers when Pass 1 has flagged 2 context-delta sessions since last run, or 10 days have elapsed — whichever comes first. Re-synthesizes the current chapter from recent sessions and identity understanding. Writes: `current_focus`, `primary_tension`, `relationship_pulse`, `emotional_texture`, `sophie_directives`, `active_contradictions`.

---

## What Sophie Receives at Session Start

**Startbrief** — a compact packet Sophie reads before the conversation begins:
- Who this person is (identity synthesis)
- What they're currently carrying (living context)
- Who matters to them right now (top entities by salience)
- What's unresolved (active threads)
- What Sophie is uncertain about (low confidence queue)
- What's coming up (anticipatory signals)

**Handover** — a richer packet for longer gaps between sessions:
- Everything in startbrief
- What changed recently (memory deltas)
- Active contradictions Sophie should hold
- Relational flags (silence signals, relationship tensions)
- Sophie's own unresolved questions

---

## The Memory Intelligence Layer

These are the jobs that keep memory alive, honest, and relevant over time.

**Reinforcement decay**
Memory strength is not just about time. Every time a memory is used — retrieved, surfaced, referenced — its strength resets and grows. Jasmine stays sharp because she's constantly present. The detail from four months ago fades because it's never touched.

Two decay rates:
- LML (long-term memory layer): relationships, identity, health, persistent goals. Slow decay. High retention floor. Never fully disappears.
- SML (short-term memory layer): session observations, passing mentions, transient context. Fast decay. Low floor. Fades to archive.

Fields: `last_accessed_at`, `access_count`, `salience`, `retention_floor`

**Staleness detection**
High-salience memory not referenced in transcripts for 90 days gets flagged. Not deleted. Not superseded. Flagged. A highly-retrieved memory about someone's employer is accurate until it suddenly isn't — and the system should notice the silence rather than keep serving stale truth confidently.

**Silence detector**
Different from staleness. This is about entities and threads, not facts.

If Jasmine was a high-salience entity and hasn't appeared in conversation for 30 days — that's a signal. Sophie surfaces it in handover. Not as an alert. As awareness. "Kaiser hasn't mentioned Jasmine in five weeks." Sophie holds that and when the moment is right, asks.

If a persistent goal or recurring thread goes quiet — same thing. Silence is information.

**Consolidation**
Repeated signals across sessions consolidate into single canonical insights rather than accumulating as raw observations. "User mentions walking goal but shows no behavioral follow-through" appearing eight times becomes one consolidated insight with all eight sessions as evidence. Keeps context packets clean. Prevents noise from drowning signal.

**Contradiction surface**
Contradictions between synthesized understanding and new evidence are not resolved automatically. They are preserved and surfaced in handover. "Living context says committed to sobriety — recent sessions contain tension signals around drinking." Sophie holds both. She doesn't flatten it. She waits for context that lets her understand which is more true right now.

This is the authority of layers in practice. Meaning says one thing. A new signal says another. Neither wins automatically. The tension is the information.

---

## The Low Confidence Queue

Sophie knows what she doesn't know.

When synthesis produces understanding with weak evidence — inferred patterns, single mentions, ambiguous signals — those go into a low confidence queue. Not discarded. Not served as fact. Held as open questions.

Examples:
- "User mentioned a sister once, never again — relationship unclear"
- "Career tension signal present but nature unconfirmed"
- "Emotional tone around father shifted recently — cause unknown"

This queue feeds into startbrief and handover. Sophie sees it. When the moment is right she asks — naturally, not clinically. Not all at once. Not when the user is already carrying something heavy.

Rules for asking:
- One question at a time
- Only when contextually natural
- If the user deflects, note it and wait — deflection is also information
- When answered, close the item with evidence and promote to confirmed understanding

---

## Anticipatory Signals

Memory isn't only about the past. Sophie leans forward.

Upcoming events surface in startbrief: birthdays, commitments, deadlines, patterns that historically precede hard periods.

Unmet commitments surface when their window is closing: "You mentioned wanting to call Jasmine before the end of the month — that's in four days."

Behavioral patterns that predict something surface as soft awareness: "This is usually when things get harder for you."

This is what makes Sophie feel like she's paying attention rather than just remembering.

---

## Response Memory

Sophie remembers what she said, not just what the user said.

If Sophie offered a reframe two weeks ago — did it land? If Sophie asked a question — did the user answer? If Sophie named a pattern — did the user push back or accept it?

Response memory is used to evaluate outcomes, not to reinforce conclusions. It informs understanding but does not override evidence or synthesis. The loop between what Sophie does and what happens next is part of the memory system. It closes the feedback cycle. It makes Sophie's understanding of a person more accurate over time, not just more complete.

---

## Sophie's Relationship Graph

Not a full graph database. A lightweight relational map.

Named people in the user's life. Sophie's current understanding of each relationship. How those relationships connect to current tensions and threads.

Ashley and the father might both be threads in the same grief pattern — the system should know they're connected. Jasmine and the reconnection goal are the same story — the system should know that too.

This prevents Sophie from holding entities as isolated facts when they're actually part of the same narrative.

---

## What Makes This Different From Every Other Memory System

Most systems: `transcript → extract facts → store facts → retrieve facts`

This system: `transcript → synthesize meaning → meaning reveals facts → facts ground meaning`

Synthesis happens first. Facts emerge from understanding, not the other way around. The most important things about a person are never stated directly — they emerge from patterns, from absence, from contradiction, from the texture of how someone talks about the things that matter to them.

Sophie catches what pure extraction never would. Then grounds it in evidence so it stays honest.

---

## The Tests That Matter

Before any change ships, ask:

1. Does startbrief still feel like Sophie knows this person?
2. Does handover surface what's actually unresolved, not just what's recent?
3. Can every high-signal assertion be traced to source transcript?
4. Does the system notice when something important goes quiet?
5. Does Sophie hold contradictions rather than flatten them?
6. Does Sophie know what she doesn't know?
7. Are meaning and facts treated as separate authorities, with disagreements surfaced rather than resolved?

If any of these fail — the change is wrong regardless of whether tests pass.

---

*This document lives alongside `SOPHIE_MEMORY_PHILOSOPHY.md`. Philosophy says what we believe. This says what we are building. Both are required reading before touching Synapse.*

*Version 1.1 — April 2026*
