# Sophie Always-On Memory Packet Spec

Version: 2026-04-23  
Status: Proposed next implementation

This document defines the compact always-on memory packet that Synapse should prepare for Sophie runtime.

It exists because:
- raw memory tables are too low-level for every-turn use
- `startbrief` and `handover` are useful, but not ideal as the always-injected packet
- Sophie runtime needs a small, stable, human-readable orientation layer on every request
- that layer should be prepared ahead of time, not rebuilt from scratch per session

This packet is not the memory authority.

The authority remains:
- explicit user-provided truth
- evidence-backed derived memory
- conservative synthesis built from those sources

The packet is a prepared serving artifact built from that authority.

---

## 1. Core Purpose

The always-on packet should do four things:

1. orient Sophie immediately at the start of every request
2. reduce rediscovery and repeated memory querying
3. give the model a compact sense of who this person is and what matters right now
4. stay conservative enough that it helps rather than distorts

In plain language:

Sophie should begin a conversation already knowing the important shape of the person, not rediscovering it from scratch every time.

---

## 2. Design Principles

These are non-negotiable.

### 2.1 The Packet Is Prepared, Not Improvised

The packet should be built in Synapse ahead of time and cached by Sophie backend.

It should not be assembled ad hoc on every request.

### 2.2 The Packet Is Small

Target:
- around `500` tokens

Not a brittle hard cap:
- size should come from good selection and good rewriting
- weak or low-value content should be dropped before truncation is used

### 2.3 The Packet Is Rewritten, Not Dumped

The packet should not be produced by simply joining fields and facts together.

Correct shape:
- deterministic collection of trusted inputs
- curated section inputs
- constrained LLM rewrite for compression and coherence

The LLM may:
- compress
- phrase
- remove duplication
- improve readability

The LLM may not:
- invent
- dramatize
- infer hidden motives
- flatten uncertainty into certainty

### 2.4 Explicit User Truth Wins

If the user has explicitly provided something through:
- onboarding
- settings/profile page
- structured voice activity / voice onboarding
- direct correction

that should outrank inferred memory.

If explicit truth is missing, the packet should fall back to derived memory.

### 2.5 Omission Is Better Than Drift

If a field is unclear:
- omit it
- or place it in an uncertainty section

Do not fill gaps with cleverness.

---

## 3. Source Priority

This is the most important rule in the spec.

For every field or section input, use this precedence:

1. explicit user-provided truth
2. explicit user corrections
3. strong durable derived memory
4. conservative synthesis from trusted derived state
5. omit if still unclear

Do not let lower-priority synthesis overwrite higher-priority explicit truth.

Examples:
- if settings says the user's name is `Kaiser`, use that
- if onboarding says location is `Cambridge`, use that
- if there is no explicit location, use strong derived location only if evidence is strong enough
- if relationship role was corrected explicitly, that correction wins over old inference

---

## 4. Proposed Truth Lanes

The packet should merge from three lanes.

### Lane A: Explicit Profile Truth

This is the highest-priority lane.

Expected sources:
- onboarding form
- settings/profile page
- explicit user-editable profile
- structured onboarding voice session
- direct user corrections captured explicitly

Typical fields:
- preferred name
- age
- location
- pronouns
- faith / religion if explicitly provided
- family roles if explicitly provided
- important projects if explicitly provided
- stated communication preferences

This lane should be treated as:
- declared truth
- user-controlled
- highest priority

### Lane B: Derived Durable Memory

This is the main fallback and the main meaning layer.

Expected sources:
- `identity_profile`
- `living_context`
- `entity_profiles`
- `open_threads`
- `memory_contradictions`
- `low_confidence_items`
- retrospective worker outputs

This lane should be treated as:
- evidence-backed
- trusted
- still subordinate to explicit user-provided truth

### Lane C: Packet Rewrite Layer

This is not an independent truth source.

It is the rendering layer that turns curated inputs into a compact readable packet.

It should never become a second authority.

---

## 5. Packet Shape

Keep the structure simple and stable.

Recommended sections:

### 5.1 Enduring Identity

What Sophie should almost always know.

Possible content:
- preferred name
- location
- faith / worldview if explicit or strongly durable
- major life roles if explicit or strongly durable
- founder / builder / parent / writer / believer / operator style if strongly supported
- one compact statement of who this person is in stable terms

This section should answer:
- who is this person across time?
- what kind of mind or role-set do they have?
- what durable commitments shape how they move through the world?

### 5.2 Important People

Who matters in this person's world, and why.

Possible content:
- daughter / son / partner / parent / close collaborator / other durable anchors
- one-line reason they matter
- one-line current state only if strongly supported and useful

Do not fill this with weak entities or social clutter.

### 5.3 Work And Building

What they are building or carrying professionally.

Possible content:
- major current projects
- founder/builder role if strongly supported
- important work context that shapes conversation
- major intellectual or creative commitments if strongly supported

This section should answer:
- what are they working on?
- what are they building or trying to make real?

### 5.4 Current Chapter

What is active right now.

Possible content:
- active threads
- current focus areas
- important transitions
- major constraints or unresolved work that are currently shaping conversation

This should feel current, not timeless.

Do not let current chapter overwrite enduring identity.

### 5.5 Handle Carefully

What Sophie should approach with care.

Possible content:
- sensitive relationship areas
- open contradictions that materially affect interpretation
- user-explicit corrections
- places where pressure, overconfidence, or forced inference would be harmful

This section should be short and practical.

### 5.6 Open Questions

Only the most important unresolved uncertainty.

Possible content:
- one or two unresolved high-value uncertainties
- only if behaviorally relevant

This is optional and should often be empty.

---

## 6. What The Packet Must Not Become

Do not let it become:
- a therapy note
- a personality essay
- a dramatic narrative
- a giant biography
- a fact dump
- a stale cached story blob

Blocked failure modes:
- motive inference
- emotional over-reading
- inferred feeling-state presented as durable identity
- flattering but unsupported language
- compressed contradictions presented as fact
- repeated duplicated facts in multiple sections

---

## 7. Suggested Output Format

Keep both a structured and a text form.

### 7.1 Stored Structured Form

Suggested shape:

```json
{
  "version": "always_on_memory_packet.v1",
  "generated_at": "2026-04-23T10:00:00Z",
  "source_fingerprint": "hash",
  "profile_truth_used": true,
  "sections": {
    "enduring_identity": [],
    "important_people": [],
    "work_and_building": [],
    "current_chapter": [],
    "handle_carefully": [],
    "open_questions": []
  },
  "packet_text": "..."
}
```

### 7.2 Runtime Injected Form

Runtime should usually inject the compact `packet_text`, not the full structured payload.

Structured sections should remain available for:
- debugging
- review
- future ranking logic
- controlled section-level rendering if needed later

---

## 8. Packet Build Flow

The correct build flow is:

1. collect explicit profile truth
2. collect strong derived memory inputs
3. merge by source priority
4. rank section candidates
5. prune weak/duplicate/low-value items
6. send the curated packet to a constrained summarizer
7. validate and store the final packet
8. push/update Sophie backend cache

The critical point:
- collection is deterministic
- rewrite is constrained
- validation is strict

---

## 9. Ranking And Pruning Rules

The packet should be compact because it selects well, not because it truncates late.

Prefer including:
- explicit profile truth
- durable anchors
- high-importance active threads
- current high-signal context
- behaviorally relevant uncertainty

Prefer dropping:
- weak one-off facts
- stale low-salience entities
- duplicate project descriptions
- low-value old context
- dramatic phrasing

If section space is tight, priority should be:

1. explicit user truth
2. enduring identity
3. durable relationships
4. major work/project context
5. current chapter
6. uncertainty / careful handling

---

## 10. Rewrite Constraints

The packet rewrite prompt should explicitly enforce:

- plain direct language
- no motive inference
- no therapeutic framing
- no personality verdicts
- no dramatic or poetic language
- no unsupported emotional conclusions
- preserve uncertainty when evidence is mixed
- prefer omission to overreach
- answer "who is this person?" before "what is hard right now?"
- separate enduring identity from the current chapter
- do not center the packet on rupture, fragility, or mood unless the user explicitly made that central

The rewrite step should be closer to:
- editorial compression

not:
- interpretive synthesis

---

## 11. Refresh Triggers

The packet should not be regenerated on every request.

Regenerate when one of these happens:

1. explicit profile truth changes
- onboarding completed
- settings updated
- user correction applied

2. material Pass 4 change
- identity profile changed in a meaningful way

3. material Pass 5 change
- living context changed in a meaningful way

4. retrospective worker changes important state
- durable anchor reinterpreted
- low-confidence item resolved in a behaviorally relevant way
- contradiction resolved
- major tentative entity promoted

5. safety fallback refresh
- periodic refresh if stale, even without explicit signal

Do not refresh for tiny meaningless churn.

---

## 12. Sophie Backend Cache Model

This packet should be cached by Sophie backend.

Correct flow:

`Synapse prepares packet`
-> `Synapse stores latest artifact`
-> `Synapse sends updated packet to Sophie backend`
-> `Sophie backend caches latest packet`
-> `runtime injects cached packet per request`

That is better than:
- querying Synapse every turn
- rebuilding packet per turn
- assembling packet ad hoc at runtime

### Cache behaviour

Sophie backend should:
- keep the latest packet per user
- replace it only when Synapse sends a newer version
- use version/fingerprint checks to avoid redundant updates

---

## 13. Validation Rules

Before a rebuilt packet is accepted:

- it must stay compact enough for always-on use
- it must not contain banned interpretive language
- it must not contradict explicit profile truth
- it must not flatten live uncertainty into fact
- it must not include low-support stale debris
- it must remain sectionally coherent

If validation fails:
- keep the previous packet
- flag the rebuild for review/logging

Do not replace a good packet with a noisy one.

---

## 14. Minimum V1 Build

Build the smallest safe version first.

V1 should include:
- source priority merge
- explicit profile truth lane
- compact packet sections
- constrained rewrite
- packet artifact storage
- refresh triggers from major source updates
- push/update path to Sophie backend cache

V1 does not need:
- fancy per-section personalization
- many packet variants
- dynamic packet types by mode
- proactive queue logic

---

## 15. Relationship To Later Proactivity

This packet should come before proactive queues.

Why:
- it improves every conversation immediately
- it tests whether memory is actually legible to the runtime model
- it provides the baseline orientation layer queues will later build on

Queues answer:
- what should Sophie surface now?

The always-on packet answers:
- who is this person and what matters right now?

Both are needed.
The packet should come first.

---

## 16. One-Sentence Summary

The always-on memory packet is a small, cached, LLM-rewritten but evidence-grounded orientation artifact built from explicit user truth plus trusted derived memory, prepared in Synapse and pushed to Sophie backend so every conversation begins with stable real understanding instead of rediscovery.
