# Synapse Product Baseline (Do Not Regress)

## 1. What already works (must be preserved)

The current synthesis system produces high-quality outputs in:

* Identity understanding (who the user is, values, patterns)
* Living context (what is going on right now, tensions, emotional state)
* Thread tracking (ongoing concerns, goals, unresolved items)
* Relationship awareness (who matters and why)
* Handover/start-of-session continuity

These outputs are considered **high quality and product-critical**.

They must not degrade during v2 migration.

---

## 2. What is NOT allowed

The following changes are forbidden unless explicitly approved:

* Rewriting identity, living context, or thread synthesis from scratch
* Removing or weakening observer-effect protection
* Replacing multi-pass synthesis with single-pass simplification
* Dropping emotional/tension/context signals for "cleaner" structure
* Converting rich synthesis into flat structured-only outputs

---

## 3. What we ARE changing

We are introducing a canonical memory layer:

* Claims (truth layer)
* Claim evidence
* Predicate policy
* Deterministic lifecycle (active/superseded/retracted)
* Mutation log + watermarks

This layer exists to:

* prevent factual drift
* ensure determinism
* improve retrieval correctness

---

## 4. Architectural rule

Canonical layer = truth
Projection layer = interpretation

Projection systems:

* may be wrong
* may change
* must never be treated as factual authority

---

## 5. Migration strategy

We are NOT rebuilding the system.

We are:

1. Building a canonical truth layer underneath
2. Re-anchoring existing synthesis outputs onto that layer
3. Only rewriting synthesis components if:

   * output quality is poor, OR
   * they cannot work with canonical inputs

---

## 6. Definition of success

Success is NOT:

* cleaner schema
* more structured data
* more "correct" architecture

Success IS:

* better continuity
* fewer wrong assumptions
* better tone and timing
* preservation or improvement of current synthesis quality
