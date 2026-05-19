# Synapse Baseline Memory and Pipeline

**Status:** Current Architecture  
**Date:** 2026-05-19

---

## 1. Why baseline memory still matters

While "Attention Items" handle the *action* layer (when to prompt, what to draft, who to notify), Attention cannot exist without a foundation of truth. 

**Baseline memory answers:**
*   What do we know? (Facts, Profile)
*   Who/what matters? (Entities, Projects)
*   What is ongoing? (Workstreams, Open Threads)
*   What has changed? (Session Changes, Living Context)
*   What is durable? (Identity)

A flat list of facts is insufficient, but a system that only stores "reminders" is equally broken. A companion needs compressed context, episodic recall, and durable identity to hold a natural conversation, even when there is no urgent "Attention Item" to surface.

## 2. End-to-end flow

The Synapse pipeline is an asynchronous, multi-stage synthesis process:

```text
Conversation/session
  ↓ (Immediate Write Path)
Raw storage (sessions, turns, buffers)
  ↓ (Background: Triage & Setup)
Fast classification (Pass 1)
  ↓ (Background: Heavy Extraction)
Extraction passes (Pass 1.5, 2, 2b, 2c, 3)
  ↓ 
Candidates / Objects / Snapshots (Baseline Memory)
  ↓ (Background: Synthesis)
Durable Profile / Living Context (Pass 4, 5)
  ↓ (Serving/Read Time)
Context serving (startbrief, query)
  ↓
Attention Preview (Dynamic read-model queue)
  ↓
Companion Packaging / Action (Sophie, Ashley, etc.)
```

## 3. Immediate write path

When a companion sends data via `/session/ingest` or the legacy `/ingest`, the write path is optimized for speed and durability, not heavy synthesis.

1.  **Raw Transcript Storage:** `session_buffer` or `sessions_v2` / `turns_v2` immediately stores the raw messages.
2.  **Explicit Mutations:** If the companion uses explicit endpoints like `/actions/items`, those mutations are applied instantly to the database.
3.  **No immediate heavy LLM work:** The request returns immediately. The heavy extraction is deferred to the background worker.

## 4. Session close / derived pipeline

The deep intelligence extraction is handled by `src/derived_pipeline.py`. This pipeline is triggered by the background worker, typically scanning for sessions that have been recently closed or have sat idle past a threshold.

## 5. Pipeline pass table

| Pass | Name | Trigger | Input | What it extracts | Output tables | Immediate/Background |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Pass 1** | Triage | Session queued | Transcript | Memory worthiness, emotional weight, routing flags. | `session_classifications` | Background |
| **Pass 1.5** | Entities | Triage flags | Transcript | People, projects, organizations. | `entity_profiles`, `object_links` | Background |
| **Pass 2a** | Actionable | Triage flags | Transcript | Tasks, reminders, follow-ups. | `actionable_candidates` | Background |
| **Pass 2b** | Changes | Triage flags | Transcript | Shifts in state, mood, decisions. | `session_changes` | Background |
| **Pass 2c** | Entity Cand. | Triage flags | Transcript | Potential new/unconfirmed entities. | `entity_candidates` | Background |
| **Pass 3** | Threads | Ongoing / Delta | Transcript + Candidates | Storylines, unresolved situations, open loops. | `open_threads`, `object_links` | Background |
| **Pass 4** | Identity | Schedule / Delta | Extracted facts | Synthesis of durable user traits/values. | `identity_profile` | Background |
| **Pass 5** | Living Ctx | Schedule / Delta | Recent changes | Recent state, tension, emotional focus. | `living_context` | Background |

*Note: Passes 1-3 heavily utilize structured LLM outputs (JSON mode).*

## 6. Background loops

Outside of the core session-triggered pipeline, `synapse-worker` runs continuous asynchronous loops:

*   **Active / Core:**
    *   `idle_close`: Sweeps abandoned sessions into the pipeline.
    *   `proactive_shadow_candidates`: Synthesizes `follow_up_candidates` and `clarification_candidates` from recent changes.
    *   `v2_invariant_checker`: Self-healing data consistency checks.
    *   `derived_silence_detection`: Flags when entities/threads haven't been mentioned in a while.
*   **Legacy (Slated for retirement):**
    *   `daily_analysis`: Legacy daily summary loop.
    *   `user_model_updater` / `user_model_enrichment`: Legacy JSON-blob profile builders.
    *   `loop_staleness_janitor`: Legacy `loops` table cleanup.

## 7. Models and LLM usage

Synapse routes LLM calls primarily through `src/openrouter_client.py`.

*   **Generic / Heavy Extraction (Passes 1-3):** `google/gemma-4-26b-a4b-it` (Configured in `config.py` for derived pipeline) or `xiaomi/mimo-v2-flash`.
*   **Summary / Identity (Pass 4, 5, Startbrief Realizer):** `amazon/nova-micro-v1`.
*   **Fallback Model:** `mistralai/ministral-3b-2512`.
*   **Vector Embeddings:** `text-embedding-3-small`.

The pipeline is highly deterministic in its routing (checking DB states before calling the LLM), but relies on LLMs for the actual semantic extraction and synthesis.

## 8. Storage by role

| Role | Tables |
| :--- | :--- |
| **Evidence/Log** | `session_transcript`, `claims`, `action_audit_log`, `session_changes` |
| **Primary Objects** | `action_items`, `calendar_items`, `entity_profiles`, `open_threads`, `durable_profile_facts` |
| **Candidates** | `actionable_candidates`, `follow_up_candidates`, `clarification_candidates` |
| **Snapshots** | `living_context`, `identity_profile`, `always_on_memory_packets` |
| **Links** | `memory_relationship_links` (Phase 1 implemented: supports domains, status, temporal validity) |
| **Caches** | `identity_cache`, `episodic_memory_embeddings` |
| **Legacy** | `loops`, `user_model`, `graphiti_outbox` |

## 9. Baseline memory vs. Attention

*   **Baseline Memory (`open_threads`, `living_context`, `entity_profiles`):**
    "Ashley exists. She is part of the Event Takedown project. The user was feeling unwell yesterday."
*   **Attention (`/internal/debug/attention`):**
    "It is 9:00 AM. The user is starting a new session. Draft a gentle check-in asking if Ashley resolved the takedown, because the user was unwell."

Attention preview sits *on top* of baseline memory. It does not replace it. It queries objects, links, and candidates, applies Companion Profile constraints (e.g., hiding emotional items for the 'ashley_ops' profile), applies temporal expiry logic, and outputs an actionable queue.

## 10. Fast handover / quick new session behaviour

**Problem:** If a user closes a session and starts a new one quickly, the deep synthesis passes may not have finished yet. That creates a continuity gap: the companion can miss the obvious "where we just left off" cues even though the raw transcript already exists.

**Fast Handover Packet:**
Synapse now writes a short-lived `session_handover_packets` record on the fast session path. It is generated directly from the current session transcript, without waiting for Pass 3/4/5.

What it contains:
*   `summary`
*   `open_questions`
*   `unresolved_decisions`
*   `pending_actions`
*   `recent_state_note`
*   `important_people`
*   `active_topics`
*   `do_not_overdo`
*   `source_turn_refs`

Why it exists:
*   It bridges the immediate continuity gap.
*   It gives Sophie or another companion a bounded "pickup point" before deep synthesis catches up.
*   It is explicitly **not** durable identity, living context, or long-term profile memory.

How it differs from Pass 4 / Pass 5:
*   **Fast Handover:** immediate, deterministic, transcript-local, short TTL, conservative.
*   **Pass 4 Identity:** durable synthesis of who the user is across sessions.
*   **Pass 5 Living Context:** broader recent-state synthesis across multiple sessions and recent changes.

Expiry rules:
*   Default TTL is short-lived: currently ~18 hours.
*   Expired packets are ignored by default when retrieving the latest packet.
*   Recent state is treated as provisional and should be re-checked before acting on it.

Current limitations:
*   It uses deterministic heuristics, not deep semantic synthesis.
*   It should capture obvious next actions and open questions well, but it will miss subtler narrative continuity.
*   It intentionally avoids turning flippant or momentary mood statements into durable truth.
*   It is retrievable for internal/debug continuity checks; it is not delivery automation.

## 11. Serving surfaces today

| Endpoint | Purpose | Status |
| :--- | :--- | :--- |
| `/session/startbrief` | Canonical startup packet for stateless continuity. | Active. Serves baseline memory context. |
| `/signals/pack` | Compact proactive steering hints. | Active. |
| `/memory/query` | Targeted recall (factual, episodic). | Active. |
| `/internal/debug/attention` | Read-only dynamic queue of Attention Items. | **Active (v1).** Filters by Companion Profile, hides expired items, supports `includeExpired=true`. |
| `/actions/items` | Mutations on actionable objects. | Active. |
| `/user/model` | Synthesized durable user profile. | Legacy. Being replaced by Identity/Profile passes. |

## 12. What is still missing

*   **Outcome Feedback:** The attention preview generates items, but there is no closed-loop tracking (e.g., `POST /attention/{id}/outcome`) to record if the companion actually surfaced it and if the user engaged.
*   **Candidate Fragmentation:** `actionable_candidates`, `follow_up_candidates`, etc., still exist as separate tables rather than being unified into primary objects with `status='detected'`.
*   **Missing Metadata:** Some primary objects still lack `primary_domain` enforcement.
*   **Evidence Refs in Attention Preview:** While `source_object_ids` and `source_link_ids` are mapped best-effort, exact turn-level `evidence_refs` are sometimes lost in the candidate abstraction layer.
*   **Graph-backed Attention:** The attention read-model is live, but it largely aggregates candidate tables rather than doing deep recursive graph walks on `object_links`.

## 13. Recommended next implementation priorities

1.  **Attention Outcome Tracking:** Implement the feedback loop (`surfaced`, `dismissed`, `acted_upon`) to prevent the companion from nagging the user with the same attention items.
2.  **Session Handover Audit:** Formally implement the short-lived handover packet to guarantee continuity during rapid session restarts.
3.  **Natural-Language Debug Query:** Build a tool to ask Synapse "Why did you generate this attention item?" using the evidence graph.
4.  **Table Consolidation:** Merge the fragmented `_candidates` tables into their respective primary domains using the `status='detected'` lifecycle stage.
5.  **Gradual Legacy Retirement:** Turn off the legacy `daily_analysis` and `user_model` loops once the V2 endpoints fully cover their product surface area.
