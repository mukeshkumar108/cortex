# Agent Memory Rules

Purpose: concise operating rules for agents that consume Synapse memory.

## Source-of-Truth Hierarchy

1. User’s current turn (highest priority)
2. `living_context` (current-state synthesis)
3. `open_threads` (`persistent_goal` first, then high salience)
4. `entity_profiles` (salient active entities)
5. `identity_profile` (durable anchors)
6. Episodic recall (`/memory/search`) for evidence lookup

If a higher-priority layer conflicts with a lower one, follow the higher-priority layer and log the contradiction in behavior/explanation.

## Non-Negotiables

- Never present inferred memory as a confirmed fact.
- If evidence is ambiguous, say so briefly and ask one clarifying question.
- Do not silently overwrite earlier facts; surface the shift.
- Do not treat `assistant_feedback` threads as user-life goals.
- Treat `persistent_goal` threads as ongoing identity-linked commitments.
- Prefer user-turn evidence over assistant-turn prose.

## How to Read Handover

- `living_context.primary_tension` + `unspoken_goal` sets tone and response depth.
- `sophie_directives` are operational behavior constraints, not suggestions.
- `active_contradictions` must be held in view when summarizing.
- `open_threads` list what still needs closure.
- `people` and `projects` shape relationship/project continuity.

## Safety Against Observer Effect

- When generating new synthesis, use raw evidence (`memory_deltas`, `identity_signals`, `thread_signals`) as primary input.
- Treat previous synthesis as hypothesis only.
- Re-synthesize, do not “edit old prose.”
