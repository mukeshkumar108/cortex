# Core Principles

## 1. Synapse owns user-state and semantic intelligence
Synapse is the canonical layer for interpreted user-state and semantic understanding across time.

## 2. Sophie is the runtime orchestrator
Sophie decides what should happen next in runtime, interacts with the user, calls tools, and writes outcomes back into Synapse.

## 3. Synapse provides the attention/policy synthesis bridge
Synapse does not stop at extraction/storage. It also synthesizes coherent, ranked, context-aware views (for example daily intelligence packets and derived attention moments) that guide Sophie's runtime behavior.

## 4. External systems own native operational artifacts
External platforms own their own artifacts. For example: Google Calendar owns Google Calendar events.

## 5. Canonical placement rules
- Sophie-native actions may live canonically in Synapse as `action_items`.
- For external artifacts, Synapse stores references, projections, and interpreted state, not duplicated operational truth.

## 6. Privacy boundary
Private sources can influence behaviour, but must not become conversational evidence.

## 7. Retention boundary
Raw private data is ephemeral. Derived state is durable and minimized.

## 8. Evidence requirements
Every state object must carry provenance, confidence, timestamps, and freshness.

## 9. Uncertainty is explicit
Uncertainty, conflict, and staleness must be explicit state, not hidden behavior.

## 10. Ownership discipline
Sophie does not maintain a separate long-lived competing user-state store.
