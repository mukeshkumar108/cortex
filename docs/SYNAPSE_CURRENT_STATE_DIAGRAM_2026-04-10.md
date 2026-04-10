# Synapse Current State Diagram (2026-04-10)

One-page view of what exists today.

```text
                                Runtime / Orchestrator
                                         |
              +--------------------------+--------------------------+
              |                                                     |
   Session start + continuity                              On-demand memory recall
   GET /session/startbrief                                POST /memory/query
              |                                                     |
              v                                                     v
      +----------------+                                   +------------------------+
      | Startbrief     |                                   | Intent router          |
      | assembler      |                                   | exact | episodic | hybrid
      +----------------+                                   +------------------------+
              |                                                     |
              | reads                                               | reads
              v                                                     v
  +-------------------------+                            +------------------------------+
  | Postgres (operational)  |                            | Graphiti/Falkor (semantic)   |
  | - loops                 |                            | - canonical nodes             |
  | - user_model            |                            |   person/project/goal/loop/   |
  | - startbrief_history    |                            |   preference/event            |
  | - daily_analysis        |                            | - canonical edges             |
  +-------------------------+                            |   related_to_user_as,         |
              ^                                          |   working_on, pursuing,       |
              |                                          |   about, prefers, involved_in,|
              |                                          |   cares_about, evidence_for   |
              |                                          | - episodic nodes              |
              |                                          | - internal labels (filtered)  |
              |                                          |   SessionSummary, Tension,    |
              |                                          |   Observation, Environment... |
              |                                          +------------------------------+
              |                                                     ^
              |                                                     |
              |                                  candidate merge + ranking + guardrails
              |                                 (embedding + lexical + recency + entity
              |                                  overlap + continuation + weakRecall)
              |                                                     |
              +---------------------------+-------------------------+
                                          |
                                          v
                            Response payload to backend:
                            - exact: factItems + entities
                            - episodic: episodes (summary/evidence/linkedEntities)
                            - hybrid: both
                            - metadata: weakRecall, queryProfile, ranking audit,
                              embeddingCoverage, provenance/domain signals


Write path (durability + enrichment):

POST /ingest or POST /session/ingest
            |
            v
  Postgres session_transcript + session_buffer
            |
            v
      graphiti_outbox jobs
            |
            v
      Graphiti ingest:
      - raw Episodic node write
      - post-ingest hooks (session summary / open loops / user model delta / daily analysis)
            |
            v
  Episodic embeddings in Postgres (episodic_memory_embeddings)
  from transcript windows (used by episodic/hybrid retrieval)
```

## Reading map
- Truth for canonical identity/relationships: Graphiti canonical nodes + edges.
- Durability/replay/evidence substrate: Postgres transcripts.
- Episodic semantic retrieval substrate: Postgres `episodic_memory_embeddings` + Graphiti episodes.
- Procedural continuity substrate: Postgres loops + user model (+ Graphiti support signals).
