# Synapse Human Overview

## 1. One-sentence explanation
Synapse is the background intelligence layer that helps companion apps remember, understand, connect, and surface what matters at the right time.

## 2. What problem Synapse solves
Normal chat memory usually just saves a transcript or extracts a flat list of facts (e.g., "User likes coffee", "User has a dog"). But storing facts and retrieving them is not enough to create a truly helpful companion.
*   **Facts lack context:** Knowing "User likes coffee" doesn't tell the AI *when* to offer to order one.
*   **Retrieval is reactive:** You only get answers if you ask the exact right question.
*   **Assistants need judgment:** They need to know what is urgent, what is a passing mood, and what requires asking for permission before taking action.

Synapse exists to solve the "What matters now?" problem. It tracks the relationships between people, tasks, and state, so the companion knows exactly when to step in and when to stay quiet.

## 3. The big product idea
Synapse is not the app you talk to; it is the brain behind it.
*   **Sophie** is just one companion (wrapper) that uses Synapse.
*   **Future Companions** could include Ashley (a ruthless ops assistant), an elderly healthcare monitor, or a chronic health companion.
*   **Wrappers provide the face:** They handle the persona, the interface (voice/text), and the external tools (sending emails).
*   **Synapse provides the continuity:** It manages discovery, state, timing, and action readiness.

The long-term moat of the product is not the wrapper—anyone can build a chat interface. The moat is the deeply structured, safely managed background intelligence that Synapse provides.

## 4. What "magic moments" mean
A "magic moment" is when the AI companion does exactly the right thing without being explicitly asked, because it connected the dots in the background.

**Examples:**
*   *"You mentioned Ashley was unwell and had an event takedown last night — did that get sorted?"*
*   *"Your doctor appointment is tomorrow. Want a symptom summary?"*
*   *"You have an unpaid invoice that has been open for a week. Want me to draft a gentle chase?"*
*   *"You seemed fried after four hours of intense work. Eat, walk, or ship one small thing?"*

To create these magic moments reliably (and safely), Synapse needs:
1.  **Objects:** The things that exist (Ashley, the Event, the Invoice).
2.  **Links:** How they connect (Ashley is *involved in* the Event).
3.  **Evidence:** The exact conversation where this was mentioned.
4.  **Timing:** The window when it makes sense to ask (e.g., the morning after).
5.  **Companion Profile:** Knowing whether the current AI is allowed to act on this.
6.  **Surfacing & Action Policy:** Knowing whether to just *suggest* something or actually *do* it.

## 5. The current architecture in plain English
Right now, the system works like this:
1.  **Conversations come in:** You talk to Sophie.
2.  **Synapse runs extraction passes:** In the background, it reads the conversation and pulls out pieces of information.
3.  **It identifies categories:** It looks for people, new tasks, changes in your mood, ongoing storylines (threads), and deep identity facts.
4.  **Workers synthesize:** Background jobs clean up the data, consolidate duplicates, and figure out your "living context" (how you're doing right now).
5.  **Serving endpoints expose context:** When Sophie needs to reply, she asks Synapse for a "start brief" or a "signals pack" to know what's going on.

*Note: We are currently in a transition phase. Old systems (like FalkorDB/Graphiti) and new systems (Postgres V2) are running at the same time while we migrate.*

## 6. The new mental model
We are moving to a unified, graph-like mental model:
*   **Synapse Objects:** The core things the system knows about (a person, a task, a goal).
*   **Domains:** The 9 primary categories these objects fit into.
*   **Links:** The relationships between objects (the "connective tissue").
*   **Evidence:** The exact turns in the chat that prove a fact or link.
*   **Attention Items:** The generated decisions of "what deserves surfacing right now."
*   **Companion Profiles:** The rules dictating who is asking and what they are allowed to do.
*   **Packaging:** The final formatted payload sent to the companion app.

**Flow:**
`Conversation → Extraction → Objects → Links → Evidence → Attention → Companion Profile → Surface / Action`

## 7. The 9 domains
Every object belongs to one of these core domains:
1.  **Profile:** Stable facts about who you are. *(e.g., "Vegan", "Direct communication style")*
2.  **People:** Your social and professional graph. *(e.g., "Sarah", "Dr. Smith")*
3.  **Goals:** Long-term aspirations. *(e.g., "Improve cardiovascular health")*
4.  **Workstreams:** Active projects and ongoing situations. *(e.g., "Q2 Product Launch", "Recovering from surgery")*
5.  **Habits / Routines:** Repeated behaviors. *(e.g., "Daily meditation")*
6.  **Obligations:** Tasks, to-dos, and follow-ups. *(e.g., "Call the insurance company")*
7.  **Events:** Time-anchored occurrences in the calendar. *(e.g., "Flight to Tokyo at 10 AM")*
8.  **State:** Your "Right Now" context—mood, stress, focus. *(e.g., "Feeling overwhelmed today")*
9.  **Opportunities:** Proactive suggestions and drafts waiting for your approval. *(e.g., "Draft an email to Mark")*

## 8. What is currently messy / being fixed
Synapse works today, but it grew organically and got messy. 
*   **Legacy loops:** We still use old tables like `loops` and `user_model` which overlap with the new domains.
*   **Fragmented candidates:** Opportunities are split across 4 different tables (actionable, follow-up, clarification, recent_change).
*   **Inconsistent metadata:** Some objects don't have standard fields like `confidence` or `salience`.
*   **Underdeveloped Links:** We tracked some relationships, but they lived in hidden arrays instead of a unified graph table.
*   **Un-unified Attention:** Synapse doesn't have a single "brain" for deciding what to surface; it's scattered across endpoints.

## 9. What we have fixed recently
We just completed a major stabilization phase:
*   **Attention Preview (V1):** Built an internal endpoint (`/internal/debug/attention`) that aggregates candidates into a unified queue of Attention Items. It does NOT replace baseline memory; it sits on top of it.
*   **Companion Filtering:** The attention preview now filters items based on static Companion Profiles (e.g., hiding emotional state from an ops-only assistant).
*   **Object Links Phase 1:** Upgraded the `memory_relationship_links` table to support source/target domains, status, and expiry times.
*   **Internal auth hardening:** Secured endpoints so memory can't be easily contaminated.
*   **Runtime split:** Separated the fast API from the heavy background workers to prevent crashes.

## 10. Where Synapse is going next
The immediate roadmap:
1.  **Make object links real:** Fully migrate to the unified `object_links` table.
2.  **Build Attention preview:** Create a read-model that gathers all "surfacable" items into one queue.
3.  **Define Companion Profiles in code:** Let the API filter attention items based on who is asking (Sophie vs. Ashley).
4.  **Create magic moment fixtures:** Build tests that prove the end-to-end magic moments work perfectly.
5.  **Retire legacy paths:** Slowly delete the old FalkorDB and `loops` code.

## 11. What success looks like
Success means Synapse can always accurately answer **"What matters now?"** and explain exactly **why**. It means the system surfaces safe, timely prompts and avoids being annoying, creepy, or stale. Ultimately, success is when multiple entirely different AI companions can plug into the exact same Synapse backend and behave perfectly according to their unique personas.

## 12. Glossary
*   **Synapse:** The background intelligence backend.
*   **Sophie:** The primary empathetic personal companion app (a wrapper).
*   **Companion / Wrapper:** The frontend app, persona, and tool-executor that talks to the user.
*   **Object:** A durable concept tracked by Synapse (a person, a task, a state).
*   **Domain:** The high-level category of an Object (e.g., People, Obligations).
*   **Link:** A typed directional relationship connecting two Objects.
*   **Evidence:** The chat transcript or external event that proves an Object or Link exists.
*   **Attention Item:** A dynamic decision that something needs to be surfaced or actioned right now.
*   **Companion Profile:** The rules governing what a specific companion can see and do.
*   **Magic Moment:** A perfectly timed, highly contextual proactive action by the AI.
*   **Candidate:** An extracted fact or task that hasn't been confirmed yet.
*   **Snapshot:** A temporary, derived view of state (like `living_context`) that is regenerated often.
*   **Legacy table:** Old database tables (like `loops`) slated for deletion.
