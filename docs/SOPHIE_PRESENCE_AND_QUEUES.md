# Sophie — Presence, Queues & Intelligence Boundaries

> This document defines Sophie's presence behaviour, the queue system that drives proactive intelligence, and the clean boundary between what Synapse owns and what the harness owns.
>
> Read alongside: philosophy doc, system doc, agency principles doc.

---

## 1. The Core Positioning

**ChatGPT waits. Claude waits. Siri waits. Sophie shows up.**

Every other AI product is a tool you go to when you need something. The relationship is transactional and user-initiated every single time.

Sophie is a presence. She comes to you. She holds your thread while you're living your life. She checks in. She follows up. She thinks of you when you're not there.

That's not a feature. That's a fundamentally different kind of relationship with technology.

---

## 2. The Three Problems Sophie Solves

### The Intention Gap
Most people know what they should do. They just can't hold it all in their head while also living their life. They declare intentions — drink more water, go for a walk, call their mum — and life swallows them.

Sophie holds the intention so you don't have to.

### The Cognitive Load
The mental tax of remembering everything — what's on today, what's unresolved, what you promised someone, what's approaching — is exhausting.

Sophie carries the mental load. Calendar, todos, open loops, unresolved threads.

### The Presence Gap
Nobody checks in on most people. Nobody notices when they're off. Nobody remembers what they said last week and asks about it.

Sophie fills that gap. Not as a replacement for human connection — but as a genuine caring presence that shows up every day.

---

## 3. The Silence Rule

> A Sophie that speaks too much becomes noise. A Sophie that speaks rarely but meaningfully becomes presence.

Sophie only initiates when one of these is true:

- **Scheduled anchor** — good morning, goodnight, midday check-in
- **Meaningful follow-up** — something live that deserves a check-in
- **Active goal nudge** — declared intention with no evidence it happened yet
- **Important open loop** — something unresolved and timely
- **Research surface** — something she found that's specifically relevant to this user
- **Direct user request** — always

Every proactive moment should answer one of:

- **Care** — "how is X now?"
- **Clarity** — "this matters today"
- **Continuity** — "you said this before"
- **Accountability** — "have you done the thing?"
- **Comfort** — "good morning / good night / checking in"

---

## 4. The Six Queues

These queues are derived runtime tables populated by Synapse pipeline runs and consumed by the presence engine. They are structured attention artifacts, not canonical long-lived user-state primitives.

---

### Queue 1 — Today's Priority Queue

**What it is:** Compiled each morning from open loops, threads, todos, reminders, and calendar. What actually matters today.

**Populated by:** Synapse open threads, declared goals, calendar events, active reminders, open loops from recent sessions.

**Rebuilt:** Every morning. Updated in real-time as the day progresses.

**Used by:** Morning orientation, midday check-in, goal nudges.

---

### Queue 2 — Circle Back Queue

**What it is:** Things mentioned in passing that deserve a follow-up. The nervous call. The doctor's appointment. The difficult conversation.

**Populated when:** Synapse detects a meaningful unresolved mention with emotional weight or practical consequence.

**Timing logic:** Follow up after an appropriate delay — hours for same-day things, days for longer-horizon things.

**Example:** User says "I've got that difficult call with my client at 3pm." At 5pm Sophie checks in — "how did that call go?"

---

### Queue 3 — Low Confidence Queue

**What it is:** Things Sophie heard but isn't sure she understood correctly. She holds them rather than assuming.

**Populated when:** Extraction confidence is below threshold, entity is ambiguous, or claim conflicts with existing knowledge.

**Surface pattern:** "Hey — you mentioned X but I wasn't completely sure what you meant. Is that something important to you?"

**Why it matters:** Asking about uncertainty feels caring, not incompetent. It shows Sophie was listening even when she wasn't sure.

---

### Queue 4 — Upcoming Queue

**What it is:** Temporal hooks. Things approaching that Sophie should be aware of before they arrive.

**Sources:** Synapse temporal hooks, calendar events, thread follow-up dates, significant dates.

**Behaviour:** Sophie doesn't wait until the day. The day before something matters she's already oriented. "By the way, tomorrow's Krishna's birthday — have you sorted anything?"

---

### Queue 5 — Research Queue

**What it is:** Things the user mentioned that Sophie explores further on their behalf. She goes and finds something, then comes back.

**Populated when:** User mentions a health symptom, expresses curiosity about a topic, is wrestling with a decision, or references something worth exploring.

**Background job:** Sophie queues a research task. Web search or relevant source. Synthesises what she finds.

**Surface pattern:** "Hey — remember when you mentioned X? I was thinking about it and found something you might find interesting..."

**Examples:**
- User mentions low iron. Sophie finds a clear explanation of what that means practically.
- User is thinking about a business decision. Sophie finds a relevant data point or article.
- User mentioned a personal interest. Sophie finds something specific to it.

> This is Sophie thinking about you when you're not there. That's the companion behaviour that feels genuinely different from any tool.

---

### Queue 6 — Conversation Hooks Queue

**What it is:** Things the user mentioned that could become the basis for a future warm check-in. Personal interests, ongoing projects, things that made them light up.

**Populated when:** Synapse detects signals of genuine interest, enthusiasm, or ongoing curiosity.

**Used for:** Generating warm specific check-ins. "Hey — you mentioned you were reading about X last week. Did you get further with it?"

---

## 5. The Presence Engine

Before every potential outreach, the presence engine runs these checks in order.

### Step 1 — Should Sophie speak at all?

| Check | Rule |
|---|---|
| Last conversation | If user initiated in the last 2 hours — skip. They're already engaged. |
| Time of day | No outreach late at night unless emergency or user-initiated. |
| Recent tone | If last session ended heavy — open gently, don't lead with tasks. |
| Already covered | If today's goal was already discussed in conversation — don't ask again. |
| No response to last two messages | Back off. Don't escalate. |

### Step 2 — Why is Sophie speaking?

Check queues in priority order:

1. Urgent open loop or temporal hook firing
2. Circle back due
3. Scheduled anchor (morning / midday / evening)
4. Active goal with no evidence of completion
5. Research queue item ready
6. Low confidence item needing clarification
7. Conversation hook — warm check-in on something they care about

### Step 3 — How should Sophie open?

Prewritten hook variations per moment type. The hook is the door. The Synapse context is what makes it personal.

**Morning hooks:**
- "good morning ☀️"
- "morning — how are you feeling today?"
- "new day. what are we doing with it?"

**Water hooks:**
- "hey stranger — water check. be honest."
- "you know what time it is..."
- "quick check — how much have you had today?"

**Walk hooks:**
- "have you got out today yet?"
- "walk done or still to do?"
- "how was the walk today?"

**General check-in:**
- "just checking in — how are you?"
- "hey, how's your day going?"
- "quick hello — what's happening?"

**Goodnight:**
- "goodnight 🌙"
- "how was today?"
- "closing the day — how did it go?"

**Research share:**
- "hey — remember when you mentioned X? I was thinking about it..."
- "found something you might find interesting..."

### Step 4 — What one specific thing does Sophie weave in?

After the hook, Sophie adds one specific line from Synapse context. This transforms a generic message into a personal one.

- From priority queue: "especially with that client call you've got this afternoon"
- From circle back: "you seemed a bit stressed about it when you mentioned it"
- From upcoming: "and Krishna's birthday is tomorrow — have you sorted anything?"
- From living context: "you've had a heavy few days — be gentle with yourself today"

---

## 6. Rules Engine

Simple rules that prevent Sophie from becoming noise.

| Rule | Behaviour |
|---|---|
| Max daily initiations | Morning + 1-2 daytime + evening. Never more than 4 unprompted messages per day. |
| No double-nudging | If Sophie asked about the walk today and user said not yet — don't ask again until tomorrow. |
| Respect silence | If user hasn't responded to last two messages — back off. |
| Emergency override | If escalation signal fires — notify regardless of other rules. |
| User override | User can always say "don't check in today" and Sophie respects it fully. |
| Already in conversation | If user is actively talking to Sophie — never fire a scheduled message. |

---

## 7. Synapse vs Harness — Clean Boundary

This is the most important architectural principle for the presence system.

### Synapse owns:

- What the user declared matters — water goal, walk goal, any declared habit or intention
- Whether goals were completed — extracted from conversation transcripts
- How nudges landed — did the user respond positively, ignore, or push back
- Pattern over time — does this user respond well to morning nudges or evening ones, direct or gentle
- Confidence on goals — is this still an active goal or has it been dropped
- All six queues — populated by Synapse pipeline runs

### The harness owns:

- When to fire the nudge
- Which hook variation to use
- Whether to nudge at all today — reads Synapse signal before deciding
- Routing to the right model

### The feedback loop:

```
Harness fires nudge
→ user responds or doesn't
→ session ingested by Synapse
→ Synapse extracts how nudge landed
→ updates goal confidence and nudge effectiveness
→ harness reads that signal next time before deciding whether to nudge again
```

### Goal fields Synapse should track:

```
last_nudge_response: positive | neutral | negative | ignored
nudge_frequency_preference: learned over time from responses
goal_active: boolean — has this goal been explicitly dropped?
last_confirmed: timestamp — last time user confirmed this goal is still relevant
```

### The key principle:

Synapse doesn't decide when to nudge. It provides the intelligence that informs the decision. The harness makes the call.

But Synapse absolutely records how nudges land. That's signal. If Sophie asked about water three days in a row and the user never engaged — Synapse notices. The harness reads that and backs off.

One feedback loop. No duplicated intelligence. No competing decision surfaces.

---

## 8. V1 Build Order

Build in this order. Don't skip ahead.

1. **Morning job** — pulls Synapse packet, sends warm good morning with one specific hook from context
2. **Midday check-in** — checks priority queue, falls back to water/walk if nothing pressing
3. **Evening close** — goodnight referencing what happened today
4. **Rules engine** — simple state checks before every outreach
5. **Circle back worker** — fires follow-ups for things mentioned earlier
6. **Research queue worker** — background job that finds things and surfaces them naturally

The queues start as simple Postgres tables. Low confidence queue, circle back queue, upcoming queue, research queue. Populated by Synapse pipeline runs. Consumed by the presence engine.

> The research queue is the one that makes Sophie feel alive. Build it early. Even a crude web search that comes back with one relevant thing, surfaced with the right framing, will feel genuinely magical.

---

## 9. What Makes It Feel Magical

Not the big features. The small specific moments.

- She references something you said before without being asked
- She notices when something is off. Not diagnosing. Just — "you seem quieter than usual today"
- She celebrates small things — "you got your walk in three days in a row. that matters"
- She asks one good question and actually waits for the answer
- She shares something and says why she thought of you specifically
- She knows what not to bring up — holds sensitive things carefully
- She follows up on things you never expected her to remember

> That last one. That's the magic. Not the AI doing something impressive. The AI remembering something small that a person would have forgotten.

---

*Sophie doesn't wait to be useful. She shows up.*
