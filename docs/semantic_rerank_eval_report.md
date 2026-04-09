# Semantic Rerank Eval Report

- Cases: 10
- Improved: 6
- Regressed: 0
- Unchanged: 4
- Embedding fallback rate: 0.000
- Embedding failure rate: 0.000

## Improvements
- `c1_relationship_finance_emotional`: hits@k 3 -> 3, dcg 1.8333 -> 2.0
- `c4_indirect_emotion`: hits@k 3 -> 3, dcg 1.8333 -> 2.0
- `c5_mixed_domain_health_work`: hits@k 3 -> 3, dcg 1.8333 -> 2.0
- `c6_multilingual_arabic_romanized_relationship`: hits@k 3 -> 3, dcg 1.8333 -> 2.0
- `c8_finance_multilingual_french`: hits@k 3 -> 3, dcg 1.3333 -> 2.0
- `c9_learning_vs_work_noise`: hits@k 3 -> 3, dcg 1.8333 -> 2.0

## Regressions
- None.

## Confidence Calibration Snapshot
- `0_50_to_0_69`: total=4, correct_top1=4, accuracy=1.000
- `0_70_to_0_84`: total=6, correct_top1=6, accuracy=1.000

## Noisy Label Patterns
- None flagged.

## Before/After Top-3 by Case
### c1_relationship_finance_emotional
- Query: what is stressing him most with his partner right now
- Before:
  - u1 | 0.580 | intent=express_emotion | type=state | conf=0.57 | My partner says she is carrying me financially and it hit hard.
  - u2 | 0.230 | intent=express_emotion | type=state | conf=0.70 | I avoid opening the rent app because my chest tightens.
  - u3 | 0.230 | intent=make_commitment | type=habit | conf=0.70 | I promised to wake by 7 and walk daily.
- After:
  - u1 | 1.078 | intent=express_emotion | type=state | conf=0.57 | My partner says she is carrying me financially and it hit hard.
  - u5 | 0.743 | intent=express_emotion | type=state | conf=0.69 | They argued late about money and feeling unsupported.
  - u2 | 0.639 | intent=express_emotion | type=state | conf=0.70 | I avoid opening the rent app because my chest tightens.

### c2_multilingual_spanish_sleep
- Query: sleep and stress pattern this week
- Before:
  - u4 | 0.980 | intent=express_emotion | type=habit | conf=0.77 | Routine slipped all week: late phone scrolling, short sleep, high stress.
  - u1 | 0.230 | intent=express_emotion | type=habit | conf=0.61 | Duermo fatal, me despierto a las 3 y luego no regreso.
  - u2 | 0.230 | intent=express_emotion | type=state | conf=0.69 | Cuando no duermo me pongo irritable con todos.
- After:
  - u4 | 1.512 | intent=express_emotion | type=habit | conf=0.77 | Routine slipped all week: late phone scrolling, short sleep, high stress.
  - u1 | 0.739 | intent=express_emotion | type=habit | conf=0.61 | Duermo fatal, me despierto a las 3 y luego no regreso.
  - u2 | 0.700 | intent=express_emotion | type=state | conf=0.69 | Cuando no duermo me pongo irritable con todos.

### c3_multilingual_german_work_anxiety
- Query: what work pressure is he feeling
- Before:
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.65 | Im Job fuehle ich mich gehetzt, alles ist dringend.
  - u2 | 0.230 | intent=make_commitment | type=event | conf=0.66 | Der Kunde droht abzuspringen wenn wir den Termin reissen.
  - u3 | 0.230 | intent=share_update | type=event | conf=0.69 | My sister visited and we cooked.
- After:
  - u1 | 0.736 | intent=express_emotion | type=state | conf=0.65 | Im Job fuehle ich mich gehetzt, alles ist dringend.
  - u4 | 0.415 | intent=express_emotion | type=event | conf=0.53 | Deadline fear plus uncertainty about team capacity.
  - u6 | 0.343 | intent=make_commitment | type=habit | conf=0.64 | Pattern: Juggles multiple active commitments and threads.

### c4_indirect_emotion
- Query: how is he feeling emotionally
- Before:
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.77 | Everything feels loud and bright and I cannot settle.
  - u2 | 0.230 | intent=express_emotion | type=state | conf=0.71 | My body is tired but my mind keeps sprinting.
  - u3 | 0.230 | intent=ask_help | type=event | conf=0.59 | Need to buy detergent and eggs.
- After:
  - u2 | 0.746 | intent=express_emotion | type=state | conf=0.71 | My body is tired but my mind keeps sprinting.
  - u1 | 0.651 | intent=express_emotion | type=state | conf=0.77 | Everything feels loud and bright and I cannot settle.
  - u4 | 0.598 | intent=express_emotion | type=habit | conf=0.73 | Overwhelm and restlessness dominated the evening.

### c5_mixed_domain_health_work
- Query: what is blocking consistency with workouts
- Before:
  - u1 | 0.580 | intent=make_commitment | type=habit | conf=0.62 | I skip workouts when meetings spill into the night.
  - u2 | 0.230 | intent=share_update | type=habit | conf=0.56 | My knee flared up again after the run.
  - u3 | 0.230 | intent=share_update | type=preference | conf=0.54 | I sent the investor memo.
- After:
  - u1 | 0.812 | intent=make_commitment | type=habit | conf=0.62 | I skip workouts when meetings spill into the night.
  - u5 | 0.467 | intent=express_emotion | type=habit | conf=0.65 | Intent to train is high but work spillover and pain interrupt routine.
  - u2 | 0.403 | intent=share_update | type=habit | conf=0.56 | My knee flared up again after the run.

### c6_multilingual_arabic_romanized_relationship
- Query: relationship tension pattern
- Before:
  - u1 | 0.230 | intent=share_update | type=event | conf=0.47 | Ana daiman baskut waqt el khena2 w ba3den andam.
  - u2 | 0.230 | intent=express_emotion | type=state | conf=0.62 | She says I disappear into work when things get hard.
  - u3 | 0.230 | intent=ask_help | type=event | conf=0.51 | Need to renew hosting and domain.
- After:
  - u4 | 0.662 | intent=reflect | type=habit | conf=0.80 | Repeated rupture-repair cycle: withdrawal during conflict, remorse later.
  - u2 | 0.587 | intent=express_emotion | type=state | conf=0.62 | She says I disappear into work when things get hard.
  - u1 | 0.329 | intent=share_update | type=event | conf=0.47 | Ana daiman baskut waqt el khena2 w ba3den andam.

### c7_goal_vs_state
- Query: is this a commitment or just reflection
- Before:
  - u4 | 0.580 | intent=make_commitment | type=habit | conf=0.79 | Clear commitment stated, but fear-based delay pattern remains.
  - u1 | 0.230 | intent=share_update | type=event | conf=0.61 | I will send the apology message tonight.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.60 | I keep rehearsing it and freezing.
- After:
  - u4 | 0.850 | intent=make_commitment | type=habit | conf=0.79 | Clear commitment stated, but fear-based delay pattern remains.
  - u1 | 0.438 | intent=share_update | type=event | conf=0.61 | I will send the apology message tonight.
  - u2 | 0.426 | intent=express_emotion | type=habit | conf=0.60 | I keep rehearsing it and freezing.

### c8_finance_multilingual_french
- Query: money anxiety and rent risk
- Before:
  - u4 | 0.550 | intent=share_update | type=event | conf=0.64 | Current focus: close onboarding bug backlog.
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.70 | Je stresse pour le loyer, le compte baisse trop vite.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.70 | I keep delaying the budget sheet because it makes me panic.
- After:
  - u5 | 1.027 | intent=express_emotion | type=state | conf=0.77 | Financial pressure is tightly linked to avoidance and shame.
  - u1 | 1.020 | intent=express_emotion | type=state | conf=0.70 | Je stresse pour le loyer, le compte baisse trop vite.
  - u2 | 0.966 | intent=express_emotion | type=habit | conf=0.70 | I keep delaying the budget sheet because it makes me panic.

### c9_learning_vs_work_noise
- Query: what learning goal is active
- Before:
  - u5 | 0.780 | intent=reflect | type=goal | conf=0.54 | Learning goals exist but are fragile under work load.
  - u1 | 0.230 | intent=set_goal | type=habit | conf=0.57 | I want to practice SQL joins every morning for 20 minutes.
  - u2 | 0.230 | intent=share_update | type=event | conf=0.70 | Production incidents keep eating my evenings.
- After:
  - u5 | 1.012 | intent=reflect | type=goal | conf=0.54 | Learning goals exist but are fragile under work load.
  - u3 | 0.416 | intent=reflect | type=event | conf=0.56 | I enrolled in a short systems design course.
  - u1 | 0.362 | intent=set_goal | type=habit | conf=0.57 | I want to practice SQL joins every morning for 20 minutes.

### c10_home_health_overlap
- Query: which home factors affect his health habits
- Before:
  - u5 | 0.580 | intent=reflect | type=habit | conf=0.73 | Home environment cues are tightly coupled with food and sleep behavior.
  - u1 | 0.230 | intent=express_emotion | type=habit | conf=0.67 | When the apartment is cluttered I stop cooking and order junk.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.66 | Noisy neighbors at night wreck my sleep.
- After:
  - u5 | 0.726 | intent=reflect | type=habit | conf=0.73 | Home environment cues are tightly coupled with food and sleep behavior.
  - u1 | 0.366 | intent=express_emotion | type=habit | conf=0.67 | When the apartment is cluttered I stop cooking and order junk.
  - u2 | 0.365 | intent=express_emotion | type=habit | conf=0.66 | Noisy neighbors at night wreck my sleep.
