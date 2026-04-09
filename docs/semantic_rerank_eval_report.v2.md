# Semantic Rerank Eval Report

- Cases: 18
- Improved: 6
- Regressed: 0
- Unchanged: 12
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
- `0_50_to_0_69`: total=10, correct_top1=10, accuracy=1.000
- `0_70_to_0_84`: total=8, correct_top1=8, accuracy=1.000

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
  - u3 | 0.230 | intent=ask_help | type=goal | conf=0.59 | Need to buy detergent and eggs.
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

### c11_indirect_shame_self_image
- Query: what shame or self-image pattern is showing up lately
- Before:
  - u5 | 0.780 | intent=reflect | type=habit | conf=0.67 | Self-image drops quickly after minor slips, then avoidance expands.
  - u1 | 0.230 | intent=express_emotion | type=preference | conf=0.51 | I keep dodging voice notes because I sound small in my own head.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.73 | When I miss one task I start feeling like a fraud all day.
- After:
  - u5 | 1.187 | intent=reflect | type=habit | conf=0.67 | Self-image drops quickly after minor slips, then avoidance expands.
  - u2 | 0.746 | intent=express_emotion | type=habit | conf=0.73 | When I miss one task I start feeling like a fraud all day.
  - u1 | 0.408 | intent=express_emotion | type=preference | conf=0.51 | I keep dodging voice notes because I sound small in my own head.

### c12_mixed_language_ar_fr_finance_identity
- Query: where is money shame leaking into identity
- Before:
  - u5 | 0.980 | intent=express_emotion | type=state | conf=0.72 | Financial stress repeatedly turns into identity shame and withdrawal.
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.71 | Je fais semblant que ca va, mais la facture du loyer me fait me sentir nul.
  - u2 | 0.230 | intent=ask_help | type=relationship | conf=0.49 | Lama ashoof hesab banki bahess enni fail, fa batjanab el mawdoo3.
- After:
  - u5 | 1.657 | intent=express_emotion | type=state | conf=0.72 | Financial stress repeatedly turns into identity shame and withdrawal.
  - u1 | 1.006 | intent=express_emotion | type=state | conf=0.71 | Je fais semblant que ca va, mais la facture du loyer me fait me sentir nul.
  - u2 | 0.666 | intent=ask_help | type=relationship | conf=0.49 | Lama ashoof hesab banki bahess enni fail, fa batjanab el mawdoo3.

### c13_overlap_work_relationship_selfworth
- Query: how are work setbacks affecting his self-worth and relationship behavior
- Before:
  - u5 | 1.020 | intent=express_emotion | type=habit | conf=0.68 | Work disappointment repeatedly spills into relationship withdrawal and self-worth collapse.
  - u2 | 0.580 | intent=express_emotion | type=state | conf=0.67 | When work slips, I feel unlovable and pull away from everyone.
  - u3 | 0.580 | intent=share_update | type=event | conf=0.57 | Need to renew SSL certificates this week.
- After:
  - u5 | 1.379 | intent=express_emotion | type=habit | conf=0.68 | Work disappointment repeatedly spills into relationship withdrawal and self-worth collapse.
  - u2 | 1.089 | intent=express_emotion | type=state | conf=0.67 | When work slips, I feel unlovable and pull away from everyone.
  - u3 | 0.534 | intent=share_update | type=event | conf=0.57 | Need to renew SSL certificates this week.

### c14_indirect_mixed_language_planning_mask
- Query: what feeling is underneath all the planning talk
- Before:
  - u5 | 0.580 | intent=make_commitment | type=habit | conf=0.67 | Planning behavior appears to function as protection from fear and overwhelm.
  - u1 | 0.230 | intent=express_emotion | type=habit | conf=0.52 | Hablo de planes todo el dia para no sentir el nudo en el pecho.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.70 | I keep making lists because stopping makes the fear louder.
- After:
  - u5 | 0.981 | intent=make_commitment | type=habit | conf=0.67 | Planning behavior appears to function as protection from fear and overwhelm.
  - u2 | 0.745 | intent=express_emotion | type=habit | conf=0.70 | I keep making lists because stopping makes the fear louder.
  - u1 | 0.570 | intent=express_emotion | type=habit | conf=0.52 | Hablo de planes todo el dia para no sentir el nudo en el pecho.

### c15_german_english_shame_performance
- Query: what shame signal appears around performance
- Before:
  - u5 | 0.780 | intent=express_emotion | type=state | conf=0.69 | Performance pressure quickly translates into shame and defensive communication.
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.68 | Wenn ich einen Fehler mache fuehle ich mich sofort wertlos im Team.
  - u2 | 0.230 | intent=express_emotion | type=habit | conf=0.60 | I over-explain every delay because I assume they think I am incompetent.
- After:
  - u5 | 1.189 | intent=express_emotion | type=state | conf=0.69 | Performance pressure quickly translates into shame and defensive communication.
  - u1 | 0.635 | intent=express_emotion | type=state | conf=0.68 | Wenn ich einen Fehler mache fuehle ich mich sofort wertlos im Team.
  - u2 | 0.581 | intent=express_emotion | type=habit | conf=0.60 | I over-explain every delay because I assume they think I am incompetent.

### c16_overlap_home_body_image
- Query: which home factors are worsening body-image anxiety
- Before:
  - u5 | 1.020 | intent=express_emotion | type=habit | conf=0.70 | Home chaos and poor sleep amplify body-image anxiety and avoidance routines.
  - u2 | 0.580 | intent=express_emotion | type=state | conf=0.66 | No sleep from noisy neighbors makes me judge my body harsher next day.
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.50 | When the apartment is messy I avoid mirrors and skip meals.
- After:
  - u5 | 1.382 | intent=express_emotion | type=habit | conf=0.70 | Home chaos and poor sleep amplify body-image anxiety and avoidance routines.
  - u2 | 0.985 | intent=express_emotion | type=state | conf=0.66 | No sleep from noisy neighbors makes me judge my body harsher next day.
  - u1 | 0.566 | intent=express_emotion | type=state | conf=0.50 | When the apartment is messy I avoid mirrors and skip meals.

### c17_mixed_language_commitment_vs_fear
- Query: is there real commitment or fear-driven delay
- Before:
  - u5 | 0.980 | intent=make_commitment | type=habit | conf=0.85 | Explicit commitment appears, but repeated fear-based freezing delays execution.
  - u1 | 0.230 | intent=share_update | type=event | conf=0.56 | I will send the message tonight, khalas.
  - u2 | 0.230 | intent=ask_help | type=relationship | conf=0.49 | Bas kul ma ajee ab3at, batjammad w arja3 a2ra el masage.
- After:
  - u5 | 1.363 | intent=make_commitment | type=habit | conf=0.85 | Explicit commitment appears, but repeated fear-based freezing delays execution.
  - u1 | 0.431 | intent=share_update | type=event | conf=0.56 | I will send the message tonight, khalas.
  - u2 | 0.383 | intent=ask_help | type=relationship | conf=0.49 | Bas kul ma ajee ab3at, batjammad w arja3 a2ra el masage.

### c18_indirect_self_image_social
- Query: how is self-image affecting social behavior
- Before:
  - u5 | 1.020 | intent=reflect | type=habit | conf=0.64 | Negative self-image repeatedly drives social withdrawal and cancellation behavior.
  - u1 | 0.230 | intent=express_emotion | type=state | conf=0.57 | I ghost close friends when I feel behind in life.
  - u2 | 0.230 | intent=express_emotion | type=state | conf=0.55 | If I feel like a burden I cancel plans at the last minute.
- After:
  - u5 | 1.424 | intent=reflect | type=habit | conf=0.64 | Negative self-image repeatedly drives social withdrawal and cancellation behavior.
  - u1 | 0.625 | intent=express_emotion | type=state | conf=0.57 | I ghost close friends when I feel behind in life.
  - u2 | 0.622 | intent=express_emotion | type=state | conf=0.55 | If I feel like a burden I cancel plans at the last minute.
