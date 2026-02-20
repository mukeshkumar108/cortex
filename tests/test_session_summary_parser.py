from src.session import SessionManager


def test_parse_summary_bridge_full_response():
    text = """
SUMMARY: The user finished a blocker and shipped a patch.
TONE: The user felt relieved after early frustration.
DECISIONS:
- Start tomorrow with failing tests before Slack.
- Send fix details to the team.
UNRESOLVED:
- Root cause write-up remains open.
MOMENT: The user said the freeze was the main problem, not capability.
BRIDGE: The patch is live and the remaining task is documenting root cause.
""".strip()

    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["summary_facts"] == "The user finished a blocker and shipped a patch."
    assert parsed["tone"] == "The user felt relieved after early frustration."
    assert parsed["moment"] == "The user said the freeze was the main problem, not capability."
    assert parsed["decisions"] == [
        "Start tomorrow with failing tests before Slack.",
        "Send fix details to the team.",
    ]
    assert parsed["unresolved"] == ["Root cause write-up remains open."]
    assert parsed["bridge"] == "The patch is live and the remaining task is documenting root cause."
    assert parsed["summary"] == (
        "The user finished a blocker and shipped a patch. "
        "The user felt relieved after early frustration. "
        "The user said the freeze was the main problem, not capability."
    )
    assert "Explicit decisions or commitments included:" not in parsed["summary"]
    assert "Significant unresolved items included:" not in parsed["summary"]


def test_parse_summary_bridge_missing_moment():
    text = """
SUMMARY: The user completed the rollout and verified production behavior.
TONE: The user sounded steady and relieved.
DECISIONS:
- Keep monitoring for regressions.
UNRESOLVED:
- None
BRIDGE: Monitoring continues and no additional blockers were raised.
""".strip()

    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["moment"] == ""
    assert parsed["decisions"] == ["Keep monitoring for regressions."]
    assert parsed["unresolved"] == []
    assert parsed["summary"] == (
        "The user completed the rollout and verified production behavior. "
        "The user sounded steady and relieved."
    )
    assert "Significant unresolved items included:" not in parsed["summary"]


def test_parse_summary_bridge_empty_decisions_unresolved():
    text = """
SUMMARY: The user checked in and completed a short review.
TONE: No emotionally significant shift occurred during the exchange.
DECISIONS:
UNRESOLVED:
MOMENT:
BRIDGE: The thread can be resumed if new details emerge.
""".strip()

    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["decisions"] == []
    assert parsed["unresolved"] == []
    assert parsed["moment"] == ""
    assert parsed["bridge"] == "The thread can be resumed if new details emerge."


def test_parse_summary_bridge_malformed_order():
    text = """
BRIDGE: The user plans to continue tomorrow.
UNRESOLVED:
- Follow-up message to team pending.
SUMMARY: The user finished the core change and validated tests.
MOMENT: The user recognized they recovered quickly after freezing.
TONE: The user moved from tension to relief.
DECISIONS:
- Start with the failing test first tomorrow.
""".strip()

    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["summary_facts"] == "The user finished the core change and validated tests."
    assert parsed["tone"] == "The user moved from tension to relief."
    assert parsed["moment"] == "The user recognized they recovered quickly after freezing."
    assert parsed["decisions"] == ["Start with the failing test first tomorrow."]
    assert parsed["unresolved"] == ["Follow-up message to team pending."]
    assert parsed["bridge"] == "The user plans to continue tomorrow."


def test_summary_dedupe_removes_high_overlap_repetition():
    text = """
SUMMARY: The user shipped the patch and confirmed tests passed. The user confirmed tests passed after shipping the patch.
TONE: The user felt relieved.
DECISIONS:
UNRESOLVED:
BRIDGE: The patch is live and monitoring continues.
""".strip()

    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["summary_facts"] == "The user shipped the patch and confirmed tests passed."


def test_parse_summary_bridge_does_not_inject_decisions_or_unresolved_into_summary():
    text = """
SUMMARY: The user completed a release candidate and checked key flows.
TONE: The user sounded calm and precise.
DECISIONS:
- Ship after one final regression pass.
UNRESOLVED:
- Write rollback notes.
MOMENT: The user called the release nearly done.
BRIDGE: Final checks remain before shipping.
""".strip()
    parsed = SessionManager._parse_summary_bridge(text)
    assert parsed["summary"] == (
        "The user completed a release candidate and checked key flows. "
        "The user sounded calm and precise. "
        "The user called the release nearly done."
    )
    assert parsed["decisions"] == ["Ship after one final regression pass."]
    assert parsed["unresolved"] == ["Write rollback notes."]
    assert "Explicit decisions" not in parsed["summary"]
    assert "Significant unresolved" not in parsed["summary"]


def test_truncate_structured_recap_fields_enforces_caps_and_removes_repetition():
    parsed = {
        "summary_facts": "The user stated the patch was ready and stated the patch was ready after tests passed and stated confidence was high for launch today with extra notes.",
        "tone": "The user stated confidence and relief after the patch was ready.",
        "moment": "The user stated the patch was ready after tests passed.",
        "decisions": ["Ship today."],
        "unresolved": [],
        "bridge": "Shipping is expected today.",
    }
    clipped = SessionManager._truncate_structured_recap_fields(parsed)
    assert SessionManager._word_count(clipped["summary_facts"]) <= SessionManager._SUMMARY_MAX_WORDS
    assert SessionManager._word_count(clipped["tone"]) <= SessionManager._TONE_MAX_WORDS
    assert SessionManager._word_count(clipped["moment"]) <= SessionManager._MOMENT_MAX_WORDS
    assert SessionManager._count_filler_verbs(
        clipped["summary_facts"], clipped["tone"], clipped["moment"]
    ) <= 1
    assert clipped["summary"] == SessionManager._compose_display_summary(
        clipped["summary_facts"], clipped["tone"], clipped["moment"]
    )


def test_compose_index_text_includes_decisions_and_unresolved_but_not_bridge():
    summary_facts = "The user completed the rollout and verified key paths."
    moment = "They highlighted a late blocker that was resolved."
    decisions = ["Ship tonight after one final test", "Post release notes to the team"]
    unresolved = ["Monitor error rate overnight"]
    bridge = "Next session should check if monitoring stayed clean."

    index_text = SessionManager._compose_index_text(
        summary_facts=summary_facts,
        moment=moment,
        decisions=decisions,
        unresolved=unresolved,
    )

    assert summary_facts in index_text
    assert moment in index_text
    assert "Decisions: Ship tonight after one final test; Post release notes to the team" in index_text
    assert "Open loops: Monitor error rate overnight" in index_text
    assert bridge not in index_text


def test_compose_index_text_caps_length_and_preserves_priority_order():
    summary_facts = "Summary facts. " * 20
    moment = "Critical moment happened and mattered."
    decisions = [f"Decision item number {i} with extra detail" for i in range(1, 20)]
    unresolved = [f"Unresolved item number {i} with extra detail" for i in range(1, 20)]

    index_text = SessionManager._compose_index_text(
        summary_facts=summary_facts,
        moment=moment,
        decisions=decisions,
        unresolved=unresolved,
    )

    assert len(index_text) <= SessionManager._INDEX_TEXT_MAX_CHARS
    assert "Summary facts." in index_text
    assert "Critical moment happened and mattered." in index_text
    assert index_text.find("Summary facts.") <= index_text.find("Critical moment")
    assert "Decisions:" in index_text


def test_classify_summary_salience_levels():
    assert SessionManager._classify_summary_salience(
        tone="calm",
        moment="",
        decisions=["Ship tonight"],
        unresolved=[],
    ) == "high"
    assert SessionManager._classify_summary_salience(
        tone="steady",
        moment="A key rupture happened.",
        decisions=[],
        unresolved=[],
    ) == "medium"
    assert SessionManager._classify_summary_salience(
        tone="The user felt anxious and frustrated.",
        moment="",
        decisions=[],
        unresolved=[],
    ) == "medium"
    assert SessionManager._classify_summary_salience(
        tone="neutral",
        moment="",
        decisions=[],
        unresolved=[],
    ) == "low"
