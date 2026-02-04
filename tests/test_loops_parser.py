import json

from src.loops import LoopManager


def test_parse_accepts_dict():
    raw = {"new_loops": [], "reinforced_loops": [], "completed_loops": [], "dropped_loops": []}
    parsed = LoopManager._safe_parse_loop_payload(raw)
    assert parsed == raw


def test_parse_accepts_json_string():
    raw = json.dumps({"new_loops": [], "reinforced_loops": [], "completed_loops": [], "dropped_loops": []})
    parsed = LoopManager._safe_parse_loop_payload(raw)
    assert parsed["new_loops"] == []


def test_parse_accepts_fenced_json():
    raw = "```json\n" + json.dumps({"new_loops": [], "reinforced_loops": [], "completed_loops": [], "dropped_loops": []}) + "\n```"
    parsed = LoopManager._safe_parse_loop_payload(raw)
    assert parsed["reinforced_loops"] == []


def test_parse_invalid_returns_none():
    raw = "{\"new_loops\": ["
    parsed = LoopManager._safe_parse_loop_payload(raw)
    assert parsed is None
