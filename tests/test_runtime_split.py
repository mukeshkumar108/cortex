from types import SimpleNamespace

from src import main as main_module


class _DummyTask:
    def cancel(self):
        return None

    def done(self):
        return False

    def cancelled(self):
        return False


def _settings(**overrides):
    base = {
        "background_loops_enabled": True,
        "runtime_role": "worker",
        "idle_close_enabled": False,
        "outbox_drain_enabled": False,
        "user_model_updater_enabled": False,
        "user_model_enrichment_enabled": False,
        "loop_staleness_janitor_enabled": False,
        "derived_pipeline_silence_detection_enabled": False,
        "derived_pipeline_audit_enabled": False,
        "proactive_shadow_candidates_enabled": False,
        "daily_analysis_enabled": False,
        "v2_invariant_checker_enabled": False,
        "v2_rollout_control_enabled": False,
        "v2_rollout_eval_enabled": False,
        "idle_close_interval_seconds": 300,
        "idle_close_threshold_minutes": 15,
        "idle_close_batch_size": 20,
        "outbox_drain_interval_seconds": 15,
        "outbox_drain_limit": 200,
        "outbox_drain_budget_seconds": 10.0,
        "outbox_drain_per_row_timeout_seconds": 45.0,
        "user_model_updater_interval_seconds": 300,
        "user_model_updater_lookback_hours": 24,
        "user_model_updater_max_users": 100,
        "user_model_low_confidence": 0.55,
        "user_model_high_confidence": 0.9,
        "user_model_enrichment_interval_seconds": 900,
        "user_model_enrichment_max_users": 100,
        "user_model_enrichment_min_confidence": 0.72,
        "user_model_enrichment_daily_lookback_hours": 24,
        "user_model_enrichment_weekly_lookback_days": 7,
        "user_model_enrichment_retry_backoff_seconds": 900,
        "user_model_enrichment_retry_max_seconds": 86400,
        "loop_staleness_janitor_interval_seconds": 86400,
        "derived_pipeline_silence_detection_interval_seconds": 86400,
        "derived_pipeline_audit_interval_seconds": 86400,
        "proactive_shadow_candidates_interval_seconds": 3600,
        "proactive_shadow_candidates_max_users": 300,
        "proactive_shadow_recent_change_lookback_days": 30,
        "daily_analysis_interval_seconds": 86400,
        "daily_analysis_target_offset_days": 1,
        "daily_analysis_max_users": 500,
        "daily_analysis_max_turns": 160,
        "v2_invariant_checker_interval_seconds": 900,
        "v2_invariant_checker_auto_repair_enabled": True,
        "v2_rollout_eval_interval_seconds": 300,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_background_loops_not_started_when_disabled(monkeypatch):
    app = SimpleNamespace(state=SimpleNamespace())
    created = []

    def _fake_create_task(coro):
        created.append(coro)
        coro.close()
        return _DummyTask()

    monkeypatch.setattr(main_module.asyncio, "create_task", _fake_create_task, raising=True)

    started = main_module._start_background_loop_tasks(
        app,
        _settings(background_loops_enabled=False, runtime_role="api", outbox_drain_enabled=True),
    )

    assert started == []
    assert created == []
    assert app.state.background_loops_enabled is False
    assert app.state.runtime_role == "api"
    assert app.state.background_loops_started == []


def test_background_loops_start_when_enabled(monkeypatch):
    app = SimpleNamespace(state=SimpleNamespace())
    created = []

    async def _stub_drain_loop(**_kwargs):
        return None

    def _fake_create_task(coro):
        created.append(coro)
        coro.close()
        return _DummyTask()

    monkeypatch.setattr(main_module.session, "drain_loop", _stub_drain_loop, raising=True)
    monkeypatch.setattr(main_module.asyncio, "create_task", _fake_create_task, raising=True)

    started = main_module._start_background_loop_tasks(
        app,
        _settings(background_loops_enabled=True, runtime_role="worker", outbox_drain_enabled=True),
    )

    assert started == ["outbox_drain"]
    assert len(created) == 1
    assert app.state.background_loops_enabled is True
    assert app.state.runtime_role == "worker"
    assert app.state.background_loops_started == ["outbox_drain"]
