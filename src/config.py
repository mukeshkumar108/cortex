from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://synapse:password@postgres:5432/synapse"

    # FalkorDB
    falkordb_host: str = "falkordb"
    falkordb_port: int = 6379

    # Graphiti LLM (optional override)
    graphiti_llm_api_key: Optional[str] = None
    graphiti_llm_model: Optional[str] = None
    graphiti_llm_small_model: Optional[str] = None
    graphiti_llm_base_url: Optional[str] = None
    graphiti_llm_temperature: float = 1.0
    graphiti_llm_max_tokens: int = 8192

    # OpenAI
    openai_api_key: str

    # OpenRouter (for LLM calls)
    openrouter_api_key: Optional[str] = None  # Falls back to openai_api_key
    openrouter_model: str = "xiaomi/mimo-v2-flash"
    llm_timeout: int = 10  # seconds
    openrouter_model_summary: str = "amazon/nova-micro-v1"
    openrouter_model_loops: str = "xiaomi/mimo-v2-flash"
    openrouter_model_session_episode: str = "xiaomi/mimo-v2-flash"
    openrouter_model_identity: str = "amazon/nova-micro-v1"
    openrouter_model_fallback: str = "mistral/ministral-3b"
    openrouter_reasoning_enabled: bool = False
    memory_semantic_enabled: bool = True
    memory_semantic_embedding_enabled: bool = True
    memory_semantic_embedding_model: str = "text-embedding-3-small"
    episodic_embedding_enabled: bool = True
    episodic_embedding_model: str = "text-embedding-3-small"
    episodic_embedding_window_size: int = 6
    episodic_embedding_stride: int = 2
    episodic_embedding_max_windows: int = 12
    episodic_embedding_max_chars: int = 1400
    episodic_embedding_user_turn_centric: bool = True
    episodic_embedding_include_assistant_turns: bool = True
    episodic_embedding_assistant_char_weight: float = 0.45

    # Session settings
    session_close_gap_minutes: int = 30
    rolling_summary_threshold: int = 6  # turns before compression
    graphiti_timeout: float = 0.5  # seconds for Tier 2 brief
    graphiti_per_turn: bool = False  # per-turn Graphiti episodes (default off)
    loops_include_assistant_turns: bool = False
    idle_close_enabled: bool = False
    idle_close_interval_seconds: int = 300
    idle_close_threshold_minutes: int = 15
    idle_close_batch_size: int = 20
    outbox_drain_enabled: bool = False
    outbox_drain_interval_seconds: int = 60
    outbox_drain_limit: int = 200
    outbox_drain_budget_seconds: float = 2.0
    outbox_drain_per_row_timeout_seconds: float = 8.0
    user_model_updater_enabled: bool = True
    user_model_updater_interval_seconds: int = 300
    user_model_updater_lookback_hours: int = 24
    user_model_updater_max_users: int = 100
    user_model_low_confidence: float = 0.55
    user_model_high_confidence: float = 0.9
    user_model_enrichment_enabled: bool = True
    user_model_enrichment_interval_seconds: int = 900
    user_model_enrichment_daily_lookback_hours: int = 24
    user_model_enrichment_weekly_lookback_days: int = 7
    user_model_enrichment_max_users: int = 100
    user_model_enrichment_min_confidence: float = 0.72
    user_model_enrichment_retry_backoff_seconds: int = 900
    user_model_enrichment_retry_max_seconds: int = 86400
    loop_staleness_janitor_enabled: bool = True
    loop_staleness_janitor_interval_seconds: int = 86400
    daily_analysis_enabled: bool = True
    daily_analysis_interval_seconds: int = 86400
    daily_analysis_target_offset_days: int = 1
    daily_analysis_max_users: int = 500
    daily_analysis_max_turns: int = 160
    daily_analysis_max_sessions: int = 12
    daily_analysis_prompt_char_budget: int = 7000
    daily_analysis_low_confidence_threshold: float = 0.6
    session_bridge_ttl_minutes: int = 30
    v2_dual_write_enabled: bool = True
    v2_dual_write_fail_open: bool = True
    extract_results_enabled: bool = True
    extract_results_model_version: str = "t4-extractor-v1"
    extract_results_prompt_version: str = "t4-prompt-v1"
    extract_results_policy_version: Optional[str] = "v2.p1"
    canonical_live_resolution_enabled: bool = True
    canonical_live_minimal_extractor_enabled: bool = True
    canonical_live_allow_assistant_authored_claims: bool = False
    derived_pipeline_enabled: bool = True
    derived_pipeline_llm_enabled: bool = True
    derived_pipeline_model_version: str = "google/gemma-4-26b-a4b-it"
    derived_pipeline_prompt_version: str = "six-pass-v8-relationship-fact-precedence"
    derived_pipeline_policy_version: str = "derived.v1"
    derived_pipeline_identity_signal_threshold: int = 3
    derived_pipeline_identity_ceiling_days: int = 14
    derived_pipeline_context_delta_threshold: int = 2
    derived_pipeline_context_ceiling_days: int = 10
    derived_pipeline_access_bump_enabled: bool = False
    derived_pipeline_reinforcement_decay_enabled: bool = False
    derived_pipeline_staleness_review_enabled: bool = False
    derived_pipeline_silence_detection_enabled: bool = True
    derived_pipeline_silence_detection_interval_seconds: int = 86400
    derived_pipeline_audit_enabled: bool = True
    derived_pipeline_audit_interval_seconds: int = 86400
    always_on_memory_packet_enabled: bool = True
    always_on_memory_packet_llm_enabled: bool = True
    always_on_memory_packet_model_version: str = "amazon/nova-micro-v1"
    always_on_memory_packet_target_chars: int = 2400
    proactive_shadow_candidates_enabled: bool = True
    proactive_shadow_candidates_interval_seconds: int = 3600
    proactive_shadow_candidates_max_users: int = 300
    proactive_shadow_recent_change_lookback_days: int = 30
    google_workspace_mcp_url: Optional[str] = None
    google_workspace_mcp_bearer_token: Optional[str] = None
    google_workspace_mcp_user_email: Optional[str] = None
    google_workspace_mcp_timeout_seconds: float = 20.0
    google_workspace_mcp_retries: int = 1
    google_calendar_import_enabled: bool = False
    retrieval_shadow_read_enabled: bool = False
    retrieval_shadow_read_sample_rate: float = 0.0
    retrieval_shadow_read_blocking: bool = False
    retrieval_shadow_read_endpoint_enabled: bool = True
    v2_invariant_checker_enabled: bool = True
    v2_invariant_checker_interval_seconds: int = 900
    v2_invariant_checker_auto_repair_enabled: bool = True
    v2_rollout_control_enabled: bool = True
    v2_rollout_eval_enabled: bool = True
    v2_rollout_eval_interval_seconds: int = 300
    startbrief_realizer_enabled: bool = False
    startbrief_realizer_validate_strict: bool = True

    # Identity cache
    identity_cache_ttl_hours: int = 6

    # Logging
    log_level: str = "INFO"
    internal_token: Optional[str] = None
    admin_api_key: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    def get_database_url(self) -> str:
        """Build database URL from environment variables"""
        if "POSTGRES_PASSWORD" in self.model_extra:
            password = self.model_extra["POSTGRES_PASSWORD"]
            return f"postgresql://synapse:{password}@postgres:5432/synapse"
        return self.database_url


@lru_cache()
def get_settings():
    return Settings()
