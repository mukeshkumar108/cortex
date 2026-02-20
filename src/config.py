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
    openrouter_model: str = "anthropic/claude-3.5-haiku"
    llm_timeout: int = 10  # seconds
    openrouter_model_summary: str = "amazon/nova-micro-v1"
    openrouter_model_loops: str = "xiaomi/mimo-v2-flash"
    openrouter_model_session_episode: str = "xiaomi/mimo-v2-flash"
    openrouter_model_identity: str = "amazon/nova-micro-v1"
    openrouter_model_fallback: str = "mistral/ministral-3b"
    openrouter_reasoning_enabled: bool = False

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
