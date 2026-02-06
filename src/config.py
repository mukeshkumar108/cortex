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
