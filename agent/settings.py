"""Typed settings read from the environment (.env in dev, compose env_file in Docker)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    agent_app_password: str = ""
    agent_ro_password: str = ""
    agent_rw_password: str = ""

    redis_url: str = "redis://localhost:6379/0"

    airflow_api_url: str = "http://localhost:8080"
    airflow_user: str = "admin"
    airflow_password: str = ""

    weather_proxy_url: str = "http://localhost:8091"
    weather_proxy_control_token: str = ""

    openrouter_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    primary_model: str = "anthropic/claude-haiku-5.5"
    fallback_model: str = "deepseek/deepseek-v4-flash"

    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"

    max_steps: int = 25
    max_cost_eur_per_incident: float = 0.20
    chaos_mode: str = "off"
    env: str = "dev"

    def dsn(self, role: str, database: str) -> str:
        password = {
            "agent_app": self.agent_app_password,
            "agent_ro": self.agent_ro_password,
            "agent_rw": self.agent_rw_password,
        }[role]
        return (
            f"postgresql://{role}:{password}@{self.postgres_host}:{self.postgres_port}/{database}"
        )

    @property
    def agent_dsn(self) -> str:
        return self.dsn("agent_app", "agent")


@lru_cache
def get_settings() -> Settings:
    return Settings()
