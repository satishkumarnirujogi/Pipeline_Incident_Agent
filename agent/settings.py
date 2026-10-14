"""Typed settings read from the environment (.env in dev, compose env_file in Docker).

The same .env serves containers and host scripts. Inside Docker (the image sets
RUNNING_IN_DOCKER=1) services are reached by compose name; on the laptop through
the published ports on localhost.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    running_in_docker: bool = False

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_host_port: int = 5433
    agent_app_password: str = ""
    agent_ro_password: str = ""
    agent_rw_password: str = ""
    pipeline_db_password: str = ""  # only operator scripts use it; agents never do

    redis_url: str = "redis://localhost:6379/0"

    airflow_api_url: str = "http://localhost:8080"
    airflow_host_url: str = "http://localhost:8080"
    airflow_user: str = "admin"
    airflow_password: str = ""

    weather_proxy_url: str = "http://localhost:8091"
    weather_proxy_host_port: int = 8091
    weather_proxy_control_token: str = ""

    data_dir: Path = Path("data")
    contracts_dir: Path = Path("contracts")

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

    def dsn(self, role: str, database: str, from_host: bool | None = None) -> str:
        """from_host: reach Postgres on the published port. Default: True outside Docker."""
        password = {
            "agent_app": self.agent_app_password,
            "agent_ro": self.agent_ro_password,
            "agent_rw": self.agent_rw_password,
            "pipeline": self.pipeline_db_password,
        }[role]
        if from_host is None:
            from_host = not self.running_in_docker
        host, port = (
            ("localhost", self.postgres_host_port)
            if from_host
            else (self.postgres_host, self.postgres_port)
        )
        return f"postgresql://{role}:{password}@{host}:{port}/{database}"

    @property
    def agent_dsn(self) -> str:
        return self.dsn("agent_app", "agent")

    @property
    def airflow_url(self) -> str:
        return self.airflow_api_url if self.running_in_docker else self.airflow_host_url

    @property
    def proxy_url(self) -> str:
        if self.running_in_docker:
            return self.weather_proxy_url
        return f"http://localhost:{self.weather_proxy_host_port}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
