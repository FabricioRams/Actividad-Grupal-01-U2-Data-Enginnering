"""Application configuration for SkyPulse Data Engineering Platform."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = Field(default="local")
    warehouse_backend: Literal["duckdb", "postgres"] = Field(default="duckdb")
    duckdb_path: Path = Field(default=Path("data/skypulse.duckdb"))
    database_url: str | None = Field(default=None)

    forecast_days: int = Field(default=3, ge=1, le=7)
    past_days: int = Field(default=2, ge=0, le=5)
    request_timeout_seconds: float = Field(default=20.0)

    dbt_project_dir: Path = Field(default=Path("dbt_project"))
    dbt_profiles_dir: Path = Field(default=Path("dbt_project"))

    @property
    def resolved_backend(self) -> Literal["duckdb", "postgres"]:
        """Prefer postgres automatically when DATABASE_URL is explicitly configured."""
        if self.database_url and self.database_url.startswith(("postgres://", "postgresql://")):
            return "postgres"
        return self.warehouse_backend

    @property
    def normalized_postgres_url(self) -> str:
        """Normalize postgres:// URLs (used by some PaaS providers) to postgresql://."""
        if not self.database_url:
            raise ValueError("DATABASE_URL must be set when using the postgres backend.")
        if self.database_url.startswith("postgres://"):
            return self.database_url.replace("postgres://", "postgresql://", 1)
        return self.database_url


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
