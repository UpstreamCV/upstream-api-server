from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="UPSTREAM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    port: int = 4000
    environment: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"

    database_url: SecretStr

    supabase_url: str
    supabase_anon_key: SecretStr
    supabase_service_role_key: SecretStr
    supabase_jwt_secret: SecretStr

    session_jwt_secret: SecretStr
    streamid_hmac_key: SecretStr

    mediamtx_webhook_secret: SecretStr
    processor_webhook_secret: SecretStr
    prometheus_auth_token: SecretStr

    # Comma-separated list in env (UPSTREAM_ALLOWED_ORIGINS=a,b,c).
    # Surfaced as ``allowed_origins: list[str]`` below.
    allowed_origins_csv: str = Field(default="http://localhost:3000", alias="ALLOWED_ORIGINS")

    sentry_dsn: str | None = None

    @property
    def allowed_origins(self) -> list[str]:
        return [s.strip() for s in self.allowed_origins_csv.split(",") if s.strip()]

    session_token_ttl_seconds: int = 14400
    db_pool_min_size: int = 5
    db_pool_max_size: int = 20

    supabase_storage_bucket: str = "snapshots"
    snapshot_max_bytes: int = 5 * 1024 * 1024  # 5 MB; Caddy must allow >= this on /v1/snapshots

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
