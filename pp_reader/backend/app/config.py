"""Application configuration using Pydantic settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # Database
    database_url: str = "postgresql://pp_reader:pp_reader@localhost:5432/pp_reader"

    # App
    debug: bool = False
    log_level: str = "INFO"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Pipeline
    portfolio_path: str = ""  # absolute path to .portfolio file; empty disables watcher
    file_poll_interval: int = 60  # seconds between file mtime checks
    enrich_interval: int = 3600  # seconds between periodic enrichment runs


settings = Settings()
