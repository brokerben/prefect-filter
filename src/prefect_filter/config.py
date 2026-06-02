"""Configuration via pydantic-settings. Reads from env vars / .env file."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Equimatch API ---
    backend_base_uri: str = ""
    equimatch_api_key: str = ""

    # --- Harness / LLM (read by equimatch-agent) ---
    openrouter_api_key: str = ""
    PHOENIX_COLLECTOR_ENDPOINT: str = ""
    phoenix_api_key: str = ""

    # --- PostHog ---
    posthog_api_key: str = ""
    posthog_host: str = "https://us.i.posthog.com"

    # --- Concurrency ---
    max_concurrency: int = 5


settings = Settings()
