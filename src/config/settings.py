"""Application settings loaded from environment variables."""
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    gcp_project_id: str = "project-fac564f6-0087-4fc3-a53"
    gcp_region: str = "europe-west8"
    bq_dataset: str = "apollinare_legacy"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
