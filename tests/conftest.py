"""Shared pytest fixtures used across all test modules."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from config.settings import get_settings


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """Clear the lru_cache on get_settings before every test."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def mock_settings(mocker) -> MagicMock:
    """Return a fully-stubbed Settings object and patch get_settings globally."""
    settings = MagicMock()
    settings.gcp_project_id = "test-project"
    settings.gcp_region = "europe-west8"
    settings.bq_dataset = "apollinare_legacy"

    for module in [
        "config.settings",
        "db.bigquery",
        "gateway.routers.eventi",
        "gateway.routers.catalogo",
        "gateway.routers.lista_carico",
        "gateway.routers.reportistica",
        "services.calcolo_lista",
        "services.calcolo_preventivo",
    ]:
        mocker.patch(f"{module}.get_settings", return_value=settings)

    return settings
