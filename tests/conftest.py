from unittest.mock import MagicMock, PropertyMock

import pytest
from sqlalchemy.orm import Session

from app.core.config import Settings, settings


@pytest.fixture
def mock_db_session():
    """Fixture for mocking SQLAlchemy session."""
    session = MagicMock(spec=Session)
    return session


@pytest.fixture
def mock_settings(monkeypatch):
    """Fixture for mocking application settings."""
    # Create a mock settings object
    mock_env = MagicMock(spec=Settings)
    type(mock_env).geoserver_url = PropertyMock(return_value="http://mock-geoserver")
    type(mock_env).geoserver_username = PropertyMock(return_value="admin")
    type(mock_env).geoserver_password = PropertyMock(return_value="geoserver")
    type(mock_env).geoserver_workspace = PropertyMock(return_value="test_workspace")

    # Patch the global settings object in app.services.geoserver_service
    # We might need to patch it where it is used, or patch the instance itself if possible.
    # Since settings is instantiated in app.core.config, usually tests might override it.
    # However, simple monkeypatching of attribute values on the existing 'settings' object is often easier.

    monkeypatch.setattr(settings, "geoserver_url", "http://mock-geoserver")
    monkeypatch.setattr(settings, "geoserver_username", "admin")
    monkeypatch.setattr(settings, "geoserver_password", "geoserver")
    monkeypatch.setattr(settings, "geoserver_workspace", "test_workspace")

    return settings
