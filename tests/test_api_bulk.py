from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api import deps
from app.main import app

client = TestClient(app)


@pytest.fixture
def override_deps():
    app.dependency_overrides = {}
    yield
    app.dependency_overrides = {}


@pytest.fixture
def mock_superuser():
    return {
        "sub": "admin-1",
        "preferred_username": "admin",
        "realm_access": {"roles": ["admin"]},
    }


@pytest.fixture
def mock_normal_user():
    return {"sub": "user-1", "realm_access": {"roles": ["user"]}}


# Success Tests
def test_import_geojson_success(override_deps, mock_superuser):
    app.dependency_overrides[deps.get_current_active_superuser] = lambda: mock_superuser

    with patch("app.tasks.import_tasks.import_geojson_task.delay") as mock_task:
        mock_task.return_value.id = "task-geo-123"
        files = {
            "file": (
                "data.geojson",
                '{"type": "FeatureCollection"}',
                "application/json",
            )
        }

        # We Mock open to avoid saving to disk (or let it save to temp and cleanup)
        # But endpoints stream to disk separately.
        # Integration test might be better to let it write.
        # Let's mock 'aiofiles' or the loop if used, but here it's just 'file.read' and 'open'.

        with patch("builtins.open", MagicMock()), patch(
            "os.path.exists", return_value=True
        ), patch("os.path.exists", return_value=True), patch("os.remove"):

            response = client.post("/api/v1/bulk/import/geojson", files=files)

            assert response.status_code == 200
            assert response.json()["task_id"] == "task-geo-123"


def test_import_timeseries_success(override_deps, mock_superuser):
    app.dependency_overrides[deps.get_current_active_superuser] = lambda: mock_superuser

    with patch("app.tasks.import_tasks.import_timeseries_task.delay") as mock_task:
        mock_task.return_value.id = "task-ts-123"
        files = {"file": ("data.json", "[{}]", "application/json")}

        with patch("builtins.open", MagicMock()), patch(
            "os.path.exists", return_value=True
        ):

            response = client.post("/api/v1/bulk/import/timeseries", files=files)
            assert response.status_code == 200
            assert response.json()["task_id"] == "task-ts-123"


# Error Tests
def test_import_geojson_too_large(override_deps, mock_superuser):
    app.dependency_overrides[deps.get_current_active_superuser] = lambda: mock_superuser

    # Mocking a large file is tricky with TestClient as it reads all into memory for request construction often.
    # However, we can patch MAX_BULK_FILE_SIZE to be small.
    with patch("app.api.v1.endpoints.bulk.MAX_BULK_FILE_SIZE", 10):  # 10 bytes limit
        files = {
            "file": (
                "large.json",
                '{"type": "FeatureCollection"....}',
                "application/json",
            )
        }

        # We need to ensure temp file cleanup is called
        with patch("builtins.open", MagicMock()), patch(
            "os.path.exists", return_value=True
        ), patch("os.remove") as mock_remove:

            response = client.post("/api/v1/bulk/import/geojson", files=files)
            assert response.status_code == 400
            assert "exceeds" in response.json()["detail"]
            mock_remove.assert_called()


# Authorization Tests
def test_import_geojson_unauthorized(override_deps, mock_normal_user):
    # Only superuser allowed
    # If we don't override get_current_active_superuser, it might call the real one or fail deps.
    # But get_current_active_superuser depends on get_current_user.

    app.dependency_overrides[deps.get_current_user] = lambda: mock_normal_user
    # We DO NOT override get_current_active_superuser, we want the real logic to run and fail

    response = client.post(
        "/api/v1/bulk/import/geojson", files={"file": ("t.json", "{}")}
    )
    assert response.status_code == 403


def test_get_import_status_superuser(override_deps, mock_superuser):
    app.dependency_overrides[deps.get_current_active_superuser] = lambda: mock_superuser

    with patch("app.api.v1.endpoints.bulk.AsyncResult") as mock_async:
        mock_async.return_value.status = "SUCCESS"
        mock_async.return_value.ready.return_value = True
        mock_async.return_value.result = "Done"

        response = client.get("/api/v1/bulk/tasks/123")
        assert response.status_code == 200
        assert response.json()["status"] == "SUCCESS"


def test_get_import_status_unauthorized(override_deps, mock_normal_user):
    app.dependency_overrides[deps.get_current_user] = lambda: mock_normal_user

    response = client.get("/api/v1/bulk/tasks/123")
    assert response.status_code == 403
