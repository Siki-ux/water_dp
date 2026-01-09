import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.main import app
from app.models.computations import ComputationJob, ComputationScript

client = TestClient(app)

# Mocks
mock_user_id = "user-123"
mock_project_id = str(uuid.uuid4())
mock_script_id = str(uuid.uuid4())
mock_task_id = "task-abc-123"


@pytest.fixture
def override_deps():
    app.dependency_overrides = {}
    yield
    app.dependency_overrides = {}


@pytest.fixture
def mock_db_session():
    return MagicMock(spec=Session)


@pytest.fixture
def mock_current_user():
    return {
        "sub": mock_user_id,
        "preferred_username": "tester",
        "realm_access": {"roles": ["user"]},
    }


@pytest.fixture
def mock_superuser():
    return {
        "sub": "admin-user",
        "preferred_username": "admin",
        "realm_access": {"roles": ["admin"]},
    }


# --- Upload Tests ---


def test_upload_computation_script_success(
    override_deps, mock_db_session, mock_current_user
):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    with patch(
        "app.services.project_service.ProjectService._check_access", return_value=True
    ):
        files = {"file": ("script.py", "print('hello')", "text/x-python")}
        data = {"name": "Test Script", "project_id": mock_project_id}

        with patch("builtins.open", MagicMock()):
            response = client.post(
                "/api/v1/computations/upload", files=files, data=data
            )

            assert response.status_code == 200
            assert response.json()["name"] == "Test Script"
            mock_db_session.add.assert_called_once()


def test_upload_invalid_extension(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    with patch(
        "app.services.project_service.ProjectService._check_access", return_value=True
    ):
        files = {"file": ("script.txt", "print('hello')")}
        data = {"name": "Test Script", "project_id": mock_project_id}

        response = client.post("/api/v1/computations/upload", files=files, data=data)
        assert response.status_code == 400
        assert ".py files" in response.json()["detail"]


def test_upload_dangerous_script(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    with patch(
        "app.services.project_service.ProjectService._check_access", return_value=True
    ):
        dangerous_code = "import os; os.system('rm -rf /')"
        files = {"file": ("evil.py", dangerous_code)}
        data = {"name": "Evil Script", "project_id": mock_project_id}

        response = client.post("/api/v1/computations/upload", files=files, data=data)
        assert response.status_code == 400
        assert "Security violation" in response.json()["detail"]


def test_upload_file_too_large(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    with patch(
        "app.services.project_service.ProjectService._check_access", return_value=True
    ):
        # Create a large file content (> 1MB)
        large_content = "a" * (1024 * 1024 + 100)
        files = {"file": ("large.py", large_content)}
        data = {"name": "Large Script", "project_id": mock_project_id}

        response = client.post("/api/v1/computations/upload", files=files, data=data)
        assert response.status_code == 400
        assert "exceeds" in response.json()["detail"]


# --- Run Tests ---


def test_run_computation_creates_job(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    mock_script = ComputationScript(
        id=mock_script_id, project_id=mock_project_id, filename="foo.py"
    )
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_script
    )

    with patch(
        "app.services.project_service.ProjectService._check_access", return_value=True
    ), patch(
        "app.api.v1.endpoints.computations.run_computation_task.delay"
    ) as mock_task, patch(
        "os.path.exists", return_value=True
    ):

        mock_task.return_value.id = mock_task_id

        response = client.post(
            f"/api/v1/computations/run/{mock_script_id}", json={"params": {}}
        )

        assert response.status_code == 200
        assert response.json()["task_id"] == mock_task_id

        # Verify Job Creation
        args, _ = mock_db_session.add.call_args
        job = args[0]
        assert isinstance(job, ComputationJob)
        assert job.id == mock_task_id
        assert str(job.script_id) == mock_script_id


def test_run_computation_script_not_found(
    override_deps, mock_db_session, mock_current_user
):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    response = client.post(
        f"/api/v1/computations/run/{mock_script_id}", json={"params": {}}
    )
    assert response.status_code == 404


# --- Status Tests ---


def test_get_status_authorized(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    mock_job = ComputationJob(id=mock_task_id, user_id=mock_user_id)
    mock_db_session.query.return_value.filter.return_value.first.return_value = mock_job

    with patch("app.api.v1.endpoints.computations.AsyncResult") as mock_async_result:
        mock_async_result.return_value.status = "SUCCESS"
        mock_async_result.return_value.ready.return_value = True
        mock_async_result.return_value.result = {"foo": "bar"}

        response = client.get(f"/api/v1/computations/tasks/{mock_task_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "SUCCESS"


def test_get_status_unauthorized(override_deps, mock_db_session, mock_current_user):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_current_user

    mock_job = ComputationJob(id=mock_task_id, user_id="other-user")
    mock_db_session.query.return_value.filter.return_value.first.return_value = mock_job

    response = client.get(f"/api/v1/computations/tasks/{mock_task_id}")
    assert response.status_code == 403


def test_get_status_superuser_authorized(
    override_deps, mock_db_session, mock_superuser
):
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[deps.get_current_user] = lambda: mock_superuser

    mock_job = ComputationJob(id=mock_task_id, user_id="other-user")
    mock_db_session.query.return_value.filter.return_value.first.return_value = mock_job

    with patch("app.api.v1.endpoints.computations.AsyncResult") as mock_async_result:
        mock_async_result.return_value.status = "PENDING"
        mock_async_result.return_value.ready.return_value = False

        response = client.get(f"/api/v1/computations/tasks/{mock_task_id}")
        assert response.status_code == 200
