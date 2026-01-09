from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.api import deps
from app.core.database import get_db
from app.main import app

client = TestClient(app)


@pytest.fixture
def mock_auth():
    def mock_get_current_user():
        return {"sub": "test-user-id", "username": "testuser"}

    app.dependency_overrides[deps.get_current_user] = mock_get_current_user
    yield
    app.dependency_overrides.pop(deps.get_current_user, None)


# Global Access Mock logic
# Since we mock ProjectService access check, we don't need to be too fancy about project membership in DB
# But we DO need to make sure get_db returns a mock session.


@pytest.fixture
def mock_db_session():
    mock_session = MagicMock()

    # Ensure it's yielded when called
    def get_mock_db():
        yield mock_session

    app.dependency_overrides[get_db] = get_mock_db
    yield mock_session
    app.dependency_overrides.pop(get_db, None)


@patch("app.api.v1.endpoints.computations.run_computation_task.delay")
@patch("os.path.exists")
@patch("app.api.v1.endpoints.computations.ProjectService._check_access")
def test_trigger_computation_success(
    mock_check_access, mock_exists, mock_delay, mock_db_session, mock_auth
):
    # Setup
    script_id = uuid4()
    project_id = uuid4()

    # Mock query result
    mock_script = MagicMock()
    mock_script.id = script_id
    mock_script.project_id = project_id
    mock_script.filename = "secure_script.py"

    # query(ComputationScript).filter(...).first()
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_script
    )

    # Mock file exists
    mock_exists.return_value = True

    # Mock celery task
    mock_task = MagicMock()
    mock_task.id = "mock-task-id"
    mock_delay.return_value = mock_task

    response = client.post(
        f"/api/v1/computations/run/{script_id}", json={"params": {"location_id": 1}}
    )

    # Verification
    assert response.status_code == 200
    assert response.json() == {"task_id": "mock-task-id", "status": "submitted"}
    mock_check_access.assert_called_once()  # Should check access
    mock_delay.assert_called_once()


def test_trigger_computation_not_found(mock_db_session, mock_auth):
    # Setup
    script_id = uuid4()

    # Mock DB returning None (script not found)
    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    response = client.post(f"/api/v1/computations/run/{script_id}", json={"params": {}})

    assert response.status_code == 404


@patch("app.api.v1.endpoints.computations.ProjectService._check_access")
def test_list_computations(mock_check_access, mock_db_session, mock_auth):
    # Setup
    project_id = uuid4()

    # Mock DB result
    script1 = MagicMock()
    script1.id = uuid4()
    script1.name = "Script 1"
    script1.project_id = project_id
    script1.filename = "s1.py"
    script1.description = "d1"

    mock_db_session.query.return_value.filter.return_value.all.return_value = [script1]

    response = client.get(f"/api/v1/computations/list/{project_id}")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "Script 1"
    mock_check_access.assert_called_once()
