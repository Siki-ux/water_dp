from unittest.mock import patch
from uuid import uuid4

import pytest

import app.services.keycloak_service as keycloak_service
from app.api import deps
from app.main import app
from app.schemas.user_context import (
    DashboardResponse,
    ProjectResponse,
)

# Mock User Data
MOCK_USER_ID = "user-123"
MOCK_USER = {"sub": MOCK_USER_ID, "realm_access": {"roles": ["viewer"]}}
MOCK_ADMIN = {"sub": "admin-123", "realm_access": {"roles": ["admin"]}}


@pytest.fixture
def normal_user_token():
    app.dependency_overrides[deps.get_current_user] = lambda: MOCK_USER
    yield
    app.dependency_overrides.pop(deps.get_current_user, None)


@pytest.fixture
def mock_project_service():
    with patch("app.api.v1.endpoints.projects.ProjectService") as mock:
        yield mock


@pytest.fixture
def mock_dashboard_service():
    with patch(
        "app.api.v1.endpoints.projects.DashboardService"
    ) as mock:  # For convenience endpoint
        with patch("app.api.v1.endpoints.dashboards.DashboardService") as mock_dash:
            yield (mock, mock_dash)


# --- Tests ---


def test_create_project(client, normal_user_token, mock_project_service):
    # Setup Mock
    project_id = uuid4()
    mock_project_service.create_project.return_value = ProjectResponse(
        id=project_id,
        name="My Project",
        description="Test Project",
        owner_id=MOCK_USER_ID,
        created_at="2024-01-01T00:00:00",
        updated_at="2024-01-01T00:00:00",
    )

    response = client.post(
        "/api/v1/projects/",
        json={
            "name": "My Project",
            "description": "Test Project",
            "authorization_provider_group_id": "group-123",
        },
    )

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "My Project"
    assert data["id"] == str(project_id)
    mock_project_service.create_project.assert_called_once()


def test_list_projects(client, normal_user_token, mock_project_service):
    mock_project_service.list_projects.return_value = [
        ProjectResponse(
            id=uuid4(),
            name="P1",
            owner_id=MOCK_USER_ID,
            created_at="2024-01-01",
            updated_at="2024-01-01",
        )
    ]

    response = client.get("/api/v1/projects/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "P1"


def test_get_project(client, normal_user_token, mock_project_service):
    pid = uuid4()
    mock_project_service.get_project.return_value = ProjectResponse(
        id=pid,
        name="P1",
        owner_id=MOCK_USER_ID,
        created_at="2024-01-01",
        updated_at="2024-01-01",
    )

    response = client.get(f"/api/v1/projects/{pid}")
    assert response.status_code == 200
    assert response.json()["id"] == str(pid)


@pytest.fixture
def mock_keycloak_service():
    with patch.object(keycloak_service, "KeycloakService") as mock:
        yield mock


# Member management via projects API is disabled.


def test_dashboard_creation(client, normal_user_token, mock_dashboard_service):
    mock_pd_service, _ = mock_dashboard_service
    pid = uuid4()
    did = uuid4()

    mock_pd_service.create_dashboard.return_value = DashboardResponse(
        id=did,
        project_id=pid,
        name="D1",
        is_public=False,
        created_at="2024-01-01",
        updated_at="2024-01-01",
    )

    response = client.post(f"/api/v1/projects/{pid}/dashboards", json={"name": "D1"})
    assert response.status_code == 200
    assert response.json()["id"] == str(did)
