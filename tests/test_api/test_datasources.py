from uuid import uuid4
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app
from app.api.deps import get_current_user, get_current_active_superuser

client = TestClient(app)

@patch("app.api.v1.endpoints.datasources.DataSourceService")
def test_create_datasource(mock_service):
    # Depending on implementation, create might need project access check which uses get_current_user
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    
    # Mock return
    stub_ds = MagicMock()
    stub_ds.id = uuid4()
    stub_ds.name = "DS1"
    stub_ds.project_id = uuid4()
    stub_ds.type = "postgres"
    stub_ds.connection_details = {}
    mock_service.return_value.create.return_value = stub_ds
    
    response = client.post(
        f"/api/v1/projects/{uuid4()}/datasources",
        json={"name": "DS1", "type": "postgres", "connection_details": {}}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "DS1"
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.datasources.DataSourceService")
def test_get_project_datasources(mock_service):
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    mock_service.return_value.get_by_project.return_value = []
    
    response = client.get(f"/api/v1/projects/{uuid4()}/datasources")
    assert response.status_code == 200
    assert response.json() == []
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.datasources.DataSourceService")
def test_delete_datasource(mock_service):
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    mock_service.return_value.delete.return_value = True
    
    response = client.delete(f"/api/v1/projects/{uuid4()}/datasources/{uuid4()}")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.datasources.DataSourceService")
def test_execute_query(mock_service):
    # Override get_current_active_superuser to return a user with admin role
    # BUT wait, the dependency implementation CHECKS roles on the user object returned by get_current_user.
    # If we override get_current_active_superuser DIRECTLY, we bypass the check.
    app.dependency_overrides[get_current_active_superuser] = lambda: {"sub": "admin", "realm_access": {"roles": ["admin"]}}

    mock_service.return_value.get.return_value = MagicMock()
    mock_service.return_value.execute_query.return_value = [{"col": "val"}]
    
    response = client.post(
        f"/api/v1/projects/{uuid4()}/datasources/{uuid4()}/query",
        json={"sql": "SELECT 1"}
    )
    assert response.status_code == 200
    assert response.json()[0]["col"] == "val"
    app.dependency_overrides = {}
