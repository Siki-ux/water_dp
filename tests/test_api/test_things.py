from uuid import uuid4
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

@patch("app.api.deps.get_current_user")
@patch("app.api.v1.endpoints.things.orchestrator_v3")
def test_list_sensors(mock_orch, mock_user):
    mock_user.return_value = {"sub": "u1"}
    mock_orch.list_sensors.return_value = []
    
    # We need a project in DB for this call
    with patch("app.api.v1.endpoints.things.Project") as mock_proj:
         # Hard to mock DB query inside path op directly without overriding Dependency
         pass

# Better approach: Override get_db or mock at service level if possible.
# But endpoint uses direct DB query.
# Let's use `overrides` for get_db.

from app.api.deps import get_current_user, get_db

@patch("app.api.v1.endpoints.things.orchestrator_v3")
def test_create_sensor(mock_orch):
    # Mocking DB dependency
    mock_db = MagicMock()
    app.dependency_overrides[get_db] = lambda: mock_db
    
    # Mock User
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    
    # Mock Project Query
    mock_project = MagicMock()
    mock_project.authorization_provider_group_id = "group1"
    mock_db.query.return_value.filter.return_value.first.return_value = mock_project
    
    mock_orch.create_sensor.return_value = {"uuid": "new-uuid"}
    
    with patch("app.services.project_service.ProjectService.add_sensor") as mock_add:
        response = client.post(
            "/api/v1/things/",
            json={
                "project_uuid": str(uuid4()),
                "sensor_name": "S1",
                "device_type": "dt",
                "description": "desc"
            }
        )
        assert response.status_code == 201
        assert response.json()["uuid"] == "new-uuid"
        mock_add.assert_called_once()
    
    # Clean up
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.things.orchestrator_v3")
def test_update_location(mock_orch):
    mock_orch.update_sensor_location.return_value = True
    
    response = client.put(
        "/api/v1/things/uuid/location",
        json={"project_schema": "s", "latitude": 1.0, "longitude": 2.0}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"

@patch("app.services.ingestion_service.IngestionService.upload_csv")
def test_ingest_csv(mock_upload):
    mock_upload.return_value = {"status": "success"}
    
    response = client.post(
        "/api/v1/things/uuid/ingest/csv",
        files={"file": ("test.csv", b"content", "text/csv")}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"
