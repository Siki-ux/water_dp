from datetime import datetime
from uuid import uuid4
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app
from app.api.deps import get_current_user

client = TestClient(app)

@patch("app.api.v1.endpoints.projects.DashboardService")
def test_create_dashboard(mock_service):
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    # Mock return must match DashboardResponse
    mock_dash = MagicMock()
    mock_dash.id = uuid4()
    mock_dash.name = "D1"
    mock_dash.project_id = uuid4()
    mock_dash.created_at = datetime.now()
    mock_dash.updated_at = datetime.now()
    mock_dash.layout_config = {}
    mock_dash.widgets = []
    mock_dash.is_public = False
    mock_service.create_dashboard.return_value = mock_dash
    
    # Use access via Projects endpoint
    pid = str(mock_dash.project_id)
    response = client.post(
        f"/api/v1/projects/{pid}/dashboards",
        json={"name": "D1", "project_id": pid, "layout_config": {}, "widgets": [], "is_public": False}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "D1"
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.projects.DashboardService")
def test_list_dashboards(mock_service):
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    mock_service.list_dashboards.return_value = []
    
    pid = str(uuid4())
    response = client.get(f"/api/v1/projects/{pid}/dashboards")
    assert response.status_code == 200
    assert response.json() == []
    app.dependency_overrides = {}

@patch("app.api.v1.endpoints.dashboards.DashboardService")
def test_get_dashboard(mock_service):
    app.dependency_overrides[get_current_user] = lambda: {"sub": "u1", "realm_access": {"roles": []}}
    dash_id = str(uuid4())
    mock_dash = MagicMock()
    mock_dash.id = dash_id
    mock_dash.project_id = uuid4()
    mock_dash.created_at = datetime.now()
    mock_dash.updated_at = datetime.now()
    mock_dash.name = "D1"
    mock_dash.layout_config = {}
    mock_dash.widgets = []
    mock_dash.is_public = False
    mock_service.get_dashboard.return_value = mock_dash
    
    response = client.get(f"/api/v1/dashboards/{dash_id}")
    assert response.status_code == 200
    assert response.json()["id"] == dash_id
    app.dependency_overrides = {}
