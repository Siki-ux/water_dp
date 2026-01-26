import uuid
from unittest.mock import MagicMock, patch
import pytest
from app.services.dashboard_service import DashboardService
from app.models.user_context import Dashboard
from app.core.exceptions import ResourceNotFoundException, AuthenticationException

@pytest.fixture
def mock_db_session():
    return MagicMock()

@pytest.fixture
def mock_user():
    return {"sub": str(uuid.uuid4()), "realm_access": {"roles": []}}

@patch("app.services.dashboard_service.ProjectService._check_access")
def test_create_dashboard(mock_check_access, mock_db_session, mock_user):
    dashboard_in = MagicMock()
    dashboard_in.project_id = uuid.uuid4()
    dashboard_in.name = "Dash 1"
    dashboard_in.layout_config = {}
    dashboard_in.widgets = []
    dashboard_in.is_public = False

    # Mock add
    mock_db_session.add.return_value = None
    mock_db_session.commit.return_value = None
    mock_db_session.refresh.return_value = None

    DashboardService.create_dashboard(mock_db_session, dashboard_in, mock_user)
    
    # Assert Project Check called
    mock_check_access.assert_called_with(
        mock_db_session, dashboard_in.project_id, mock_user, required_role="editor"
    )
    # Assert ADD called
    mock_db_session.add.assert_called_once()
    assert mock_db_session.add.call_args[0][0].name == "Dash 1"

def test_get_dashboard_public(mock_db_session):
    dashboard_id = uuid.uuid4()
    dashboard = Dashboard(id=dashboard_id, is_public=True, name="Public")
    mock_db_session.query.return_value.filter.return_value.first.return_value = dashboard

    result = DashboardService.get_dashboard(mock_db_session, dashboard_id, user=None)
    assert result.name == "Public"

@patch("app.services.dashboard_service.ProjectService._check_access")
def test_get_dashboard_private_authenticated(mock_check_access, mock_db_session, mock_user):
    dashboard_id = uuid.uuid4()
    project_id = uuid.uuid4()
    dashboard = Dashboard(id=dashboard_id, is_public=False, project_id=project_id, name="Private")
    mock_db_session.query.return_value.filter.return_value.first.return_value = dashboard

    result = DashboardService.get_dashboard(mock_db_session, dashboard_id, user=mock_user)
    assert result.name == "Private"
    mock_check_access.assert_called_with(mock_db_session, project_id, mock_user, required_role="viewer")

def test_get_dashboard_private_unauthenticated(mock_db_session):
    dashboard_id = uuid.uuid4()
    dashboard = Dashboard(id=dashboard_id, is_public=False, name="Private")
    mock_db_session.query.return_value.filter.return_value.first.return_value = dashboard

    with pytest.raises(AuthenticationException):
        DashboardService.get_dashboard(mock_db_session, dashboard_id, user=None)

@patch("app.services.dashboard_service.ProjectService._check_access")
def test_update_dashboard(mock_check_access, mock_db_session, mock_user):
    dashboard_id = uuid.uuid4()
    dashboard = Dashboard(id=dashboard_id, name="Old")
    mock_db_session.query.return_value.filter.return_value.first.return_value = dashboard

    dashboard_in = MagicMock()
    dashboard_in.name = "New"
    # Other fields None

    DashboardService.update_dashboard(mock_db_session, dashboard_id, dashboard_in, mock_user)
    
    assert dashboard.name == "New"
    mock_check_access.assert_called_once()

@patch("app.services.dashboard_service.ProjectService._check_access")
def test_delete_dashboard(mock_check_access, mock_db_session, mock_user):
    dashboard_id = uuid.uuid4()
    dashboard = Dashboard(id=dashboard_id)
    mock_db_session.query.return_value.filter.return_value.first.return_value = dashboard

    DashboardService.delete_dashboard(mock_db_session, dashboard_id, mock_user)
    mock_db_session.delete.assert_called_with(dashboard)
