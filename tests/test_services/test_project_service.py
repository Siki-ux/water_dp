import uuid
from unittest.mock import MagicMock, patch
import pytest
from app.services.project_service import ProjectService
from app.models.user_context import Project, ProjectMember
from app.core.exceptions import AuthorizationException, ResourceNotFoundException

@pytest.fixture
def mock_db_session():
    return MagicMock()

@pytest.fixture
def mock_user():
    return {
        "sub": str(uuid.uuid4()),
        "preferred_username": "testuser",
        "realm_access": {"roles": []},
        "groups": ["group1"],
        "eduperson_entitlement": []
    }

@pytest.fixture
def mock_admin_user():
    return {
        "sub": str(uuid.uuid4()),
        "preferred_username": "admin",
        "realm_access": {"roles": ["admin"]},
        "groups": [],
        "eduperson_entitlement": []
    }

def test_create_project_success(mock_db_session, mock_user):
    project_in = MagicMock()
    project_in.name = "Test Project"
    project_in.description = "Desc"
    project_in.authorization_provider_group_id = "group1"

    # Mock DB add
    mock_db_session.add.return_value = None
    mock_db_session.commit.return_value = None
    mock_db_session.refresh.return_value = None

    project = ProjectService.create_project(mock_db_session, project_in, mock_user)
    
    assert mock_db_session.add.called
    added_project = mock_db_session.add.call_args[0][0]
    assert added_project.name == "Test Project"
    assert added_project.owner_id == mock_user["sub"]

def test_get_project_owner_access(mock_db_session, mock_user):
    project_id = uuid.uuid4()
    project = Project(id=project_id, owner_id=mock_user["sub"], authorization_provider_group_id="other")
    
    mock_db_session.query.return_value.filter.return_value.first.return_value = project

    result = ProjectService.get_project(mock_db_session, project_id, mock_user)
    assert result == project

def test_get_project_group_access(mock_db_session, mock_user):
    project_id = uuid.uuid4()
    # User has group1, project has group1
    project = Project(id=project_id, owner_id="other", authorization_provider_group_id="group1")
    
    mock_db_session.query.return_value.filter.return_value.first.return_value = project

    result = ProjectService.get_project(mock_db_session, project_id, mock_user)
    assert result == project

def test_get_project_no_access(mock_db_session, mock_user):
    project_id = uuid.uuid4()
    project = Project(id=project_id, owner_id="other", authorization_provider_group_id="other_group")
    
    mock_db_session.query.return_value.filter.return_value.first.return_value = project
    # Mock no member found
    mock_db_session.query.return_value.filter.return_value.first.side_effect = [project, None]

    with pytest.raises(AuthorizationException):
        ProjectService.get_project(mock_db_session, project_id, mock_user)

def test_delete_project_owner(mock_db_session, mock_user):
    project_id = uuid.uuid4()
    project = Project(id=project_id, owner_id=mock_user["sub"])
    
    mock_db_session.query.return_value.filter.return_value.first.return_value = project

    ProjectService.delete_project(mock_db_session, project_id, mock_user)
    mock_db_session.delete.assert_called_with(project)

def test_delete_project_not_owner_not_admin(mock_db_session, mock_user):
    project_id = uuid.uuid4()
    project = Project(id=project_id, owner_id="other")
    
    mock_db_session.query.return_value.filter.return_value.first.return_value = project

    with pytest.raises(AuthorizationException):
        ProjectService.delete_project(mock_db_session, project_id, mock_user)

@patch("app.services.timeio.orchestrator_v3.orchestrator_v3")
def test_list_sensors_rich(mock_orchestrator, mock_db_session, mock_user):
    project_id = uuid.uuid4()
    project = Project(id=project_id, name="p1", owner_id=mock_user["sub"], authorization_provider_group_id="g1")
    
    # Mock project find
    mock_db_session.query.return_value.filter.return_value.first.return_value = project
    
    # Mock linked sensors
    mock_db_session.execute.return_value.scalars.return_value.all.return_value = ["uuid1"]
    
    # Mock orchestrator
    mock_orchestrator.list_sensors.return_value = [{"uuid": "uuid1", "name": "s1"}, {"uuid": "uuid2", "name": "s2"}]
    
    result = ProjectService.list_sensors(mock_db_session, project_id, mock_user, rich=True)
    
    assert len(result) == 1
    assert result[0]["uuid"] == "uuid1"
