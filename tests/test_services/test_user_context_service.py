from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.models.user_context import Dashboard, Project, ProjectMember
from app.schemas.user_context import (
    DashboardCreate,
    ProjectCreate,
    ProjectMemberCreate,
)
from app.services.dashboard_service import DashboardService
from app.services.project_service import ProjectService

# Constants
USER_OWNER = {"sub": "owner-123", "realm_access": {"roles": ["user"]}}
USER_ADMIN = {"sub": "admin-999", "realm_access": {"roles": ["admin"]}}
USER_MEMBER = {"sub": "member-456", "realm_access": {"roles": ["user"]}}
USER_OTHER = {"sub": "other-789", "realm_access": {"roles": ["user"]}}


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def sample_project():
    return Project(
        id=uuid4(), name="Test Project", description="Desc", owner_id=USER_OWNER["sub"]
    )


@pytest.fixture
def sample_dashboard(sample_project):
    return Dashboard(
        id=uuid4(), project_id=sample_project.id, name="Test Dash", is_public=False
    )


class TestProjectService:
    def test_create_project(self, mock_db):
        p_in = ProjectCreate(name="New Project", description="New Desc")
        result = ProjectService.create_project(mock_db, p_in, USER_OWNER)

        assert result.name == "New Project"
        assert result.owner_id == USER_OWNER["sub"]
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    def test_get_project_owner_success(self, mock_db, sample_project):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        result = ProjectService.get_project(mock_db, sample_project.id, USER_OWNER)
        assert result.id == sample_project.id

    def test_get_project_admin_success(self, mock_db, sample_project):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        result = ProjectService.get_project(mock_db, sample_project.id, USER_ADMIN)
        assert result.id == sample_project.id

    def test_get_project_member_success(self, mock_db, sample_project):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        # Mock member query
        member = ProjectMember(
            project_id=sample_project.id, user_id=USER_MEMBER["sub"], role="viewer"
        )
        # The service calls .first() on the member query.
        # Logic: project query first, then member query.
        # We need to setup side_effect for query or checking call sequence.

        # Simplified mock setup for chained query
        # Because Service makes multiple queries, we need to be careful.
        # 1. Project Query
        # 2. Member Query

        # Let's use side_effect for db.query to return different mocks
        query_mock = MagicMock()
        mock_db.query.return_value = query_mock

        # We need to distinguish between Project query and ProjectMember query.
        # This is tricky with simple mocks.
        # Easier strategy: Mock .filter().first() to return based on logic or use specific checking.

        # But here, let's just assume standard flow.
        # We can inspect the arguments to filter if needed, but 'return_value' is often shared.

        # Alternative: Just mock the specific lines.
        # or use a more robust mock library or just patch the _check_access method if we trusted it?
        # No, we want to test _check_access logic.

        # Let's mock the project return
        # The service does: project = db.query(Project).filter(...).first()
        # Then: member = db.query(ProjectMember).filter(...).first()

        # If we return sample_project for ANY first(), the second query will return sample_project (which is wrong type).
        # We must make db.query(Project) return one mock, db.query(ProjectMember) another.

        def query_side_effect(model):
            m = MagicMock()
            if model == Project:
                m.filter.return_value.first.return_value = sample_project
            elif model == ProjectMember:
                m.filter.return_value.first.return_value = member
            return m

        mock_db.query.side_effect = query_side_effect

        result = ProjectService.get_project(mock_db, sample_project.id, USER_MEMBER)
        assert result == sample_project

    def test_get_project_access_denied(self, mock_db, sample_project):
        def query_side_effect(model):
            m = MagicMock()
            if model == Project:
                m.filter.return_value.first.return_value = sample_project
            elif model == ProjectMember:
                m.filter.return_value.first.return_value = None  # Not a member
            return m

        mock_db.query.side_effect = query_side_effect

        with pytest.raises(HTTPException) as exc:
            ProjectService.get_project(mock_db, sample_project.id, USER_OTHER)
        assert exc.value.status_code == 403

    def test_add_member_owner_success(self, mock_db, sample_project):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        m_in = ProjectMemberCreate(user_id="new-user", role="viewer")
        result = ProjectService.add_member(mock_db, sample_project.id, m_in, USER_OWNER)

        assert result.user_id == "new-user"
        mock_db.add.assert_called()

    def test_add_member_non_owner_fail(self, mock_db, sample_project):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        m_in = ProjectMemberCreate(user_id="new-user", role="viewer")
        with pytest.raises(HTTPException) as exc:
            ProjectService.add_member(mock_db, sample_project.id, m_in, USER_OTHER)
        assert exc.value.status_code == 403


class TestDashboardService:
    def test_get_public_dashboard(self, mock_db, sample_dashboard):
        sample_dashboard.is_public = True
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_dashboard
        )

        result = DashboardService.get_dashboard(mock_db, sample_dashboard.id, None)
        assert result.id == sample_dashboard.id

    def test_get_private_dashboard_no_auth(self, mock_db, sample_dashboard):
        sample_dashboard.is_public = False
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_dashboard
        )

        with pytest.raises(HTTPException) as exc:
            DashboardService.get_dashboard(mock_db, sample_dashboard.id, None)
        assert exc.value.status_code == 401

    def test_create_dashboard_success(self, mock_db, sample_project):
        # Must mock ProjectService._check_access or set up DB mocks to pass it.
        # Since DashboardService calls ProjectService._check_access explicitly...
        # We should rely on db mocks behaving correctly for that check.

        # create_dashboard calls check_access(project_id, user, "editor")

        # Mock DB to return project and member=editor for USER_MEMBER
        editor_member = ProjectMember(
            project_id=sample_project.id, user_id=USER_MEMBER["sub"], role="editor"
        )

        def query_side_effect(model):
            m = MagicMock()
            if model == Project:
                m.filter.return_value.first.return_value = sample_project
            elif model == ProjectMember:
                m.filter.return_value.first.return_value = editor_member
            return m

        mock_db.query.side_effect = query_side_effect

        d_in = DashboardCreate(
            project_id=sample_project.id,
            name="New Dash",
            widgets=[{"type": "chart", "sensor_id": "123"}],
        )
        result = DashboardService.create_dashboard(mock_db, d_in, USER_MEMBER)

        assert result.name == "New Dash"
        assert result.project_id == sample_project.id
        assert isinstance(result.widgets, list)
        assert len(result.widgets) == 1

    def test_update_dashboard_success(self, mock_db, sample_dashboard):
        # Mock check_access to return the dashboard (meaning user has access)
        # DashboardService.update_dashboard calls check_access with 'editor'

        # Mock DB queries
        # 1. check_access -> get project -> get member
        # 2. update -> commit -> refresh

        # Simplify by mocking check_access logic if possible, or setup DB mocks
        # Setup: project exists, member is editor

        # Mock Project query for check_access (DashboardService.check_access -> ProjectService.check_access)
        # Wait, DashboardService.update_dashboard calls ProjectService._check_access directly?
        # No, it calls DashboardService.get_dashboard or similar usually?
        # Let's check implementation.
        # DashboardService.update_dashboard(db, dashboard_id, update_in, user)
        # It gets dashboard, checks permissions.

        # Let's assume standard flow:
        # DashboardService.update_dashboard checks if user is editor of project.

        # Mock finding the dashboard
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_dashboard
        )

        # Mock ProjectService._check_access call? No, integration test style or mock Service method.
        # But we are testing Service, so we should mock DB.

        # We need to mock:
        # 1. db.query(Dashboard).filter().first() -> sample_dashboard
        # 2. ProjectService._check_access(db, project_id, user, 'editor') -> Project

        # Since _check_access is static, we can patch it or mock DB to satisfy it.
        # Let's patch ProjectService._check_access for simplicity in this test file to avoid complex DB mocking for permission checks again.
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(ProjectService, "_check_access", MagicMock())

            from app.schemas.user_context import DashboardUpdate

            d_in = DashboardUpdate(name="Updated Dash")

            result = DashboardService.update_dashboard(
                mock_db, sample_dashboard.id, d_in, USER_MEMBER
            )

            assert result.name == "Updated Dash"
            mock_db.commit.assert_called()

    def test_delete_dashboard_success(self, mock_db, sample_dashboard):
        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_dashboard
        )

        # Mock check_access
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(ProjectService, "_check_access", MagicMock())

            DashboardService.delete_dashboard(mock_db, sample_dashboard.id, USER_MEMBER)

            mock_db.delete.assert_called_with(sample_dashboard)
            mock_db.commit.assert_called()


class TestProjectServiceExtended:
    """Additional tests for coverage."""

    def test_update_project(self, mock_db, sample_project):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                ProjectService, "_check_access", MagicMock(return_value=sample_project)
            )

            from app.schemas.user_context import ProjectUpdate

            p_in = ProjectUpdate(name="Updated Name")

            result = ProjectService.update_project(
                mock_db, sample_project.id, p_in, USER_OWNER
            )
            assert result.name == "Updated Name"
            mock_db.commit.assert_called()

    def test_delete_project(self, mock_db, sample_project):
        # delete_project checks if user is owner or admin directly, mostly.
        # Logic: get project, check owner_id vs user sub.

        mock_db.query.return_value.filter.return_value.first.return_value = (
            sample_project
        )

        ProjectService.delete_project(mock_db, sample_project.id, USER_OWNER)

        mock_db.delete.assert_called_with(sample_project)
        mock_db.commit.assert_called()

    def test_remove_sensor(self, mock_db, sample_project):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(ProjectService, "_check_access", MagicMock())

            ProjectService.remove_sensor(
                mock_db, sample_project.id, "sensor-1", USER_MEMBER
            )

            # Verify execute called for delete
            mock_db.execute.assert_called()
            mock_db.commit.assert_called()
