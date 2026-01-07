from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.user_context import Dashboard
from app.schemas.user_context import DashboardCreate, DashboardUpdate
from app.services.project_service import ProjectService


class DashboardService:
    @staticmethod
    def create_dashboard(
        db: Session, dashboard_in: DashboardCreate, user: Dict[str, Any]
    ) -> Dashboard:
        # Check if user has editor access to project
        ProjectService._check_access(
            db, dashboard_in.project_id, user, required_role="editor"
        )

        db_dashboard = Dashboard(
            project_id=dashboard_in.project_id,
            name=dashboard_in.name,
            layout_config=dashboard_in.layout_config,
            widgets=dashboard_in.widgets,
            is_public=dashboard_in.is_public,
        )
        db.add(db_dashboard)
        db.commit()
        db.refresh(db_dashboard)
        return db_dashboard

    @staticmethod
    def get_dashboard(
        db: Session, dashboard_id: UUID, user: Optional[Dict[str, Any]] = None
    ) -> Dashboard:
        """
        Get dashboard. Allows public access if is_public=True.
        If private, requires authenticated user validation against Project.
        """
        dashboard = db.query(Dashboard).filter(Dashboard.id == dashboard_id).first()
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")

        if dashboard.is_public:
            return dashboard

        # Private: Check user access
        if not user:
            raise HTTPException(
                status_code=401, detail="Authentication required for private dashboard"
            )

        # Delegate to ProjectService to check role on parent project
        # Just calling it will raise exception if no access
        ProjectService._check_access(
            db, dashboard.project_id, user, required_role="viewer"
        )

        return dashboard

    @staticmethod
    def list_dashboards(
        db: Session, project_id: UUID, user: Dict[str, Any]
    ) -> List[Dashboard]:
        # Typically list is for project members, so check project access
        ProjectService._check_access(db, project_id, user, required_role="viewer")
        return db.query(Dashboard).filter(Dashboard.project_id == project_id).all()

    @staticmethod
    def update_dashboard(
        db: Session,
        dashboard_id: UUID,
        dashboard_in: DashboardUpdate,
        user: Dict[str, Any],
    ) -> Dashboard:
        dashboard = db.query(Dashboard).filter(Dashboard.id == dashboard_id).first()
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")

        # Check write access to parent project
        ProjectService._check_access(
            db, dashboard.project_id, user, required_role="editor"
        )

        if dashboard_in.name is not None:
            dashboard.name = dashboard_in.name
        if dashboard_in.layout_config is not None:
            dashboard.layout_config = dashboard_in.layout_config
        if dashboard_in.widgets is not None:
            dashboard.widgets = dashboard_in.widgets
        if dashboard_in.is_public is not None:
            dashboard.is_public = dashboard_in.is_public

        db.commit()
        db.refresh(dashboard)
        return dashboard

    @staticmethod
    def delete_dashboard(
        db: Session, dashboard_id: UUID, user: Dict[str, Any]
    ) -> Dashboard:
        dashboard = db.query(Dashboard).filter(Dashboard.id == dashboard_id).first()
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")

        # Check write access to parent project
        ProjectService._check_access(
            db, dashboard.project_id, user, required_role="editor"
        )

        db.delete(dashboard)
        db.commit()
        return dashboard
