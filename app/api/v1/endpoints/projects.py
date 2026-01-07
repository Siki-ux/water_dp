from typing import Any, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.schemas.user_context import (
    DashboardCreate,
    DashboardResponse,
    ProjectCreate,
    ProjectMemberCreate,
    ProjectMemberResponse,
    ProjectResponse,
    ProjectSensorResponse,
    ProjectUpdate,
)
from app.services.dashboard_service import DashboardService
from app.services.project_service import ProjectService

router = APIRouter()

# --- Projects ---


@router.post("/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
def create_project(
    project_in: ProjectCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Create a new project."""
    return ProjectService.create_project(db, project_in, current_user)


@router.get("/", response_model=List[ProjectResponse])
def list_projects(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List projects (owned or member of)."""
    return ProjectService.list_projects(db, current_user, skip=skip, limit=limit)


@router.get("/{project_id}", response_model=ProjectResponse)
def get_project(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Get project details."""
    return ProjectService.get_project(db, project_id, current_user)


@router.put("/{project_id}", response_model=ProjectResponse)
def update_project(
    project_id: UUID,
    project_in: ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Update project details."""
    return ProjectService.update_project(db, project_id, project_in, current_user)


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> None:
    """Delete a project."""
    ProjectService.delete_project(db, project_id, current_user)
    return


# --- Project Members ---


@router.get("/{project_id}/members", response_model=List[ProjectMemberResponse])
def list_project_members(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List project members."""
    return ProjectService.list_members(db, project_id, current_user)


@router.post("/{project_id}/members", response_model=ProjectMemberResponse)
def add_project_member(
    project_id: UUID,
    member_in: ProjectMemberCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Add a member to the project."""
    return ProjectService.add_member(db, project_id, member_in, current_user)


# --- Project Sensors ---


@router.get("/{project_id}/sensors", response_model=List[str])
def list_project_sensors(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List sensors in project."""
    return ProjectService.list_sensors(db, project_id, current_user)


@router.post("/{project_id}/sensors", response_model=ProjectSensorResponse)
def add_project_sensor(
    project_id: UUID,
    sensor_id: str = Query(..., description="TimeIO Thing ID"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Add a sensor to the project."""
    return ProjectService.add_sensor(db, project_id, sensor_id, current_user)


@router.delete("/{project_id}/sensors/{sensor_id}")
def remove_project_sensor(
    project_id: UUID,
    sensor_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Remove a sensor from the project."""
    return ProjectService.remove_sensor(db, project_id, sensor_id, current_user)


# --- Project Dashboards (Convenience) ---


@router.get("/{project_id}/dashboards", response_model=List[DashboardResponse])
def list_project_dashboards(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List dashboards in project."""
    return DashboardService.list_dashboards(db, project_id, current_user)


@router.post("/{project_id}/dashboards", response_model=DashboardResponse)
def create_project_dashboard(
    project_id: UUID,
    dashboard_in: DashboardCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Create a dashboard in the project."""
    # Ensure project_id matches if passed in body
    if dashboard_in.project_id and dashboard_in.project_id != project_id:
        raise HTTPException(
            status_code=400,
            detail="Project ID in body does not match URL parameter",
        )
    dashboard_in = dashboard_in.model_copy(update={"project_id": project_id})
    return DashboardService.create_dashboard(db, dashboard_in, current_user)
