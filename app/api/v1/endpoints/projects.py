"""
v1 Projects Endpoints

.. deprecated::
    Some endpoints in this module are deprecated in favor of v2 endpoints.
    For thing management with automatic TimeIO fixes, use `/api/v2/projects/{id}/things`.
    For TimeIO diagnostics, use `/api/v2/admin/timeio/`.
"""

import logging
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
    ProjectMemberUpdate,
    ProjectResponse,
    ProjectSensorResponse,
    ProjectUpdate,
)
from app.services.dashboard_service import DashboardService
from app.services.project_service import ProjectService
from app.services.timeio.timeio_db import TimeIODatabase

router = APIRouter()
logger = logging.getLogger(__name__)

# --- Projects ---


@router.post("/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
def create_project(
    project_in: ProjectCreate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Create a new project."""
    return ProjectService.create_project(database, project_in, user)


@router.get("/", response_model=List[ProjectResponse])
def list_projects(
    skip: int = 0,
    limit: int = 100,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """List projects (owned or member of)."""
    return ProjectService.list_projects(database, user, skip=skip, limit=limit)


@router.get("/{project_id}", response_model=ProjectResponse)
def get_project(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Get project details."""
    return ProjectService.get_project(database, project_id, user)


@router.put("/{project_id}", response_model=ProjectResponse)
def update_project(
    project_id: UUID,
    project_in: ProjectUpdate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    # Need to import ProjectUpdate schema if not available, but it should be in user_context
    return ProjectService.update_project(database, project_id, project_in, user)


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> None:
    """Delete a project."""
    ProjectService.delete_project(database, project_id, user)
    return


# --- Project Members ---


@router.get("/{project_id}/members", response_model=List[ProjectMemberResponse])
def list_project_members(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """List project members."""
    return ProjectService.list_members(database, project_id, user)


@router.post("/{project_id}/members", response_model=ProjectMemberResponse)
def add_project_member(
    project_id: UUID,
    member_in: ProjectMemberCreate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """
    Add a member to the project.
    Can provide `user_id` (UUID) OR `username`.
    """
    from app.services.keycloak_service import KeycloakService

    if member_in.username:
        # Resolve username to ID
        target_user = KeycloakService.get_user_by_username(member_in.username)
        if not target_user:
            raise HTTPException(
                status_code=404, detail=f"User '{member_in.username}' not found."
            )
        member_in.user_id = target_user.get("id")

    if not member_in.user_id:
        raise HTTPException(
            status_code=400, detail="Either user_id or username is required."
        )

    return ProjectService.add_member(database, project_id, member_in, user)


@router.put("/{project_id}/members/{user_id}", response_model=ProjectMemberResponse)
def update_project_member(
    project_id: UUID,
    user_id: str,
    member_in: ProjectMemberUpdate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Update a member's role."""
    return ProjectService.update_member(
        database, project_id, user_id, member_in.role, user
    )


@router.delete("/{project_id}/members/{user_id}")
def remove_project_member(
    project_id: UUID,
    user_id: str,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Remove a member from the project."""
    return ProjectService.remove_member(database, project_id, user_id, user)


# --- Project Sensors ---

@router.get("/{project_id}/sensors", response_model=List[Any])
def list_project_sensors(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """List sensors in project with basic metadata from database."""
    return ProjectService.get_linked_sensors(database, project_id, user)
    

@router.get("/{project_id}/available-sensors", response_model=List[Any])
def get_available_sensors(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """List sensors available in FROST that are NOT linked to this project."""
    return ProjectService.get_available_sensors(database, project_id, user)


@router.post("/{project_id}/sensors", response_model=ProjectSensorResponse)
def add_project_sensor(
    project_id: UUID,
    sensor_id: str = Query(..., description="TimeIO Thing UUID"),
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """
    Link a sensor to the project.
    """
    return ProjectService.add_sensor(database, project_id, sensor_id, user)


@router.delete("/{project_id}/sensors/{sensor_id}")
def remove_project_sensor(
    project_id: UUID,
    sensor_id: str,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Remove a sensor from the project."""
    return ProjectService.remove_sensor(database, project_id, sensor_id, user)


# --- Project Dashboards (Convenience) ---


@router.get("/{project_id}/dashboards", response_model=List[DashboardResponse])
def list_project_dashboards(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """List dashboards in project."""
    return DashboardService.list_dashboards(database, project_id, user)


@router.post("/{project_id}/dashboards", response_model=DashboardResponse)
def create_project_dashboard(
    project_id: UUID,
    dashboard_in: DashboardCreate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Create a dashboard in the project."""
    # Ensure project_id matches if passed in body
    if dashboard_in.project_id and dashboard_in.project_id != project_id:
        raise HTTPException(
            status_code=400,
            detail="Project ID in body does not match URL parameter",
        )
    dashboard_in = dashboard_in.model_copy(update={"project_id": project_id})
    return DashboardService.create_dashboard(database, dashboard_in, user)
