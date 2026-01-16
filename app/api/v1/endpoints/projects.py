import logging
from datetime import datetime
from typing import Any, List, Optional
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
    SensorCreate,
    SensorDataPoint,
    SensorDetail,
)
from app.services.dashboard_service import DashboardService
from app.services.project_service import ProjectService

router = APIRouter()
logger = logging.getLogger(__name__)

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
    # Need to import ProjectUpdate schema if not available, but it should be in user_context
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
    """
    Add a member to the project.
    Can provide `user_id` (UUID) OR `username`.
    """
    from app.services.keycloak_service import KeycloakService

    if member_in.username:
        # Resolve username to ID
        user = KeycloakService.get_user_by_username(member_in.username)
        if not user:
            raise HTTPException(
                status_code=404, detail=f"User '{member_in.username}' not found."
            )
        member_in.user_id = user.get("id")

    if not member_in.user_id:
        raise HTTPException(
            status_code=400, detail="Either user_id or username is required."
        )

    return ProjectService.add_member(db, project_id, member_in, current_user)


@router.put("/{project_id}/members/{user_id}", response_model=ProjectMemberResponse)
def update_project_member(
    project_id: UUID,
    user_id: str,
    member_in: ProjectMemberUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Update a member's role."""
    return ProjectService.update_member(
        db, project_id, user_id, member_in.role, current_user
    )


@router.delete("/{project_id}/members/{user_id}")
def remove_project_member(
    project_id: UUID,
    user_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Remove a member from the project."""
    return ProjectService.remove_member(db, project_id, user_id, current_user)


# --- Project Sensors ---


@router.get("/{project_id}/sensors", response_model=List[SensorDetail])
def list_project_sensors(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List sensors in project with details."""
    # 1. Get List of IDs
    sensor_ids = ProjectService.list_sensors(db, project_id, current_user)

    # 2. Fetch Details from TimeSeriesService
    from app.services.time_series_service import TimeSeriesService

    ts_service = TimeSeriesService(db)

    results = []

    for sid in sensor_ids:
        try:
            # Get Station (Thing)
            station = ts_service.get_station(sid)
            if not station:
                continue

            # Consolidate ID
            real_id = str(station.get("id"))

            # Get Latest Data using consolidated ID
            latest_data_raw = ts_service.get_latest_data(real_id)

            # Map latest data
            data_points = []
            last_timestamp = None

            for d in latest_data_raw:
                dp = SensorDataPoint(
                    parameter=d.get("parameter", "unknown"),
                    value=d.get("value"),
                    unit=d.get("unit", ""),
                    timestamp=d.get("timestamp"),
                )
                data_points.append(dp)

                # Track most recent update
                if not last_timestamp or (
                    dp.timestamp and dp.timestamp > last_timestamp
                ):
                    last_timestamp = dp.timestamp

            results.append(
                SensorDetail(
                    id=real_id,
                    name=station.get("name") or "Unknown Sensor",
                    description=station.get("description"),
                    latitude=station.get("latitude"),
                    longitude=station.get("longitude"),
                    status=station.get("status", "active"),
                    last_activity=last_timestamp,
                    updated_at=station.get("updated_at")
                    or last_timestamp
                    or datetime.now(),
                    latest_data=data_points,
                    station_type=station.get("station_type", "unknown"),
                    properties=station.get("properties", {}),
                )
            )

        except Exception as e:
            logger.warning(f"Failed to fetch details for sensor {sid}: {e}")
            continue

    return results


@router.get("/{project_id}/available-sensors", response_model=List[Any])
def get_available_sensors(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """List sensors available in FROST that are NOT linked to this project."""
    return ProjectService.get_available_sensors(db, project_id, current_user)


@router.post("/{project_id}/sensors", response_model=ProjectSensorResponse)
def add_project_sensor(
    project_id: UUID,
    sensor_id: Optional[str] = Query(None, description="TimeIO Thing ID"),
    sensor_data: Optional[SensorCreate] = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """
    Add a sensor to the project.

    - If `sensor_id` is provided: Links an existing sensor.
    - If `sensor_data` is provided: Creates a new sensor and links it.
    """
    if sensor_id:
        return ProjectService.add_sensor(db, project_id, sensor_id, current_user)
    elif sensor_data:
        return ProjectService.create_and_link_sensor(
            db, project_id, sensor_data, current_user
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Must provide either sensor_id (to link) or body (to create).",
        )


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
