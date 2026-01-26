from typing import Any, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_current_active_superuser, get_current_user, get_db
from app.schemas.datasource import (
    DataSourceCreate,
    DataSourceResponse,
    DataSourceUpdate,
    QueryRequest,
)
from app.services.datasource_service import DataSourceService

router = APIRouter()


@router.get(
    "/projects/{project_id}/datasources", response_model=List[DataSourceResponse]
)
def get_project_datasources(
    project_id: UUID,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Get all datasources for a project.
    """
    datasource_service = DataSourceService(database)
    # Ideally verify project access here
    datasources = datasource_service.get_by_project(project_id)

    return datasources


@router.post("/projects/{project_id}/datasources", response_model=DataSourceResponse)
def create_datasource(
    project_id: UUID,
    schema: DataSourceCreate,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Create a new datasource.
    """
    datasource_service = DataSourceService(database)
    datasource = datasource_service.create(project_id, schema)

    return datasource


@router.put(
    "/projects/{project_id}/datasources/{datasource_id}",
    response_model=DataSourceResponse,
)
def update_datasource(
    project_id: UUID,
    datasource_id: UUID,
    schema: DataSourceUpdate,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Update a datasource.
    """
    datasource_service = DataSourceService(database)
    datasource = datasource_service.update(datasource_id, schema)
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")

    return datasource


@router.delete("/projects/{project_id}/datasources/{datasource_id}")
def delete_datasource(
    project_id: UUID,
    datasource_id: UUID,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Delete a datasource.
    """
    datasource_service = DataSourceService(database)
    success = datasource_service.delete(datasource_id)
    if not success:
        raise HTTPException(status_code=404, detail="Datasource not found")
    return {"status": "success"}


@router.post("/projects/{project_id}/datasources/{datasource_id}/test")
def test_connection(
    project_id: UUID,
    datasource_id: UUID,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Test connection to datasource.
    """
    datasource_service = DataSourceService(database)
    datasource = datasource_service.get(datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")

    success = datasource_service.test_connection(datasource)
    if success:
        return {"status": "success", "message": "Connection successful"}
    else:
        raise HTTPException(status_code=400, detail="Connection failed")


@router.post("/projects/{project_id}/datasources/{datasource_id}/query")
def execute_query(
    project_id: UUID,
    datasource_id: UUID,
    query: QueryRequest,
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_active_superuser),  # Admin only
):
    """
    Execute raw SQL on a datasource.
    RESTRICTED: Admins only.
    """
    datasource_service = DataSourceService(database)
    datasource = datasource_service.get(datasource_id)
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")

    try:
        result = datasource_service.execute_query(datasource, query.sql)
        return result
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@router.get("/datasources/available-sensors")
def get_available_sensors(
    database: Session = Depends(get_db),
    user: Any = Depends(get_current_user),
):
    """
    Get list of available sensors (Things) from TimeIO (FROST).
    Used for linking sensors to projects.
    """
    import requests

    from app.core.config import settings

    frost_url = settings.frost_url
    try:
        # Fetch Things from FROST
        # We limit to 100 for now, or implement pagination if needed.
        # Minimal fields: id, name, description, properties
        response = requests.get(
            f"{frost_url}/Things?$select=@iot.id,name,description,properties&$top=100",
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        things = data.get("value", [])

        # Transform for frontend if needed, or return raw.
        # Frontend expects id, name, description. properties.
        # FROST returns @iot.id. We map it to id.
        formatted = []
        for thing in things:
            formatted.append(
                {
                    "id": str(thing.get("@iot.id")),
                    "name": thing.get("name"),
                    "description": thing.get("description"),
                    "properties": thing.get("properties", {}),
                }
            )

        return formatted

    except Exception as error:
        raise HTTPException(
            status_code=502, detail=f"Failed to fetch sensors from TimeIO: {error}"
        )
