import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api import deps
from app.api.deps import get_db
from app.models.user_context import Project
from app.services.timeio.orchestrator_v3 import orchestrator_v3

logger = logging.getLogger(__name__)


router = APIRouter()


class SensorProperty(BaseModel):
    name: str = Field(..., description="Machine-readable name (e.g. 'temp')")
    unit: str = Field("Unknown", description="Unit of measurement (e.g. 'Celsius')")
    label: Optional[str] = Field(
        None, description="Human-readable label (e.g. 'Air Temperature')"
    )


class SensorCreate(BaseModel):
    project_uuid: str = Field(
        ...,
        description="Project UUID from water_dp-api",
        example="1bfde64c-a785-416a-a513-6be718055ce1",
    )
    sensor_name: str = Field(
        ..., description="Name of the sensor/thing", example="Station 01"
    )
    description: str = Field("", example="Main monitoring station at the river")
    device_type: str = Field("chirpstack_generic", example="chirpstack_generic")
    latitude: Optional[float] = Field(None, example=51.34)
    longitude: Optional[float] = Field(None, example=12.37)
    properties: Optional[List[SensorProperty]] = Field(
        None, description="List of properties with units"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "project_uuid": "1bfde64c-a785-416a-a513-6be718055ce1",
                "sensor_name": "Station 01",
                "description": "Main monitoring station at the river",
                "device_type": "chirpstack_generic",
                "latitude": 51.34,
                "longitude": 12.37,
                "properties": [
                    {"name": "temp", "unit": "Celsius", "label": "Air Temperature"},
                    {
                        "name": "humidity",
                        "unit": "Percent",
                        "label": "Relative Humidity",
                    },
                ],
            }
        }
    }


class SensorLocationUpdate(BaseModel):
    project_schema: str = Field(
        ..., description="Project database schema (e.g. 'user_water_dp')"
    )
    latitude: float
    longitude: float


class DatastreamRich(BaseModel):
    name: str
    unit: str
    label: str
    properties: Optional[Dict[str, Any]] = None


class SensorRich(BaseModel):
    uuid: str
    name: str
    description: Optional[str] = ""
    properties: Optional[Dict[str, Any]] = None
    datastreams: List[DatastreamRich]


@router.get(
    "/",
    response_model=List[SensorRich],
    summary="List Sensors (Rich Metadata)",
    description="Returns all sensors for a project with human-readable units, labels, and metadata.",
)
async def list_sensors(project_uuid: str, database: Session = Depends(get_db)):
    """
    Fetch all sensors for a project with full human-readable details.
    """
    try:
        project = database.query(Project).filter(Project.id == project_uuid).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        return orchestrator_v3.list_sensors(
            project_name=project.name,
            project_group=project.authorization_provider_group_id,
        )
    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list sensors: {str(error)}",
        )


@router.post(
    "/",
    response_model=Dict[str, Any],
    status_code=status.HTTP_201_CREATED,
    summary="Create Sensor (Autonomous v3)",
    description="Registers a new sensor in ConfigDB and triggers TSM workers via MQTT. Bypasses legacy APIs.",
)
async def create_sensor(
    sensor: SensorCreate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
):
    """
    Create a new sensor autonomously (v3).

    This bypasses the legacy thing-management-api and works directly with
    the TimeIO ConfigDB and MQTT bus.
    """
    try:
        location = None
        if sensor.latitude is not None and sensor.longitude is not None:
            location = {"latitude": sensor.latitude, "longitude": sensor.longitude}

        # Fetch project details for refined schema naming
        project = database.query(Project).filter(Project.id == sensor.project_uuid).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        result = orchestrator_v3.create_sensor(
            project_group_id=project.authorization_provider_group_id,
            sensor_name=sensor.sensor_name,
            description=sensor.description,
            device_type=sensor.device_type,
            location=location,
            properties=(
                [prop.dict() for prop in sensor.properties] if sensor.properties else None
            ),
        )

        # Automatic Link to Project
        from app.services.project_service import ProjectService

        try:
            # Check permissions implicitly via add_sensor (requires 'editor')
            # The result['uuid'] is the new Thing UUID
            ProjectService.add_sensor(
                database,
                project_id=sensor.project_uuid,
                sensor_id=result["uuid"],
                user=user,
            )
        except Exception as error:
            # We log but don't fail the whole request because the sensor IS created in TimeIO
            # The user might just need to link it manually if this failed (e.g. permissions/race condition)
            print(f"Failed to auto-link sensor to project: {error}")

        return result
    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create sensor: {str(error)}",
        )


@router.put(
    "/{uuid}/location",
    response_model=Dict[str, Any],
    summary="Update Sensor Location",
    description="Directly updates sensor coordinates in the project's dedicated database schema.",
)
async def update_location(uuid: str, update: SensorLocationUpdate):
    """
    Update sensor location directly in the project database.
    """
    try:
        success = orchestrator_v3.update_sensor_location(
            thing_uuid=uuid,
            project_schema=update.project_schema,
            latitude=update.latitude,
            longitude=update.longitude,
        )
        if not success:
            raise HTTPException(
                status_code=404, detail="Sensor not found or update failed"
            )
        return {"status": "success", "uuid": uuid}
    except Exception as error:
        logger.error(f"Error updating sensor location: {error}")
        raise HTTPException(status_code=500, detail=str(error))


@router.post(
    "/{uuid}/ingest/csv",
    response_model=Dict[str, Any],
    summary="Ingest CSV Data",
    description="Upload a CSV file to the Thing's S3 bucket for ingestion.",
)
async def ingest_csv(
    uuid: str,
    file: UploadFile = File(...),
    database: Session = Depends(get_db),
    # user: dict = Depends(deps.get_current_user) # Optional auth check
):
    """
    Upload CSV for ingestion.
    """
    from app.services.ingestion_service import IngestionService

    return await IngestionService.upload_csv(uuid, file)
