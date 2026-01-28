import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api import deps
from app.api.deps import get_db
from app.models.user_context import Project
from app.services.timeio.orchestrator_v3 import orchestrator_v3

from app.services.timeio.frost_client import get_cached_frost_client
from app.core.config import settings
from app.services.timeio.timeio_db import TimeIODatabase
from app.services.thing_service import ThingService
from app.schemas.frost.thing import Thing
from app.schemas.frost.datastream import Datastream, Observation


from app.core.exceptions import (
    AuthorizationException,
    ResourceNotFoundException,
    ValidationException,
)

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
    parser_id: Optional[int] = Field(None, description="ID of the CSV Parser to associate")

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
                    {
                        "name": "temperature", 
                        "unit": "Celsius", 
                        "symbol": "°C",
                        "label": "Air Temperature"
                    },
                    {
                        "name": "humidity",
                        "unit": "Percent",
                        "symbol": "%",
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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    properties: Optional[Dict[str, Any]] = None
    datastreams: List[DatastreamRich]

@router.get(
    "/{schema_name}/all",
    response_model=List[Thing],
    summary="List Sensors",
    description="Returns all sensors for a project.",
)
async def list_sensors(schema_name: str, expand: list[str] = ["Locations","Datastreams"]):
    """
    Fetch all sensors for a project.
    """
    
    things = ThingService.get_all_things(schema_name, expand)
    
    if things is None:
        return []
    return things

@router.get(
    "/{sensor_uuid}",
    response_model=Thing,
    summary="Get Sensor Details",
    description="Fetch Sensor details from via FROST.",
)
async def get_thing_details(sensor_uuid: str, expand: list[str] = ["Locations","Datastreams"]):
    """
    Get Sensor details from via FROST.
    """
    schema_name = TimeIODatabase().get_schema_from_uuid(sensor_uuid)
    if schema_name is None:
        raise ResourceNotFoundException("Schema not found")
    thing_service = ThingService(schema_name)
    thing = thing_service.get_thing(sensor_uuid, expand)
    if not thing:
        raise ResourceNotFoundException("Thing not found")
    return thing


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
            parser_id=sensor.parser_id,
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

        if location:
            result["latitude"] = location["latitude"]
            result["longitude"] = location["longitude"]

        return result
    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create sensor: {str(error)}",
        )

@router.get(
    "/{uuid}/datastreams",
    response_model=List[Datastream],
    summary="Get Sensor Datastreams (FROST)",
    description="Get datastreams for a sensor via FROST.",
)
async def get_sensor_datastreams(
    sensor_uuid: str,
):
    """
    Get datastreams for a sensor via FROST.
    """
    schema_name = TimeIODatabase().get_schema_from_uuid(sensor_uuid)
    if schema_name is None:
        raise ResourceNotFoundException("Schema not found")
    thing_service = ThingService(schema_name)
    datastreams = thing_service.get_sensor_datastreams(sensor_uuid)
    if not datastreams:
        return []
    return datastreams

@router.get(
    "/{sensor_uuid}/datastreams/{datastream_name}",
    response_model=Datastream,
    summary="Get Sensor Datastream (FROST)",
    description="Get datastream for a sensor via FROST.",
)
async def get_sensor_datastream(
    sensor_uuid: str,
    datastream_name: str,
):
    """
    Get datastream for a sensor via FROST.
    """
    schema_name = TimeIODatabase().get_schema_from_uuid(sensor_uuid)
    if schema_name is None:
        raise ResourceNotFoundException("Schema not found")
    thing_service = ThingService(schema_name)
    datastream = thing_service.get_sensor_datastream(sensor_uuid, datastream_name)
    if not datastream:
        raise ResourceNotFoundException("Datastream not found")
    return datastream

@router.get(
    "/{sensor_uuid}/datastream/{datastream_name}/observations",
    response_model=List[Observation],
    summary="Get Sensor Data (FROST)",
    description="Get generic time-series data for a sensor via FROST. Optionally filter by datastream name.",
)
async def get_sensor_observations(
    sensor_uuid: str, 
    datastream_name: Optional[str] = None, 
    limit: int = 100,
    start_time: str = None,
    end_time: str = None,
    order_by: str = "phenomenonTime desc",
    select: str = "@iot.id,phenomenonTime,result,resultTime"
):
    """
    Get generic time-series data for a sensor via FROST.
    """
    schema_name = TimeIODatabase().get_schema_from_uuid(sensor_uuid)
    if schema_name is None:
        raise ResourceNotFoundException("Schema not found")
    thing_service = ThingService(schema_name)
    observations = thing_service.get_observations_by_name_from_sensor_uuid(
        sensor_uuid,
        datastream_name,
        start_time,
        end_time,
        limit,
        order_by,
        select
    )
    if not observations:
        return []
    return observations




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
class DataPoint(BaseModel):
    timestamp: Any
    value: Any
    datastream: Optional[str] = None
    unit: Optional[str] = None

