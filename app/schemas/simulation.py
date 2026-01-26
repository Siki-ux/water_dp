from typing import List, Optional

from pydantic import BaseModel, Field

# --- Thing Schema (Matches V3) ---


class SensorProperty(BaseModel):
    name: str = Field(..., description="Machine-readable name (e.g. 'temp')")
    unit: str = Field("Unknown", description="Unit of measurement (e.g. 'Celsius')")
    label: Optional[str] = Field(
        None, description="Human-readable label (e.g. 'Air Temperature')"
    )


class ThingCreateRequest(BaseModel):
    project_uuid: str = Field(..., description="Project UUID")
    sensor_name: str = Field(..., description="Name of the sensor")
    description: str = Field("", description="Description")
    device_type: str = Field("chirpstack_generic", description="Device Type")
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    properties: Optional[List[SensorProperty]] = None


# --- Simulation Schema ---


class DatastreamRange(BaseModel):
    min: float
    max: float


class DatastreamConfig(BaseModel):
    name: str
    range: Optional[DatastreamRange] = None
    interval: str = "60s"
    type: str = "random"
    enabled: bool = True


class SimulationConfig(BaseModel):
    enabled: bool = True
    datastreams: List[DatastreamConfig]


# --- Main Request Schema ---


class CreateSimulatedThingRequest(BaseModel):
    thing: ThingCreateRequest
    simulation: SimulationConfig
