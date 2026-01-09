"""
Pydantic schemas for water data API models.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StationType(str, Enum):
    """Water station types."""

    RIVER = "river"
    LAKE = "lake"
    GROUNDWATER = "groundwater"
    RESERVOIR = "reservoir"
    WELL = "well"
    SPRING = "spring"
    DATASET = "dataset"
    UNKNOWN = "unknown"


class StationStatus(str, Enum):
    """Station status values."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"
    DECOMMISSIONED = "decommissioned"
    UNKNOWN = "unknown"


class ParameterType(str, Enum):
    """Water data parameters."""

    WATER_LEVEL = "water_level"
    FLOW_RATE = "flow_rate"
    TEMPERATURE = "temperature"
    PH = "ph"
    DISSOLVED_OXYGEN = "dissolved_oxygen"
    TURBIDITY = "turbidity"
    CONDUCTIVITY = "conductivity"
    PRECIPITATION = "precipitation"
    EVAPORATION = "evaporation"


class QualityFlag(str, Enum):
    """Data quality flags."""

    GOOD = "good"
    QUESTIONABLE = "questionable"
    BAD = "bad"
    MISSING = "missing"


class WaterStationBase(BaseModel):
    station_id: str = Field(..., description="Unique station identifier")
    name: str = Field(..., description="Station name")
    description: Optional[str] = Field(None, description="Station description")
    latitude: Optional[float] = Field(
        None, ge=-90, le=90, description="Latitude coordinate"
    )
    longitude: Optional[float] = Field(
        None, ge=-180, le=180, description="Longitude coordinate"
    )
    elevation: Optional[float] = Field(
        None, description="Elevation above sea level (meters)"
    )
    station_type: StationType = Field(..., description="Type of water station")
    status: StationStatus = Field(
        default=StationStatus.ACTIVE, description="Station status"
    )
    organization: Optional[str] = Field(None, description="Managing organization")
    properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional properties"
    )


class WaterStationCreate(WaterStationBase):
    pass


class WaterStationUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    elevation: Optional[float] = None
    station_type: Optional[StationType] = None
    status: Optional[StationStatus] = None
    organization: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class WaterStationResponse(WaterStationBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WaterDataPointBase(BaseModel):
    station_id: int = Field(..., description="Station ID")
    timestamp: datetime = Field(..., description="Measurement timestamp")
    parameter: ParameterType = Field(..., description="Measured parameter")
    value: float = Field(..., description="Measured value")
    unit: str = Field(..., description="Unit of measurement")
    quality_flag: QualityFlag = Field(
        default=QualityFlag.GOOD, description="Data quality flag"
    )
    uncertainty: Optional[float] = Field(None, description="Measurement uncertainty")
    measurement_method: Optional[str] = Field(
        None, description="Measurement method used"
    )
    properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional properties"
    )


class WaterDataPointCreate(WaterDataPointBase):
    pass


class WaterDataPointUpdate(BaseModel):
    value: Optional[float] = None
    quality_flag: Optional[QualityFlag] = None
    uncertainty: Optional[float] = None
    measurement_method: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class WaterDataPointResponse(WaterDataPointBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WaterQualityBase(BaseModel):
    station_id: int = Field(..., description="Station ID")
    timestamp: datetime = Field(..., description="Measurement timestamp")
    temperature: Optional[float] = Field(None, description="Water temperature (°C)")
    ph: Optional[float] = Field(None, ge=0, le=14, description="pH level")
    dissolved_oxygen: Optional[float] = Field(
        None, ge=0, description="Dissolved oxygen (mg/L)"
    )
    turbidity: Optional[float] = Field(None, ge=0, description="Turbidity (NTU)")
    conductivity: Optional[float] = Field(
        None, ge=0, description="Conductivity (μS/cm)"
    )
    total_dissolved_solids: Optional[float] = Field(
        None, ge=0, description="TDS (mg/L)"
    )
    nitrates: Optional[float] = Field(None, ge=0, description="Nitrates (mg/L)")
    phosphates: Optional[float] = Field(None, ge=0, description="Phosphates (mg/L)")
    bacteria_count: Optional[float] = Field(
        None, ge=0, description="Bacteria count (CFU/100mL)"
    )
    overall_quality: Optional[str] = Field(
        None, description="Overall quality assessment"
    )
    quality_notes: Optional[str] = Field(None, description="Quality assessment notes")


class WaterQualityCreate(WaterQualityBase):
    pass


class WaterQualityUpdate(BaseModel):
    temperature: Optional[float] = None
    ph: Optional[float] = Field(None, ge=0, le=14)
    dissolved_oxygen: Optional[float] = Field(None, ge=0)
    turbidity: Optional[float] = Field(None, ge=0)
    conductivity: Optional[float] = Field(None, ge=0)
    total_dissolved_solids: Optional[float] = Field(None, ge=0)
    nitrates: Optional[float] = Field(None, ge=0)
    phosphates: Optional[float] = Field(None, ge=0)
    bacteria_count: Optional[float] = Field(None, ge=0)
    overall_quality: Optional[str] = None
    quality_notes: Optional[str] = None


class WaterQualityResponse(WaterQualityBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StationQuery(BaseModel):
    """Query parameters for stations."""

    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(
        default=100, ge=1, le=1000, description="Maximum number of records to return"
    )
    station_type: Optional[StationType] = Field(
        None, description="Filter by station type"
    )
    status: Optional[StationStatus] = Field(None, description="Filter by status")
    organization: Optional[str] = Field(None, description="Filter by organization")
    bbox: Optional[List[float]] = Field(
        None, description="Bounding box [min_lon, min_lat, max_lon, max_lat]"
    )


class DataPointQuery(BaseModel):
    """Query parameters for data points."""

    station_id: int = Field(..., description="Station ID")
    start_time: Optional[datetime] = Field(None, description="Start time filter")
    end_time: Optional[datetime] = Field(None, description="End time filter")
    parameter: Optional[ParameterType] = Field(None, description="Filter by parameter")
    quality_filter: Optional[QualityFlag] = Field(
        None, description="Filter by quality flag"
    )
    limit: int = Field(
        default=1000, ge=1, le=10000, description="Maximum number of records"
    )


class BulkDataPointCreate(BaseModel):
    """Bulk creation of data points."""

    data_points: List[WaterDataPointCreate] = Field(
        ..., description="List of data points to create"
    )

    @field_validator("data_points")
    @classmethod
    def validate_data_points(
        cls, v: List[WaterDataPointCreate]
    ) -> List[WaterDataPointCreate]:
        if len(v) > 1000:
            raise ValueError("Cannot create more than 1000 data points at once")
        return v


class StationListResponse(BaseModel):
    """Response for station list."""

    stations: List[WaterStationResponse]
    total: int
    skip: int
    limit: int


class DataPointListResponse(BaseModel):
    """Response for data point list."""

    data_points: List[WaterDataPointResponse]
    total: int
    station_id: int
    parameter: Optional[str] = None
    time_range: Optional[Dict[str, datetime]] = None


class StationStatistics(BaseModel):
    """Station statistics response."""

    station_id: int
    time_range: Dict[str, Optional[datetime]]
    parameters: List[Dict[str, Any]]
    total_measurements: int
    data_quality_summary: Dict[str, int]
