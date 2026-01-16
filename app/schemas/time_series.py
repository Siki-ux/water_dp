"""
Pydantic schemas for time series data API models.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class SourceType(str, Enum):
    """Time series source types."""

    SENSOR = "sensor"
    MODEL = "model"
    MANUAL = "manual"
    CALCULATED = "calculated"
    EXTERNAL = "external"


class DataType(str, Enum):
    """Time series data types."""

    CONTINUOUS = "continuous"
    DISCRETE = "discrete"
    EVENT = "event"


class QualityLevel(str, Enum):
    """Data quality levels."""

    RAW = "raw"
    PROCESSED = "processed"
    VALIDATED = "validated"
    QUALITY_CONTROLLED = "quality_controlled"


class AggregationMethod(str, Enum):
    """Aggregation methods."""

    MEAN = "mean"
    AVG = "avg"  # Alias for mean
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    COUNT = "count"
    STD = "std"
    MEDIAN = "median"
    PERCENTILE = "percentile"


class AggregationInterval(str, Enum):
    """Aggregation intervals."""

    MINUTE_1 = "1min"
    MINUTE_5 = "5min"
    MINUTE_15 = "15min"
    MINUTE_30 = "30min"
    HOUR_1 = "1h"
    HOUR_6 = "6h"
    HOUR_12 = "12h"
    DAY_1 = "1D"
    WEEK_1 = "1W"
    MONTH_1 = "1M"
    YEAR_1 = "1Y"


class TimeSeriesMetadataBase(BaseModel):
    series_id: str = Field(..., description="Unique series identifier")
    name: str = Field(..., description="Series name")
    description: Optional[str] = Field(None, description="Series description")
    source_type: SourceType = Field(..., description="Data source type")
    source_id: Optional[str] = Field(None, description="Source system ID")
    station_id: Optional[str] = Field(None, description="Related station ID")
    start_time: datetime = Field(..., description="Series start time")
    end_time: Optional[datetime] = Field(None, description="Series end time")
    time_zone: str = Field(default="UTC", description="Time zone")
    parameter: str = Field(..., description="Measured parameter")
    unit: str = Field(..., description="Unit of measurement")
    data_type: DataType = Field(..., description="Data type")
    sampling_rate: Optional[str] = Field(None, description="Sampling rate")
    quality_level: QualityLevel = Field(
        default=QualityLevel.RAW, description="Data quality level"
    )
    processing_notes: Optional[str] = Field(None, description="Processing notes")
    properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional properties"
    )


class TimeSeriesMetadataCreate(TimeSeriesMetadataBase):
    pass


class TimeSeriesMetadataUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    end_time: Optional[datetime] = None
    quality_level: Optional[QualityLevel] = None
    processing_notes: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class TimeSeriesMetadataResponse(TimeSeriesMetadataBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TimeSeriesDataBase(BaseModel):
    series_id: str = Field(..., description="Series ID")
    timestamp: datetime = Field(..., description="Data timestamp")
    value: float = Field(..., description="Data value")
    quality_flag: str = Field(default="good", description="Quality flag")
    uncertainty: Optional[float] = Field(None, description="Measurement uncertainty")
    is_interpolated: bool = Field(
        default=False, description="Whether value is interpolated"
    )
    is_aggregated: bool = Field(
        default=False, description="Whether value is aggregated"
    )
    aggregation_method: Optional[str] = Field(
        None, description="Aggregation method used"
    )
    properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional properties"
    )


class TimeSeriesDataCreate(TimeSeriesDataBase):
    pass


class TimeSeriesDataUpdate(BaseModel):
    value: Optional[float] = None
    quality_flag: Optional[str] = None
    uncertainty: Optional[float] = None
    is_interpolated: Optional[bool] = None
    is_aggregated: Optional[bool] = None
    aggregation_method: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class TimeSeriesDataResponse(TimeSeriesDataBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TimeSeriesQuery(BaseModel):
    """Query parameters for time series data."""

    series_id: str = Field(..., description="Series ID")
    start_time: Optional[datetime] = Field(None, description="Start time filter")
    end_time: Optional[datetime] = Field(None, description="End time filter")
    limit: int = Field(
        default=1000, ge=1, le=100000, description="Maximum number of records"
    )
    offset: int = Field(default=0, ge=0, description="Number of records to skip")
    quality_filter: Optional[str] = Field(None, description="Filter by quality flag")
    include_interpolated: bool = Field(
        default=True, description="Include interpolated values"
    )
    include_aggregated: bool = Field(
        default=True, description="Include aggregated values"
    )
    sort_order: str = Field(
        default="asc", pattern="^(asc|desc)$", description="Sort order (asc or desc)"
    )

    @field_validator("end_time")
    @classmethod
    def validate_time_range(
        cls, v: Optional[datetime], info: ValidationInfo
    ) -> Optional[datetime]:
        if v and info.data.get("start_time"):
            if v <= info.data["start_time"]:
                raise ValueError("end_time must be after start_time")
        return v


class TimeSeriesAggregation(BaseModel):
    """Time series aggregation parameters."""

    series_id: str = Field(..., description="Series ID")
    start_time: datetime = Field(..., description="Aggregation start time")
    end_time: datetime = Field(..., description="Aggregation end time")
    aggregation_method: AggregationMethod = Field(..., description="Aggregation method")
    aggregation_interval: AggregationInterval = Field(
        ..., description="Aggregation interval"
    )
    time_zone: str = Field(default="UTC", description="Time zone for aggregation")
    include_metadata: bool = Field(
        default=True, description="Include aggregation metadata"
    )

    @field_validator("end_time")
    @classmethod
    def validate_time_range(cls, v: datetime, info: ValidationInfo) -> datetime:
        if info.data.get("start_time") and v <= info.data["start_time"]:
            raise ValueError("end_time must be after start_time")
        return v


class BulkTimeSeriesDataCreate(BaseModel):
    """Bulk creation of time series data."""

    series_id: str = Field(..., description="Series ID")
    data_points: List[TimeSeriesDataCreate] = Field(
        ..., description="List of data points"
    )

    @field_validator("data_points")
    @classmethod
    def validate_data_points(
        cls, v: List[TimeSeriesDataCreate]
    ) -> List[TimeSeriesDataCreate]:
        if len(v) > 10000:
            raise ValueError("Cannot create more than 10000 data points at once")
        return v


class TimeSeriesListResponse(BaseModel):
    """Response for time series data list."""

    data_points: List[TimeSeriesDataResponse]
    total: int
    series_id: str
    time_range: Optional[Dict[str, datetime]] = None
    aggregation_info: Optional[Dict[str, Any]] = None


class TimeSeriesMetadataListResponse(BaseModel):
    """Response for time series metadata list."""

    series: List[TimeSeriesMetadataResponse]
    total: int
    skip: int
    limit: int


class AggregatedDataPoint(BaseModel):
    """Aggregated data point."""

    timestamp: datetime
    value: float
    count: int
    aggregation_method: str
    aggregation_interval: str
    quality_flags: List[str]
    min: Optional[float] = None
    max: Optional[float] = None
    avg: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class InterpolatedDataPoint(BaseModel):
    """Interpolated data point."""

    timestamp: datetime
    value: float
    is_interpolated: bool
    quality_flag: str
    properties: Optional[Dict[str, Any]] = None


class AggregatedTimeSeriesResponse(BaseModel):
    """Response for aggregated time series data."""

    series_id: str
    aggregation_method: str
    aggregation_interval: str
    time_range: Dict[str, datetime]
    data_points: List[AggregatedDataPoint]
    total_points: int
    metadata: Optional[Dict[str, Any]] = None


class TimeSeriesStatistics(BaseModel):
    """Time series statistics."""

    series_id: str
    time_range: Dict[str, datetime]
    total_points: int
    statistics: Dict[str, float]
    quality_summary: Dict[str, int]
    gaps: List[Dict[str, datetime]]
    metadata: Optional[Dict[str, Any]] = None


class InterpolationRequest(BaseModel):
    """Request for time series interpolation."""

    series_id: str
    start_time: datetime
    end_time: datetime
    method: str = Field(default="linear", description="Interpolation method")
    interval: AggregationInterval = Field(
        default=AggregationInterval.HOUR_1, description="Output interval"
    )
    fill_gaps: bool = Field(default=True, description="Fill gaps in data")
    max_gap_duration: Optional[str] = Field(
        None, description="Maximum gap duration to fill"
    )


class InterpolationResponse(BaseModel):
    """Response for interpolation."""

    series_id: str
    interpolated_data: List[InterpolatedDataPoint]
    method: str
    interval: str
    gaps_filled: int
    metadata: Dict[str, Any]


class StationBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    station_id: str = Field(..., description="Unique station identifier")
    station_type: str = Field(default="unknown")
    status: str = Field(default="unknown")
    organization: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    properties: Optional[Dict[str, Any]] = None


class StationCreate(StationBase):
    pass


class StationUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    station_type: Optional[str] = None
    status: Optional[str] = None
    organization: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    properties: Optional[Dict[str, Any]] = None


class StationResponse(StationBase):
    id: int
    created_at: datetime
    updated_at: datetime
    # Computed fields
    is_active: bool = False
    last_activity: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)
