"""
Time series data models for temporal data processing.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field
from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel, PydanticBase


class TimeSeriesMetadata(Base, BaseModel):
    """Metadata for time series datasets."""

    __tablename__ = "time_series_metadata"

    series_id = Column(String(100), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)

    # Data source information
    source_type = Column(String(50), nullable=False)  # sensor, model, manual, etc.
    source_id = Column(String(100), nullable=True)  # ID of the source system
    station_id = Column(String(50), nullable=True)  # Related station ID

    # Temporal information
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=True, index=True)
    time_zone = Column(String(50), nullable=False, default="UTC")

    # Data characteristics
    parameter = Column(
        String(50), nullable=False, index=True
    )  # water_level, temperature, etc.
    unit = Column(String(20), nullable=False)
    data_type = Column(String(20), nullable=False)  # continuous, discrete, event
    sampling_rate = Column(String(20), nullable=True)  # 1min, 1hour, daily, etc.

    # Quality and processing
    quality_level = Column(String(20), default="raw")  # raw, processed, validated
    processing_notes = Column(Text, nullable=True)

    # Additional metadata
    properties = Column(JSONB, nullable=True)

    data_points = relationship("TimeSeriesData", back_populates="series_metadata")

    __table_args__ = (
        Index("idx_series_parameter", "parameter"),
        Index("idx_series_source", "source_type", "source_id"),
        Index("idx_series_time_range", "start_time", "end_time"),
        Index("idx_series_quality", "quality_level"),
    )


class TimeSeriesData(Base, BaseModel):
    """Individual time series data points."""

    __tablename__ = "time_series_data"

    series_id = Column(
        String(100), ForeignKey("time_series_metadata.series_id"), nullable=False
    )

    # Temporal data
    timestamp = Column(DateTime, nullable=False, index=True)
    value = Column(Float, nullable=False)

    # Data quality
    quality_flag = Column(
        String(20), default="good"
    )  # good, questionable, bad, missing
    uncertainty = Column(Float, nullable=True)

    # Processing information
    is_interpolated = Column(String(10), default="false")
    is_aggregated = Column(String(10), default="false")
    aggregation_method = Column(String(50), nullable=True)  # mean, max, min, sum, etc.

    # Additional metadata
    properties = Column(JSONB, nullable=True)

    series_metadata = relationship("TimeSeriesMetadata", back_populates="data_points")

    __table_args__ = (
        Index("idx_ts_series_timestamp", "series_id", "timestamp"),
        Index("idx_ts_timestamp", "timestamp"),
        Index("idx_ts_quality", "quality_flag"),
        Index("idx_ts_interpolated", "is_interpolated"),
    )


# Pydantic schemas for API
class TimeSeriesMetadataBase(PydanticBase):
    series_id: str
    name: str
    description: Optional[str] = None
    source_type: str
    source_id: Optional[str] = None
    station_id: Optional[str] = None
    start_time: datetime
    end_time: Optional[datetime] = None
    time_zone: str = "UTC"
    parameter: str
    unit: str
    data_type: str
    sampling_rate: Optional[str] = None
    quality_level: str = "raw"
    processing_notes: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class TimeSeriesMetadataCreate(TimeSeriesMetadataBase):
    pass


class TimeSeriesMetadataResponse(TimeSeriesMetadataBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TimeSeriesDataBase(PydanticBase):
    series_id: str
    timestamp: datetime
    value: float
    quality_flag: str = "good"
    uncertainty: Optional[float] = None
    is_interpolated: str = "false"
    is_aggregated: str = "false"
    aggregation_method: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class TimeSeriesDataCreate(TimeSeriesDataBase):
    pass


class TimeSeriesDataResponse(TimeSeriesDataBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TimeSeriesQuery(PydanticBase):
    """Query parameters for time series data."""

    series_id: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: Optional[int] = Field(default=1000, le=10000)
    quality_filter: Optional[str] = None  # good, questionable, bad, all
    include_interpolated: bool = True
    include_aggregated: bool = True


class TimeSeriesAggregation(PydanticBase):
    """Time series aggregation parameters."""

    series_id: str
    start_time: datetime
    end_time: datetime
    aggregation_method: str = Field(
        ..., pattern="^(mean|max|min|sum|count|std|median)$"
    )
    aggregation_interval: str = Field(
        ..., pattern="^(1min|5min|15min|1hour|1day|1week|1month)$"
    )
    time_zone: str = "UTC"
