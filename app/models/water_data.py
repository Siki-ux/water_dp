"""
Water data models for stations, measurements, and quality data.
"""

from datetime import datetime
from typing import Optional

from pydantic import ConfigDict
from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel, PydanticBase


class WaterStation(Base, BaseModel):
    """Water monitoring station model."""

    __tablename__ = "water_stations"

    # Basic station information
    station_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)

    # Location
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    elevation = Column(Float, nullable=True)

    # Station metadata
    station_type = Column(String(50), nullable=False)  # river, lake, groundwater, etc.
    status = Column(String(20), default="active")  # active, inactive, maintenance
    organization = Column(String(100), nullable=True)

    # Additional metadata
    properties = Column(JSONB, nullable=True)

    data_points = relationship("WaterDataPoint", back_populates="station")
    quality_data = relationship("WaterQuality", back_populates="station")

    __table_args__ = (
        Index("idx_station_location", "latitude", "longitude"),
        Index("idx_station_type", "station_type"),
        Index("idx_station_status", "status"),
    )


class WaterDataPoint(Base, BaseModel):
    """Individual water data measurement."""

    __tablename__ = "water_data_points"

    station_id = Column(Integer, ForeignKey("water_stations.id"), nullable=False)

    # Measurement data
    timestamp = Column(DateTime, nullable=False, index=True)
    parameter = Column(
        String(50), nullable=False, index=True
    )  # water_level, flow_rate, etc.
    value = Column(Float, nullable=False)
    unit = Column(String(20), nullable=False)

    # Data quality
    quality_flag = Column(String(20), default="good")  # good, questionable, bad
    uncertainty = Column(Float, nullable=True)

    # Additional metadata
    measurement_method = Column(String(100), nullable=True)
    properties = Column(JSONB, nullable=True)

    station = relationship("WaterStation", back_populates="data_points")

    __table_args__ = (
        Index("idx_wd_timestamp", "timestamp"),
        Index("idx_wd_parameter", "parameter"),
        Index("idx_wd_station_timestamp", "station_id", "timestamp"),
        Index("idx_wd_quality", "quality_flag"),
    )


class WaterQuality(Base, BaseModel):
    """Water quality measurements."""

    __tablename__ = "water_quality"

    station_id = Column(Integer, ForeignKey("water_stations.id"), nullable=False)

    # Quality parameters
    timestamp = Column(DateTime, nullable=False, index=True)
    temperature = Column(Float, nullable=True)  # °C
    ph = Column(Float, nullable=True)
    dissolved_oxygen = Column(Float, nullable=True)  # mg/L
    turbidity = Column(Float, nullable=True)  # NTU
    conductivity = Column(Float, nullable=True)  # μS/cm
    total_dissolved_solids = Column(Float, nullable=True)  # mg/L

    # Additional quality parameters
    nitrates = Column(Float, nullable=True)  # mg/L
    phosphates = Column(Float, nullable=True)  # mg/L
    bacteria_count = Column(Float, nullable=True)  # CFU/100mL

    # Quality assessment
    overall_quality = Column(String(20), nullable=True)  # excellent, good, fair, poor
    quality_notes = Column(Text, nullable=True)

    station = relationship("WaterStation", back_populates="quality_data")

    __table_args__ = (
        Index("idx_quality_timestamp", "timestamp"),
        Index("idx_quality_station_timestamp", "station_id", "timestamp"),
        Index("idx_quality_overall", "overall_quality"),
    )


# Pydantic schemas for API
class WaterStationBase(PydanticBase):
    station_id: str
    name: str
    description: Optional[str] = None
    latitude: float
    longitude: float
    elevation: Optional[float] = None
    station_type: str
    status: str = "active"
    organization: Optional[str] = None
    properties: Optional[dict] = None


class WaterStationCreate(WaterStationBase):
    pass


class WaterStationResponse(WaterStationBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WaterDataPointBase(PydanticBase):
    station_id: int
    timestamp: datetime
    parameter: str
    value: float
    unit: str
    quality_flag: str = "good"
    uncertainty: Optional[float] = None
    measurement_method: Optional[str] = None
    properties: Optional[dict] = None


class WaterDataPointCreate(WaterDataPointBase):
    pass


class WaterDataPointResponse(WaterDataPointBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WaterQualityBase(PydanticBase):
    station_id: int
    timestamp: datetime
    temperature: Optional[float] = None
    ph: Optional[float] = None
    dissolved_oxygen: Optional[float] = None
    turbidity: Optional[float] = None
    conductivity: Optional[float] = None
    total_dissolved_solids: Optional[float] = None
    nitrates: Optional[float] = None
    phosphates: Optional[float] = None
    bacteria_count: Optional[float] = None
    overall_quality: Optional[str] = None
    quality_notes: Optional[str] = None


class WaterQualityCreate(WaterQualityBase):
    pass


class WaterQualityResponse(WaterQualityBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
