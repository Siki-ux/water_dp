"""
Geospatial data models for GeoServer integration.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from geoalchemy2 import Geometry
from pydantic import ConfigDict
from sqlalchemy import Column, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel, PydanticBase


class GeoLayer(Base, BaseModel):
    """Geospatial layer model for GeoServer integration."""

    __tablename__ = "geo_layers"

    layer_name = Column(String(100), unique=True, nullable=False, index=True)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)

    workspace = Column(String(50), nullable=False, default="water_data")
    store_name = Column(String(100), nullable=False)
    srs = Column(
        String(20), nullable=False, default="EPSG:4326"
    )  # Spatial Reference System

    layer_type = Column(String(50), nullable=False)  # vector, raster, wms, wfs
    geometry_type = Column(String(20), nullable=True)  # point, line, polygon, etc.
    style_name = Column(String(100), nullable=True)

    data_source = Column(String(200), nullable=True)  # URL or path to data
    data_format = Column(String(20), nullable=True)  # shapefile, geojson, postgis, etc.

    is_published = Column(String(10), default="true")  # true, false
    is_public = Column(String(10), default="false")  # true, false

    properties = Column(JSONB, nullable=True)
    style_config = Column(JSONB, nullable=True)  # SLD or CSS styling

    features = relationship("GeoFeature", back_populates="layer")

    __table_args__ = (
        Index("idx_layer_workspace", "workspace"),
        Index("idx_layer_type", "layer_type"),
        Index("idx_layer_published", "is_published"),
    )


class GeoFeature(Base, BaseModel):
    """Individual geospatial feature."""

    __tablename__ = "geo_features"

    layer_id = Column(String(100), ForeignKey("geo_layers.layer_name"), nullable=False)

    feature_id = Column(String(100), nullable=False, index=True)
    feature_type = Column(String(50), nullable=False)  # point, line, polygon, etc.

    geometry = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)

    properties = Column(JSONB, nullable=True)

    valid_from = Column(DateTime, nullable=True)
    valid_to = Column(DateTime, nullable=True)

    is_active = Column(String(10), default="true")

    layer = relationship("GeoLayer", back_populates="features")

    __table_args__ = (
        Index("idx_feature_layer", "layer_id"),
        Index("idx_feature_type", "feature_type"),
        Index("idx_feature_active", "is_active"),
        Index("idx_feature_valid_from", "valid_from"),
        Index("idx_feature_geometry", "geometry", postgresql_using="gist"),
    )


# Pydantic schemas for API
class GeoLayerBase(PydanticBase):
    layer_name: str
    title: str
    description: Optional[str] = None
    workspace: str = "water_data"
    store_name: str
    srs: str = "EPSG:4326"
    layer_type: str
    geometry_type: Optional[str] = None
    style_name: Optional[str] = None
    data_source: Optional[str] = None
    data_format: Optional[str] = None
    is_published: str = "true"
    is_public: str = "false"
    properties: Optional[Dict[str, Any]] = None
    style_config: Optional[Dict[str, Any]] = None


class GeoLayerCreate(GeoLayerBase):
    pass


class GeoLayerResponse(GeoLayerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class GeoFeatureBase(PydanticBase):
    layer_id: str
    feature_id: str
    feature_type: str
    geometry: Dict[str, Any]  # GeoJSON geometry
    properties: Optional[Dict[str, Any]] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    is_active: str = "true"


class GeoFeatureCreate(GeoFeatureBase):
    pass


class GeoFeatureResponse(GeoFeatureBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
