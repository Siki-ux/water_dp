"""
Pydantic schemas for geospatial data API models.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from pydantic import BaseModel, ConfigDict, Field, field_validator
from shapely.geometry import mapping


class LayerType(str, Enum):
    """Geospatial layer types."""

    VECTOR = "vector"
    RASTER = "raster"
    WMS = "wms"
    WFS = "wfs"
    WCS = "wcs"


class GeometryType(str, Enum):
    """Geometry types."""

    POINT = "point"
    LINE = "line"
    POLYGON = "polygon"
    MULTIPOINT = "multipoint"
    MULTILINE = "multiline"
    MULTIPOLYGON = "multipolygon"


class DataFormat(str, Enum):
    """Data format types."""

    SHAPEFILE = "shapefile"
    GEOJSON = "geojson"
    POSTGIS = "postgis"
    GEOTIFF = "geotiff"
    KML = "kml"
    GML = "gml"


class GeoLayerBase(BaseModel):
    layer_name: str = Field(..., description="Unique layer name")
    title: str = Field(..., description="Layer title")
    description: Optional[str] = Field(None, description="Layer description")
    workspace: str = Field(default="water_data", description="GeoServer workspace")
    store_name: str = Field(..., description="Data store name")
    srs: str = Field(default="EPSG:4326", description="Spatial reference system")
    layer_type: LayerType = Field(..., description="Type of layer")
    geometry_type: Optional[GeometryType] = Field(
        None, description="Geometry type for vector layers"
    )
    style_name: Optional[str] = Field(None, description="Style name")
    data_source: Optional[str] = Field(None, description="Data source URL or path")
    data_format: Optional[DataFormat] = Field(None, description="Data format")
    is_published: bool = Field(default=True, description="Whether layer is published")
    is_public: bool = Field(
        default=False, description="Whether layer is publicly accessible"
    )
    properties: Optional[Dict[str, Any]] = Field(
        None, description="Additional properties"
    )
    style_config: Optional[Dict[str, Any]] = Field(
        None, description="Style configuration"
    )


class GeoLayerCreate(GeoLayerBase):
    pass


class GeoLayerUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    style_name: Optional[str] = None
    data_source: Optional[str] = None
    is_published: Optional[bool] = None
    is_public: Optional[bool] = None
    properties: Optional[Dict[str, Any]] = None
    style_config: Optional[Dict[str, Any]] = None


class GeoLayerResponse(GeoLayerBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class GeoFeatureBase(BaseModel):
    layer_id: str = Field(..., description="Layer name")
    feature_id: str = Field(..., description="Feature identifier")
    feature_type: str = Field(..., description="Feature type (e.g. region, country)")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")
    properties: Optional[Dict[str, Any]] = Field(None, description="Feature properties")
    valid_from: Optional[datetime] = Field(None, description="Valid from timestamp")
    valid_to: Optional[datetime] = Field(None, description="Valid to timestamp")
    is_active: bool = Field(default=True, description="Whether feature is active")

    @field_validator("geometry", mode="before")
    @classmethod
    def geometry_to_dict(cls, v):
        if isinstance(v, WKBElement):
            return mapping(to_shape(v))
        return v


class GeoFeatureCreate(GeoFeatureBase):
    pass


class GeoFeatureUpdate(BaseModel):
    geometry: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, Any]] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    is_active: Optional[bool] = None


class GeoFeatureResponse(GeoFeatureBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class LayerQuery(BaseModel):
    """Query parameters for layers."""

    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(
        default=100, ge=1, le=1000, description="Maximum number of records"
    )
    workspace: Optional[str] = Field(None, description="Filter by workspace")
    layer_type: Optional[LayerType] = Field(None, description="Filter by layer type")
    is_published: Optional[bool] = Field(
        None, description="Filter by publication status"
    )
    is_public: Optional[bool] = Field(None, description="Filter by public access")


class FeatureQuery(BaseModel):
    """Query parameters for features."""

    layer_name: str = Field(..., description="Layer name")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(
        default=1000, ge=1, le=10000, description="Maximum number of records"
    )
    feature_type: Optional[GeometryType] = Field(
        None, description="Filter by feature type"
    )
    is_active: Optional[bool] = Field(None, description="Filter by active status")
    bbox: Optional[List[float]] = Field(
        None, description="Bounding box [min_lon, min_lat, max_lon, max_lat]"
    )
    properties_filter: Optional[Dict[str, Any]] = Field(
        None, description="Filter by properties"
    )


class SpatialQuery(BaseModel):
    """Spatial query parameters."""

    geometry: Dict[str, Any] = Field(..., description="Query geometry (GeoJSON)")
    spatial_relation: str = Field(
        default="intersects",
        description="Spatial relation (intersects, contains, within, etc.)",
    )
    layer_names: Optional[List[str]] = Field(
        None, description="Specific layers to query"
    )


class LayerListResponse(BaseModel):
    """Response for layer list."""

    layers: List[GeoLayerResponse]
    total: int
    skip: int
    limit: int


class FeatureListResponse(BaseModel):
    """Response for feature list."""

    features: List[GeoFeatureResponse]
    total: int
    layer_name: str
    skip: int
    limit: int


class SpatialQueryResponse(BaseModel):
    """Response for spatial queries."""

    features: List[GeoFeatureResponse]
    total: int
    query_geometry: Dict[str, Any]
    spatial_relation: str


class GeoServerLayerInfo(BaseModel):
    """GeoServer layer information."""

    name: str
    title: str
    abstract: Optional[str] = None
    workspace: str
    store: str
    srs: str
    native_srs: str
    bounds: Dict[str, float]
    keywords: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class GeoServerStyleInfo(BaseModel):
    """GeoServer style information."""

    name: str
    title: str
    abstract: Optional[str] = None
    format: str
    filename: Optional[str] = None
    sld_body: Optional[str] = None


class LayerPublishRequest(BaseModel):
    """Request to publish a layer."""

    layer_name: str
    workspace: str
    store_name: str
    style_name: Optional[str] = None
    is_public: bool = False
    metadata: Optional[Dict[str, Any]] = None


class LayerUnpublishRequest(BaseModel):
    """Request to unpublish a layer."""

    layer_name: str
    workspace: str
