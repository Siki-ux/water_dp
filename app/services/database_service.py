"""
Database service for CRUD operations and data management.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, asc, desc, func
from sqlalchemy.orm import Session

from app.models.geospatial import GeoFeature, GeoLayer
from app.models.time_series import TimeSeriesData, TimeSeriesMetadata
from app.models.water_data import WaterDataPoint, WaterStation
from app.schemas.geospatial import (
    GeoFeatureCreate,
    GeoFeatureUpdate,
    GeoLayerCreate,
    GeoLayerUpdate,
)
from app.schemas.time_series import TimeSeriesDataCreate, TimeSeriesMetadataCreate
from app.schemas.water_data import (
    WaterDataPointCreate,
    WaterStationCreate,
)

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for database operations."""

    def __init__(self, db: Session):
        self.db = db

    # Water Station Operations
    def create_station(self, station_data: WaterStationCreate) -> WaterStation:
        """Create a new water station."""
        try:
            station = WaterStation(**station_data.model_dump())
            self.db.add(station)
            self.db.commit()
            self.db.refresh(station)
            logger.info(f"Created station: {station.station_id}")
            return station
        except Exception as e:
            logger.error(f"Failed to create station: {e}")
            self.db.rollback()
            raise

    def get_station(self, station_id: str) -> Optional[WaterStation]:
        """Get station by ID."""
        return (
            self.db.query(WaterStation)
            .filter(WaterStation.station_id == station_id)
            .first()
        )

    def get_stations(
        self,
        skip: int = 0,
        limit: int = 100,
        station_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[WaterStation]:
        """Get stations with optional filtering."""
        query = self.db.query(WaterStation)

        if station_type:
            query = query.filter(WaterStation.station_type == station_type)
        if status:
            query = query.filter(WaterStation.status == status)

        return query.offset(skip).limit(limit).all()

    def update_station(
        self, station_id: str, update_data: Dict[str, Any]
    ) -> Optional[WaterStation]:
        """Update station data."""
        try:
            station = self.get_station(station_id)
            if not station:
                return None

            for key, value in update_data.items():
                if hasattr(station, key):
                    setattr(station, key, value)

            station.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(station)
            logger.info(f"Updated station: {station_id}")
            return station
        except Exception as e:
            logger.error(f"Failed to update station {station_id}: {e}")
            self.db.rollback()
            raise

    # Water Data Operations
    def create_data_point(self, data_point: WaterDataPointCreate) -> WaterDataPoint:
        """Create a new data point."""
        try:
            point = WaterDataPoint(**data_point.model_dump())
            self.db.add(point)
            self.db.commit()
            self.db.refresh(point)
            return point
        except Exception as e:
            logger.error(f"Failed to create data point: {e}")
            self.db.rollback()
            raise

    def get_data_points(
        self,
        station_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        parameter: Optional[str] = None,
        limit: int = 1000,
    ) -> List[WaterDataPoint]:
        """Get data points with filtering."""
        query = self.db.query(WaterDataPoint).filter(
            WaterDataPoint.station_id == station_id
        )

        if start_time:
            query = query.filter(WaterDataPoint.timestamp >= start_time)
        if end_time:
            query = query.filter(WaterDataPoint.timestamp <= end_time)
        if parameter:
            query = query.filter(WaterDataPoint.parameter == parameter)

        return query.order_by(desc(WaterDataPoint.timestamp)).limit(limit).all()

    def get_latest_data(
        self, station_id: int, parameter: Optional[str] = None
    ) -> List[WaterDataPoint]:
        """Get latest data points for a station."""
        if parameter:
            # We must filter the subquery, not just the outer query, for correctness if grabbing "latest" per parameter
            # However, the original code attempted to filter the outer query `query = query.filter...` but then largely ignored `query` to build `latest_timestamps`.
            # Correct approach: Filter `latest_timestamps` subquery construction if parameter is present.
            pass

        # Get latest timestamp for each parameter
        base_stmt = self.db.query(
            WaterDataPoint.parameter,
            func.max(WaterDataPoint.timestamp).label("latest_time"),
        ).filter(WaterDataPoint.station_id == station_id)

        if parameter:
            base_stmt = base_stmt.filter(WaterDataPoint.parameter == parameter)

        latest_timestamps = base_stmt.group_by(WaterDataPoint.parameter).subquery()

        return (
            self.db.query(WaterDataPoint)
            .join(
                latest_timestamps,
                and_(
                    WaterDataPoint.parameter == latest_timestamps.c.parameter,
                    WaterDataPoint.timestamp == latest_timestamps.c.latest_time,
                ),
            )
            .all()
        )

    # Time Series Operations
    def create_time_series_metadata(
        self, metadata: TimeSeriesMetadataCreate
    ) -> TimeSeriesMetadata:
        """Create time series metadata."""
        try:
            ts_metadata = TimeSeriesMetadata(**metadata.model_dump())
            self.db.add(ts_metadata)
            self.db.commit()
            self.db.refresh(ts_metadata)
            return ts_metadata
        except Exception as e:
            logger.error(f"Failed to create time series metadata: {e}")
            self.db.rollback()
            raise

    def add_time_series_data(
        self, data_points: List[TimeSeriesDataCreate]
    ) -> List[TimeSeriesData]:
        """Add multiple time series data points."""
        try:
            points = [TimeSeriesData(**point.model_dump()) for point in data_points]
            self.db.add_all(points)
            self.db.commit()
            logger.info(f"Added {len(points)} time series data points")
            return points
        except Exception as e:
            logger.error(f"Failed to add time series data: {e}")
            self.db.rollback()
            raise

    def get_time_series_data(
        self,
        series_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[TimeSeriesData]:
        """Get time series data with filtering."""
        query = self.db.query(TimeSeriesData).filter(
            TimeSeriesData.series_id == series_id
        )

        if start_time:
            query = query.filter(TimeSeriesData.timestamp >= start_time)
        if end_time:
            query = query.filter(TimeSeriesData.timestamp <= end_time)

        return query.order_by(asc(TimeSeriesData.timestamp)).limit(limit).all()

    # GeoServer Operations
    def create_geo_layer(self, layer_data: GeoLayerCreate) -> GeoLayer:
        """Create a new geospatial layer."""
        try:
            layer = GeoLayer(**layer_data.model_dump())
            self.db.add(layer)
            self.db.commit()
            self.db.refresh(layer)
            logger.info(f"Created geo layer: {layer.layer_name}")
            return layer
        except Exception as e:
            logger.error(f"Failed to create geo layer: {e}")
            self.db.rollback()
            raise

    def get_geo_layers(
        self, workspace: Optional[str] = None, layer_type: Optional[str] = None
    ) -> List[GeoLayer]:
        """Get geospatial layers with filtering."""
        query = self.db.query(GeoLayer)

        if workspace:
            query = query.filter(GeoLayer.workspace == workspace)
        if layer_type:
            query = query.filter(GeoLayer.layer_type == layer_type)

        return query.all()

    def get_geo_layer(self, layer_name: str) -> Optional[GeoLayer]:
        """Get a specific geospatial layer."""
        return self.db.query(GeoLayer).filter(GeoLayer.layer_name == layer_name).first()

    def update_geo_layer(
        self, layer_name: str, layer_update: GeoLayerUpdate
    ) -> Optional[GeoLayer]:
        """Update a geospatial layer."""
        try:
            layer = self.get_geo_layer(layer_name)
            if not layer:
                return None

            update_data = layer_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                if hasattr(layer, key):
                    setattr(layer, key, value)

            self.db.commit()
            self.db.refresh(layer)
            logger.info(f"Updated geo layer: {layer_name}")
            return layer
        except Exception as e:
            logger.error(f"Failed to update geo layer {layer_name}: {e}")
            self.db.rollback()
            raise

    def delete_geo_layer(self, layer_name: str) -> bool:
        """Delete a geospatial layer."""
        try:
            layer = self.get_geo_layer(layer_name)
            if not layer:
                return False

            self.db.delete(layer)
            self.db.commit()
            logger.info(f"Deleted geo layer: {layer_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete geo layer {layer_name}: {e}")
            self.db.rollback()
            raise

    def create_geo_feature(self, feature_data: GeoFeatureCreate) -> GeoFeature:
        """Create a new geospatial feature."""
        try:
            feature = GeoFeature(**feature_data.model_dump())
            self.db.add(feature)
            self.db.commit()
            self.db.refresh(feature)
            return feature
        except Exception as e:
            logger.error(f"Failed to create geo feature: {e}")
            self.db.rollback()
            raise

    def get_geo_features(
        self,
        layer_name: str,
        skip: int = 0,
        limit: int = 1000,
        feature_type: Optional[str] = None,
        is_active: Optional[bool] = None,
        bbox: Optional[str] = None,
    ) -> List[GeoFeature]:
        """Get geospatial features with filtering."""
        query = self.db.query(GeoFeature).filter(GeoFeature.layer_id == layer_name)

        if feature_type:
            query = query.filter(GeoFeature.feature_type == feature_type)
        if is_active is not None:
            query = query.filter(GeoFeature.is_active == str(is_active).lower())

        if bbox:
            try:
                # bbox format: min_lon,min_lat,max_lon,max_lat
                coords = [float(x) for x in bbox.split(",")]
                if len(coords) == 4:
                    # Create envelope (SRID 4326)
                    envelope = func.ST_MakeEnvelope(
                        coords[0], coords[1], coords[2], coords[3], 4326
                    )
                    query = query.filter(
                        func.ST_Intersects(GeoFeature.geometry, envelope)
                    )
            except Exception as e:
                logger.warning(f"Invalid BBOX format: {bbox}, error: {e}")

        return query.offset(skip).limit(limit).all()

    def get_geo_feature(self, feature_id: str, layer_name: str) -> Optional[GeoFeature]:
        """Get a specific geospatial feature."""
        return (
            self.db.query(GeoFeature)
            .filter(
                GeoFeature.feature_id == feature_id, GeoFeature.layer_id == layer_name
            )
            .first()
        )

    def update_geo_feature(
        self, feature_id: str, layer_name: str, feature_update: GeoFeatureUpdate
    ) -> Optional[GeoFeature]:
        """Update a geospatial feature."""
        try:
            feature = self.get_geo_feature(feature_id, layer_name)
            if not feature:
                return None

            update_data = feature_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                if hasattr(feature, key):
                    setattr(feature, key, value)

            self.db.commit()
            self.db.refresh(feature)
            return feature
        except Exception as e:
            logger.error(f"Failed to update geo feature {feature_id}: {e}")
            self.db.rollback()
            raise

    def delete_geo_feature(self, feature_id: str, layer_name: str) -> bool:
        """Delete a geospatial feature."""
        try:
            feature = self.get_geo_feature(feature_id, layer_name)
            if not feature:
                return False

            self.db.delete(feature)
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to delete geo feature {feature_id}: {e}")
            self.db.rollback()
            raise

    def get_time_series_metadata(
        self,
        skip: int = 0,
        limit: int = 100,
        parameter: Optional[str] = None,
        source_type: Optional[str] = None,
        station_id: Optional[str] = None,
    ) -> List[TimeSeriesMetadata]:
        """Get time series metadata with filtering."""
        query = self.db.query(TimeSeriesMetadata)

        if parameter:
            query = query.filter(TimeSeriesMetadata.parameter == parameter)
        if source_type:
            query = query.filter(TimeSeriesMetadata.source_type == source_type)
        if station_id:
            query = query.filter(TimeSeriesMetadata.station_id == station_id)

        if station_id:
            query = query.filter(TimeSeriesMetadata.station_id == station_id)

        return query.offset(skip).limit(limit).all()

    def get_time_series_metadata_by_id(
        self, series_id: str
    ) -> Optional[TimeSeriesMetadata]:
        """Get specific time series metadata."""
        return (
            self.db.query(TimeSeriesMetadata)
            .filter(TimeSeriesMetadata.series_id == series_id)
            .first()
        )

    # Analytics and Statistics
    def get_station_statistics(
        self,
        station_id: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get statistical summary for a station."""
        query = self.db.query(
            WaterDataPoint.parameter,
            func.count(WaterDataPoint.id).label("count"),
            func.avg(WaterDataPoint.value).label("avg_value"),
            func.min(WaterDataPoint.value).label("min_value"),
            func.max(WaterDataPoint.value).label("max_value"),
            func.stddev(WaterDataPoint.value).label("std_value"),
        ).filter(WaterDataPoint.station_id == station_id)

        if start_time:
            query = query.filter(WaterDataPoint.timestamp >= start_time)
        if end_time:
            query = query.filter(WaterDataPoint.timestamp <= end_time)

        stats = query.group_by(WaterDataPoint.parameter).all()

        return {
            "station_id": station_id,
            "time_range": {"start": start_time, "end": end_time},
            "parameters": [
                {
                    "parameter": stat.parameter,
                    "count": stat.count,
                    "average": float(stat.avg_value) if stat.avg_value else None,
                    "minimum": float(stat.min_value) if stat.min_value else None,
                    "maximum": float(stat.max_value) if stat.max_value else None,
                    "standard_deviation": (
                        float(stat.std_value) if stat.std_value else None
                    ),
                }
                for stat in stats
            ],
        }
