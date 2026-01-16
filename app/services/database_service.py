"""
Database service for CRUD operations and data management.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.exceptions import DatabaseException, ResourceNotFoundException
from app.models.geospatial import GeoFeature, GeoLayer
from app.schemas.geospatial import (
    GeoFeatureCreate,
    GeoFeatureUpdate,
    GeoLayerCreate,
    GeoLayerUpdate,
)

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for database operations."""

    def __init__(self, db: Session):
        self.db = db

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
            raise DatabaseException(f"Failed to create geo layer: {e}")

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
        try:
            layer = (
                self.db.query(GeoLayer)
                .filter(GeoLayer.layer_name == layer_name)
                .first()
            )
            if not layer:
                raise ResourceNotFoundException(f"Geo layer '{layer_name}' not found.")
            return layer
        except Exception as e:
            if isinstance(e, ResourceNotFoundException):
                raise
            logger.error(f"Failed to get geo layer {layer_name}: {e}")
            raise DatabaseException(f"Failed to get geo layer: {e}")

    def update_geo_layer(
        self, layer_name: str, layer_update: GeoLayerUpdate
    ) -> Optional[GeoLayer]:
        """Update a geospatial layer."""
        try:
            layer = self.get_geo_layer(
                layer_name
            )  # Will raise ResourceNotFoundException if not found

            update_data = layer_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                if hasattr(layer, key):
                    setattr(layer, key, value)

            self.db.commit()
            self.db.refresh(layer)
            logger.info(f"Updated geo layer: {layer_name}")
            return layer
        except (ResourceNotFoundException, DatabaseException):
            raise
        except Exception as e:
            logger.error(f"Failed to update geo layer {layer_name}: {e}")
            self.db.rollback()
            raise DatabaseException(f"Failed to update geo layer: {e}")

    def delete_geo_layer(self, layer_name: str) -> bool:
        """Delete a geospatial layer."""
        try:
            layer = self.get_geo_layer(layer_name)  # Raises ResourceNotFoundException

            self.db.delete(layer)
            self.db.commit()
            logger.info(f"Deleted geo layer: {layer_name}")
            return True
        except (ResourceNotFoundException, DatabaseException):
            raise
        except Exception as e:
            logger.error(f"Failed to delete geo layer {layer_name}: {e}")
            self.db.rollback()
            raise DatabaseException(f"Failed to delete geo layer: {e}")

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
            raise DatabaseException(f"Failed to create geo feature: {e}")

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
        feature = (
            self.db.query(GeoFeature)
            .filter(
                GeoFeature.feature_id == feature_id, GeoFeature.layer_id == layer_name
            )
            .first()
        )
        if not feature:
            raise ResourceNotFoundException(
                f"Feature '{feature_id}' not found in layer '{layer_name}'."
            )
        return feature

    def update_geo_feature(
        self, feature_id: str, layer_name: str, feature_update: GeoFeatureUpdate
    ) -> Optional[GeoFeature]:
        """Update a geospatial feature."""
        try:
            feature = self.get_geo_feature(feature_id, layer_name)

            update_data = feature_update.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                if hasattr(feature, key):
                    setattr(feature, key, value)

            self.db.commit()
            self.db.refresh(feature)
            return feature
        except (ResourceNotFoundException, DatabaseException):
            raise
        except Exception as e:
            logger.error(f"Failed to update geo feature {feature_id}: {e}")
            self.db.rollback()
            raise DatabaseException(f"Failed to update geo feature: {e}")

    def delete_geo_feature(self, feature_id: str, layer_name: str) -> bool:
        """Delete a geospatial feature."""
        try:
            feature = self.get_geo_feature(feature_id, layer_name)

            self.db.delete(feature)
            self.db.commit()
            return True
        except (ResourceNotFoundException, DatabaseException):
            raise
        except Exception as e:
            logger.error(f"Failed to delete geo feature {feature_id}: {e}")
            self.db.rollback()
            raise DatabaseException(f"Failed to delete geo feature: {e}")

    def get_sensors_in_layer(self, layer_name: str) -> List[Dict[str, Any]]:
        """
        Get all sensors (Things) that are spatially within the geometry of a layer's features.
        Assumes FROST schema Tables (THINGS, LOCATIONS) are present.
        """
        from sqlalchemy import text

        # 1. Check if layer exists
        self.get_geo_layer(layer_name)

        # 2. Perform Spatial Join
        # Note: We join geo_features for the specific layer with FROST Locations
        try:
            query = text(
                """
                SELECT DISTINCT
                    t."ID" as id,
                    t."NAME" as name,
                    t."DESCRIPTION" as description,
                    ST_X(ST_Centroid(ST_GeomFromGeoJSON(l."LOCATION"::jsonb))) as lng,
                    ST_Y(ST_Centroid(ST_GeomFromGeoJSON(l."LOCATION"::jsonb))) as lat
                FROM "THINGS" t
                JOIN "THINGS_LOCATIONS" tl ON t."ID" = tl."THING_ID"
                JOIN "LOCATIONS" l ON tl."LOCATION_ID" = l."ID"
                JOIN "geo_features" gf ON ST_Intersects(ST_GeomFromGeoJSON(l."LOCATION"::jsonb), gf.geometry)
                WHERE gf.layer_id = :layer_name
            """
            )

            result = self.db.execute(query, {"layer_name": layer_name})

            sensors = []
            for row in result:
                # Handle potential case sensitivity or type mismatch by dict access
                # SQLAlchemy row is accessible by column name, but let's be safe
                sensors.append(
                    {
                        "id": str(row[0]),  # Ensure ID is string (handle int/str IDs)
                        "name": row[1],
                        "description": row[2],
                        "latitude": row[4],
                        "longitude": row[3],
                    }
                )

            return sensors

        except Exception as e:
            logger.error(f"Failed to query sensors in layer {layer_name}: {e}")
            # Identify if it's a table-not-found error (e.g. FROST not set up or lowercase tables)
            if "relation" in str(e) and "does not exist" in str(e):
                # Try lowercase fallback?
                logger.warning(
                    "FROST Tables not found in uppercase, checking lowercase fallback..."
                )
                # For now just re-raise, but good to know for debugging
            raise DatabaseException(f"Failed to query sensors in layer: {e}")

    def get_layer_bbox(self, layer_name: str) -> Optional[List[float]]:
        """
        Get the bounding box of a layer.
        Returns [min_lon, min_lat, max_lon, max_lat].
        """
        from sqlalchemy import text

        try:
            # Check layer exists
            self.get_geo_layer(layer_name)

            # Query for cleaner output using ST_XMin, ST_YMin, etc.
            query = text(
                """
                SELECT
                    ST_XMin(ST_Extent(geometry)),
                    ST_YMin(ST_Extent(geometry)),
                    ST_XMax(ST_Extent(geometry)),
                    ST_YMax(ST_Extent(geometry))
                FROM geo_features
                WHERE layer_id = :layer_name
            """
            )
            result = self.db.execute(query, {"layer_name": layer_name}).fetchone()

            if result and all(x is not None for x in result):
                return [
                    float(result[0]),
                    float(result[1]),
                    float(result[2]),
                    float(result[3]),
                ]
            return None

        except Exception as e:
            logger.error(f"Failed to get bbox for layer {layer_name}: {e}")
            raise DatabaseException(f"Failed to get layer bbox: {e}")
