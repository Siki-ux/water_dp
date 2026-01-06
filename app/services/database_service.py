"""
Database service for CRUD operations and data management.
"""

import logging
from typing import List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

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
