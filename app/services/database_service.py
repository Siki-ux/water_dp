"""
Database service for CRUD operations and data management.
"""

import logging
from typing import Any, Dict, List, Optional

import requests
from shapely.geometry import shape
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import settings
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
        Fetches layer geometry from GeoServer (WFS) and uses FROST OGC Spatial Filters.
        """
        from app.services.geoserver_service import GeoServerService

        # 1. Fetch features from GeoServer WFS
        try:
            gs_service = GeoServerService()
            geojson_data = gs_service.get_wfs_features(layer_name)
            features = geojson_data.get("features", [])
        except Exception as e:
            logger.error(f"Failed to fetch layer {layer_name} from GeoServer: {e}")
            return []

        if not features:
            logger.warning(f"No features found in layer {layer_name} from GeoServer.")
            return []

        try:
            # 2. Build geometries from GeoJSON
            polygons = []
            for f in features:
                try:
                    geom = f.get("geometry")
                    if geom:
                        s = shape(geom)
                        polygons.append(s)
                except Exception as ex:
                    fid = f.get("id", "unknown")
                    logger.warning(f"Skipping invalid geometry in feature {fid}: {ex}")

            if not polygons:
                return []

            # 3. Calculate BBOX for FROST Optimization
            # Union all polygons to get the total bounds
            from shapely.ops import unary_union

            combined = unary_union(polygons)
            minx, miny, maxx, maxy = combined.bounds

            # 4. Fetch Things from FROST using Spatial Filter (BBOX)
            frost_url = settings.frost_url
            if not frost_url:
                logger.warning("FROST_URL not set, cannot retrieve sensors.")
                return []

            next_link = f"{frost_url}/Things?$expand=Locations"

            # Construct WKT Polygon for BBOX
            wkt_polygon = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"

            # Append filter to FROST request
            filter_param = (
                f"st_intersects(Locations/location, geography'{wkt_polygon}')"
            )
            next_link += f"&$filter={filter_param}"

            logger.debug(
                f"Using spatial optimization for layer {layer_name}. WKT: {wkt_polygon}"
            )

            sensors = []

            # Safety limit for pages
            page_count = 0
            max_pages = 50

            while next_link and page_count < max_pages:
                try:
                    resp = requests.get(next_link, timeout=20)
                    if resp.status_code != 200:
                        logger.error(f"FROST Error: {resp.status_code} {resp.text}")
                        break

                    data = resp.json()
                    things = data.get("value", [])
                    next_link = data.get("@iot.nextLink")
                    page_count += 1

                    for thing in things:
                        locations = thing.get("Locations", [])
                        if not locations:
                            continue

                        # Use first location
                        loc_entity = locations[0]
                        loc_geo = loc_entity.get("location")

                        if not loc_geo:
                            continue

                        # Parse GeoJSON location
                        try:
                            # Shapely shape from dict
                            thing_point = shape(loc_geo)

                            # Check intersection with ANY layer polygon (Precise check)
                            match = False
                            for poly in polygons:
                                if poly.intersects(thing_point):
                                    match = True
                                    break

                            if match:
                                sensors.append(
                                    {
                                        "id": str(thing.get("@iot.id")),
                                        "name": thing.get("name"),
                                        "description": thing.get("description"),
                                        "latitude": thing_point.y,
                                        "longitude": thing_point.x,
                                    }
                                )
                        except Exception as ex:
                            logger.warning(
                                f"Failed to parse location for thing {thing.get('@iot.id')}: {ex}"
                            )
                            continue

                except Exception as e:
                    logger.error(f"Error fetching from FROST: {e}")
                    break

            return sensors

        except Exception as e:
            logger.error(f"Failed to process sensors in layer {layer_name}: {e}")
            raise DatabaseException(f"Failed to get sensors in layer: {e}")

    def get_layer_bbox(self, layer_name: str) -> Optional[List[float]]:
        """
        Get the bounding box of a layer from GeoServer WFS data.
        Returns: [minx, miny, maxx, maxy] or None
        """
        from app.services.geoserver_service import GeoServerService

        try:
            gs_service = GeoServerService()
            geojson_data = gs_service.get_wfs_features(layer_name)
            features = geojson_data.get("features", [])

            if not features:
                return None

            polygons = []
            for f in features:
                geom = f.get("geometry")
                if geom:
                    try:
                        s = shape(geom)
                        polygons.append(s)
                    except Exception as ex:
                        fid = f.get("id", "unknown")
                        logger.warning(
                            f"Skipping invalid geometry for bbox calc in feature {fid}: {ex}"
                        )

            if not polygons:
                return None

            from shapely.ops import unary_union

            combined = unary_union(polygons)
            return list(combined.bounds)

        except Exception as e:
            logger.error(f"Failed to calculate bbox for {layer_name} from WFS: {e}")
            return None
