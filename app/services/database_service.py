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

    def get_sensors_in_layer(self, layer_name: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all sensors (Things) that are spatially within the geometry of a layer's features.
        Fetches layer geometry from GeoServer (WFS) and uses FROST OGC Spatial Filters.
        """
        logger.info(f"ENTER: get_sensors_in_layer for {layer_name}, schema_name: {schema_name}")
        from app.services.geoserver_service import GeoServerService

        # 1. Fetch features from GeoServer WFS
        try:
            logger.info(f"Fetching features for layer {layer_name} from GeoServer...")
            gs_service = GeoServerService()
            geojson_data = gs_service.get_wfs_features(layer_name)
            features = geojson_data.get("features", [])
            logger.info(f"Fetched {len(features)} features for layer {layer_name}.")
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
                        if not s.is_valid:
                            s = s.buffer(0)
                        
                        if s.is_valid:
                            polygons.append(s)
                        else:
                            fid = f.get("id", "unknown")
                            logger.warning(f"Geometry for feature {fid} is invalid even after buffer(0). Skipping.")
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
            print(f"DEBUG_GEO: Layer {layer_name} BBOX: {minx}, {miny}, {maxx}, {maxy}")
            logger.info(f"Layer {layer_name} BBOX: minx={minx}, miny={miny}, maxx={maxx}, maxy={maxy}")

            # 4. Fetch Things from FROST across all projects (or specific tenant)
            from app.models.user_context import Project
            from app.services.thing_service import ThingService
            
            # Fetch projects
            query = self.db.query(Project).filter(Project.schema_name.isnot(None))
            if schema_name:
                query = query.filter(Project.schema_name == schema_name)
                
            projects = query.all()
            logger.info(f"Found {len(projects)} projects for sensor discovery.")
            if not projects:
                return []

            # Construct WKT Polygon for BBOX
            wkt_polygon = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
            
            # Construct FROST OGC Spatial Filter
            filter_param = f"st_intersects(Locations/location, geography'{wkt_polygon}')"

            logger.info(f"Searching for sensors in {len(projects)} projects using spatial filter: {filter_param}")

            sensors = []
            seen_iot_ids = set()

            for project in projects:
                try:
                    thing_service = ThingService(project.schema_name)
                    # Use the newly added filter support
                    project_things = thing_service.get_things(
                        expand=["Locations", "Datastreams"], 
                        filter=filter_param,
                        top=1000 # Limit per project for safety
                    )
                    
                    for thing_model in project_things:
                        # Deduplicate if necessary (unlikely given schema isolation but safe)
                        if thing_model.thing_id in seen_iot_ids:
                            continue
                        seen_iot_ids.add(thing_model.thing_id)

                        # Precise Check: Verify intersection with actual layer polygons (not just BBOX)
                        if thing_model.location and thing_model.location.coordinates:
                            thing_point = shape({
                                "type": "Point",
                                "coordinates": [
                                    thing_model.location.coordinates.longitude,
                                    thing_model.location.coordinates.latitude
                                ]
                            })
                            
                            match = False
                            for poly in polygons:
                                if poly.intersects(thing_point):
                                    match = True
                                    break
                            
                            if match:
                                # Standardized dict from model
                                sensor_dict = thing_model.model_dump()
                                # Frontend specific mapping
                                sensor_dict["id"] = thing_model.thing_id
                                if thing_model.properties:
                                    sensor_dict["station_type"] = thing_model.properties.get("station_type")
                                
                                # Flatten coordinates for frontend
                                if thing_model.location and thing_model.location.coordinates:
                                     sensor_dict["latitude"] = thing_model.location.coordinates.latitude
                                     sensor_dict["longitude"] = thing_model.location.coordinates.longitude
                                
                                sensors.append(sensor_dict)
                except Exception as project_ex:
                    logger.error(f"Failed to fetch sensors for project {project.name} ({project.schema_name}): {project_ex}")
                    continue

            logger.info(f"Found {len(sensors)} sensors in layer {layer_name} across all projects.")
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
