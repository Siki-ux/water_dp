"""
Database seeding module for development and testing.
Populates the database with initial data including Czech Republic regions and time series data.
"""

import json
import logging
import math
import os
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon, box, shape
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.geospatial import GeoFeature, GeoLayer
from app.models.time_series import TimeSeriesData, TimeSeriesMetadata
from app.models.water_data import WaterStation

logger = logging.getLogger(__name__)

# Czech Republic Bounding Box (approximate)
# min_lon, min_lat, max_lon, max_lat
CR_BBOX = (12.09, 48.55, 18.86, 51.06)


def generate_grid_polygons(
    bbox: Tuple[float, float, float, float], rows: int = 3, cols: int = 4
) -> List[Polygon]:
    """Generate a grid of polygons covering the bounding box."""
    min_lon, min_lat, max_lon, max_lat = bbox

    lon_step = (max_lon - min_lon) / cols
    lat_step = (max_lat - min_lat) / rows

    polygons = []

    for i in range(cols):
        for j in range(rows):
            cell_min_lon = min_lon + i * lon_step
            cell_max_lon = min_lon + (i + 1) * lon_step
            cell_min_lat = min_lat + j * lat_step
            cell_max_lat = min_lat + (j + 1) * lat_step

            # Create rectangle
            poly = box(cell_min_lon, cell_min_lat, cell_max_lon, cell_max_lat)
            polygons.append(poly)

    return polygons


def seed_data(db: Session) -> None:
    """
    Seed the database with initial data if it's empty.
    Only runs if SEEDING=true in settings.
    """
    if not settings.seeding:
        logger.info("Skipping seeding (SEEDING=false)")
        return

    logger.info("Checking if database seeding is needed...")

    # Check if we already have data
    data_exists = False
    if db.query(GeoLayer).filter(GeoLayer.layer_name == "czech_regions").first():
        logger.info("Database records for czech_regions already exist.")
        data_exists = True

    if not data_exists:
        logger.info("Starting database seeding...")

    try:
        if not data_exists:
            # 1. Create GeoLayer for Czech Republic Regions
            cr_layer = GeoLayer(
                layer_name="czech_regions",
                title="Czech Republic Regions",
                description="Grid regions covering the Czech Republic for data segmentation.",
                store_name="water_data_store",
                layer_type="vector",
                geometry_type="polygon",
                is_published="true",
                is_public="true",
            )
            db.add(cr_layer)
            db.flush()  # Flush to get ID if needed, though we use layer_name as FK

            # 2. Generate Grid Regions (GeoFeatures)
            # Check for GeoJSON file
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            geojson_path = os.path.join(data_dir, "czech_regions.json")
            if not os.path.exists(geojson_path):
                geojson_path = os.path.join(data_dir, "czech_regions.geojson")

            logger.info(f"Geojson Path: {geojson_path}")
            grid_polys = []
            region_features = []  # Tuple of (Feature, Polygon/Shape)

            if os.path.exists(geojson_path):
                logger.info(f"Loading regions from {geojson_path}")
                try:
                    with open(geojson_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    features = data.get("features", [])
                    if not features and data.get("type") == "Feature":
                        features = [data]

                    for idx, feature_data in enumerate(features):
                        props = feature_data.get("properties", {})
                        # Try to get existing name or create one
                        # Try to get existing name or create one
                        region_id_val = props.get("NAZ_CZNUTS3") or f"Region_{idx+1}"
                        region_name = props.get("name") or region_id_val
                        feature_id_raw = (
                            props.get("id")
                            or feature_data.get("id")
                            or f"region_{idx+1}"
                        )

                        # Parse geometry
                        geom_shape = shape(feature_data["geometry"])
                        wkt_geom = from_shape(geom_shape, srid=4326)

                        # Create Feature
                        feature = GeoFeature(
                            layer_id=cr_layer.layer_name,
                            feature_id=feature_id_raw,
                            feature_type="region",
                            geometry=wkt_geom,
                            properties=props,
                            is_active="true",
                        )
                        db.add(feature)
                        region_features.append((feature, geom_shape))

                    logger.info(f"Loaded {len(region_features)} regions from GeoJSON.")
                except Exception as e:
                    logger.error(f"Failed to load GeoJSON: {e}. Falling back to grid.")
                    # Fallback flag handled by empty region_features

            if not region_features:
                logger.info(
                    "Generating synthetic grid regions (GeoJSON missing or failed)."
                )
                grid_polys = generate_grid_polygons(CR_BBOX, rows=3, cols=4)

                for idx, poly in enumerate(grid_polys):
                    region_name = f"Region_{idx+1}"

                    # Create Feature
                    wkt_geom = from_shape(poly, srid=4326)

                    feature = GeoFeature(
                        layer_id=cr_layer.layer_name,
                        feature_id=f"cr_region_{idx+1}",
                        feature_type="region",
                        geometry=wkt_geom,
                        properties={"name": region_name, "code": f"CZ-{idx+1}"},
                        is_active="true",
                    )
                    db.add(feature)
                    region_features.append((feature, poly))

            # --- SEED CZECH REPUBLIC LAYER ---
            logger.info("Seeding Czech Republic layer...")
            cz_rep_layer = GeoLayer(
                layer_name="czech_republic",
                title="Czech Republic",
                description="Boundary of the Czech Republic.",
                store_name="water_data_store",
                layer_type="vector",
                geometry_type="polygon",
                is_published="true",
                is_public="true",
            )
            # Check if layer exists first to avoid dupes if re-running without clean db
            if (
                not db.query(GeoLayer)
                .filter(GeoLayer.layer_name == "czech_republic")
                .first()
            ):
                db.add(cz_rep_layer)
                db.flush()

                cz_rep_geojson_path = os.path.join(data_dir, "czech_republic.json")
                if not os.path.exists(cz_rep_geojson_path):
                    cz_rep_geojson_path = os.path.join(
                        data_dir, "czech_republic.geojson"
                    )

                if os.path.exists(cz_rep_geojson_path):
                    logger.info(f"Loading Czech Republic from {cz_rep_geojson_path}")
                    try:
                        with open(cz_rep_geojson_path, "r", encoding="utf-8") as f:
                            cz_data = json.load(f)

                        cz_features = cz_data.get("features", [])
                        if not cz_features and cz_data.get("type") == "Feature":
                            cz_features = [cz_data]

                        for idx, feature_data in enumerate(cz_features):
                            props = feature_data.get("properties", {})
                            feature_id_raw = (
                                props.get("id")
                                or feature_data.get("id")
                                or f"cz_rep_{idx}"
                            )

                            # Parse geometry
                            geom_shape = shape(feature_data["geometry"])
                            wkt_geom = from_shape(geom_shape, srid=4326)

                            feature = GeoFeature(
                                layer_id="czech_republic",
                                feature_id=feature_id_raw,
                                feature_type="country",
                                geometry=wkt_geom,
                                properties=props,
                                is_active="true",
                            )
                            db.add(feature)
                        logger.info("Loaded Czech Republic boundary.")
                    except Exception as e:
                        logger.error(f"Failed to load Czech Republic GeoJSON: {e}")
                else:
                    logger.warning("czech_republic.json not found.")
            else:
                logger.info("Czech Republic layer already exists in DB.")
            # ---------------------------------

            stations = []

            for feature, poly in region_features:
                region_name = feature.properties.get(
                    "name", f"Region_{feature.feature_id}"
                )

                # 3. Create Water Stations in each region (centroid)
                centroid = poly.centroid
                station = WaterStation(
                    station_id=f"STATION_{feature.feature_id}",
                    name=f"Station {region_name}",
                    description=f"Monitoring station in {region_name}",
                    latitude=centroid.y,
                    longitude=centroid.x,
                    elevation=random.uniform(200, 1000),
                    station_type="river",
                    status="active",
                )
                db.add(station)
                stations.append(station)

                # Link Feature to Station
                if feature.properties is None:
                    feature.properties = {}
                # Create a copy to ensure SQLAlchemy detects change
                props = dict(feature.properties)
                props["station_id"] = station.station_id
                feature.properties = props

            db.flush()

            # 4. Create Time Series Data for each station
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(days=30)

            for station in stations:
                # Metadata
                ts_meta = TimeSeriesMetadata(
                    series_id=f"TS_{station.station_id}_LEVEL",
                    name=f"Water Level - {station.name}",
                    source_type="sensor",
                    station_id=station.station_id,
                    start_time=start_time,
                    end_time=now,
                    parameter="water_level",
                    unit="cm",
                    data_type="continuous",
                    sampling_rate="1hour",
                )
                db.add(ts_meta)
                db.flush()

                # Data Points (Sine wave with noise)
                data_points = []
                current_time = start_time

                # Random parameters for the wave
                base_level = random.uniform(100, 200)
                amplitude = random.uniform(20, 50)
                frequency = random.uniform(0.5, 2.0)
                phase = random.uniform(0, 2 * math.pi)

                while current_time <= now:
                    # Seconds from start
                    t = (current_time - start_time).total_seconds() / (
                        24 * 3600
                    )  # days

                    # Value calculation
                    val = base_level + amplitude * math.sin(
                        2 * math.pi * frequency * t + phase
                    )
                    # Add noise
                    val += random.gauss(0, 2)

                    point = TimeSeriesData(
                        series_id=ts_meta.series_id,
                        timestamp=current_time,
                        value=round(val, 2),
                        quality_flag="good",
                    )
                    data_points.append(point)

                    current_time += timedelta(hours=1)

                    # Batch add to avoid memory issues for large datasets
                    if len(data_points) >= 1000:
                        db.bulk_save_objects(data_points)
                        data_points = []

                if data_points:
                    db.bulk_save_objects(data_points)

            db.commit()
            logger.info("Database seeding completed successfully!")

        # 5. Publish to GeoServer
        try:
            logger.info("Publishing to GeoServer...")
            from app.services.geoserver_service import GeoServerService

            gs_service = GeoServerService()
            if gs_service.test_connection():
                workspace_name = "water_data"
                store_name = "water_data_store"

                # Ensure workspace exists
                gs_service.create_workspace(workspace_name)

                # Ensure DataStore exists
                # Using 'postgres' as host because this runs in Docker network
                connection_params = {
                    "host": "postgres",
                    "port": "5432",
                    "database": "water_data",
                    "user": "postgres",
                    "passwd": "postgres",
                    "dbtype": "postgis",
                    "schema": "public",
                }
                gs_service.create_datastore(
                    store_name, connection_params=connection_params
                )

                # Publish SQL View for Czech Regions
                # We filter by layer_id to only show relevant features for this layer
                layer_name = "czech_regions"  # Explicitly set for reconnection
                sql = f"SELECT * FROM geo_features WHERE layer_id = '{layer_name}'"

                gs_service.publish_sql_view(
                    layer_name=layer_name,
                    store_name=store_name,
                    sql=sql,
                    title="Czech Republic Regions",
                    workspace=workspace_name,
                )
                logger.info(f"Successfully published layer {layer_name} to GeoServer")

                # Publish SQL View for Czech Republic
                layer_name_cz = "czech_republic"
                sql_cz = (
                    f"SELECT * FROM geo_features WHERE layer_id = '{layer_name_cz}'"
                )

                gs_service.publish_sql_view(
                    layer_name=layer_name_cz,
                    store_name=store_name,
                    sql=sql_cz,
                    title="Czech Republic",
                    workspace=workspace_name,
                )
                logger.info(
                    f"Successfully published layer {layer_name_cz} to GeoServer"
                )
            else:
                logger.warning("Could not connect to GeoServer. Skipping publication.")

        except Exception as e:
            logger.error(f"GeoServer publication failed: {e}")
            # Do not rollback DB as data is already committed

    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        db.rollback()
        # Don't raise, just log error so app can still start
