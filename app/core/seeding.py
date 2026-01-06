"""
Database seeding module for development and testing.
Populates the database with initial data including Czech Republic regions and time series data.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import requests  # Added for TimeIO seeding
from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon, box, shape
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.geospatial import GeoFeature, GeoLayer

logger = logging.getLogger(__name__)

# Czech Republic Bounding Box (approximate)
# min_lon, min_lat, max_lon, max_lat
CR_BBOX = (12.09, 48.55, 18.86, 51.06)
# Seeding Configuration (values can be overridden via environment variables)
FROST_CHECK_TIMEOUT = int(os.getenv("FROST_CHECK_TIMEOUT", "10"))
SEED_TIMEOUT = int(os.getenv("SEED_TIMEOUT", "30"))
SEED_MAX_RETRIES = int(os.getenv("SEED_MAX_RETRIES", "3"))
SEED_RETRY_DELAY = int(os.getenv("SEED_RETRY_DELAY", "5"))
SEED_OBSERVATIONS_DAYS = 4
SEED_OBSERVATIONS_INTERVAL_MIN = 15


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
            db.flush()

            # 2. Generate Grid Regions (GeoFeatures)
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            geojson_path = os.path.join(data_dir, "czech_regions.json")
            if not os.path.exists(geojson_path):
                geojson_path = os.path.join(data_dir, "czech_regions.geojson")

            logger.info(f"Geojson Path: {geojson_path}")
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

                        feature_id_raw = (
                            props.get("id")
                            or feature_data.get("id")
                            or f"region_{idx+1}"
                        )

                        geom_shape = shape(feature_data["geometry"])
                        wkt_geom = from_shape(geom_shape, srid=4326)

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

            if not region_features:
                logger.info(
                    "Generating synthetic grid regions (GeoJSON missing or failed)."
                )
                grid_polys = generate_grid_polygons(CR_BBOX, rows=3, cols=4)

                for idx, poly in enumerate(grid_polys):
                    region_name = f"Region_{idx+1}"
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
            # Check existence
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
                    except Exception as e:
                        logger.error(f"Failed to load Czech Republic GeoJSON: {e}")

            # ---------------------------------
            # 3. Create TimeIO Things for each Region (Instead of WaterStations)

            logger.info("Seeding TimeIO data...")
            FROST_URL = settings.frost_url
            # Fallback for local development (outside Docker)
            try:
                requests.get(FROST_URL, timeout=FROST_CHECK_TIMEOUT)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                # Assuming default external port 8083 from docker-compose.yml
                # If this fails, the user should configure FROST_URL in .env
                fallback_url = "http://localhost:8083/FROST-Server/v1.1"
                logger.warning(
                    f"Could not connect to configured FROST_URL ({FROST_URL}). "
                    f"Falling back to {fallback_url} (External Docker Port). "
                    "Please update timeio.env if this is intended for production."
                )
                FROST_URL = fallback_url
            except Exception as e:
                logger.error(f"Unexpected error checking FROST_URL {FROST_URL}: {e}")
                # Don't fallback on other errors (e.g., 401 Auth, 404 Found but invalid path)
                raise

            # Helper for Frost Entity Creation
            def ensure_frost_entity(endpoint, payload, force_recreate=False):
                url = f"{FROST_URL}/{endpoint}"
                try:
                    # Check if exists by name if supported
                    if "name" in payload:
                        chk = requests.get(
                            f"{url}?$filter=name eq '{payload['name']}'",
                            timeout=SEED_TIMEOUT,
                        )
                        if chk.status_code == 200:
                            v = chk.json().get("value")
                            if v:
                                if force_recreate:
                                    # DELETE first
                                    existing_id = v[0]["@iot.id"]
                                    logger.info(
                                        f"Recreating {endpoint} {payload['name']} (Delete ID: {existing_id})"
                                    )
                                    del_resp = requests.delete(
                                        f"{url}({existing_id})", timeout=SEED_TIMEOUT
                                    )
                                    if del_resp.status_code not in [200, 204]:
                                        logger.error(
                                            f"Failed to delete existing {endpoint}: {del_resp.status_code} {del_resp.text}"
                                        )
                                        # If delete failed, we cannot recreate safely.
                                        raise RuntimeError(
                                            f"Failed to delete existing {endpoint} {payload['name']} (ID: {existing_id}) during force recreation."
                                        )

                                    # Proceed to create
                                else:
                                    return v[0]["@iot.id"]

                    resp = requests.post(url, json=payload, timeout=SEED_TIMEOUT)
                    if resp.status_code == 201:
                        # Extract ID from Location header
                        frost_id = resp.headers["Location"].split("(")[1].split(")")[0]
                        try:
                            # Try clear any non-numeric
                            return int(frost_id)
                        except ValueError:
                            return frost_id
                    else:
                        logger.error(
                            f"Failed to create {endpoint}: {resp.status_code} {resp.text}"
                        )
                except Exception as ex:
                    logger.error(f"Frost error {endpoint}: {ex}")
                return None

            # Common Sensor/ObsProp
            sensor_id = ensure_frost_entity(
                "Sensors",
                {
                    "name": "Standard Sensor",
                    "description": "Auto-generated",
                    "encodingType": "application/pdf",
                    "metadata": "none",
                },
            )
            op_id = ensure_frost_entity(
                "ObservedProperties",
                {
                    "name": "Water Level",
                    "description": "River Level",
                    "definition": "http://example.org",
                },
            )

            for feature, poly in region_features:
                region_name = feature.properties.get("name") or feature.feature_id
                centroid = poly.centroid

                # Create Thing with Deep Insert Location
                thing_payload = {
                    "name": f"Station {region_name}",
                    "description": f"Monitoring Station for {region_name}",
                    "properties": {
                        "station_id": f"STATION_{feature.feature_id}",
                        "region": region_name,
                        "type": "river",
                        "status": "active",
                    },
                    "Locations": [
                        {
                            "name": f"Loc {region_name}",
                            "description": f"Location of {region_name}",
                            "encodingType": "application/vnd.geo+json",
                            "location": {
                                "type": "Point",
                                "coordinates": [centroid.x, centroid.y],
                            },
                        }
                    ],
                }
                # FORCE RECREATE THING to ensure linking isn't broken
                thing_id = ensure_frost_entity(
                    "Things", thing_payload, force_recreate=True
                )

                if thing_id:
                    # Update Feature Property with Thing ID
                    if not feature.properties:
                        feature.properties = {}
                    props = dict(feature.properties)  # copy
                    props["station_id"] = thing_id
                    feature.properties = props

                    # Datastream
                    ds_name = f"DS_{thing_id}_LEVEL"
                    ds_payload = {
                        "name": ds_name,
                        "description": "Water Level Datastream",
                        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                        "unitOfMeasurement": {
                            "name": "Meter",
                            "symbol": "m",
                            "definition": "http://example.org",
                        },
                        "Thing": {"@iot.id": thing_id},
                        "Sensor": {"@iot.id": sensor_id},
                        "ObservedProperty": {"@iot.id": op_id},
                    }
                    # Datastream might persist? Deleting Thing deletes Datastreams too (Cascade).
                    # So Datastream should be gone.
                    ds_id = ensure_frost_entity("Datastreams", ds_payload)

                    if ds_id:
                        # Retry loop for observation seeding (network/startup resilience)
                        for attempt in range(SEED_MAX_RETRIES):
                            try:
                                # Quick check if data exists
                                cnt = requests.get(
                                    f"{FROST_URL}/Observations?$filter=Datastream/id eq {ds_id}&$count=true&$top=0",
                                    timeout=SEED_TIMEOUT,
                                )

                                if cnt.status_code == 200:
                                    count = cnt.json().get("@iot.count", 0)
                                    if count == 0:
                                        logger.info(
                                            f"Seeding observations for {ds_name}..."
                                        )
                                        base_time = datetime.now(
                                            timezone.utc
                                        ) - timedelta(days=SEED_OBSERVATIONS_DAYS)

                                        # Calculate total points
                                        total_points = (
                                            SEED_OBSERVATIONS_DAYS
                                            * 24
                                            * 60
                                            // SEED_OBSERVATIONS_INTERVAL_MIN
                                        )

                                        # Build all observations first
                                        observations = []
                                        for i in range(total_points):
                                            t = base_time + timedelta(
                                                minutes=i
                                                * SEED_OBSERVATIONS_INTERVAL_MIN
                                            )
                                            val = 150 + random.uniform(-20, 20)
                                            observations.append(
                                                {
                                                    "phenomenonTime": t.isoformat(),
                                                    "result": round(val, 2),
                                                    "Datastream": {"@iot.id": ds_id},
                                                }
                                            )

                                        # Send observations in batches to improve performance
                                        BATCH_SIZE = 100
                                        total_batches = (
                                            len(observations) + BATCH_SIZE - 1
                                        ) // BATCH_SIZE

                                        logger.info(
                                            f"Inserting {len(observations)} observations in {total_batches} batches..."
                                        )

                                        for batch_idx in range(total_batches):
                                            start_idx = batch_idx * BATCH_SIZE
                                            end_idx = min(
                                                start_idx + BATCH_SIZE,
                                                len(observations),
                                            )
                                            batch = observations[start_idx:end_idx]

                                            try:
                                                resp = requests.post(
                                                    f"{FROST_URL}/CreateObservations",
                                                    json=batch,
                                                    timeout=SEED_TIMEOUT,
                                                )
                                                if resp.status_code not in [200, 201]:
                                                    logger.warning(
                                                        f"Batch {batch_idx + 1}/{total_batches} failed: {resp.status_code} - {resp.text}"
                                                    )
                                                else:
                                                    logger.debug(
                                                        f"Batch {batch_idx + 1}/{total_batches} inserted successfully ({len(batch)} observations)"
                                                    )
                                            except Exception as e:
                                                logger.error(
                                                    f"Error inserting batch {batch_idx + 1}: {e}"
                                                )
                                                # Continue with next batch
                                    else:
                                        logger.info(
                                            f"Observations already exist for {ds_name} (Count: {count}). Skipping."
                                        )

                                else:
                                    # If check failed but not exception, raise to trigger retry
                                    raise requests.exceptions.RequestException(
                                        f"Status {cnt.status_code}"
                                    )

                                # If we get here (either seeded or exists), break loop
                                break

                            except Exception as e:
                                if attempt < SEED_MAX_RETRIES - 1:
                                    logger.warning(
                                        f"Observation seeding failed (attempt {attempt+1}): {e}. Retrying..."
                                    )
                                    time.sleep(SEED_RETRY_DELAY)
                                else:
                                    logger.error(
                                        f"Observation seeding failed after {SEED_MAX_RETRIES} attempts: {e}"
                                    )

            db.flush()
            db.commit()

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
                layer_name = "czech_regions"
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

    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        db.rollback()
