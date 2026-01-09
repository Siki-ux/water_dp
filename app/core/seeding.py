"""
Database seeding module for development and testing.
Populates the database with initial data including Czech Republic regions and time series data.
"""

import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import requests  # Added for TimeIO seeding
from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon, box, shape
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.geospatial import GeoFeature, GeoLayer
from app.models.user_context import Dashboard, Project, project_sensors

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

    # -------------------------------------------------------------------------
    # PART 1: Seed GeoLayers (Regions)
    # -------------------------------------------------------------------------
    try:
        region_features = []  # List of (GeoFeature, ShapelyGeometry)

        # Check if Czech Regions layer exists
        cr_layer_exists = (
            db.query(GeoLayer).filter(GeoLayer.layer_name == "czech_regions").first()
        )

        if not cr_layer_exists:
            logger.info("Seeding Czech Regions GeoLayer...")
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

            if os.path.exists(geojson_path):
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
                except Exception as e:
                    logger.error(f"Failed to load GeoJSON: {e}")

            if not region_features:
                logger.info("Generating synthetic grid regions.")
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

            db.flush()
            db.commit()
        else:
            # Load existing features for TimeIO seeding
            logger.info(
                "Czech Regions layer exists. Loading features for TimeIO check..."
            )
            existing_features = (
                db.query(GeoFeature)
                .filter(GeoFeature.layer_id == "czech_regions")
                .all()
            )
            for f in existing_features:
                # We need Shapely geometry for centroid calculation
                # WKT is in f.geometry (as WKBElement or str depending on GeoAlchemy2 mapping)
                # Converting WKB/WKT to shapely
                from shapely import wkt

                try:
                    # GeoAlchemy2.shape.to_shape(f.geometry) is standard
                    from geoalchemy2.shape import to_shape

                    geom_shape = to_shape(f.geometry)
                    region_features.append((f, geom_shape))
                except Exception as e:
                    logger.warning(
                        f"Could not parse geometry for feature {f.feature_id}: {e}"
                    )

        # Seed Czech Republic Layer (Independent check)
        if (
            not db.query(GeoLayer)
            .filter(GeoLayer.layer_name == "czech_republic")
            .first()
        ):
            logger.info("Seeding Czech Republic layer...")
            # (Keep existing logic, omitted for brevity if unchanged logic is sufficient, but replacing full block)
            # Reimplementing roughly to ensure it's not lost
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
            db.add(cz_rep_layer)
            db.flush()
            # Try load geojson
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            cz_path = os.path.join(data_dir, "czech_republic.json")
            if not os.path.exists(cz_path):
                cz_path = os.path.join(data_dir, "czech_republic.geojson")

            if os.path.exists(cz_path):
                try:
                    with open(cz_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    fs = data.get("features", [])
                    if not fs and data.get("type") == "Feature":
                        fs = [data]
                    for idx, fd in enumerate(fs):
                        props = fd.get("properties", {})
                        fid = props.get("id") or fd.get("id") or f"cz_rep_{idx}"
                        gs = shape(fd["geometry"])
                        wkt = from_shape(gs, srid=4326)
                        feat = GeoFeature(
                            layer_id="czech_republic",
                            feature_id=fid,
                            feature_type="country",
                            geometry=wkt,
                            properties=props,
                            is_active="true",
                        )
                        db.add(feat)
                except Exception as e:
                    logger.error(f"Failed to load CZ Rep GeoJSON: {e}")
            db.commit()

        # -------------------------------------------------------------------------
        # PART 2: Seed TimeIO (FROST) - Always Run Check
        # -------------------------------------------------------------------------
        logger.info("Checking/Seeding TimeIO data...")

        # Frost URL setup
        FROST_URL = settings.frost_url
        fallback_url = "http://localhost:8083/FROST-Server/v1.1"

        try:
            requests.get(FROST_URL, timeout=FROST_CHECK_TIMEOUT)
        except Exception:
            logger.warning(
                f"FROST check failed for {FROST_URL}. Trying fallback {fallback_url}"
            )
            FROST_URL = fallback_url

        # Check if FROST is actually reachable before loop
        try:
            requests.get(FROST_URL, timeout=FROST_CHECK_TIMEOUT)
        except Exception as e:
            logger.warning(
                f"FROST service unreachable at {FROST_URL}. Skipping TimeIO seeding: {e}"
            )
            return

        # Helper
        def ensure_frost_entity(endpoint, payload, force_recreate=False):
            # (Keep existing helper logic)
            url = f"{FROST_URL}/{endpoint}"
            try:
                if "name" in payload:
                    chk = requests.get(
                        f"{url}?$filter=name eq '{payload['name']}'",
                        timeout=SEED_TIMEOUT,
                    )
                    if chk.status_code == 200:
                        v = chk.json().get("value")
                        if v:
                            if force_recreate:
                                eid = v[0]["@iot.id"]
                                requests.delete(f"{url}({eid})", timeout=SEED_TIMEOUT)
                            else:
                                return v[0]["@iot.id"]
                resp = requests.post(url, json=payload, timeout=SEED_TIMEOUT)
                if resp.status_code == 201:
                    loc = resp.headers["Location"]
                    try:
                        return int(loc.split("(")[1].split(")")[0])
                    except Exception:
                        return loc.split("(")[1].split(")")[0]
            except Exception as e:
                logger.error(f"Frost error {endpoint}: {e}")
            return None

        # Sensors/ObsProps
        sensor_id = ensure_frost_entity(
            "Sensors",
            {
                "name": "Standard Sensor",
                "description": "Auto",
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

        # Things/Datastreams
        if region_features:
            logger.info(
                f"Processing {len(region_features)} regions for TimeIO seeding..."
            )
            for feature, poly in region_features:
                # (Keep existing Thing/Datastream/Observation logic)
                region_name = feature.properties.get("name") or feature.feature_id
                centroid = poly.centroid

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

                thing_id = ensure_frost_entity(
                    "Things", thing_payload, force_recreate=False
                )

                if thing_id:
                    # Update local prop
                    if not feature.properties:
                        feature.properties = {}
                    # We can't update feature in DB easily if it came from query without session attach/merge.
                    # But db.flush() earlier means it might be attached.
                    # If loaded from DB (else block), they are attached.
                    props = dict(feature.properties)
                    props["station_id"] = thing_id
                    feature.properties = props

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
                    ds_id = ensure_frost_entity("Datastreams", ds_payload)

                    if ds_id:
                        # Check observations (Reuse logic)
                        try:
                            cnt = requests.get(
                                f"{FROST_URL}/Observations?$filter=Datastream/id eq {ds_id}&$count=true&$top=0",
                                timeout=SEED_TIMEOUT,
                            )
                            if (
                                cnt.status_code == 200
                                and cnt.json().get("@iot.count", 0) == 0
                            ):
                                logger.info(f"Seeding observations for {ds_name}...")
                                base_time = datetime.now(timezone.utc) - timedelta(
                                    days=SEED_OBSERVATIONS_DAYS
                                )
                                total_points = (
                                    SEED_OBSERVATIONS_DAYS
                                    * 24
                                    * 60
                                    // SEED_OBSERVATIONS_INTERVAL_MIN
                                )
                                observations = []
                                for i in range(total_points):
                                    t = base_time + timedelta(
                                        minutes=i * SEED_OBSERVATIONS_INTERVAL_MIN
                                    )
                                    val = 150 + random.uniform(-20, 20)
                                    observations.append(
                                        {
                                            "phenomenonTime": t.isoformat(),
                                            "result": round(val, 2),
                                            "Datastream": {"@iot.id": ds_id},
                                        }
                                    )

                                logger.info(
                                    f"Inserting {len(observations)} observations individually..."
                                )
                                success_count = 0
                                for idx, obs in enumerate(observations):
                                    try:
                                        resp = requests.post(
                                            f"{FROST_URL}/Observations",
                                            json=obs,
                                            timeout=SEED_TIMEOUT,
                                        )
                                        if resp.status_code in [200, 201]:
                                            success_count += 1
                                        else:
                                            if idx % 50 == 0:
                                                logger.warning(
                                                    f"Failed obs {idx}: {resp.status_code}"
                                                )
                                    except Exception as e:
                                        logger.error(f"Error obs {idx}: {e}")
                                logger.info(
                                    f"Successfully inserted {success_count}/{len(observations)} observations."
                                )
                        except Exception as e:
                            logger.error(f"Observation seed fail: {e}")

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

        # -------------------------------------------------------------------------
        # PART 3: Seed User Context (Projects, Dashboards)
        # -------------------------------------------------------------------------
        DEMO_USER_ID = "f5655555-5555-5555-5555-555555555555"  # Demo User ID

        # Check if project exists
        project = db.query(Project).filter(Project.name == "Demo Project").first()
        if not project:
            logger.info("Seeding Demo Project...")
            project = Project(
                name="Demo Project",
                description="A sample project showing water levels.",
                owner_id=DEMO_USER_ID,
            )
            db.add(project)
            db.commit()
            db.refresh(project)

            # Add Sensors to Project
            # Get some thing IDs from features
            features_with_ids = (
                db.query(GeoFeature)
                .filter(GeoFeature.layer_id == "czech_regions")
                .limit(3)
                .all()
            )
            for f in features_with_ids:
                props = f.properties
                if props and "station_id" in props:
                    sensor_id = props["station_id"]
                    try:
                        sql = project_sensors.insert().values(
                            project_id=project.id, sensor_id=str(sensor_id)
                        )
                        db.execute(sql)
                        logger.info(f"Linked sensor {sensor_id} to project.")
                    except Exception as e:
                        logger.warning(f"Failed to link sensor: {e}")
            db.commit()

            # Create Dashboard
            logger.info("Seeding Demo Dashboard...")
            dashboard = Dashboard(
                project_id=project.id,
                name="Water Levels Overview",
                is_public=True,
                layout_config={"layout": "grid"},
                widgets=[
                    {
                        "type": "chart",
                        "title": "Main River Level",
                        "sensor_id": "STATION_1",
                    },
                    {"type": "map", "title": "Region Map"},
                ],
            )
            db.add(dashboard)
            db.commit()
        # Seed Computation Script (Run regardless of project creation status, just needs project to exist)
        if project:
            logger.info("Seeding Computation Script...")
            import shutil
            import uuid

            from app.models.computations import ComputationScript

            script_name = "Flood Prediction"
            script_filename = "flood_prediction.py"

            # Check if exists
            existing_script = (
                db.query(ComputationScript)
                .filter(
                    ComputationScript.project_id == project.id,
                    ComputationScript.name == script_name,
                )
                .first()
            )

            if not existing_script:
                # Source path (assuming it exists in app/computations/flood_prediction.py as a template)
                source_path = os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "computations",
                    script_filename,
                )

                if os.path.exists(source_path):
                    # Generate secure filename
                    project_hex = (
                        project.id.hex
                        if hasattr(project.id, "hex")
                        else str(project.id).replace("-", "")
                    )
                    secure_filename = (
                        f"{project_hex}_{uuid.uuid4().hex[:8]}_{script_filename}"
                    )
                    dest_path = os.path.join(
                        os.path.dirname(os.path.dirname(__file__)),
                        "computations",
                        secure_filename,
                    )

                    try:
                        shutil.copy(source_path, dest_path)

                        comp_script = ComputationScript(
                            name=script_name,
                            description="Prediction model for flood risk assessment.",
                            filename=secure_filename,
                            project_id=project.id,
                            uploaded_by=DEMO_USER_ID,
                            metadata_json=json.dumps(
                                {"version": "1.0", "type": "demonstration"}
                            ),
                        )
                        db.add(comp_script)
                        db.commit()
                        logger.info(f"Seeded computation script: {script_name}")
                    except Exception as e:
                        logger.error(f"Failed to copy/seed script file: {e}")
                else:
                    logger.warning(
                        f"Source script {source_path} not found for seeding."
                    )
            else:
                logger.info("Computation script already exists/checked.")

        logger.info("Demo Project and Dashboard seeded/checked.")

    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        db.rollback()
        raise e
