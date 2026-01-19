"""
Database seeding module for development and testing.
Populates the database with initial data including Czech Republic regions and time series data.
"""

import json
import logging
import os
from typing import List, Tuple

import requests  # Added for TimeIO seeding
from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon, box, shape
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
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

        # Seed Czech Regions Praha Layer: SKIP (Managed by GeoServer Stack)
        # We rely on GeoServer WFS for this layer now.
        pass

        # -------------------------------------------------------------------------
        # PART 2: Seed TimeIO (FROST) - CHECK ONLY (Consumer Mode)
        # -------------------------------------------------------------------------
        # Updated Logic: We assume TimeIO Stack is the Producer.
        # We only check if FROST is up. We do NOT create standard sensors/obs props.
        logger.info("Checking TimeIO data (Consumer Mode)...")

        FROST_URL = settings.frost_url

        # Check if FROST is actually reachable with retries
        # Check if FROST is actually reachable with retries
        import time

        max_retries = 12
        for attempt in range(max_retries):
            try:
                requests.get(FROST_URL, timeout=FROST_CHECK_TIMEOUT)
                logger.info(f"FROST service is reachable at {FROST_URL}.")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.info(
                        f"FROST not ready (Attempt {attempt+1}/{max_retries}). Retrying in 5s..."
                    )
                    time.sleep(5)
                else:
                    logger.warning(
                        f"FROST service unreachable at {FROST_URL} after {max_retries} attempts. Skipping TimeIO seeding: {e}"
                    )
                    FROST_URL = None

        # Helper
        def ensure_frost_entity(endpoint, payload, force_recreate=False):
            if not FROST_URL:
                return None
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
        if FROST_URL:
            sensor_id = ensure_frost_entity(
                "Sensors",
                {
                    "name": "Standard Sensor",
                    "description": "Auto",
                    "encodingType": "application/pdf",
                    "metadata": "none",
                },
            )
            _ = ensure_frost_entity(
                "ObservedProperties",
                {
                    "name": "Water Level",
                    "description": "River Level",
                    "definition": "http://example.org",
                },
            )

            # Things/Datastreams
            # [CONSUMER CHANGE] TimeIO Stack owns Thing Creation.
            # We skip creating "Station X" things here.
            # However, we still iterate regions to Update Local GeoFeature Props with IDs if found.

            if region_features:
                logger.info(
                    f"Linking {len(region_features)} regions to existing TimeIO Things..."
                )

            for feature, poly in region_features:
                region_name = feature.properties.get("name") or feature.feature_id

                # Check if thing exists by name
                thing_name = f"Station {region_name}"
                check_url = f"{FROST_URL}/Things?$filter=name eq '{thing_name}'"
                thing_id = None
                try:
                    r = requests.get(check_url, timeout=SEED_TIMEOUT)
                    if r.status_code == 200:
                        val = r.json().get("value")
                        if val:
                            thing_id = val[0]["@iot.id"]
                            logger.info(
                                f"Found existing Thing {thing_name} (ID: {thing_id})"
                            )
                except Exception:
                    pass

                if thing_id:
                    # Update local prop
                    if not feature.properties:
                        feature.properties = {}

                    from sqlalchemy.orm.attributes import flag_modified

                    props = dict(feature.properties)
                    props["station_id"] = thing_id
                    feature.properties = props
                    flag_modified(feature, "properties")
                    logger.info(f"Linked {region_name} to FROST Thing ID: {thing_id}")
                else:
                    # [MOD] Be more flexible: try lookup by region name directly (e.g. "Station Region_1")
                    # if the Feature ID was something else (e.g. "CZ01...").
                    # This allows the synthetic TimeIO grid to match the GeoJSON features.
                    idx_match = None
                    if "region_" in feature.feature_id.lower():
                        idx_match = feature.feature_id.split("_")[-1]

                    if idx_match:
                        alt_name = f"Station Region_{idx_match}"
                        try:
                            r = requests.get(
                                f"{FROST_URL}/Things?$filter=name eq '{alt_name}'",
                                timeout=SEED_TIMEOUT,
                            )
                            if r.status_code == 200 and r.json().get("value"):
                                thing_id = r.json().get("value")[0]["@iot.id"]
                                logger.info(
                                    f"Found existing Thing via Alt Name {alt_name} (ID: {thing_id})"
                                )

                                if not feature.properties:
                                    feature.properties = {}

                                props = dict(feature.properties)
                                props["station_id"] = thing_id
                                feature.properties = props
                                flag_modified(feature, "properties")
                        except Exception:
                            pass

                if not thing_id:
                    logger.warning(
                        f"Thing {thing_name} not found in FROST. Waiting for TimeIO seeding?"
                    )

            db.commit()

        # 5. Publish to GeoServer - SKIPPED
        # [MODIFIED] We no longer publish layers from water_dp seeding.
        # GeoServer is now independent and seeded via geoserver_stack/scripts/seed_geoserver.py.
        # This prevents overwriting the DataStore configuration.
        logger.info("Skipping GeoServer publication (managed by geoserver_stack).")

        # -------------------------------------------------------------------------
        # PART 3: Seed User Context (Projects, Dashboards)
        # -------------------------------------------------------------------------
        logger.info("[SEEDING] Starting Part 3: User Context Seeding")
        DEMO_USER_ID = "f5655555-5555-5555-5555-555555555555"  # Demo User ID

        # Check if project exists
        project = db.query(Project).filter(Project.name == "Demo Project").first()
        if not project:
            logger.info("Seeding Demo Project...")
            project = Project(
                name="Demo Project",
                description="A sample project showing water levels.",
                owner_id=DEMO_USER_ID,
                authorization_provider_group_id="UFZ-TSM:MyProject",
            )
            db.add(project)
            db.commit()
            db.refresh(project)

        # Ensure Sensors are Linked (Idempotent)
        # Get some thing IDs from features
        logger.info("[SEEDING] Linking sensors to demo project...")
        features_with_ids = (
            db.query(GeoFeature).filter(GeoFeature.layer_id == "czech_regions")
            # .limit(3) # Removed limit
            .all()
        )
        logger.info(
            f"[SEEDING] Found {len(features_with_ids)} features to check for linking."
        )

        for f in features_with_ids:
            props = f.properties
            logger.info(f"Checking feature {f.feature_id} props: {props}")
            if props and "station_id" in props:
                # In our seeding logic (line 353), station_id property IS the things IoT ID (int or str)
                # We store this in the association table.
                sensor_id = str(props["station_id"])

                # Verify it's not None
                if sensor_id:
                    try:
                        # Use nested transaction (savepoint) to handle potential errors safely without rolling back everything
                        with db.begin_nested():
                            # Check existence (redundant but safe)
                            exists_stmt = project_sensors.select().where(
                                and_(
                                    project_sensors.c.project_id == project.id,
                                    project_sensors.c.sensor_id == sensor_id,
                                )
                            )
                            if not db.execute(exists_stmt).first():
                                stmt = project_sensors.insert().values(
                                    project_id=project.id, sensor_id=sensor_id
                                )
                                db.execute(stmt)
                                logger.info(
                                    f"Linked sensor (Thing ID) {sensor_id} to project."
                                )

                        db.commit()  # Commit this success immediately
                    except IntegrityError:
                        logger.info(
                            f"Sensor {sensor_id} already linked (IntegrityError ignored)."
                        )
                        # No need to rollback explicitely, begin_nested handles savepoint rollback
                    except Exception as e:
                        logger.warning(f"Failed to link sensor {sensor_id}: {e}")
                        # No need to rollback entire session
        # db.commit() # Already committed individually

        # [USER REQUEST] Link ALL other available sensors (except 'unlinked') to Demo Project
        logger.info(
            "[SEEDING] Linking ALL other available sensors (excluding 'unlinked') to Demo Project..."
        )
        try:
            # Fetch all things from FROST
            r_all = requests.get(f"{FROST_URL}/Things", timeout=SEED_TIMEOUT)
            if r_all.status_code == 200:
                all_things = r_all.json().get("value", [])
                for t in all_things:
                    t_name = t.get("name", "")
                    t_id = t.get("@iot.id")

                    # 1. Skip if name contains "unlinked" (case-insensitive)
                    if "unlinked" in t_name.lower():
                        continue

                    # 2. Skip if already linked (we can try insertion and ignore conflict)
                    # Convert to string for DB
                    s_id_str = str(t_id)

                    try:
                        with db.begin_nested():
                            stmt = project_sensors.select().where(
                                and_(
                                    project_sensors.c.project_id == project.id,
                                    project_sensors.c.sensor_id == s_id_str,
                                )
                            )
                            if not db.execute(stmt).first():
                                db.execute(
                                    project_sensors.insert().values(
                                        project_id=project.id, sensor_id=s_id_str
                                    )
                                )
                                logger.info(
                                    f"Auto-linked sensor '{t_name}' ({s_id_str}) to Demo Project."
                                )
                        db.commit()
                    except Exception as ex:
                        logger.warning(f"Failed to auto-link sensor {t_name}: {ex}")
        except Exception as e:
            logger.error(f"Error checking for additional sensors to link: {e}")

        if not db.query(Dashboard).filter(Dashboard.project_id == project.id).first():
            # Create Dashboard using project.id
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

        # -------------------------------------------------------------------------
        # PART 4: Advanced Scenarios & Simulator Setup
        # -------------------------------------------------------------------------
        logger.info("[SEEDING] Starting Part 4: Advanced Scenarios & Simulator")
        seed_advanced_logic(db)
        seed_simulator_entities()  # Ensure simulator entities are seeded if needed

        # -------------------------------------------------------------------------
        # PART 5: Seeding Inactive Sensors and Datasets
        # -------------------------------------------------------------------------
        logger.info("[SEEDING] Starting Part 5: Inactive Sensors & Datasets")

        # 1. Seed Inactive Sensor
        inactive_sensor_id = ensure_frost_entity(
            "Things",
            {
                "name": "Inactive Station",
                "description": "Legacy station, currently inactive.",
                "properties": {
                    "station_id": "STATION_INACTIVE_1",
                    "region": "Region_1",
                    "type": "river",
                    "status": "inactive",
                },
                "Locations": [
                    {
                        "name": "Loc Inactive",
                        "description": "Location of Inactive Station",
                        "encodingType": "application/vnd.geo+json",
                        "location": {
                            "type": "Point",
                            "coordinates": [14.5, 50.0],  # Arbitrary point
                        },
                    }
                ],
            },
        )
        if inactive_sensor_id:
            # Link to project so it appears in list (but as inactive)
            try:
                with db.begin_nested():
                    exists = db.execute(
                        project_sensors.select().where(
                            and_(
                                project_sensors.c.project_id == project.id,
                                project_sensors.c.sensor_id == str(inactive_sensor_id),
                            )
                        )
                    ).first()
                    if not exists:
                        db.execute(
                            project_sensors.insert().values(
                                project_id=project.id, sensor_id=str(inactive_sensor_id)
                            )
                        )
                        logger.info(
                            f"Linked Inactive Sensor {inactive_sensor_id} to project."
                        )
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to link inactive sensor: {e}")

        # 2. Seed Non-Sensor Dataset
        # This represents a dataset (e.g., CSV upload) that isn't a physical sensor
        dataset_id = ensure_frost_entity(
            "Things",
            {
                "name": "Historic Flood Data 2010",
                "description": "Imported dataset of 2010 flood levels.",
                "properties": {
                    "station_id": "DATASET_FLOOD_2010",  # ID scheme for datasets
                    "type": "dataset",  # Key discriminator
                    "status": "static",
                    "source": "csv_import",
                },
            },
        )
        if dataset_id:
            logger.info(
                f"Seeded Dataset Thing: {dataset_id} (Historic Flood Data 2010)"
            )
            # We optionally LINK it to project if we want it visible in "Datasets" tab for that project
            # Assuming 'project_sensors' is generic for 'project_things'
            try:
                with db.begin_nested():
                    exists = db.execute(
                        project_sensors.select().where(
                            and_(
                                project_sensors.c.project_id == project.id,
                                project_sensors.c.sensor_id == str(dataset_id),
                            )
                        )
                    ).first()
                    if not exists:
                        db.execute(
                            project_sensors.insert().values(
                                project_id=project.id, sensor_id=str(dataset_id)
                            )
                        )
                        logger.info(f"Linked Dataset {dataset_id} to project.")
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to link dataset: {e}")

        logger.info("Demo Project, Dashboard, and Advanced Scenarios seeded/checked.")

    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        db.rollback()
        raise e


def seed_advanced_logic(db: Session):
    """
    Creates multiple projects and users to test access control.
    Merged from scripts/seed_advanced.py
    """
    from app.models.user_context import Project, ProjectMember, project_sensors

    SIKI_ID = "user-siki-123"
    USER2_ID = "user-2-456"

    # 1. Add Siki to Demo Project
    p1 = db.query(Project).filter(Project.name == "Demo Project").first()
    if p1:
        member = (
            db.query(ProjectMember).filter_by(project_id=p1.id, user_id=SIKI_ID).first()
        )
        if not member:
            logger.info(f"Adding Siki ({SIKI_ID}) to Demo Project...")
            db.add(ProjectMember(project_id=p1.id, user_id=SIKI_ID, role="editor"))
            db.commit()

    # 2. Create Project 2
    p2 = db.query(Project).filter(Project.name == "Demo Project 2").first()
    if not p2:
        logger.info("Creating Demo Project 2...")
        p2 = Project(
            name="Demo Project 2",
            description="Second project for access control testing.",
            owner_id=USER2_ID,
        )
        db.add(p2)
        db.commit()
        db.refresh(p2)

        # Add User 2 as Admin
        db.add(ProjectMember(project_id=p2.id, user_id=USER2_ID, role="admin"))
        db.commit()

    # 3. Link Sensors to Project 2 (IDs 1 and 5 if available)
    # 3. Link Sensors to Project 2 (IDs 1 and 5 if available)
    target_sensors = ["1", "5"]
    for sid in target_sensors:
        stmt = project_sensors.select().where(
            and_(
                project_sensors.c.project_id == p2.id,
                project_sensors.c.sensor_id == sid,
            )
        )
        exists = db.execute(stmt).first()
        if not exists:
            # Only link if not already linked (simple check)
            # We don't check if sensor exists in FROST here, assuming basic seeding created ID 1
            insert_stmt = project_sensors.insert().values(
                project_id=p2.id, sensor_id=sid
            )
            try:
                db.execute(insert_stmt)
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to link sensor {sid}: {e}")
                db.rollback()


def seed_simulator_entities():
    """
    Creates sensors specifically for the Simulator service and Generic Unlinked testing.
    """
    FROST_URL = settings.frost_url
    try:
        requests.get(FROST_URL, timeout=5)
    except requests.RequestException:
        FROST_URL = "http://localhost:8083/FROST-Server/v1.1"

    # Helper
    def create_thing(payload):
        try:
            # Check if exists by name
            name = payload["name"]
            r = requests.get(f"{FROST_URL}/Things?$filter=name eq '{name}'", timeout=5)
            if r.status_code == 200 and r.json().get("value"):
                return  # Already exists

            resp = requests.post(f"{FROST_URL}/Things", json=payload, timeout=5)
            if resp.status_code == 201:
                logger.info(f"Created Simulator Thing: {name}")
        except Exception as e:
            logger.warning(f"Failed to create sim thing {payload.get('name')}: {e}")

    # 1. Auto-Simulated Sensor (For Simulator Service)
    create_thing(
        {
            "name": "Auto-Simulated Sensor",
            "description": "Automatically created for Simulator service.",
            "properties": {
                "station_id": "SIM_AUTO_01",
                "simulated": "true",
                "type": "river",
                "status": "active",
            },
            "Locations": [
                {
                    "name": "Sim Location",
                    "description": "Virtual",
                    "encodingType": "application/vnd.geo+json",
                    "location": {"type": "Point", "coordinates": [14.5, 50.1]},
                }
            ],
        }
    )

    # 2. Unlinked Sensors (For UI Testing)
    for i in range(1, 6):
        create_thing(
            {
                "name": f"Unlinked Sensor {i}",
                "description": "Available for linking.",
                "properties": {
                    "station_id": f"UNLINKED_0{i}",
                    "status": "active",
                    "type": "river",
                },
                "Locations": [
                    {
                        "name": f"Loc {i}",
                        "description": "Location",
                        "encodingType": "application/vnd.geo+json",
                        "location": {
                            "type": "Point",
                            "coordinates": [14.4 + (i * 0.01), 50.0],
                        },
                    }
                ],
            }
        )
