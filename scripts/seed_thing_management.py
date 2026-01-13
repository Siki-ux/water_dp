import datetime
import os
import sys
import time
import uuid

import requests
from loguru import logger

# Ensure we can import from src
sys.path.append("/app")

try:
    from src.core.db import DBConnection
    from src.core.logging.logging_config import configure_logger
    from src.utils.data_utils.generate_data import get_random_chars
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# Configure Loguru
configure_logger()

# Configuration
FROST_URL = os.getenv("FROST_URL", "http://frost:8080/FROST-Server/v1.1")
# Try internal service query if env not set correctly for container-to-container
if "localhost" in FROST_URL:
    FROST_URL = "http://frost:8080/FROST-Server/v1.1"


def patch_db(cursor, db_connection):
    logger.info("Checking/Applying Database Patch...")
    try:
        # Check if already patched
        cursor.execute("SELECT to_regclass('project_tbl')")
        if cursor.fetchone()["to_regclass"]:
            logger.info("Database already patched (project_tbl exists).")
            return

        logger.info("Renaming 'project' table to 'project_tbl'...")
        cursor.execute('ALTER TABLE "project" RENAME TO "project_tbl"')

        logger.info("Dropping NOT NULL constraint on 'mqtt_id'...")
        cursor.execute('ALTER TABLE "project_tbl" ALTER COLUMN "mqtt_id" DROP NOT NULL')

        logger.info("Creating 'project' View...")
        cursor.execute(
            """
            CREATE VIEW "project" AS
            SELECT id, name, uuid, database_id, authorization_provider_group_id
            FROM "project_tbl"
        """
        )
        db_connection.commit()
        logger.success("Database Patch Applied Successfully.")
    except Exception as e:
        db_connection.rollback()
        logger.error(f"Failed to patch database: {e}")
        raise


def import_sensors(cursor, db_connection, project_id, user_id):
    """
    Imports sensors from FROST to Thing Management.
    Enables simulation for ALL sensors.
    """
    logger.info("Starting Sensor Sync/Import...")

    max_retries = 30  # increased from 12 to allow up to 5 minutes wait
    retry_delay = 10  # increased from 5 seconds for longer intervals

    min_sensors_required = 10  # wait for at least this many sensors (grid regions)
    things = []

    for attempt in range(max_retries):
        try:
            # 1. Fetch all Things from FROST
            url = f"{FROST_URL}/Things?$top=2000"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to fetch Things (Attempt {attempt+1}/{max_retries}): {resp.status_code}"
                )
            else:
                things = resp.json().get("value", [])
                if len(things) >= min_sensors_required:
                    logger.info(f"Found {len(things)} Things in FROST.")
                    break
                else:
                    logger.info(
                        f"Only {len(things)} Things found (Attempt {attempt+1}/{max_retries}). Waiting for more sensors..."
                    )
        except Exception as e:
            logger.warning(
                f"Connection error checking FROST (Attempt {attempt+1}/{max_retries}): {e}"
            )
        time.sleep(retry_delay)

    if not things:
        logger.warning(
            f"No sensors found in FROST after {max_retries*retry_delay} seconds. Skipping import."
        )
        return

    for t in things:
        t_name = t.get("name")
        t_id = t.get("@iot.id")
        t_props = t.get("properties", {})

        # Skip datasets if identified (placeholder logic)
        if t_props.get("type") == "dataset":
            logger.info(f"Skipping dataset: {t_name}")
            continue

        # 2. Check if Thing exists in TM
        # We assume name uniqueness for sync or properties.tm_uuid check
        tm_uuid = t_props.get("tm_uuid")
        existing_id = None

        if tm_uuid:
            cursor.execute('SELECT id FROM "thing" WHERE uuid = %s', (tm_uuid,))
            row = cursor.fetchone()
            if row:
                existing_id = row["id"]

        if not existing_id:
            # Try by Name
            cursor.execute(
                'SELECT id, uuid FROM "thing" WHERE name = %s AND project_id = %s',
                (t_name, project_id),
            )
            row = cursor.fetchone()
            if row:
                existing_id = row["id"]
                tm_uuid = str(row["uuid"])

        # 3. Insert or Update
        if not existing_id:
            logger.info(f"Importing Sensor: {t_name}")
            new_uuid = str(uuid.uuid4())

            try:
                cursor.execute(
                    """
                        INSERT INTO "thing" (name, description, uuid, project_id, ingest_type_id, created_by, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                        """,
                    (
                        t_name,
                        t.get("description", "Imported Sensor"),
                        new_uuid,
                        project_id,
                        2,
                        user_id,
                        datetime.datetime.now(datetime.timezone.utc),
                    ),
                )
                existing_id = cursor.fetchone()["id"]
                tm_uuid = new_uuid
                db_connection.commit()
                logger.success(f"Imported {t_name} (ID: {existing_id})")
            except Exception as e:
                db_connection.rollback()
                logger.error(f"Failed to insert thing {t_name}: {e}")
                continue
        else:
            logger.info(f"Sensor {t_name} already in TM (ID: {existing_id}).")

        # 4. Update FROST Properties (tm_uuid and simulated=true)
        updates_needed = False
        if t_props.get("tm_uuid") != tm_uuid:
            t_props["tm_uuid"] = tm_uuid
            updates_needed = True

        if t_props.get("simulated") != "true":
            t_props["simulated"] = "true"
            # Ensure station_id has SIM prefix for simulator regex?
            # Simulator check: substringof('SIM', properties/station_id) or properties/simulated eq 'true'
            # So simulated: true is enough.
            updates_needed = True

        if updates_needed:
            logger.info(
                f"Updating FROST properties for {t_name} (Enabling Simulation)..."
            )
            try:
                patch_url = f"{FROST_URL}/Things({t_id})"
                # For PATCH/PUT in FROST to update properties?
                # Valid FROST PATCH allows updating properties dict.
                r_patch = requests.patch(patch_url, json={"properties": t_props})
                if r_patch.status_code not in [200, 204]:
                    logger.error(
                        f"Failed to update FROST thing {t_name}: {r_patch.text}"
                    )
            except Exception as e:
                logger.error(f"Error updating FROST: {e}")


def run_seed():
    logger.info("Starting raw SQL seeding process...")

    # 1. Init DB
    try:
        db = DBConnection()
        cursor = db.get_cursor()

        # Apply Patch First
        patch_db(cursor, db.db_connection)

    except Exception as e:
        logger.error(f"Failed to connect/patch DB: {e}")
        return

    # User ID for creation (admin-siki)
    cursor.execute("SELECT id FROM \"user\" WHERE username = 'admin-siki'")
    user_row = cursor.fetchone()
    if user_row:
        user_id = user_row["id"]
    else:
        logger.info("User 'admin-siki' not found. Creating default admin user...")
        try:
            cursor.execute(
                """
                INSERT INTO "user" (username, password, email, first_name, last_name, name, authorization_provider_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (
                    "admin-siki",
                    "dummy",
                    "admin@example.com",
                    "Admin",
                    "Siki",
                    "Admin Siki",
                    "admin-siki-uuid-1234",
                ),
            )
            user_id = cursor.fetchone()["id"]
            db.db_connection.commit()
            logger.success(f"Created user 'admin-siki' with ID: {user_id}")
        except Exception as e:
            db.db_connection.rollback()
            # Fallback to any user or 1 if creation fails (race condition?)
            logger.warning(f"Failed to create admin user: {e}. Trying fallback...")
            cursor.execute('SELECT id FROM "user" LIMIT 1')
            first_user = cursor.fetchone()
            user_id = first_user["id"] if first_user else 1

    group_name = "MyProject"
    project_uuid = str(uuid.uuid4())
    project_id = None

    # Check if project exists (In View or Table)
    cursor.execute('SELECT id FROM "project" WHERE name = %s', (group_name,))
    row = cursor.fetchone()
    if row:
        logger.info(
            f"Project '{group_name}' already exists (ID: {row['id']}). Skipping creation."
        )
        project_id = row["id"]
    else:
        logger.info(f"Creating Project '{group_name}' via raw SQL...")

        # Generate credentials
        db_schema = f"project_{get_random_chars(8).lower()}"
        db_user = f"user_{get_random_chars(8).lower()}"
        db_password = get_random_chars(16)
        ro_user = f"ro_{get_random_chars(8).lower()}"
        ro_password = get_random_chars(16)
        mqtt_user = f"mqtt_{get_random_chars(8).lower()}"
        mqtt_password = get_random_chars(16)

        try:
            # 1. Insert Database
            cursor.execute(
                'INSERT INTO "database" (db_schema, "user", "password", ro_user, ro_password) VALUES (%s, %s, %s, %s, %s) RETURNING id',
                (db_schema, db_user, db_password, ro_user, ro_password),
            )
            db_row = cursor.fetchone()
            database_id = db_row["id"]

            # 2. Insert MQTT (Optional but good to have)
            cursor.execute(
                'INSERT INTO "mqtt" ("user", "password") VALUES (%s, %s) RETURNING id',
                (mqtt_user, mqtt_password),
            )
            # We don't link it because View doesn't have mqtt_id

            # 3. Insert Project (Into View)
            cursor.execute(
                'INSERT INTO "project" ("uuid", "database_id", "name") VALUES (%s, %s, %s) RETURNING id',
                (project_uuid, database_id, group_name),
            )
            project_row = cursor.fetchone()
            project_id = project_row["id"]

            # 4. Update schema name w/ ID
            cursor.execute(
                'UPDATE "database" SET db_schema = %s WHERE id = %s',
                (f"{db_schema}_{project_id}", database_id),
            )

            db.db_connection.commit()
            logger.success(f"Created Project '{group_name}' with ID: {project_id}")

        except Exception as e:
            db.db_connection.rollback()
            logger.error(f"Failed to create project: {e}")
            return

    # 3. Seed CSV Parser
    parser_name = "My CSV Parser"
    csv_parser_type_id = 1

    cursor.execute(
        "SELECT id FROM file_parser WHERE name = %s AND project_id = %s",
        (parser_name, project_id),
    )
    parser_row = cursor.fetchone()

    if parser_row:
        logger.info(
            f"File Parser '{parser_name}' already exists (ID: {parser_row['id']})."
        )
    else:
        logger.info(f"Creating File Parser: {parser_name}")
        settings = '{"delimiter": ",", "headerLineno": 0, "headlinesToExclude": 0, "footlinesToExclude": 0, "commentMarkers": [], "pandasReadCsv": "{}", "timestampColumns": []}'

        try:
            cursor.execute(
                """
                INSERT INTO file_parser (project_id, name, settings, file_parser_type_id, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (
                    project_id,
                    parser_name,
                    settings,
                    csv_parser_type_id,
                    user_id,
                    datetime.datetime.now(datetime.timezone.utc),
                ),
            )
            parser_id = cursor.fetchone()["id"]
            db.db_connection.commit()
            logger.success(f"Created File Parser '{parser_name}' with ID: {parser_id}")
        except Exception as e:
            db.db_connection.rollback()
            logger.error(f"Failed to create parser: {e}")

    # 4. Import/Sync Sensors from FROST
    if project_id:
        import_sensors(cursor, db.db_connection, project_id, user_id)


if __name__ == "__main__":
    run_seed()
