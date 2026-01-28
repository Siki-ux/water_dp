import logging
import secrets
import string
import uuid as uuid_pkg
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.services.keycloak_service import KeycloakService
from app.services.timeio.mqtt_client import MQTTClient
from app.services.timeio.timeio_db import TimeIODatabase
from app.services.timeio.frost_client import get_cached_frost_client

logger = logging.getLogger(__name__)


class TimeIOOrchestratorV3:
    """
    v3 Orchestrator: Autonomous sensor management.

    Directly manages ConfigDB and MQTT events to bypass legacy APIs.
    """

    def __init__(self):
        self._mqtt_client = None
        self._db = None
        self._keycloak = None

    @property
    def mqtt(self) -> MQTTClient:
        if not self._mqtt_client:
            self._mqtt_client = MQTTClient()
        return self._mqtt_client

    @property
    def db(self) -> TimeIODatabase:
        if not self._db:
            self._db = TimeIODatabase()
        return self._db

    @property
    def keycloak(self) -> KeycloakService:
        if not self._keycloak:
            self._keycloak = KeycloakService()
        return self._keycloak

    def _generate_credentials(self, length: int = 16) -> str:
        """Generate a secure random string for credentials."""
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def sanitize_slug(self, name: str) -> str:
        """Sanitize a name to be used as a database schema or slug."""
        if not name:
            return "default"
        # Strip common prefixes like 'UFZ-TSM:' or 'project_'
        clean = (
            name.replace("UFZ-TSM:", "")
            .replace("ufz-tsm:", "")
            .replace("project_", "")
            .strip()
        )
        # Replace spaces and hyphens with underscores, lowercase
        return clean.lower().replace(" ", "_").replace("-", "_")

    def create_sensor(
        self,
        sensor_name: str,
        project_group_id: str,
        description: str = "",
        device_type: str = "chirpstack_generic",
        location: Optional[Dict[str, float]] = None,
        properties: Optional[List[Dict[str, str]]] = None,
        parser_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a new sensor (Thing) autonomously.

        Args:
            project_group: Keycloak authorization group ID or name
            properties: List of dicts with {"name": "temp", "unit": "Celsius", "label": "Air Temp"}
        """
        keycloak_group = self.keycloak.get_group(project_group_id)

        keycloak_group_name = keycloak_group["name"]
        logger.info(f"Creating sensor '{sensor_name}' in group '{keycloak_group_name}'")

        # 1. Resolve Project Name and Schema
        config_project_name = None
        target_schema = None
        project_id = None

        # Resolve true project name from Keycloak Group Name
        # e.g. "UFZ-TSM:MyProject" -> "MyProject"
        def extract_group_name(raw: str) -> str:
            if not raw:
                return "default"
            parts = raw.split(":")
            return parts[-1]

        timeio_project_name = extract_group_name(keycloak_group_name)
        config_project_name = timeio_project_name

        # A. Check if project already exists in CONFIG_DB (Primary)
        existing_config_project = self.db.get_config_project_by_name(
            config_project_name
        )
        if existing_config_project:
            project_id = existing_config_project["id"]
            target_schema = existing_config_project["db_schema"]
            logger.info(
                f"Found existing ConfigDB project '{config_project_name}' (ID: {project_id}, Schema: {target_schema})"
            )

        # B. If not in ConfigDB, check THING_MANAGEMENT_DB (Legacy/Split-Brain)
        elif not project_id:
            legacy_schema = self.db.get_tsm_db_schema_by_name(config_project_name)
            if legacy_schema:
                target_schema = legacy_schema
                logger.info(
                    f"Found existing Legacy project '{config_project_name}' (Schema: {target_schema}). Will sync to ConfigDB."
                )

        # C. Default / New Project
        if not target_schema:
            safe_name = self.sanitize_slug(config_project_name)
            # Find next available schema number to prevent collision
            # e.g. project_myproject_1, project_myproject_2
            next_id = self.db.get_next_project_schema_number(safe_name)
            target_schema = f"project_{safe_name}_{next_id}"
            logger.info(f"Using new target schema: {target_schema}")

        # Ensure Project exists in ConfigDB (Create or Get ID)
        if not project_id:
            safe_name = self.sanitize_slug(config_project_name)
            db_user = f"user_{safe_name}"  # Legacy convention
            db_pass = db_user
            ro_user = f"ro_user_{safe_name}"  # Legacy convention
            ro_pass = ro_user

            project_id = self.db.get_or_create_config_project(
                uuid=project_group_id,  # Link V3 Group UUID to this project
                name=config_project_name,
                db_schema=target_schema,
                db_user=db_user,
                db_pass=db_pass,
                ro_user=ro_user,
                ro_pass=ro_pass,
            )

        # 2. Prepare Thing
        thing_uuid = str(uuid_pkg.uuid4())
        mqtt_user = f"mq_{thing_uuid[:8]}"
        mqtt_pass = self._generate_credentials()

        # 3. Create ConfigDB record with metadata
        config_ids = self.db.create_thing_config(
            uuid=thing_uuid,
            name=sensor_name,
            project_id=project_id,
            mqtt_user=mqtt_user,
            mqtt_pass=mqtt_pass,
            description=description,
            properties={
                "v3_managed": True,
                "properties": properties,
            },  # Store basic info in ConfigDB
            mqtt_device_type_name=device_type,
        )

        # 4. Trigger TSM Workers (The Worker will create the schema and Thing)
        topic = getattr(settings, "topic_config_db_update", "thing_creation")
        payload = {"thing": thing_uuid}

        logger.info(
            f"Triggering TSM workers via MQTT topic '{topic}' for thing {thing_uuid}"
        )
        success = self.mqtt.publish_message(
            topic=topic,
            payload=payload,
            username=settings.mqtt_username,
            password=settings.mqtt_password,
        )

        if not success:
            logger.error(f"Failed to trigger TSM workers for thing {thing_uuid}")
            raise RuntimeError("Failed to trigger TSM workflow")

        # 5. Wait for Worker to populate Project DB (Polling)
        # We need the ID (FROST View ID) which is generated by the worker's insert.
        import time

        frost_id = None
        max_retries = 20
        sleep_time = 0.5

        logger.info(f"Waiting for worker to populate schema '{target_schema}'...")

        for retry_index in range(max_retries):
            # Try to fetch ID (and implicitely check if schema/table exists)
            try:
                # We reuse upsert_thing_to_project_db just to getting ID?
                # No, upsert WRITES. We want to READ.
                # But wait, we also wanted to register properties metadata...
                # The worker registers properties only if they are in ConfigDB Properties?
                # setup_user_database.py: json.dumps(thing.properties)
                # It inserts thing.properties into the table.
                # BUT it does NOT populate sms_static_location etc. unless we do it?
                # Or create_frost_views?

                # We need to wait for the 'thing' table to exist and the record to exist.
                # We can use a simple SELECT query via DB helper.

                # Check if thing exists and get ID
                config_id = self.db.get_thing_id_in_project_db(
                    target_schema, thing_uuid
                )
                if config_id:
                    frost_id = config_id
                    logger.info(f"Worker finished! Thing ID: {frost_id}")
                    break
            except Exception:
                # Schema might not exist yet
                pass

            time.sleep(sleep_time)

        if not frost_id:
            logger.warning("Worker timed out. Thing might not be ready in FROST yet.")
            # We return success but without ID (or we could fail?)
            # Returning success allows user to retry listing later.

        # 6. Post-Processing (Metadata that Worker might miss?)
        # The worker does NOT seem to register 'sms_device_property' etc based on 'setup_user_database.py' review.
        # It creates views and grants permissions.
        # It does NOT populating 'sms_datastream_link' etc.
        # So we STILL need to do that, but only AFTER schema exists.

        if frost_id and properties:
            try:
                # 5.1 Ensure Datastreams in Project DB (if Worker didn't do it)
                # Worker deploy_ddl creates tables. But does it create datastreams?
                # Worker upsert_thing inserts the THING.
                # It does NOT insert datastreams.
                # So WE must do it.
                self.db.ensure_datastreams_in_project_db(
                    schema=target_schema, thing_uuid=thing_uuid, properties=properties
                )

                # 5.2 Register Metadata (Units/Labels) in public.sms_*
                self.db.register_sensor_metadata(
                    thing_uuid=thing_uuid, properties=properties
                )
            except Exception as error:
                logger.error(f"Failed to register metadata: {error}")
                
        # 6 (New). Link Parser if requested
        if parser_id:
            try:
                # Link Parser to Thing's S3 Store in ConfigDB
                success = self.db.link_thing_to_parser(thing_uuid, parser_id)
                if success:
                    logger.info(f"Linked thing {thing_uuid} to parser {parser_id}")
                else:
                    logger.warning(f"Failed to link parser {parser_id} to thing {thing_uuid}")
            except Exception as error:
                logger.error(f"Failed to link parser: {error}")

        # 7. Register Location (in properties)
        if location:
            try:
                self.update_sensor_location(
                    thing_uuid=thing_uuid,
                    project_schema=target_schema,
                    latitude=location["latitude"],
                    longitude=location["longitude"]
                )
                logger.info(f"Registered location for thing {thing_uuid} in properties")
            except Exception as error:
                logger.error(f"Failed to register location: {error}")

        return {
            "id": frost_id,  # FROST View ID (might be None if timeout)
            "uuid": thing_uuid,
            "name": sensor_name,
            "project_id": project_id,
            "schema": target_schema,
            "mqtt": {
                "username": mqtt_user,
                "password": mqtt_pass,
                "topic": f"mqtt_ingest/{mqtt_user}/data",
            },
            "config_ids": config_ids,
            "latitude": location["latitude"] if location else None,
            "longitude": location["longitude"] if location else None,
            "properties": properties,
        }

    def update_sensor_location(
        self, thing_uuid: str, project_schema: str, latitude: float, longitude: float
    ):
        """Update sensor location directly in project database."""
        # Location is stored as GeoJSON in properties field in the project's 'thing' table
        # matching the LOCATIONS view definition.
        return self.db.update_thing_properties(
            thing_uuid=thing_uuid,
            properties={
                "location": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
            },
        )

    def list_sensors(
        self, project_name: str, project_group: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all sensors in a project with rich metadata."""
        # Identify Target Schema (same logic as create_sensor)
        schema = self.resolve_schema(project_name, project_group)
        print(f"DEBUG: Listing sensors for project '{project_name}' from schema '{schema}'", flush=True)
        
        try:
            sensors = self.db.get_sensors_rich(schema)
            print(f"DEBUG: Found {len(sensors)} sensors in schema '{schema}'", flush=True)
            if len(sensors) == 0:
                 # Debug: Check if schema exists?
                 print(f"DEBUG: Schema '{schema}' appears empty or invalid.", flush=True)
            return sensors
        except Exception as e:
            print(f"DEBUG: Error listing sensors: {e}", flush=True)
            return []

    def list_sensors_paginated(
        self,
        project_name: str,
        project_group: Optional[str],
        uuids: List[str],
        skip: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List specific sensors in a project with basic metadata and pagination."""
        schema = self.resolve_schema(project_name, project_group)
        logger.info(
            f"Listing paginated sensors for project '{project_name}' from schema '{schema}'"
        )
        return self.db.get_sensors_by_uuids(schema, uuids, skip, limit)

    def list_all_sensors_basic(
        self, project_name: str, project_group: Optional[str]
    ) -> List[Dict[str, Any]]:
        """List all sensors in a project schema with basic metadata (bypasses ConfigDB)."""
        schema = self.resolve_schema(project_name, project_group)
        logger.info(
            f"Listing all sensors basic for project '{project_name}' from schema '{schema}'"
        )
        frost_client = get_cached_frost_client(
            base_url=settings.frost_api_url,
            project_name=schema,
            version=settings.frost_api_version,
            frost_server=settings.frost_api_server,
            timeout=settings.frost_api_timeout,
        )
        linked_sensors = frost_client.get_things()
        return self.db.get_all_sensors_basic(schema)

    def resolve_schema(
        self, project_name: str, project_group: Optional[str] = None
    ) -> str:
        """Resolve schema name from project/group names."""
        config_project_name = project_name

        if project_group:
            # 1. Try Keycloak Resolution (Primary)
            try:
                keycloak_group_data = self.keycloak.get_group(project_group)
                if keycloak_group_data and keycloak_group_data.get("name"):
                    raw_name = keycloak_group_data["name"]
                    # Extract "MyProject" from "UFZ-TSM:MyProject"
                    if ":" in raw_name:
                        config_project_name = raw_name.split(":")[-1]
                    elif "/" in raw_name:
                        config_project_name = raw_name.split("/")[-1]
                    else:
                        config_project_name = raw_name
                    logger.info(
                        f"Resolved Project Name from Keycloak: {config_project_name}"
                    )
            except Exception as error:
                logger.warning(
                    f"Failed Keycloak resolution for group {project_group}: {error}"
                )

            # 2. Try DB Resolution (Legacy / Backup)
            if not config_project_name:
                legacy_name = self.db.resolve_project_name_by_group_id(project_group)
                if legacy_name:
                    config_project_name = legacy_name

            # 3. Fallback to extracting from string if it wasn't a UUID?
            # (Use case: project_group passed as "UFZ-TSM:Foo" directly?)
            if not config_project_name and ":" in project_group:
                config_project_name = project_group.split(":")[-1]

        # 4. Fallback to provided project_name
        if not config_project_name:
            config_project_name = project_name

        # 1. Try to find existing schema in ConfigDB
        if config_project_name:
            existing_project = self.db.get_config_project_by_name(config_project_name)
            if existing_project:
                return existing_project["db_schema"]

            # 2. Check Legacy TSM DB
            legacy_schema = self.db.get_tsm_db_schema_by_name(config_project_name)
            if legacy_schema:
                return legacy_schema

        # 3. Fallback: Check if ANY schema exists in Postgres matching pattern
        safe_name = self.sanitize_slug(config_project_name)
        existing_schema = self.db.find_project_schema(safe_name)
        if existing_schema:
            return existing_schema

        # 4. Default Guess
        return f"project_{safe_name}_1"

    def get_schema_by_project_name(self, project_name: str) -> str:
        """Get schema name from project name."""
        return self.db.get_tsm_db_schema_by_name(project_name)

    def get_thing_observations(
        self,
        thing_uuid: str,
        datastream_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch observations for a thing."""
        schema = self.db.get_thing_schema(thing_uuid)
        if not schema:
             logger.warning(f"Could not find schema for thing {thing_uuid}")
             return []
        
        return self.db.get_thing_observations(schema, thing_uuid, datastream_name, limit)


    def get_sensor_datastreams(
        self,
        schema: str,
        thing_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Get datastreams for a sensor via FROST.
        """
        try:
            frost_project_client = get_cached_frost_client(
                base_url=settings.frost_url, 
                project_name=schema, 
                version=settings.frost_version, 
                frost_server=settings.frost_server
            )
            datastreams = frost_project_client.list_datastreams(thing_id=thing_id)
            if not datastreams:
                return []
            return datastreams
        except Exception as error:
            raise Exception(f"Failed to fetch data from FROST: {str(error)}")

    def get_sensor_observations(
        self,
        thing_id: str,
        schema: str,
        datastream_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get observations for a sensor via FROST.
        """
        try:
            frost_project_client = get_cached_frost_client(
                base_url=settings.frost_url, 
                project_name=schema, 
                version=settings.frost_version, 
                frost_server=settings.frost_server
            )
            observations = frost_project_client.get_observations(thing_id=thing_id, datastream_name=datastream_name, limit=limit)
            if not observations:
                return []
            return observations
        except Exception as error:
            raise Exception(f"Failed to fetch data from FROST: {str(error)}")

orchestrator_v3 = TimeIOOrchestratorV3()
