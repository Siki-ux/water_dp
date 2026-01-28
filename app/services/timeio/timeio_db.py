"""
TimeIO Database Service

Provides direct database access for TimeIO fixes that cannot be done via APIs.
Handles schema mapping corrections and FROST view creation.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from app.core.config import settings
from app.services.timeio.crypto_utils import decrypt_password, encrypt_password

logger = logging.getLogger(__name__)


class TimeIODatabase:
    """
    Direct database access for TimeIO fixes.

    This service connects to the TimeIO PostgreSQL database to apply fixes
    for known issues:
    - schema_thing_mapping incorrect schema names
    - Missing FROST views (OBSERVATIONS, DATASTREAMS, THINGS)

    Uses a separate connection from the main water_dp-api database.
    """

    def __init__(
        self,
        db_host: str = None,
        db_port: int = None,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "postgres",
    ):
        """
        Initialize TimeIO database connection parameters.

        Args:
            db_host: Database host (default: from settings or "localhost")
            db_port: Database port (default: from settings or 5432)
            database: Database name
            user: Database user
            password: Database password
        """
        self._db_host = db_host or getattr(settings, "timeio_db_host", "localhost")
        self._db_port = db_port or getattr(settings, "timeio_db_port", 5432)
        self.database = database
        self.user = user
        self.password = password

    def _get_connection(self):
        """Create database connection."""
        return psycopg2.connect(
            host=self._db_host,
            port=self._db_port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    # ========== Schema Mapping Fixes ==========

    def get_schema_mappings(self) -> List[Dict[str, str]]:
        """
        Get all schema_thing_mapping entries.

        Returns:
            List of {schema, thing_uuid} dicts
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT schema, thing_uuid FROM public.schema_thing_mapping"
                )
                rows = cursor.fetchall()
                return [{"schema": row[0], "thing_uuid": row[1]} for row in rows]
        finally:
            connection.close()

    def get_user_schemas(self) -> List[str]:
        """
        Get all user_* schemas in the database.

        Returns:
            List of schema names
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name LIKE 'user_%'
                """
                )
                return [row[0] for row in cursor.fetchall()]
        finally:
            connection.close()

    def fix_schema_mapping(self, thing_uuid: str, correct_schema: str) -> bool:
        """
        Fix schema mapping for a specific thing.

        Args:
            thing_uuid: Thing UUID
            correct_schema: Correct schema name (e.g., "user_myproject")

        Returns:
            True if updated
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE public.schema_thing_mapping SET schema = %s WHERE thing_uuid = %s",
                    (correct_schema, thing_uuid),
                )
                connection.commit()
                return cursor.rowcount > 0
        finally:
            connection.close()

    def fix_all_schema_mappings(self) -> Tuple[int, List[str]]:
        """
        Fix all incorrect schema mappings.

        Converts project_* schema names to user_* format.

        Returns:
            Tuple of (fixed_count, list of fixed UUIDs)
        """
        connection = self._get_connection()
        fixed_uuids = []

        try:
            with connection.cursor() as cursor:
                # Get current mappings
                cursor.execute(
                    "SELECT schema, thing_uuid FROM public.schema_thing_mapping"
                )
                mappings = cursor.fetchall()

                # Get actual schemas
                cursor.execute(
                    """
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name LIKE 'user_%'
                """
                )
                actual_schemas = {row[0] for row in cursor.fetchall()}

                for schema, thing_uuid in mappings:
                    if schema.startswith("project_") and schema not in actual_schemas:
                        # Convert project_myproject_1 -> user_myproject
                        parts = schema.split("_")
                        if len(parts) >= 2:
                            # Remove 'project_' prefix and '_N' suffix
                            project_name = (
                                "_".join(parts[1:-1]) if len(parts) > 2 else parts[1]
                            )
                            new_schema = f"user_{project_name}"

                            if new_schema in actual_schemas:
                                cursor.execute(
                                    "UPDATE public.schema_thing_mapping SET schema = %s WHERE thing_uuid = %s",
                                    (new_schema, thing_uuid),
                                )
                                fixed_uuids.append(thing_uuid)
                                logger.info(
                                    f"Fixed mapping: {thing_uuid} -> {new_schema}"
                                )

                connection.commit()
                return len(fixed_uuids), fixed_uuids

        finally:
            connection.close()

    # ========== FROST Views ==========

    def check_frost_views_exist(self, schema: str) -> bool:
        """
        Check if FROST views exist for a schema.

        Args:
            schema: Schema name (e.g., "user_myproject")

        Returns:
            True if all required views exist
        """
        required_views = ["OBSERVATIONS", "DATASTREAMS", "THINGS"]
        connection = self._get_connection()

        try:
            with connection.cursor() as cursor:
                for view in required_views:
                    cursor.execute(
                        """
                        SELECT 1 FROM information_schema.views
                        WHERE table_schema = %s AND table_name = %s
                    """,
                        (schema, view),
                    )
                    if not cursor.fetchone():
                        return False
                return True
        finally:
            connection.close()

    def create_frost_views(self, schema: str) -> bool:
        """
        Create FROST-compatible views for a schema.

        Creates OBSERVATIONS, DATASTREAMS, THINGS views with proper
        uppercase column names and MULTI_DATASTREAM_ID column.

        Args:
            schema: Schema name (e.g., "user_myproject")

        Returns:
            True if created successfully
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Check if schema exists (handled by subsequent operations or exception)
                logger.info(f"Creating FROST views for schema '{schema}'")

                # Use sql module for proper identifier escaping
                schema_id = sql.Identifier(schema)

                # OBSERVATIONS view
                cursor.execute(
                    sql.SQL(
                        """
                    DROP VIEW IF EXISTS {schema}."OBSERVATIONS" CASCADE;
                    CREATE VIEW {schema}."OBSERVATIONS" AS
                    SELECT
                        o.id AS "ID",
                        o.phenomenon_time_start AS "PHENOMENON_TIME_START",
                        o.phenomenon_time_end AS "PHENOMENON_TIME_END",
                        o.result_time AS "RESULT_TIME",
                        o.result_type AS "RESULT_TYPE",
                        o.result_number AS "RESULT_NUMBER",
                        o.result_string AS "RESULT_STRING",
                        o.result_json AS "RESULT_JSON",
                        o.result_boolean AS "RESULT_BOOLEAN",
                        o.result_quality AS "RESULT_QUALITY",
                        o.valid_time_start AS "VALID_TIME_START",
                        o.valid_time_end AS "VALID_TIME_END",
                        o.parameters AS "PARAMETERS",
                        o.datastream_id AS "DATASTREAM_ID",
                        NULL::bigint AS "MULTI_DATASTREAM_ID",
                        NULL::bigint AS "FEATURE_ID"
                    FROM {schema}.observation o
                """
                    ).format(schema=schema_id)
                )

                # DATASTREAMS view
                cursor.execute(
                    sql.SQL(
                        """
                    DROP VIEW IF EXISTS {schema}."DATASTREAMS" CASCADE;
                    CREATE VIEW {schema}."DATASTREAMS" AS
                    SELECT
                        d.id AS "ID",
                        d.name AS "NAME",
                        d.name AS "DESCRIPTION",
                        'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement' AS "OBSERVATION_TYPE",
                        d.properties AS "PROPERTIES",
                        NULL::geometry AS "OBSERVED_AREA",
                        NULL::timestamptz AS "PHENOMENON_TIME_START",
                        NULL::timestamptz AS "PHENOMENON_TIME_END",
                        NULL::timestamptz AS "RESULT_TIME_START",
                        NULL::timestamptz AS "RESULT_TIME_END",
                        d.thing_id AS "THING_ID",
                        NULL::bigint AS "SENSOR_ID",
                        NULL::bigint AS "OBS_PROPERTY_ID"
                    FROM {schema}.datastream d
                """
                    ).format(schema=schema_id)
                )

                # THINGS view
                cursor.execute(
                    sql.SQL(
                        """
                    DROP VIEW IF EXISTS {schema}."THINGS" CASCADE;
                    CREATE VIEW {schema}."THINGS" AS
                    SELECT
                        t.id AS "ID",
                        t.name AS "NAME",
                        t.description AS "DESCRIPTION",
                        t.properties AS "PROPERTIES",
                        t.uuid AS "UUID"
                    FROM {schema}.thing t
                """
                    ).format(schema=schema_id)
                )

                # LOCATIONS view
                cursor.execute(
                    sql.SQL(
                        """
                    DROP VIEW IF EXISTS {schema}."LOCATIONS" CASCADE;
                    CREATE VIEW {schema}."LOCATIONS" AS
                    SELECT
                        t.id AS "ID",
                        t.name AS "NAME",
                        'Location of ' || t.name AS "DESCRIPTION",
                        'application/vnd.geo+json'::varchar AS "ENCODING_TYPE",
                        COALESCE(t.properties->'location', '{"type": "Point", "coordinates": [0,0]}'::jsonb) AS "LOCATION",
                        CASE 
                            WHEN jsonb_typeof(t.properties) = 'object' THEN t.properties
                            WHEN t.properties IS NULL THEN '{}'::jsonb
                            ELSE jsonb_build_object('wrapped_value', t.properties)
                        END AS "PROPERTIES",
                        ST_GeomFromGeoJSON(COALESCE(t.properties->'location', '{"type": "Point", "coordinates": [0,0]}'::jsonb)::text) AS "GEOM"
                    FROM {schema}.thing t
                """
                    ).format(schema=schema_id)
                )

                # THINGS_LOCATIONS view
                cursor.execute(
                    sql.SQL(
                        """
                    DROP VIEW IF EXISTS {schema}."THINGS_LOCATIONS" CASCADE;
                    CREATE VIEW {schema}."THINGS_LOCATIONS" AS
                    SELECT
                        t.id AS "THING_ID",
                        t.id AS "LOCATION_ID"
                    FROM {schema}.thing t
                """
                    ).format(schema=schema_id)
                )

                # Grant permissions
                cursor.execute(
                    sql.SQL('GRANT SELECT ON {schema}."OBSERVATIONS" TO PUBLIC').format(
                        schema=schema_id
                    )
                )
                cursor.execute(
                    sql.SQL('GRANT SELECT ON {schema}."DATASTREAMS" TO PUBLIC').format(
                        schema=schema_id
                    )
                )
                cursor.execute(
                    sql.SQL('GRANT SELECT ON {schema}."THINGS" TO PUBLIC').format(
                        schema=schema_id
                    )
                )

                connection.commit()
                logger.info(f"Successfully created FROST views for '{schema}'")
                return True

        except Exception as error:
            logger.error(f"Failed to create FROST views for '{schema}': {error}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def ensure_frost_views(self, schema: str) -> bool:
        """
        Ensure FROST views exist for a schema, creating if needed.

        Args:
            schema: Schema name

        Returns:
            True if views exist or were created
        """
        if self.check_frost_views_exist(schema):
            logger.debug(f"FROST views already exist for '{schema}'")
            return True
        return self.create_frost_views(schema)

    def apply_all_fixes(self) -> Dict[str, Any]:
        """
        Apply all TimeIO fixes.

        Returns:
            Summary of fixes applied
        """
        result = {
            "schema_mappings_fixed": 0,
            "fixed_uuids": [],
            "schemas_checked": 0,
            "views_created": [],
        }

        # Fix schema mappings
        fixed_count, fixed_uuids = self.fix_all_schema_mappings()
        result["schema_mappings_fixed"] = fixed_count
        result["fixed_uuids"] = fixed_uuids

        # Ensure FROST views for all user schemas
        schemas = self.get_user_schemas()
        result["schemas_checked"] = len(schemas)

        for schema in schemas:
            if not self.check_frost_views_exist(schema):
                if self.create_frost_views(schema):
                    result["views_created"].append(schema)

        return result

    # ========== Parser Management ==========

    def _get_parser_type_id(self, type_name: str) -> int:
        """Get parser type ID from config_db."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Map 'CsvParser' to 'csvparser'
                normalized_name = type_name.lower()
                cursor.execute(
                    "SELECT id FROM config_db.file_parser_type WHERE name = %s", 
                    (normalized_name,)
                )
                row = cursor.fetchone()
                if row:
                    return row[0]
                raise ValueError(f"Parser type '{type_name}' not found")
        finally:
            connection.close()

    def create_parser(
        self, name: str, group_id: str, settings: Dict[str, Any], type_name: str = "CsvParser"
    ) -> int:
        """
        Create a new parser configuration in config_db.
        
        Args:
            name: Parser name
            group_id: Keycloak Group ID (Project UUID)
            settings: Dictionary matching CsvParserSettings
            
        Returns:
            New Parser ID
        """
        import uuid as uuid_pkg
        
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # 1. Get Type ID
                type_id = self._get_parser_type_id(type_name)

                # 2. Generate UUID using same logic as configdb.py (Name + ProjectUUID)
                # uuid5(NAMESPACE_DNS, f"{parser['name']}{proj_uuid}")
                # We assume group_id IS the project_uuid (V3 convention)
                uuid_base = f"{name}{group_id}"
                parser_uuid = str(uuid_pkg.uuid5(uuid_pkg.NAMESPACE_DNS, uuid_base))

                # 3. Prepare Params JSON
                # Map Schema fields to ConfigDB Params
                # Schema: delimiter, exclude_headlines, exclude_footlines, timestamp_columns
                # ConfigDB: delimiter, skiprows, skipfooter, timestamp_columns
                # Note: configdb.py mapping:
                # "skiprows": exclude_headlines
                # "skipfooter": exclude_footlines
                params = {
                    "delimiter": settings.get("delimiter", ","),
                    "skiprows": settings.get("exclude_headlines", 0),
                    "skipfooter": settings.get("exclude_footlines", 0),
                    "timestamp_columns": settings.get("timestamp_columns", []),
                    "header": 0, # Default to 0 based on create_thing_config example
                    "comment": "#",
                }
                if settings.get("pandas_read_csv"):
                    params["pandas_read_csv"] = settings.get("pandas_read_csv")
                
                # 4. Upsert Parser (Manual check due to missing UNIQUE constraint on uuid)
                cursor.execute("SELECT id FROM config_db.file_parser WHERE uuid = %s", (parser_uuid,))
                existing_row = cursor.fetchone()

                if existing_row:
                    parser_id = existing_row[0]
                    cursor.execute(
                        """
                        UPDATE config_db.file_parser
                        SET name = %s, params = %s, file_parser_type_id = %s
                        WHERE id = %s
                        """,
                        (name, json.dumps(params), type_id, parser_id)
                    )
                    logger.info(f"Updated existing parser '{name}' (ID: {parser_id})")
                else:
                    cursor.execute(
                        """
                        INSERT INTO config_db.file_parser (file_parser_type_id, name, params, uuid)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                        """,
                        (type_id, name, json.dumps(params), parser_uuid)
                    )
                    parser_id = cursor.fetchone()[0]
                    logger.info(f"Created new parser '{name}' (ID: {parser_id})")
                
                connection.commit()
                logger.info(f"Created parser '{name}' (ID: {parser_id}) for project {group_id}")
                return parser_id
                
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create parser: {e}")
            raise
        finally:
            connection.close()

    def get_parsers_by_group(self, group_id: str) -> List[Dict[str, Any]]:
        """
        Get all parsers associated with a project (via UUID matching).
        """
        # Since parsers are stored with a UUID derived from ProjectUUID, we technically can't query by Project ID easily
        # unless we parse all UUIDs?
        # NO, `file_parser` table does NOT have `project_id`.
        # It ONLY has `uuid`.
        # And the UUID is `uuid5(NAMESPACE_DNS, name + group_id)`.
        # This makes it hard to "List all parsers for a group" efficiently without a reverse lookup or extra table.
        # BUT, `config_db.s3_store` links `file_parser_id`.
        # And `thing` links `s3_store` which links `project`.
        # So we can find parsers CURRENTLY USED by things in the project.
        # But "Unused" parsers created via API? We can't easily find them if we rely only on the UUID hash.
        # Wait, create_thing_config: `uuid_base = f"{parser['name']}{proj_uuid}"`.
        # If we list ALL parsers, we can't tell which ones belong to which project easily.
        
        # User requirement: "input should be our water-dp-api project_uuid which should be used to resolve group".
        # If I can't filter by group in DB, I return ALL?
        # Or I add a column/table?
        # I can't change schema easily (migrations).
        
        # Alternative: We store the project_id in `params`?
        
        # For now, I will list ALL parsers and filter in Python if possible,
        # OR just list logic:
        # User can only see parsers linked to Things in their project?
        # The user wants to CREATE a parser then Link it.
        # If I return all parsers, it might be messy.
        
        # Let's verify `file_parser` columns again. Maybe I missed `project_id`?
        # configdb.py: columns=["file_parser_type_id", "name", "params", "uuid"]
        # No project_id.
        
        # WORKAROUND:
        # Query `file_parser`.
        # For each parser, try to match UUID check against `uuid5(name + group_id)`.
        # If match, it belongs to this group.
        # This is CPU intensive if many parsers, but for current scale likely fine.
        
        import uuid as uuid_pkg
        
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT 
                        fp.id, fp.name, fp.params, fp.uuid, fpt.name as type
                    FROM config_db.file_parser fp
                    JOIN config_db.file_parser_type fpt ON fp.file_parser_type_id = fpt.id
                    """
                )
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    # Check if this parser belongs to the requested group
                    # Reconstruct UUID
                    if group_id:
                        expected_uuid = str(uuid_pkg.uuid5(uuid_pkg.NAMESPACE_DNS, f"{row['name']}{group_id}"))
                        if expected_uuid != str(row["uuid"]):
                            continue # Skip if uuid doesn't match this group derivation
                    
                    # Map params back to settings schema
                    params = row["params"] if isinstance(row["params"], dict) else json.loads(row["params"])
                    
                    results.append({
                        "id": row["id"],
                        "name": row["name"],
                        "group_id": group_id, # Can't know for sure if no group_id passed, but here we filtered
                        "type": "CsvParser" if row["type"] == "csvparser" else row["type"],
                        "settings": {
                            "delimiter": params.get("delimiter"),
                            "exclude_headlines": params.get("skiprows"),
                            "exclude_footlines": params.get("skipfooter"),
                            "timestamp_columns": params.get("timestamp_columns"),
                            "pandas_read_csv": params.get("pandas_read_csv")
                        }
                    })
                return results
        finally:
            connection.close()

    def link_thing_to_parser(self, thing_uuid: str, parser_id: int) -> bool:
        """
        Link a Thing's S3 Store to a Parser in config_db.
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # 1. Find Thing ID and S3 Store ID from config_db.thing
                cursor.execute(
                    "SELECT s3_store_id FROM config_db.thing WHERE uuid = %s",
                    (thing_uuid,)
                )
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Thing {thing_uuid} not found in config_db")
                    return False
                s3_store_id = row[0]
                
                if not s3_store_id:
                     logger.warning(f"Thing {thing_uuid} has no S3 store")
                     return False

                # 2. Update S3 Store with new parser_id
                cursor.execute(
                    "UPDATE config_db.s3_store SET file_parser_id = %s WHERE id = %s",
                    (parser_id, s3_store_id)
                )
                connection.commit()
                logger.info(f"Linked thing {thing_uuid} (S3 Store {s3_store_id}) to parser {parser_id}")
                return True
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to link thing {thing_uuid} to parser {parser_id}: {e}")
            return False
        finally:
            connection.close()

    # Deprecated / Revert helpers
    # def create_legacy_thing... (Removed as we pivot to config_db)
    
    # ========== Schema Management ==========


    def resolve_project_name_by_group_id(self, group_uuid: str) -> Optional[str]:
        """
        Resolve human-readable project name from thing_management_db using the group UUID.
        This ensures compatibility with projects created by the legacy UI.
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Query thing_management_db.project which maps UUID (auth group) to Name
                # This query will always return empty since authorization_provider_group_id is always empty.
                cursor.execute(
                    """
                    SELECT name FROM thing_management_db.project
                    WHERE authorization_provider_group_id = %s
                """,
                    (group_uuid,),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.warning(
                f"Failed to resolve project name from group ID {group_uuid}: {error}"
            )
            return None
        finally:
            connection.close()

    def get_tsm_db_schema_by_name(self, name: str) -> Optional[str]:
        """Fetch project schema by name from thing_management_db."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT d.db_schema
                    FROM thing_management_db.project p
                    JOIN thing_management_db.database d ON p.database_id = d.id
                    WHERE p.name = %s
                """
                cursor.execute(query, (name,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            # If table doesn't exist (fresh DB), just return None
            logger.debug(f"Failed to fetch TSM project by name '{name}': {error}")
            return None
        finally:
            connection.close()

    def get_thing_config_by_uuid(self, thing_uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch MQTT credentials for a thing by UUID from ConfigDB."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT m."user", m.password
                    FROM config_db.thing t
                    JOIN config_db.mqtt m ON t.mqtt_id = m.id
                    WHERE t.uuid = %s
                """
                cursor.execute(query, (thing_uuid,))
                result = cursor.fetchone()
                if result:
                    return {"mqtt_user": result[0], "mqtt_pass": result[1]}
                return None
        except Exception as error:
            logger.error(f"Failed to fetch thing config {thing_uuid}: {error}")
            return None
        finally:
            connection.close()

    def get_thing_configs_by_uuids(
        self, thing_uuids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch MQTT credentials for multiple things by UUIDs from ConfigDB."""
        if not thing_uuids:
            return {}

        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                query = """
                    SELECT t.uuid, m."user", m.password
                    FROM config_db.thing t
                    JOIN config_db.mqtt m ON t.mqtt_id = m.id
                    WHERE t.uuid = ANY(%s::uuid[])
                 """
                cursor.execute(query, (thing_uuids,))
                rows = cursor.fetchall()
                results = {}
                for row in rows:
                    # row[0] is UUID object, stringify for dictionary key safety
                    results[str(row[0])] = {"mqtt_user": row[1], "mqtt_pass": row[2]}
                return results
        except Exception as error:
            logger.error(f"Failed to fetch thing configs batch: {error}")
            return {}
        finally:
            connection.close()

    def get_active_simulations(self) -> List[Dict[str, Any]]:
        """Fetch all things that have simulation configuration."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                # We assume simulation config is stored in properties->'simulation_config'
                # returning uuid, mqtt credentials, and the config itself
                query = """
                    SELECT t.uuid, t.name, t.mqtt_user, t.mqtt_pass, t.properties->'simulation_config'
                    FROM config_db.thing t
                    WHERE t.properties ? 'simulation_config'
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    results.append(
                        {
                            "uuid": row[0],
                            "name": row[1],
                            "mqtt_user": row[2],
                            "mqtt_pass": row[3],
                            "config": row[4],
                        }
                    )
                return results
        except Exception as error:
            logger.error(f"Failed to fetch active simulations: {error}")
            return []
        finally:
            connection.close()

    def get_config_project_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Fetch project details (id, db_schema) by name from config_db."""
        connection = (
            self._get_admin_connection()
        )  # Use admin connection for cross-schema safety or config_db lookup
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # We need to JOIN project and database tables to get the schema
                query = """
                    SELECT p.id, d.schema as db_schema, p.uuid
                    FROM config_db.project p
                    JOIN config_db.database d ON p.database_id = d.id
                    WHERE p.name = %s
                """
                cursor.execute(query, (name,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as error:
            logger.error(f"Failed to fetch config project by name '{name}': {error}")
            return None
        finally:
            connection.close()

    def find_project_schema(self, project_slug: str) -> Optional[str]:
        """
        Find existing project schema matching project_{slug}_{N}.

        Args:
            project_slug: Project slug (e.g., "myproject")

        Returns:
            Schema name if found, None otherwise
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Look for project_{slug}_% pattern
                pattern = f"project_{project_slug}_%"
                cursor.execute(
                    """
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name LIKE %s
                    ORDER BY schema_name
                    LIMIT 1
                """,
                    (pattern,),
                )
                row = cursor.fetchone()
                return row[0] if row else None
        finally:
            connection.close()

    def clone_schema_structure(self, source_schema: str, target_schema: str) -> bool:
        """
        Clone table structure from source schema to target schema.

        Creates the target schema and copies table definitions (no data).
        Source is always 'user_myproject' as the template.

        Args:
            source_schema: Source schema name (e.g., "user_myproject")
            target_schema: Target schema name (e.g., "project_myproject_1")

        Returns:
            True if successful
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                source_id = sql.Identifier(source_schema)
                target_id = sql.Identifier(target_schema)

                # Create target schema if not exists
                cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(target_id))
                logger.info(f"Created schema '{target_schema}'")

                # Get list of tables from source schema
                cursor.execute(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """,
                    (source_schema,),
                )
                tables = [row[0] for row in cursor.fetchall()]

                logger.info(
                    f"Found {len(tables)} tables in source schema '{source_schema}': {tables}"
                )

                if not tables:
                    logger.warning(
                        f"No tables found in source schema '{source_schema}'. Check if schema exists."
                    )
                    # Try to list all schemas to debug
                    cursor.execute(
                        "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'user_%'"
                    )
                    user_schemas = [row[0] for row in cursor.fetchall()]
                    logger.info(f"Available user schemas: {user_schemas}")
                    connection.commit()
                    return True  # Schema created, but no tables to clone

                # Clone each table structure
                for table in tables:
                    table_id = sql.Identifier(table)
                    try:
                        cursor.execute(
                            sql.SQL(
                                """
                            CREATE TABLE IF NOT EXISTS {target}.{table}
                            (LIKE {source}.{table} INCLUDING ALL)
                        """
                            ).format(target=target_id, source=source_id, table=table_id)
                        )
                        logger.info(
                            f"Cloned table '{source_schema}.{table}' -> '{target_schema}.{table}'"
                        )
                    except Exception as table_error:
                        logger.error(f"Failed to clone table '{table}': {table_error}")
                        raise

                # Grant permissions to PUBLIC for read access
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(target_id)
                )
                for table in tables:
                    table_id = sql.Identifier(table)
                    cursor.execute(
                        sql.SQL("GRANT SELECT ON {schema}.{table} TO PUBLIC").format(
                            schema=target_id, table=table_id
                        )
                    )

                connection.commit()
                logger.info(
                    f"Successfully cloned schema structure from '{source_schema}' to '{target_schema}'"
                )
                return True

        except Exception as error:
            logger.error(f"Failed to clone schema: {error}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def get_next_project_schema_number(self, project_slug: str) -> int:
        """
        Get the next available project schema number.

        Args:
            project_slug: Project slug

        Returns:
            Next available number (1 if none exist)
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                pattern = f"project_{project_slug}_%"
                cursor.execute(
                    """
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name LIKE %s
                """,
                    (pattern,),
                )
                schemas = [row[0] for row in cursor.fetchall()]

                if not schemas:
                    return 1

                # Extract numbers and find max
                numbers = []
                for schema_name in schemas:
                    parts = schema_name.split("_")
                    if parts and parts[-1].isdigit():
                        numbers.append(int(parts[-1]))

                return max(numbers) + 1 if numbers else 1
        finally:
            connection.close()

    def create_legacy_thing(self, uuid: str, name: str, description: str = "") -> Optional[int]:
        """
        Create a Thing record in the legacy tsm_thing table.
        Required for associating Parsers.
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Check if exists first
                cursor.execute("SELECT id FROM public.tsm_thing WHERE thing_id = %s", (uuid,))
                row = cursor.fetchone()
                if row:
                    return row[0]

                # Create
                # We default group to 1 (admins/public) because we lack the legacy auth group mapping
                # datasource_type='MQTT' or custom? 
                cursor.execute(
                    """
                    INSERT INTO public.tsm_thing 
                    (thing_id, name, description, "group", datasource_type)
                    VALUES (%s, %s, %s, 1, 'MQTT')
                    RETURNING id
                    """,
                    (uuid, name, description)
                )
                thing_id = cursor.fetchone()[0]
                connection.commit()
                return thing_id
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create legacy thing {uuid}: {e}")
            return None
        finally:
            connection.close()
            
    # ========== User Management ==========

    def check_user_exists(self, username: str) -> bool:
        """Check if a database user exists."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
                return bool(cursor.fetchone())
        finally:
            connection.close()

    def create_user(self, username: str, password: str) -> bool:
        """Create a database user."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
                        sql.Identifier(username)
                    ),
                    (password,),
                )
                connection.commit()
                logger.info(f"Created database user: {username}")
                return True
        except Exception as error:
            logger.error(f"Failed to create user {username}: {error}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def grant_schema_access(
        self, schema: str, username: str, write: bool = False
    ) -> bool:
        """
        Grant access to a schema for a user.

        Args:
            schema: Schema name
            username: Database username
            write: If True, grant INSERT/UPDATE/DELETE; otherwise just SELECT

        Returns:
            True if successful
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                schema_id = sql.Identifier(schema)
                user_id = sql.Identifier(username)

                # Grant USAGE on schema
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema_id, user_id)
                )

                if write:
                    # Grant full DML
                    cursor.execute(
                        sql.SQL(
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}"
                        ).format(schema_id, user_id)
                    )
                    cursor.execute(
                        sql.SQL(
                            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
                        ).format(schema_id, user_id)
                    )
                else:
                    # Grant read-only
                    cursor.execute(
                        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                            schema_id, user_id
                        )
                    )
                    cursor.execute(
                        sql.SQL(
                            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
                        ).format(schema_id, user_id)
                    )

                connection.commit()
                logger.info(
                    f"Granted {'write' if write else 'read'} access on {schema} to {username}"
                )
                return True
        except Exception as error:
            logger.error(f"Failed to grant access: {error}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def ensure_frost_user(self, schema: str, ro_user: str, ro_password: str) -> bool:
        """
        Ensure FROST read-only user exists and has access.

        Args:
            schema: Target schema (e.g., project_myproject_1)
            ro_user: Read-only user name
            ro_password: Password to set (if creating)

        Returns:
            True if successful
        """
        try:
            # Create user if not exists
            if not self.check_user_exists(ro_user):
                if not self.create_user(ro_user, ro_password):
                    return False

            # Grant read access
            return self.grant_schema_access(schema, ro_user, write=False)

        except Exception as error:
            logger.error(f"Error ensuring FROST user: {error}")
            return False

    # ========== ConfigDB Management (v3) ==========

    def _get_admin_connection(self):
        """Get an admin connection to the system database."""
        return psycopg2.connect(
            host=self._db_host,
            port=self._db_port,
            user=settings.timeio_db_user,
            password=settings.timeio_db_password,
            database=settings.timeio_db_name,
        )

    def get_config_id(self, schema_table: str, name: str) -> Optional[int]:
        """Get ID of a record by name in config_db."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT id FROM {table} WHERE name = %s").format(
                        table=sql.Identifier("config_db", schema_table)
                    ),
                    [name],
                )
                result = cursor.fetchone()
                return result[0] if result else None
        finally:
            connection.close()

    def get_or_create_config_project(
        self,
        uuid: str,
        name: str,
        db_schema: str,
        db_user: str,
        db_pass: str,
        ro_user: str,
        ro_pass: str,
    ) -> int:
        """Ensure project exists in config_db. Returns project_id."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                # 1. Check if project exists by UUID
                cursor.execute("SELECT id FROM config_db.project WHERE uuid = %s", [uuid])
                result = cursor.fetchone()
                if result:
                    return result[0]

                # 2. Check if project exists by NAME (prevent duplicate "MyProject")
                cursor.execute(
                    "SELECT id, uuid FROM config_db.project WHERE name = %s", [name]
                )
                result = cursor.fetchone()
                if result:
                    existing_id, existing_uuid = result
                    logger.info(
                        f"Project '{name}' exists with UUID {existing_uuid}. Reusing ID {existing_id} instead of creating new with UUID {uuid}."
                    )
                    return existing_id

                # 3. Create database entry
                # We also need to populate 'url' and 'ro_url' for FROST context generation
                db_host_internal = "database"  # Workers connect to this host
                db_url = f"postgresql://{db_host_internal}:5432/postgres"
                # ro_url logic might vary, but for now reuse same host
                ro_url = f"postgresql://{db_host_internal}:5432/postgres"

                cursor.execute(
                    """
                    INSERT INTO config_db.database (schema, "user", password, ro_user, ro_password, url, ro_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """,
                    [
                        db_schema,
                        db_user,
                        encrypt_password(db_pass),
                        ro_user,
                        encrypt_password(ro_pass),
                        db_url,
                        ro_url,
                    ],
                )
                db_id = cursor.fetchone()[0]

                # 4. Create project entry
                cursor.execute(
                    "INSERT INTO config_db.project (name, uuid, database_id) VALUES (%s, %s, %s) RETURNING id",
                    [name, uuid, db_id],
                )
                project_id = cursor.fetchone()[0]
                connection.commit()
                return project_id
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to create config project: {error}")
            raise
        finally:
            connection.close()

    def create_thing_config(
        self,
        uuid: str,
        name: str,
        project_id: int,
        mqtt_user: str,
        mqtt_pass: str,
        description: str = "",
        properties: Optional[Dict[str, Any]] = None,
        mqtt_device_type_name: str = "chirpstack_generic",
    ) -> Dict[str, Any]:
        """Create thing and related records in config_db."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                # 1. Get constant IDs
                cursor.execute("SELECT id FROM config_db.ingest_type WHERE name = 'mqtt'")
                ingest_type_id = cursor.fetchone()[0]

                # Ensure mqtt_device_type exists
                cursor.execute(
                    "SELECT id FROM config_db.mqtt_device_type WHERE name = %s",
                    [mqtt_device_type_name],
                )
                result = cursor.fetchone()
                if not result:
                    cursor.execute(
                        "INSERT INTO config_db.mqtt_device_type (name) VALUES (%s) RETURNING id",
                        [mqtt_device_type_name],
                    )
                    device_type_id = cursor.fetchone()[0]
                else:
                    device_type_id = result[0]

                # 2. Create MQTT entry
                cursor.execute(
                    """
                    INSERT INTO config_db.mqtt ("user", password, password_hashed, topic, mqtt_device_type_id)
                    VALUES (%s, %s, %s, %s, %s) RETURNING id
                    """,
                    [
                        mqtt_user,
                        encrypt_password(mqtt_pass),
                        mqtt_pass,
                        f"mqtt_ingest/{mqtt_user}/data",
                        device_type_id,
                    ],
                )
                mqtt_id = cursor.fetchone()[0]

                # 3. Create S3 Store entry (dummy parser)
                cursor.execute(
                    "SELECT id FROM config_db.file_parser_type WHERE name = 'csvparser'"
                )
                parser_type_id = cursor.fetchone()[0]
                # Default CSV Parser Parameters
                default_params = json.dumps(
                    {
                        "timestamp_columns": [
                            {"column": 0, "format": "%Y-%m-%dT%H:%M:%S.%fZ"}
                        ],
                        "delimiter": ",",
                        "header": 0,
                        "comment": "#",
                    }
                )
                cursor.execute(
                    "INSERT INTO config_db.file_parser (file_parser_type_id, name, params) VALUES (%s, %s, %s) RETURNING id",
                    [parser_type_id, f"parser_{uuid}", default_params],
                )
                parser_id = cursor.fetchone()[0]
                # Sanitize bucket name (S3/MinIO does not allow underscores)
                bucket_name = mqtt_user.replace("_", "-")
                cursor.execute(
                    'INSERT INTO config_db.s3_store ("user", password, bucket, file_parser_id, filename_pattern) VALUES (%s, %s, %s, %s, %s) RETURNING id',
                    [
                        mqtt_user,
                        encrypt_password(mqtt_pass),
                        bucket_name,
                        parser_id,
                        "*",
                    ],
                )
                s3_id = cursor.fetchone()[0]

                # 4. Create Thing entry with metadata (Note: 'properties' column doesn't exist in config_db.thing)
                cursor.execute(
                    """
                    INSERT INTO config_db.thing (uuid, name, project_id, ingest_type_id, s3_store_id, mqtt_id, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """,
                    [
                        uuid,
                        name,
                        project_id,
                        ingest_type_id,
                        s3_id,
                        mqtt_id,
                        description,
                    ],
                )
                thing_id = cursor.fetchone()[0]

                connection.commit()
                return {
                    "id": thing_id,
                    "uuid": uuid,
                    "project_id": project_id,
                    "mqtt_id": mqtt_id,
                    "s3_id": s3_id,
                }
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to create thing config: {error}")
            raise
        finally:
            connection.close()

    def register_sensor_metadata(
        self, thing_uuid: str, properties: List[Dict[str, str]]
    ):
        """Register units and labels for a sensor's datastreams."""
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                for prop in properties:
                    name = prop.get("name")
                    if not name:
                        logger.warning(f"Skipping property without a name: {prop}")
                        continue

                    unit = prop.get("unit", "Unknown")
                    label = prop.get("label", name)

                    # 1. Ensure device property exists
                    cursor.execute(
                        """
                        INSERT INTO public.sms_device_property (property_name, unit_name, label)
                        VALUES (%s, %s, %s) RETURNING id
                        """,
                        [name, unit, label],
                    )
                    prop_id = cursor.fetchone()[0]

                    # 2. Link it to the thing by name
                    cursor.execute(
                        """
                        INSERT INTO public.sms_datastream_link (thing_id, datastream_name, device_property_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (thing_id, datastream_name) DO UPDATE SET
                        device_property_id = EXCLUDED.device_property_id
                        """,
                        [thing_uuid, name, prop_id],
                    )
                connection.commit()
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to register sensor metadata: {error}")
            raise
        finally:
            connection.close()

    # ========== Utility ==========

    def health_check(self) -> bool:
        """Check if TimeIO database is accessible."""
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            connection.close()
            return True
        except Exception:
            return False

    def get_thing_schema(self, thing_uuid: str) -> Optional[str]:
        """
        Get the schema for a thing.

        Args:
            thing_uuid: Thing UUID

        Returns:
            Schema name or None
        """
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT schema FROM public.schema_thing_mapping WHERE thing_uuid = %s",
                    (thing_uuid,),
                )
                row = cursor.fetchone()
                return row[0] if row else None
        finally:
            connection.close()

    def update_thing_properties(
        self, thing_uuid: str, properties: Dict[str, Any]
    ) -> bool:
        """Update thing properties directly in the database."""
        schema = self.get_thing_schema(thing_uuid)
        if not schema:
            logger.error(f"Could not find schema for thing {thing_uuid}")
            return False

        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Use jsonb concatenation to merge properties
                query = sql.SQL(
                    "UPDATE {schema}.thing SET properties = properties || %s::jsonb WHERE uuid = %s"
                ).format(schema=sql.Identifier(schema))

                cursor.execute(query, [json.dumps(properties), thing_uuid])
                connection.commit()
                return True
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to update thing properties: {error}")
            return False
        finally:
            connection.close()

    def upsert_thing_to_project_db(
        self,
        schema: str,
        uuid: str,
        name: str,
        description: str = "",
        properties: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Ensure a thing exists in the project-specific database schema. Returns database ID."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.thing (name, uuid, description, properties)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (uuid) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        properties = EXCLUDED.properties
                        RETURNING id
                        """
                    ).format(schema=sql.Identifier(schema)),
                    [name, uuid, description, json.dumps(properties or {})],
                )
                result = cursor.fetchone()
                thing_id = result[0] if result else None
                connection.commit()
                return thing_id
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to upsert thing to project db: {error}")
            return None
        finally:
            connection.close()

    def get_thing_id_in_project_db(self, schema: str, thing_uuid: str) -> Optional[int]:
        """Get database ID of a thing in project DB. Returns None if not found or schema missing."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # Check if schema exists first to avoid error spam in logs?
                # No, standard query will just raise Exception which caller catches.
                cursor.execute(
                    sql.SQL("SELECT id FROM {schema}.thing WHERE uuid = %s").format(
                        schema=sql.Identifier(schema)
                    ),
                    [thing_uuid],
                )
                result = cursor.fetchone()
                return result[0] if result else None
        finally:
            connection.close()

    def ensure_datastreams_in_project_db(
        self, schema: str, thing_uuid: str, properties: List[Dict[str, str]]
    ):
        """Pre-create datastream entries in the project database for each property."""
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                # 1. Get thing ID in the project schema
                cursor.execute(
                    sql.SQL("SELECT id FROM {schema}.thing WHERE uuid = %s").format(
                        schema=sql.Identifier(schema)
                    ),
                    [thing_uuid],
                )
                row = cursor.fetchone()
                if not row:
                    logger.error(f"Thing {thing_uuid} not found in schema {schema}")
                    return False
                thing_id = row[0]

                # 2. Create datastream for each property
                for prop in properties:
                    name = prop.get("name")
                    if not name:
                        continue

                    unit = prop.get("unit", "Unknown")
                    ds_props = json.dumps(
                        {
                            "unitOfMeasurement": {
                                "name": unit,
                                "symbol": unit,
                                "definition": unit,
                            }
                        }
                    )

                    cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {schema}.datastream (name, thing_id, position, properties)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (thing_id, position) DO UPDATE SET
                            properties = EXCLUDED.properties
                            """
                        ).format(schema=sql.Identifier(schema)),
                        [name, thing_id, name, ds_props],
                    )

                connection.commit()
                return True
        except Exception as error:
            connection.rollback()
            logger.error(f"Failed to ensure datastreams in project db: {error}")
            return False
        finally:
            connection.close()

    def get_sensors_by_uuids(
        self, schema: str, uuids: List[str], skip: int = 0, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch basic metadata for a specific set of sensors in a project schema."""
        if not uuids:
            return []

        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Join with SMS tables to get location if missing from properties
                query = sql.SQL(
                    """
                    SELECT
                        t.uuid,
                        t.name,
                        t.description,
                        COALESCE(
                            (t.properties->'wrapped_value'->1->'location'->'coordinates'->>1)::float,
                            (t.properties->'location'->'coordinates'->>1)::float,
                            (t.properties->'location'->>'latitude')::float,
                            (loc.location->'coordinates'->>1)::float,
                            sloc.y,
                            0.0
                        ) as latitude,
                        COALESCE(
                            (t.properties->'wrapped_value'->1->'location'->'coordinates'->>0)::float,
                            (t.properties->'location'->'coordinates'->>0)::float,
                            (t.properties->'location'->>'longitude')::float,
                            (loc.location->'coordinates'->>0)::float,
                            sloc.x,
                            0.0
                        ) as longitude,
                        t.properties
                    FROM {schema}.thing t
                    LEFT JOIN {schema}.thing_location tl ON t.id = tl.thing_id
                    LEFT JOIN {schema}.location loc ON tl.location_id = loc.id
                    LEFT JOIN public.sms_datastream_link l ON t.uuid = l.thing_id
                    LEFT JOIN public.sms_device_mount_action m ON l.device_mount_action_id = m.id
                    LEFT JOIN public.sms_configuration_static_location_begin_action sloc ON m.configuration_id = sloc.configuration_id
                    WHERE t.uuid = ANY(%s::uuid[])
                    GROUP BY t.uuid, t.name, t.description, t.properties, loc.location, sloc.y, sloc.x
                    ORDER BY t.name
                    OFFSET %s
                    LIMIT %s
                    """
                ).format(schema=sql.Identifier(schema))

                cursor.execute(query, [uuids, skip, limit])
                return [dict(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error(f"Failed to fetch sensors by UUIDs for schema {schema}: {error}")
            return []
        finally:
            connection.close()

    def get_all_sensors_basic(self, schema: str) -> List[Dict[str, Any]]:
        """Fetch basic metadata for all sensors in a project schema, including location from SMS joins."""
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                query = sql.SQL(
                    """
                    SELECT
                        t.uuid,
                        t.name,
                        t.description,
                        COALESCE(
                            (t.properties->'location'->'coordinates'->>1)::float,
                            (t.properties->'location'->>'latitude')::float,
                            sloc.y,
                            0.0
                        ) as latitude,
                        COALESCE(
                            (t.properties->'location'->'coordinates'->>0)::float,
                            (t.properties->'location'->>'longitude')::float,
                            sloc.x,
                            0.0
                        ) as longitude,
                        t.properties
                    FROM {schema}.thing t
                    LEFT JOIN public.sms_datastream_link l ON t.uuid = l.thing_id
                    LEFT JOIN public.sms_device_mount_action m ON l.device_mount_action_id = m.id
                    LEFT JOIN public.sms_configuration_static_location_begin_action sloc ON m.configuration_id = sloc.configuration_id
                    GROUP BY t.uuid, t.name, t.description, t.properties, sloc.y, sloc.x
                    ORDER BY t.name
                    """
                ).format(schema=sql.Identifier(schema))

                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error(f"Failed to fetch all sensors basic for schema {schema}: {error}")
            return []
        finally:
            connection.close()

    def get_sensors_rich(self, schema: str) -> List[Dict[str, Any]]:
        """Fetch all sensors in a schema with human-readable datastream info."""
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Optimized query joining project schema with SMS link tables
                query = sql.SQL(
                    """
                    SELECT
                        t.uuid as thing_uuid,
                        t.name as thing_name,
                        t.description as thing_description,
                        t.properties as thing_properties,
                        loc.location as location_json,
                        d.name as ds_name,
                        d.properties as ds_properties,
                        dp.unit_name as unit,
                        dp.label as property_label,
                        dma.label as action_label
                    FROM {schema}.thing t
                    LEFT JOIN {schema}.thing_location tl ON t.id = tl.thing_id
                    LEFT JOIN {schema}.location loc ON tl.location_id = loc.id
                    LEFT JOIN {schema}.datastream d ON t.id = d.thing_id
                    LEFT JOIN public.sms_datastream_link sdl ON t.uuid = sdl.thing_id AND d.name = sdl.datastream_name
                    LEFT JOIN public.sms_device_property dp ON sdl.device_property_id = dp.id
                    LEFT JOIN public.sms_device_mount_action dma ON sdl.device_mount_action_id = dma.id
                    ORDER BY t.name, d.name
                    """
                ).format(schema=sql.Identifier(schema))

                cursor.execute(query)
                rows = cursor.fetchall()

                # Group by thing
                things = {}
                for row in rows:
                    t_uuid = str(row["thing_uuid"])
                    if t_uuid not in things:
                        # Extract location from properties (GeoJSON or Legacy)
                        props = row["thing_properties"] or {}
                        # Ensure props is valid
                        props = row["thing_properties"]
                        
                        # 1. Handle wrapped_value dict
                        if isinstance(props, dict) and "wrapped_value" in props:
                            props = props["wrapped_value"]
                        
                        # 2. Handle List properties (e.g. [null, dict]) from View or Wrapped
                        if isinstance(props, list):
                            found_dict = {}
                            for item in props:
                                if isinstance(item, dict):
                                    found_dict = item
                                    # Usually the last valid dict contains our location
                            props = found_dict
                        
                        # 3. Final safety check
                        if not isinstance(props, dict):
                            props = {}

                        # Try props
                        loc_props = props.get("location", {})
                        if not isinstance(loc_props, dict):
                            loc_props = {}
                            
                        lat = 0.0
                        lon = 0.0
                        
                        # Try FROST Location Table (Fallback)
                        loc_json = row["location_json"] or {}

                        if isinstance(loc_json, dict) and loc_json.get("type") == "Point" and loc_json.get("coordinates"):
                             lon, lat = loc_json["coordinates"]
                        elif isinstance(loc_props, dict) and loc_props.get("type") == "Point" and loc_props.get("coordinates"):
                            # GeoJSON in props
                            lon, lat = loc_props["coordinates"]
                        else:
                            # Legacy in props
                            lat = float(props.get("latitude") or loc_props.get("latitude") or 0.0)
                            lon = float(props.get("longitude") or loc_props.get("longitude") or 0.0)

                        things[t_uuid] = {
                            "uuid": t_uuid,
                            "name": row["thing_name"],
                            "description": row["thing_description"],
                            "properties": props,
                            "latitude": lat,
                            "longitude": lon,
                            "datastreams": [],
                        }

                    if row["ds_name"]:
                        # Determine most descriptive label
                        label = (
                            row["property_label"]
                            or row["action_label"]
                            or row["ds_name"]
                        )

                        things[t_uuid]["datastreams"].append(
                            {
                                "name": row["ds_name"],
                                "unit": row["unit"] or "Unknown",
                                "label": label,
                                "properties": row["ds_properties"],
                            }
                        )

                return list(things.values())
        except Exception as error:
            logger.error(f"Failed to fetch rich sensors for schema {schema}: {error}")
            return []
        finally:
            connection.close()

    def get_sensor_rich(self, thing_uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch a single sensor with rich metadata including location."""
        schema = self.get_thing_schema(thing_uuid)
        if not schema:
            return None

        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # 1. Fetch Thing + Datastreams
                query = sql.SQL(
                    """
                    SELECT
                        t.uuid as thing_uuid,
                        t.name as thing_name,
                        t.description as thing_description,
                        t.properties as thing_properties,
                        d.name as ds_name,
                        d.properties as ds_properties,
                        dp.unit_name as unit,
                        dp.label as property_label,
                        dma.label as action_label
                    FROM {schema}.thing t
                    LEFT JOIN {schema}.datastream d ON t.id = d.thing_id
                    LEFT JOIN public.sms_datastream_link sdl ON t.uuid = sdl.thing_id AND d.name = sdl.datastream_name
                    LEFT JOIN public.sms_device_property dp ON sdl.device_property_id = dp.id
                    LEFT JOIN public.sms_device_mount_action dma ON sdl.device_mount_action_id = dma.id
                    WHERE t.uuid = %s
                    """
                ).format(schema=sql.Identifier(schema))

                cursor.execute(query, (thing_uuid,))
                rows = cursor.fetchall()
                if not rows:
                    return None

                # Construct result
                res = {
                    "uuid": thing_uuid,
                    "name": rows[0]["thing_name"],
                    "description": rows[0]["thing_description"],
                    "properties": rows[0]["thing_properties"],
                    "datastreams": [],
                    "location": None
                }

                for row in rows:
                    if row["ds_name"]:
                         label = row["property_label"] or row["action_label"] or row["ds_name"]
                         res["datastreams"].append({
                             "name": row["ds_name"],
                             "unit": row["unit"] or "Unknown",
                             "label": label,
                             "properties": row["ds_properties"]
                         })

                # 2. Extract Location from thing_properties if available (Primary source in TimeIO)
                thing_properties = rows[0]["thing_properties"]
                if thing_properties and "location" in thing_properties:
                    loc_data = thing_properties["location"]
                    # Handle GeoJSON Point structure
                    if isinstance(loc_data, dict) and loc_data.get("type") == "Point":
                        coords = loc_data.get("coordinates", [0, 0])
                        res["location"] = {
                            "latitude": coords[1],
                            "longitude": coords[0],
                            "encodingType": "application/vnd.geo+json"
                        }
                    # Handle flat structure (legacy fallback)
                    elif isinstance(loc_data, dict) and "latitude" in loc_data:
                        res["location"] = {
                            "latitude": loc_data["latitude"],
                            "longitude": loc_data["longitude"],
                            "encodingType": "application/json"
                        }
                
                # Helper for flattened access
                if res["location"]:
                    res["latitude"] = res["location"]["latitude"]
                    res["longitude"] = res["location"]["longitude"]

                return res
        except Exception as error:
            logger.error(f"Failed to fetch rich sensor {thing_uuid}: {error}")
            return None
        finally:
            connection.close()

    def get_s3_config(self, thing_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Fetch S3 credentials (bucket, user, pass) for a thing.
        Returns: {bucket, user, password (plaintext), filename_pattern}
        """
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                # Join Thing -> S3 Store
                query = """
                    SELECT s.bucket, s."user", s.password, s.filename_pattern
                    FROM config_db.thing t
                    JOIN config_db.s3_store s ON t.s3_store_id = s.id
                    WHERE t.uuid = %s
                """
                cursor.execute(query, (thing_uuid,))
                result = cursor.fetchone()

                if not result:
                    return None

                bucket, user, encrypted_pass, pattern = result

                return {
                    "bucket": bucket,
                    "user": user,
                    "password": decrypt_password(encrypted_pass),
                    "filename_pattern": pattern,
                }
        except Exception as error:
            logger.error(f"Failed to fetch S3 config for {thing_uuid}: {error}")
            return None
        finally:
            connection.close()


    def get_schema_from_uuid(self, uuid: str) -> Optional[str]:
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                query = "SELECT schema FROM public.schema_thing_mapping WHERE thing_uuid = %s"
                cursor.execute(query, (uuid,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.error(f"Failed to fetch schema for {uuid}: {error}")
            return None
        finally:
            connection.close()

    def get_thing_id_from_uuid(self, uuid: str) -> Optional[str]:
        connection = self._get_admin_connection()
        schema = self.get_schema_from_uuid(uuid)
        try:
            with connection.cursor() as cursor:
                query = "SELECT id FROM {schema}.thing WHERE uuid = %s"
                cursor.execute(query.format(schema=schema), (uuid,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.error(f"Failed to fetch thing ID for {uuid}: {error}")
            return None
        finally:
            connection.close()

    def get_locations_from_project_uuid(self, project_uuid: str) -> Optional[List[Dict[str, Any]]]:
        connection = self._get_admin_connection()
        try:
            with connection.cursor() as cursor:
                query = "SELECT location FROM public.schema_thing_mapping WHERE project_uuid = %s"
                cursor.execute(query, (project_uuid,))
                result = cursor.fetchall()
                return result if result else None
        except Exception as error:
            logger.error(f"Failed to fetch locations for {project_uuid}: {error}")
            return None
        finally:
            connection.close()

    def get_thing_observations(
        self,
        schema: str,
        thing_uuid: str,
        datastream_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch observations for a thing, optionally filtered by datastream."""
        connection = self._get_connection()
        try:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Base query
                query_str = """
                    SELECT
                        o.result_time as timestamp,
                        o.result_number as value,
                        d.name as datastream_name,
                        COALESCE(dp.unit_name, u.symbol) as unit
                    FROM {schema}.observation o
                    JOIN {schema}.datastream d ON o.datastream_id = d.id
                    JOIN {schema}.thing t ON d.thing_id = t.id
                    LEFT JOIN {schema}.unit u ON d.unit_id = u.id
                    LEFT JOIN public.sms_datastream_link sdl ON t.uuid = sdl.thing_id AND d.name = sdl.datastream_name
                    LEFT JOIN public.sms_device_property dp ON sdl.device_property_id = dp.id
                    WHERE t.uuid = %s
                """
                params = [thing_uuid]

                if datastream_name:
                    query_str += " AND d.name = %s"
                    params.append(datastream_name)

                query_str += " ORDER BY o.result_time DESC LIMIT %s"
                params.append(limit)

                cursor.execute(
                    sql.SQL(query_str).format(schema=sql.Identifier(schema)), params
                )
                logger.info(f"Query: {cursor.query}")
                rows = cursor.fetchall()
                logger.info(f"Rows: {rows}")
                return [
                    {
                        "timestamp": row["timestamp"],
                        "value": row["value"],
                        "datastream": row["datastream_name"],
                        "unit": row["unit"],
                    }
                    for row in rows
                ]
        except Exception as error:
            logger.error(
                f"Failed to fetch observations for thing {thing_uuid} in {schema}: {error}"
            )
            return []
        finally:
            connection.close()
