import logging
import os
import sys

# Add the parent directory to sys.path to allow importing app modules
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from app.services.geoserver_service import GeoServerService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_geoserver():
    logger.info("Starting GeoServer configuration...")

    geoserver = GeoServerService()

    if not geoserver.test_connection():
        logger.error("Could not connect to GeoServer. Exiting.")
        sys.exit(1)

    workspace_name = "water_dp"
    store_name = "water_data_ts"

    # Ensure workspace exists
    logger.info(f"Ensuring workspace '{workspace_name}' exists...")
    geoserver.create_workspace(workspace_name)

    # Create Data Store for TimescaleDB (PostGIS)
    logger.info(f"Creating Data Store '{store_name}'...")

    # Database connection parameters (can be overridden via environment variables)
    connection_params = {
        "host": os.getenv("DATABASE_HOST", "postgres"),
        "port": int(os.getenv("DATABASE_PORT", "5432")),
        "database": os.getenv("DATABASE_NAME", "water_data"),
        "user": os.getenv("DATABASE_USER", "postgres"),
        "passwd": os.getenv("DATABASE_PASSWORD", "postgres"),
        "dbtype": "postgis",
        "schema": "public",
        "validateConnection": True,
        "encode functions": True,
        "Support on the fly geometry simplification": True,
        "create database": False,
        "create database params": {"encoding": "UTF-8"},
        "Expose primary keys": True,
    }

    try:
        geoserver.create_datastore(
            store_name, store_type="postgis", connection_params=connection_params
        )
        logger.info(
            f"Successfully connected GeoServer to TimescaleDB (Store: {store_name})"
        )
    except Exception as e:
        logger.error(f"Failed to create data store: {e}")
        sys.exit(1)


if __name__ == "__main__":
    configure_geoserver()
