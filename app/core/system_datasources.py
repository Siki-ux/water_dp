import logging

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.datasource import DataSource
from app.models.user_context import Project
from app.services.encryption_service import encryption_service

logger = logging.getLogger(__name__)


def register_system_datasources(db: Session):
    """
    Registers the default system datasources (backend DB, etc.) if they don't exist.
    This should run on application startup.
    """
    logger.info("Checking/Registering System Datasources...")

    # Parse primary DATABASE_URL to get details
    # Pydantic PostgresDsn usually has user, password, host, port, path
    # But settings.database_url is likely a string.
    # We can try to parse it.
    from sqlalchemy.engine.url import make_url

    try:
        url = make_url(settings.database_url)

        # We need a project to attach to.
        # Ideally, system datasources might be global?
        # But our model requires project_id.
        # For now, attach to the first available project or "System Project" if we created one.
        # Fallback: Find 'Demo Project' or first project.
        project = db.query(Project).filter(Project.name == "Demo Project").first()
        if not project:
            project = db.query(Project).first()

        if not project:
            logger.warning(
                "No projects found. Skipping System Datasource registration."
            )
            return

        system_sources = [
            {
                "name": "Water Data Platform DB (Primary)",
                "type": "POSTGRES",
                "connection_details": {
                    "host": url.host,
                    "port": url.port or 5432,
                    "database": url.database,
                    "user": url.username,
                    "password": url.password,
                },
            }
        ]

        # Helper: TimeIO (Defaults for docker stack)
        # In a real scenario, these should be in settings.
        # We'll assume standard docker names 'postgres' port 5432 for the sibling service if not configured.
        # Since we don't have TIMEIO_DB_URL in settings, we add a placeholder or hardcoded default
        # only if we are in a known dev environment?
        # User requested: "Check water_dp... Is water_dp hardcoded to use TimeIO... If so... show them"
        # Since we don't strictly have TimeIO SQL access config, we can add it manually if known.
        # Let's add it with the assumption of the standard 'configs' db for TimeIO if reachable.

        system_sources.append(
            {
                "name": "TimeIO DB (Internal)",
                "type": "TIMEIO",
                "connection_details": {
                    "host": "postgres",  # Service name in docker-compose
                    "port": 5432,
                    "database": "configs",  # Default TimeIO DB
                    "user": url.username,  # Use same user as primary
                    "password": url.password,  # Use same password as primary
                },
            }
        )

        # GeoServer DB is usually the same as Primary Water Data DB
        # (since we publish from it), so we might just alias it or leave it as Primary.
        # But if it were separate:
        # system_sources.append({...})

        for source in system_sources:
            # Encrypt password immediately for payload
            details = source["connection_details"]
            if "password" in details and details["password"]:
                details["password"] = encryption_service.encrypt(details["password"])

            exists = (
                db.query(DataSource)
                .filter(
                    DataSource.project_id == project.id,
                    DataSource.name == source["name"],
                )
                .first()
            )

            if not exists:
                logger.info(f"Registering system datasource: {source['name']}")
                ds = DataSource(
                    project_id=project.id,
                    name=source["name"],
                    type=source["type"],
                    connection_details=details,
                )
                db.add(ds)
            else:
                # Update connection details to ensure encryption key is current
                # and settings are synced.
                logger.info(f"Updating system datasource credentials: {source['name']}")
                exists.connection_details = details
                exists.type = source["type"]  # Ensure type is robust check
                # We do NOT add, just modify attached object

        db.commit()

    except Exception as e:
        logger.error(f"Failed to register system datasources: {e}")
        db.rollback()
