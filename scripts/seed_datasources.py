import logging
import os
import sys

# Ensure app is in path
sys.path.append(os.getcwd())

from app.core.config import settings
from app.core.database import SessionLocal
from app.models.datasource import DataSource
from app.models.user_context import Project
from app.services.encryption_service import encryption_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def seed_datasources():
    db = SessionLocal()
    try:
        logger.info("Seeding default datasources...")

        # 1. Find Demo Project (or any project to attach to)
        project = db.query(Project).filter(Project.name == "Demo Project").first()
        if not project:
            logger.warning(
                "Demo Project not found. Cannot seed datasources without a project."
            )
            # Try to find ANY project
            project = db.query(Project).first()
            if not project:
                logger.error("No projects found.")
                return

        logger.info(f"Attaching datasources to project: {project.name} ({project.id})")

        # 2. Define default datasources
        defaults = [
            {
                "name": "Local GeoServer",
                "type": "GEOSERVER",
                "connection_details": {
                    "host": "localhost",  # Or docker service name if inside docker
                    "port": 5432,
                    "database": "water_data",
                    "user": settings.geoserver_username or "postgres",
                    "password": settings.geoserver_password or "postgres",
                },
            },
            {
                "name": "TimeIO DB",
                "type": "TIMEIO",
                "connection_details": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "configs",
                    "user": os.getenv("TIMEIO_DB_USER", "postgres"),
                    "password": os.getenv("TIMEIO_DB_PASSWORD", "password"),
                },
            },
        ]

        for ds_data in defaults:
            # Check if exists
            exists = (
                db.query(DataSource)
                .filter(
                    DataSource.project_id == project.id,
                    DataSource.name == ds_data["name"],
                )
                .first()
            )

            if not exists:
                logger.info(f"Creating datasource: {ds_data['name']}")

                # Encrypt password
                details = ds_data["connection_details"]
                if "password" in details:
                    details["password"] = encryption_service.encrypt(
                        details["password"]
                    )

                ds = DataSource(
                    project_id=project.id,
                    name=ds_data["name"],
                    type=ds_data["type"],
                    connection_details=details,
                )
                db.add(ds)
            else:
                logger.info(f"Datasource already exists: {ds_data['name']}")

        db.commit()
        logger.info("Datasource seeding complete.")

    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    seed_datasources()
