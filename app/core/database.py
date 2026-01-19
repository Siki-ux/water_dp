"""
Database configuration and session management.
"""

import logging
from typing import Generator

from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from app.core.config import settings

logger = logging.getLogger(__name__)

engine = create_engine(
    settings.database_url,
    poolclass=QueuePool,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    pool_pre_ping=True,
    echo=settings.debug,
)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


Base = declarative_base()


metadata = MetaData()


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    """
    Initialize database tables.
    Creates tables and indexes if they don't exist.
    Handles existing objects gracefully.
    """
    import time

    max_retries = 30
    retry_interval = 2

    for attempt in range(max_retries):
        try:
            # Import all models explicitly to ensure they are registered with Base.metadata
            # This is required for SQLAlchemy's declarative base to discover and create tables
            from sqlalchemy import text

            from app.models import GeoFeature, GeoLayer  # noqa: F401

            # Use checkfirst=True to avoid errors if tables already exist
            logger.info(
                f"Targeting tables for creation: {list(Base.metadata.tables.keys())}"
            )

            # Ensure PostGIS extension exists
            with engine.connect() as connection:
                connection.execute(
                    text("CREATE EXTENSION IF NOT EXISTS postgis CASCADE;")
                )
                connection.commit()

            Base.metadata.create_all(bind=engine, checkfirst=True)
            logger.info("Database tables initialized successfully")
            return  # Success
        except (ProgrammingError, OperationalError) as e:
            error_str = str(e).lower()
            if "already exists" in error_str or "duplicate" in error_str:
                logger.warning(
                    f"Some database objects already exist (this is normal): {e}"
                )
                logger.info("Database schema is up to date")
                return  # Success
            elif isinstance(e, OperationalError) and "connection refused" in error_str:
                logger.warning(
                    f"Database connection refused (Attempt {attempt + 1}/{max_retries}). Retrying in {retry_interval}s..."
                )
                time.sleep(retry_interval)
            else:
                logger.error(
                    f"Database initialization exception details: {e}"
                )  # Force log the error
                raise
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    logger.error("Max retries exceeded. Could not connect to database.")
    raise Exception("Max retries exceeded. Could not connect to database.")
