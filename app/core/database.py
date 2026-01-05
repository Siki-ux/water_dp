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
    try:
        # Import models to ensure they are registered with Base.metadata

        # Use checkfirst=True to avoid errors if tables already exist
        logger.info(
            f"Targeting tables for creation: {list(Base.metadata.tables.keys())}"
        )
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("Database tables initialized successfully")
    except (ProgrammingError, OperationalError) as e:
        # Handle cases where indexes or constraints already exist
        error_str = str(e).lower()
        logger.error(
            f"Database initialization exception details: {e}"
        )  # Force log the error
        if "already exists" in error_str or "duplicate" in error_str:
            logger.warning(f"Some database objects already exist (this is normal): {e}")
            logger.info("Database schema is up to date")
        else:
            logger.error(f"Database initialization error: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
