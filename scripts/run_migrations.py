#!/usr/bin/env python3
"""
Database migration runner for the Water Data Platform.
"""
import logging
import subprocess
import sys
from pathlib import Path

# Add the app directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.logging_config import configure_logging

logger = logging.getLogger(__name__)


def run_migrations():
    """Run database migrations."""
    logger.info("Running database migrations...")

    try:
        # Run Alembic upgrade
        result = subprocess.run(
            ["alembic", "upgrade", "head"], check=True, capture_output=True, text=True
        )

        logger.info("Migrations completed successfully.")
        logger.info(f"Output: {result.stdout}")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during migration: {e}")
        return False


def create_migration(message: str):
    """Create a new migration."""
    logger.info(f"Creating migration: {message}")

    try:
        result = subprocess.run(
            ["alembic", "revision", "--autogenerate", "-m", message],
            check=True,
            capture_output=True,
            text=True,
        )

        logger.info("Migration created successfully.")
        logger.info(f"Output: {result.stdout}")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Migration creation failed: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during migration creation: {e}")
        return False


def main():
    """Main migration function."""
    configure_logging()

    if len(sys.argv) < 2:
        print("Usage: python run_migrations.py [upgrade|create] [message]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "upgrade":
        success = run_migrations()
    elif command == "create":
        if len(sys.argv) < 3:
            print("Usage: python run_migrations.py create 'migration message'")
            sys.exit(1)
        message = sys.argv[2]
        success = create_migration(message)
    else:
        print("Unknown command. Use 'upgrade' or 'create'")
        sys.exit(1)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
