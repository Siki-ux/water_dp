#!/usr/bin/env python3
"""
Setup script for the Water Data Platform.
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

# Add the app directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.database import init_db
from app.core.logging_config import configure_logging

logger = logging.getLogger(__name__)


def run_command(command: str, description: str) -> bool:
    """Run a shell command and return success status."""
    try:
        logger.info(f"Running: {description}")
        subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Success: {description}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed: {description}")
        logger.error(f"Error: {e.stderr}")
        return False


def setup_environment():
    """Set up the development environment."""
    logger.info("Setting up Water Data Platform environment...")

    # Create necessary directories
    directories = ["logs", "data", "exports", "temp"]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

    # Create .env file if it doesn't exist
    env_file = Path(".env")
    env_example = Path("env.example")

    if not env_file.exists() and env_example.exists():
        logger.info("Creating .env file from template...")
        with open(env_example, "r") as f:
            content = f.read()
        with open(env_file, "w") as f:
            f.write(content)
        logger.info("Created .env file. Please update with your configuration.")

    logger.info("Environment setup completed.")


def install_dependencies():
    """Install Python dependencies."""
    logger.info("Installing Python dependencies...")

    # Prefer Poetry for dependency management
    command = "poetry install"
    if not run_command(command, "Install Python dependencies with Poetry"):
        logger.error("Failed to install dependencies with Poetry")
        logger.error("Make sure Poetry is installed: pip install poetry")
        return False

    logger.info("Dependencies installed successfully with Poetry.")
    return True


def setup_database():
    """Set up the database."""
    logger.info("Setting up database...")

    try:
        # Initialize database tables
        init_db()
        logger.info("Database tables created successfully.")

        # Run Alembic migrations
        if run_command("alembic upgrade head", "Run database migrations"):
            logger.info("Database migrations completed successfully.")
        else:
            logger.error("Database migrations failed.")
            return False

        return True
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False


def setup_geoserver():
    """Set up GeoServer integration."""
    logger.info("Setting up GeoServer integration...")

    try:
        from app.services.geoserver_service import GeoServerService

        geoserver_service = GeoServerService()

        # Test connection
        if geoserver_service.test_connection():
            logger.info("GeoServer connection successful.")

            # Create workspace
            if geoserver_service.create_workspace():
                logger.info("GeoServer workspace created successfully.")
            else:
                logger.warning("Failed to create GeoServer workspace.")
        else:
            logger.warning(
                "Cannot connect to GeoServer. Please check your configuration."
            )

        return True
    except Exception as e:
        logger.error(f"GeoServer setup failed: {e}")
        return False


def run_tests():
    """Run the test suite."""
    logger.info("Running tests...")

    if run_command("pytest tests/ -v", "Run test suite"):
        logger.info("All tests passed.")
        return True
    else:
        logger.error("Some tests failed.")
        return False


def main():
    """Main setup function."""
    # Configure logging
    configure_logging()

    logger.info("Starting Water Data Platform setup...")

    # Setup steps
    steps = [
        ("Environment setup", setup_environment),
        ("Install dependencies", install_dependencies),
        ("Database setup", setup_database),
        ("GeoServer setup", setup_geoserver),
    ]

    success = True

    for step_name, step_function in steps:
        logger.info(f"Running step: {step_name}")
        if not step_function():
            logger.error(f"Step failed: {step_name}")
            success = False
            break

    if success:
        logger.info("Water Data Platform setup completed successfully!")
        logger.info(
            "You can now start the application with: uvicorn app.main:app --reload"
        )
    else:
        logger.error("Setup failed. Please check the logs and fix any issues.")
        sys.exit(1)


if __name__ == "__main__":
    main()
