import logging
import os
import sys

# Add parent directory to path so we can import app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from app.core.database import SessionLocal
from app.core.seeding import seed_data

# Force logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Database Seeding (Project, Users, Sensors, Simulator)...")
    # Force settings.seeding to True just in case
    settings.seeding = True

    db = SessionLocal()
    try:
        seed_data(db)
        logger.info("ALL Seeding completed successfully.")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
