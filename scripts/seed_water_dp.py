import logging
import os
import sys
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from app.core.database import SessionLocal

# We re-use the core seeding logic but wrapped for specific execution
from app.core.seeding import seed_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def wait_for_services():
    """Wait for essential external services."""
    import requests

    # 1. Wait for Local DB (Implicit in SessionLocal connection, handled by docker depends_on mostly)

    # 2. Wait for FROST (TimeIO Stack)
    frost_url = os.getenv("FROST_URL", settings.frost_url)
    if not frost_url:
        frost_url = (
            "http://timeio-frost:8080/FROST-Server/v1.1"  # Default for hyphenated name
        )
    logger.info(f"Waiting for FROST at {frost_url}...")
    for _ in range(60):
        try:
            if requests.get(frost_url, timeout=5).status_code == 200:
                logger.info("FROST is Up.")
                return
        except Exception:
            logger.debug("FROST not ready yet, retrying...")
        time.sleep(2)
    logger.warning("FROST unreachable. Seeding might be partial.")

    # Core logic: wait for services and then run seeding from app.core.seeding
    # TimeIO parts are handled gracefully inside seed_data if configured correctly.
    logger.info("Service seeding initialization...")


def main():
    logger.info("--- Starting WaterDP Application Seeding (Consumer Mode) ---")

    # Ensure configuration
    settings.seeding = True

    wait_for_services()

    db = SessionLocal()
    try:
        seed_data(db)
        logger.info("--- WaterDP Seeding Completed Successfully ---")
    except Exception as e:
        logger.error(f"Seeding Failed: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
