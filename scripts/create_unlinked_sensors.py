import logging
import os
import random
import sys

import requests

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings

# Force logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_unlinked_sensors():
    logger.info("Creating Unlinked Sensors in FROST...")

    FROST_URL = settings.frost_url

    # Try fallback if default unreachable (assuming localhost env)
    try:
        requests.get(FROST_URL, timeout=5)
    except requests.RequestException:
        FROST_URL = "http://localhost:8083/FROST-Server/v1.1"
        logger.info(f"Using fallback FROST URL: {FROST_URL}")

    # Create 5 sensors
    for i in range(5):
        unique_id = random.randint(1000, 9999)
        name = f"Unlinked Sensor {unique_id}"

        payload = {
            "name": name,
            "description": "A sensor available for linking.",
            "properties": {
                "station_id": f"UNLINKED_{unique_id}",
                "status": "active",
                "type": "river",
            },
            "Locations": [
                {
                    "name": f"Loc {unique_id}",
                    "description": "Location",
                    "encodingType": "application/vnd.geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [14.4 + (i * 0.1), 50.0 + (i * 0.1)],
                    },
                }
            ],
        }

        try:
            resp = requests.post(f"{FROST_URL}/Things", json=payload)
            if resp.status_code == 201:
                loc = resp.headers["Location"]
                tid = loc.split("(")[1].split(")")[0]
                logger.info(f"Created Thing {tid}: {name}")
            else:
                logger.error(f"Failed to create {name}: {resp.text}")
        except Exception as e:
            logger.error(f"Error creating {name}: {e}")


if __name__ == "__main__":
    create_unlinked_sensors()
