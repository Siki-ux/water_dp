import logging
import math
import random
import time
from datetime import datetime, timezone

import requests

from app.core.config import settings

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simulator")


def get_frost_url():
    # Use internal container settings or default
    return settings.frost_url


def get_entity_id_by_name(endpoint, name):
    """Helper to find an entity ID by its name."""
    frost_url = get_frost_url()
    try:
        r = requests.get(
            f"{frost_url}/{endpoint}",
            params={"$filter": f"name eq '{name}'", "$select": "id"},
            timeout=5
        )
        if r.status_code == 200:
            val = r.json().get("value", [])
            if val:
                return val[0]["@iot.id"]
    except Exception as e:
        logger.warning(f"Failed to lookup {endpoint} '{name}': {e}")
    return None


def ensure_datastream(thing_id, ds_name="Water Level"):
    """Ensure a Datastream exists for the Thing."""
    frost_url = get_frost_url()

    # 1. Search for existing
    search_url = f"{frost_url}/Datastreams"
    params = {
        "$filter": f"name eq '{ds_name}' and Thing/id eq {thing_id}",
        "$select": "id",
    }

    try:
        r = requests.get(search_url, params=params)
        if r.status_code == 200:
            val = r.json().get("value", [])
            if val:
                return val[0]["@iot.id"]
    except Exception as e:
        logger.error(f"Error checking datastream: {e}")

    # 2. Create if not exists
    # Dynamically find Sensor and ObsProp IDs
    sensor_id = get_entity_id_by_name("Sensors", "Standard Sensor")
    op_id = get_entity_id_by_name("ObservedProperties", "Water Level")

    if not sensor_id:
        logger.warning("Sensor 'Standard Sensor' not found. Defaulting to ID 1.")
        sensor_id = 1
    if not op_id:
        logger.warning("ObservedProperty 'Water Level' not found. Defaulting to ID 1.")
        op_id = 1

    payload = {
        "name": ds_name,
        "description": "Simulated Water Level",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "Meter",
            "symbol": "m",
            "definition": "http://example.org",
        },
        "Thing": {"@iot.id": thing_id},
        "Sensor": {"@iot.id": sensor_id},
        "ObservedProperty": {"@iot.id": op_id},
    }

    try:
        r = requests.post(f"{frost_url}/Datastreams", json=payload)
        if r.status_code == 201:
            loc = r.headers["Location"]
            return loc.split("(")[1].split(")")[0]
        else:
            logger.error(f"Failed to create Datastream: {r.text}")
    except Exception as e:
        logger.error(f"Error creating datastream: {e}")
    return None


def run_simulator():
    logger.info("Starting Sensor Simulator...")
    frost_url = get_frost_url()

    # Wait for FROST to be ready
    while True:
        try:
            requests.get(frost_url, timeout=2)
            break
        except requests.RequestException:
            logger.info("Waiting for FROST API...")
            time.sleep(5)

    logger.info(f"Connected to FROST at {frost_url}")

    while True:
        try:
            # 1. Find Things marked as 'simulated'
            # We look for property 'simulated' OR 'station_id' starting with 'SIM'
            url = f"{frost_url}/Things?$filter=substringof('SIM', properties/station_id) or properties/simulated eq 'true'"
            r = requests.get(url)
            things = []
            if r.status_code == 200:
                things = r.json().get("value", [])

            # 2. If no things, create a default one
            if not things:
                logger.info(
                    "No simulated sensors found. Creating 'Auto-Simulated Sensor'..."
                )
                payload = {
                    "name": "Auto-Simulated Sensor",
                    "description": "Automatically created by Simulator service.",
                    "properties": {
                        "station_id": "SIM_AUTO_01",
                        "simulated": "true",
                        "type": "river",
                        "status": "active",
                    },
                    "Locations": [
                        {
                            "name": "Sim Location",
                            "description": "Virtual",
                            "encodingType": "application/vnd.geo+json",
                            "location": {"type": "Point", "coordinates": [14.5, 50.1]},
                        }
                    ],
                }
                r = requests.post(f"{frost_url}/Things", json=payload)
                if r.status_code == 201:
                    logger.info("Created 'Auto-Simulated Sensor'.")
                    continue  # Restart loop to pick it up

            # 3. Simulate Data for each
            for t in things:
                # Check Status
                props = t.get("properties", {})
                status = props.get("status", "active")
                if status != "active":
                    continue

                tid = t["@iot.id"]
                tname = t["name"]

                # Ensure Datastream
                ds_id = ensure_datastream(tid, ds_name=f"DS_{tid}_SIM")

                if ds_id:
                    # Generate Sine Wave Value based on time
                    now = datetime.now(timezone.utc)
                    ts = now.timestamp()
                    # Period 3600s, Amplitude 2m, Base 150m
                    val = (
                        150
                        + 2 * math.sin(2 * math.pi * ts / 3600)
                        + random.uniform(-0.1, 0.1)
                    )

                    obs = {
                        "phenomenonTime": now.isoformat(),
                        "result": round(val, 3),
                        "Datastream": {"@iot.id": ds_id},
                    }

                    try:
                        r = requests.post(f"{frost_url}/Observations", json=obs)
                        if r.status_code in [200, 201]:
                            logger.info(f"[{tname}] Pushed {val:.2f}m")

                            # Trigger Alert Evaluation
                            try:
                                from app.core.database import SessionLocal
                                from app.services.alert_evaluator import AlertEvaluator

                                db = SessionLocal()
                                try:
                                    evaluator = AlertEvaluator(db)
                                    # Use Thing ID as station_id, and hardcoded "water_level" as parameter for this sim
                                    evaluator.evaluate_sensor_data(
                                        str(tid), val, "water_level"
                                    )
                                finally:
                                    db.close()
                            except Exception as alert_err:
                                logger.error(f"Failed to evaluate alert: {alert_err}")

                    except Exception as e:
                        logger.error(f"Failed push: {e}")

        except Exception as e:
            logger.error(f"Simulator Loop Error: {e}")

        time.sleep(10)  # Run every 10 seconds


if __name__ == "__main__":
    run_simulator()
