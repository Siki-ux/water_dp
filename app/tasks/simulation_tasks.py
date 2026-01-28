import json
import logging
import math
import random
import time
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.database import SessionLocal
from app.models.simulation import Simulation
from app.services.timeio.timeio_db import TimeIODatabase

logger = logging.getLogger(__name__)


@celery_app.task
def run_simulation_step():
    """
    Task to process all running simulations and generate data.
    Reads active simulations from LOCAL (water-dp-postgres) 'simulations' table.
    Fetches MQTT credentials from TSM ConfigDB.
    """
    db_session = SessionLocal()  # Local App DB
    tsm_db = TimeIODatabase()  # TSM Config DB

    try:
        # 1. Fetch active simulations from Local DB
        logger.info("Fetching active simulations from Local DB")
        active_sims = (
            db_session.query(Simulation).filter(Simulation.is_enabled == True).all()
        )

        if not active_sims:
            logger.debug("No active simulations found.")
            return
        
        logger.info(f"Found {len(active_sims)} active simulations.")
        
        # Filter by Interval FIRST to avoid unnecessary TSM lookups
        sims_to_run = []
        now = datetime.now(timezone.utc)

        for sim in active_sims:
            if sim.last_run:
                delta = (now - sim.last_run).total_seconds()
                interval = sim.interval_seconds or 60
                logger.debug(f"Sim {sim.id}: last_run={sim.last_run}, delta={delta}, interval={interval}")
                if delta < interval:
                    continue
            else:
                 logger.debug(f"Sim {sim.id}: First run.")

            sims_to_run.append(sim)

        if not sims_to_run:
            logger.debug("No simulations due to run this tick.")
            return

        logger.info(f"Simulations to run: {[str(s.id) for s in sims_to_run]}")

        # 2. Batch Fetch MQTT Configs from TSM
        thing_uuids = [s.thing_uuid for s in sims_to_run]
        logger.debug(f"Fetching configs for things: {thing_uuids}")
        thing_configs = tsm_db.get_thing_configs_by_uuids(thing_uuids)
        logger.debug(f"Got thing configs: {list(thing_configs.keys())}")

        # Connect to MQTT once for the batch
        client = mqtt.Client(client_id=f"worker-simulator-{time.time()}")
        if settings.mqtt_username:
            client.username_pw_set(settings.mqtt_username, settings.mqtt_password)

        try:
            client.connect(settings.mqtt_broker_host, 1883, 60)
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return

        count = 0
        for sim in sims_to_run:
            try:
                # 3. Get Credentials from Batch
                t_config = thing_configs.get(sim.thing_uuid)

                if not t_config:
                    # TSM Thing might have been deleted?
                    logger.warning(
                        f"Simulated Thing {sim.thing_uuid} not found in TSM config. Skipping."
                    )
                    # Optional: Auto-disable?
                    # sim.is_enabled = False
                    # db_session.add(sim)
                    continue

                mqtt_user = t_config.get("mqtt_user")

                # 4. Generate and Publish
                process_single_simulation(client, sim.config, mqtt_user)
                count += 1

                # Update last run
                sim.last_run = now
                db_session.add(sim)

            except Exception as e:
                logger.error(f"Error processing simulation {sim.id}: {e}")

        db_session.commit()
        client.disconnect()

        if count > 0:
            logger.info(f"Processed {count} simulations.")

    except Exception as e:
        logger.error(f"Simulation step failed: {e}")
        db_session.rollback()
    finally:
        db_session.close()


def process_single_simulation(mqtt_client: mqtt.Client, config: Any, mqtt_user: str):
    """
    Generate data for a single simulated thing and publish to MQTT.
    """
    if not isinstance(config, list):
        return

    payload_data = {}
    current_time = time.time()
    logger.info(f"Processing simulation for user: {mqtt_user} and config: {config}")
    for datastream in config:
        # Expects new nested structure: {name, ..., config: {type, range, ...}}
        name = datastream.get("name")
        inner = datastream.get("config", {})

        pattern = inner.get("type", "random")
        rng = inner.get("range", {})

        val = 0.0
        min_val = float(rng.get("min", 0))
        max_val = float(rng.get("max", 100))

        if pattern == "sine":
            period = float(inner.get("period", 60))
            amplitude = (max_val - min_val) / 2
            offset = min_val + amplitude
            val = offset + amplitude * math.sin(2 * math.pi * current_time / period)

        elif pattern == "random":
            val = random.uniform(min_val, max_val)

        else:
            val = min_val

        payload_data[name] = val

    final_payload = {"object": payload_data, "time": datetime.utcnow().isoformat()}
    logger.info(f"Final payload: {final_payload}")
    if mqtt_user:
        topic = f"mqtt_ingest/{mqtt_user}/data"
        mqtt_client.publish(topic, json.dumps(final_payload), qos=0)
        logger.info(f"Published payload to topic: {topic}")
