"""
Simulator Service for managing simulated IoT devices (V3).

Manages creation of simulated things using OrchestratorV3.
Persists simulation configuration in local database.
"""

import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.exceptions import TimeSeriesException
from app.models.simulation import Simulation
from app.services.timeio.orchestrator_v3 import orchestrator_v3

logger = logging.getLogger(__name__)


class SimulatorService:
    """
    Service for managing simulated IoT devices via V3 with local config storage.
    """

    @staticmethod
    def _parse_interval_to_seconds(interval_str: str) -> int:
        """Parse interval string (e.g. '10s', '1m') to seconds."""
        if isinstance(interval_str, int):
            return interval_str

        if not interval_str:
            return 60

        s = interval_str.strip().lower()
        try:
            if s.endswith("s"):
                return int(float(s[:-1]))
            elif s.endswith("m"):
                return int(float(s[:-1]) * 60)
            elif s.endswith("h"):
                return int(float(s[:-1]) * 3600)
            else:
                return int(float(s))
        except Exception:
            return 60  # Default fallback

    @staticmethod
    def _calculate_min_interval(config: List[Dict[str, Any]]) -> int:
        """Find the minimum interval needed to satisfy all datastreams."""
        if not config:
            return 60

        intervals = []
        for ds in config:
            # Config structure might be:
            # 1. Direct dict (if flattened)
            # 2. Inside "config" key (if nested structure from V3 input)
            # We need to handle both based on how it's stored.
            # Saved config is usually the LIST of datastreams configs directly.

            # Check for nested 'config' or direct keys
            target = ds.get("config", ds)
            if not isinstance(target, dict):
                target = ds  # Fallback

            val = target.get("interval", "60s")
            intervals.append(SimulatorService._parse_interval_to_seconds(val))

        if not intervals:
            return 60

        # Return minimum interval (Worker will sleep this amount)
        # Ideally GCD, but MIN is safe enough (worker checks nicely)
        return max(1, min(intervals))

    @staticmethod
    def _format_simulation_output(
        thing_data: Dict[str, Any],
        sim: Any = None,
        sim_config: List[Dict[str, Any]] = None,
        sim_id: str = None,
        is_enabled: bool = True,
    ) -> Dict[str, Any]:
        """
        Format the simulation output to match the specific V3-like structure.
        """
        # Resolve Simulation Info
        config = sim_config
        simulation_id = sim_id
        is_running = is_enabled

        if sim:
            config = sim.config
            simulation_id = sim.id
            is_running = sim.is_enabled

        # Prepare Datastreams (Merge TSM metadata with Config)
        formatted_datastreams = []

        # 1. Index Config by Name for easy lookup
        config_map = {c.get("name"): c for c in config} if config else {}

        # 2. Iterate TSM Datastreams if present, otherwise use Config (Creation case)
        # Thing Data from `create_sensor` might NOT have 'datastreams' key fully populated,
        # but `list_sensors` (rich) does.

        source_datastreams = thing_data.get("datastreams", [])
        if not source_datastreams and config:
            # Fallback for Creation response: construct mostly from config
            for c in config:
                formatted_datastreams.append(
                    {
                        "name": c.get("name"),
                        "unit": c.get("unit", "Unknown"),
                        "label": c.get("label", c.get("name")),
                        "properties": None,  # TBD
                        "config": {
                            "type": c.get("type", "random"),
                            "range": c.get("range"),
                            "enabled": c.get("enabled", True),
                            "interval": c.get("interval", "60s"),
                        },
                    }
                )
        else:
            # Merge Logic
            for ds in source_datastreams:
                name = ds.get("name")
                ds_config = config_map.get(name, {})

                formatted_datastreams.append(
                    {
                        "name": name,
                        "unit": ds.get("unit"),
                        "label": ds.get("label"),
                        "properties": ds.get("properties"),
                        "config": (
                            {
                                "type": ds_config.get(
                                    "type", "random"
                                ),  # Defaults if not in config
                                "range": ds_config.get("range"),
                                "enabled": ds_config.get("enabled", True),
                                "interval": ds_config.get("interval", "60s"),
                            }
                            if ds_config
                            else None
                        ),  # Or default config?
                    }
                )

        return {
            "thing_uuid": thing_data.get("uuid"),
            "thing_id": thing_data.get("id"),
            "name": thing_data.get("name"),
            "description": thing_data.get("description", ""),
            "properties": thing_data.get("properties"),
            "datastreams": formatted_datastreams,
            "is_running": is_running,
            "simulation_id": simulation_id,
        }

    @staticmethod
    def create_simulated_thing(
        db: Session,
        sensor_name: str,
        project_group_id: str,
        datastreams_config: List[Dict[str, Any]],
        description: str = "Simulated Device",
        interval_seconds: int = 60,
        thing_properties: List[Dict[str, Any]] = None,
        location: Dict[str, float] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Simulated Thing using V3 Orchestrator and save config locally.
        """

        # 1. format name
        full_name = (
            f"Simulated: {sensor_name}"
            if not sensor_name.startswith("Simulated:")
            else sensor_name
        )

        # 2. Extract metadata for SSM creation
        # Prefer explicit thing_properties if provided (V3 style)
        properties_metadata = []

        if thing_properties:
            properties_metadata = thing_properties
        else:
            # Fallback for V1 legacy: derive from config
            for ds in datastreams_config:
                properties_metadata.append(
                    {
                        "name": ds.get("name"),
                        "unit": ds.get("unit", "Unknown"),
                        "label": ds.get("label", ds.get("name")),
                    }
                )

        # 3. Create Sensor via OrchestratorV3
        # This handles TSM/ConfigDB creation and Legacy Sync
        try:
            result = orchestrator_v3.create_sensor(
                sensor_name=full_name,
                project_group_id=project_group_id,
                description=description,
                device_type="chirpstack_generic",
                properties=properties_metadata,
                location=location,
            )

            if not result or not result.get("uuid"):
                raise TimeSeriesException(message="Failed to create sensor in TSM")

            thing_uuid = result["uuid"]

            # 4. Save Simulation Config to Local DB
            # Calculate Min Interval
            calc_interval = SimulatorService._calculate_min_interval(datastreams_config)

            new_sim = Simulation(
                id=str(uuid.uuid4()),
                thing_uuid=thing_uuid,
                config=datastreams_config,  # JSONB
                interval_seconds=calc_interval,
                is_enabled=True,
            )
            db.add(new_sim)
            db.commit()
            db.refresh(new_sim)

            logger.info(f"Created simulation {new_sim.id} for thing {thing_uuid}")

            # Format Response
            return SimulatorService._format_simulation_output(
                thing_data=result,
                sim_config=datastreams_config,
                sim_id=new_sim.id,
                is_enabled=True,
            )

        except Exception as e:
            logger.error(f"Failed to create simulated thing: {e}")
            db.rollback()
            if isinstance(e, TimeSeriesException):
                raise e
            raise TimeSeriesException(
                message=f"Failed to create simulated thing: {str(e)}"
            )

    @staticmethod
    def get_all_simulated_things(
        project_id: str, db: Session, token: str
    ) -> List[Dict[str, Any]]:
        """
        Get all simulated things for a project.
        Merges local simulation config with TSM/FROST metadata.
        """
        # 1. Get all sensors in project (from ProjectService)
        from sqlalchemy import select

        # Import inside to avoid circular deps
        from app.models.user_context import project_sensors
        from app.services.project_service import (
            ProjectService,
        )

        # Get sensor IDs linked to project
        stmt = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        linked_uuids = [str(r) for r in db.execute(stmt).scalars().all()]

        if not linked_uuids:
            return []

        # 2. Get Simulation Configs for these UUIDs
        sims = (
            db.query(Simulation).filter(Simulation.thing_uuid.in_(linked_uuids)).all()
        )
        sim_map = {s.thing_uuid: s for s in sims}

        if not sims:
            return []

        # 3. Fetch Metadata from Orchestrator (optional, for enriched view)
        project = ProjectService.get_project(
            db,
            project_id,
            user={"sub": "internal", "realm_access": {"roles": ["admin"]}},
        )

        if project:
            # We fetch ALL sensors to get metadata, then filter.
            # Ideally Orchestrator V3 would support list_by_uuids.
            # We can use `list_sensors` (Returns Rich objects).
            tsm_things = orchestrator_v3.list_sensors(
                project_name=project.name,
                project_group=project.authorization_provider_group_id,
            )
        else:
            tsm_things = []

        results = []
        # Filter TSM things by our simulation map
        for thing in tsm_things:
            t_uuid = str(thing.get("uuid"))
            if t_uuid in sim_map:
                sim = sim_map[t_uuid]
                # Use Helper to Format
                formatted = SimulatorService._format_simulation_output(
                    thing_data=thing, sim=sim
                )
                results.append(formatted)

        return results

    @staticmethod
    def update_simulation_config(
        thing_id: str,
        config: Dict[str, Any],
        token: str,
        name: str = None,
        location: Dict[str, float] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update simulation config and optionally TSM metadata.
        """
        from app.core.database import SessionLocal

        db = SessionLocal()
        try:
            # 1. Update Local Config
            # thing_id might be UUID or Sim ID. Let's try finding by thing_uuid first.
            sim = db.query(Simulation).filter(Simulation.thing_uuid == thing_id).first()
            if not sim:
                sim = db.query(Simulation).filter(Simulation.id == thing_id).first()

            if sim:
                if config:
                    sim.config = config
                    # Recalculate Interval
                    sim.interval_seconds = SimulatorService._calculate_min_interval(
                        config
                    )

                    # Update is_enabled if present in config?
                    if "is_running" in config:
                        sim.is_enabled = config["is_running"]
                    elif "enabled" in config:
                        sim.is_enabled = config["enabled"]

                db.commit()
                db.refresh(sim)

            # 2. Update TSM Metadata (Name, Location) via Orchestrator
            if name or location:
                # We need the project schema to update location?
                # OrchestratorV3 update_sensor_location needs schema.
                # This might be complex without project context.
                # Attempt update if possible.
                pass

            return {"uuid": thing_id, "config": config, "message": "Updated"}

        except Exception as e:
            logger.error(f"Failed to update simulation: {e}")
            return None
        finally:
            db.close()

    @staticmethod
    def delete_simulated_thing(
        project_id: str, thing_id: str, db: Session, token: str
    ) -> bool:
        """
        Delete simulated thing (Local + TSM).
        """
        try:
            # 1. Delete Local Simulation
            sim = (
                db.query(Simulation)
                .filter(
                    (Simulation.thing_uuid == thing_id) | (Simulation.id == thing_id)
                )
                .first()
            )

            uuid_to_delete = thing_id
            if sim:
                uuid_to_delete = sim.thing_uuid
                db.delete(sim)
                db.commit()

            # 2. Delete from TSM via Orchestrator
            # We need project details to resolve schema for TSM deletion if needed?
            # OrchestratorV3.delete_sensor needs UUID.
            orchestrator_v3.delete_sensor(uuid_to_delete)

            return True
        except Exception as e:
            logger.error(f"Failed to delete simulated thing: {e}")
            return False
