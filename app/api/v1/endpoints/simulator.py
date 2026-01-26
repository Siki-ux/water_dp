from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from app.api import deps
from app.api.deps import get_current_user
from app.core.database import get_db
from app.schemas.simulation import CreateSimulatedThingRequest
from app.services.project_service import ProjectService
from app.services.simulator_service import SimulatorService

router = APIRouter()


def check_admin_access(user: dict, project_id: UUID, database: Session):
    # Verify access using ProjectService (requires Editor role or higher)
    # Return the project to get group ID
    return ProjectService._check_access(
        database, project_id, user, required_role="editor"
    )


@router.get("/projects/{project_id}/simulator/status")
def get_simulator_status(
    project_id: UUID = Path(...),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """
    Check availability and access to simulator for this project.
    """
    check_admin_access(user, project_id, database)
    return {"status": "available", "access": "granted"}


@router.post("/projects/{project_id}/simulator/things")
def create_simulated_thing(
    simulator_request: CreateSimulatedThingRequest,
    project_id: UUID = Path(...),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    project_db_obj = check_admin_access(user, project_id, database)

    # Validate Project UUID match (Optional but good practice)
    if str(simulator_request.thing.project_uuid) != str(project_id):
        # We can enforce it, or just ignore simulator_request.thing.project_uuid
        # Let's enforce it to avoid confusion
        raise HTTPException(
            status_code=400, detail="Path project_id must match body project_uuid"
        )

    # Prepare data for Service
    location = None
    if simulator_request.thing.latitude is not None and simulator_request.thing.longitude is not None:
        location = {"latitude": simulator_request.thing.latitude, "longitude": simulator_request.thing.longitude}

    thing_props = None
    if simulator_request.thing.properties:
        thing_props = [prop.model_dump() for prop in simulator_request.thing.properties]

    datastreams = [ds.model_dump() for ds in simulator_request.simulation.datastreams]

    # Call Service
    result = SimulatorService.create_simulated_thing(
        db=database,
        sensor_name=simulator_request.thing.sensor_name,
        project_group_id=project_db_obj.authorization_provider_group_id,
        datastreams_config=datastreams,
        description=simulator_request.thing.description,
        interval_seconds=60,  # TBD: Per-stream or global? Using default for now.
        thing_properties=thing_props,
        location=location,
    )

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create simulated thing")

    # Link to Project
    try:
        # Check for thing_uuid (new format) or uuid (legacy/raw)
        sensor_id = result.get("thing_uuid") or result.get("uuid") or result.get("id")

        if sensor_id:
            ProjectService.add_sensor(database, project_id, str(sensor_id), user)
    except Exception:
        # Log but don't fail, user will see the thing created but maybe not linked
        pass

    return result


@router.get("/projects/{project_id}/simulator/simulations")
def list_simulations(
    project_id: UUID = Path(...),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    check_admin_access(user, project_id, database)
    return SimulatorService.get_all_simulated_things(str(project_id), database, token)


@router.post("/projects/{project_id}/simulator/simulations")
def update_simulation_config(
    project_id: UUID = Path(...),
    thing_id: str = Body(...),
    config: dict = Body(...),
    name: str = Body(None),
    location: dict = Body(None),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    """
    Update simulation config (incl start/stop) for a Thing.
    Also supports Name and Location updates.
    """
    check_admin_access(user, project_id, database)
    result = SimulatorService.update_simulation_config(
        thing_id, config, token, name=name, location=location
    )
    if not result:
        raise HTTPException(status_code=404, detail="Thing not found or update failed")
    return result


@router.post("/projects/{project_id}/simulator/simulations/{simulation_id}/start")
def start_simulation(
    project_id: UUID = Path(...),
    simulation_id: str = Path(
        ...
    ),  # In this context, simulation_id is the thing_id/uuid
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    check_admin_access(user, project_id, database)
    # 1. Fetch current config to toggle
    things = SimulatorService.get_all_simulated_things(str(project_id), database, token)
    target = next(
        (thing_item for thing_item in things if thing_item["uuid"] == simulation_id or thing_item["id"] == simulation_id),
        None,
    )

    if not target:
        raise HTTPException(status_code=404, detail="Simulation Thing not found")

    config = target.get("config", {}) or {}
    config["is_running"] = True

    result = SimulatorService.update_simulation_config(target["id"], config, token)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to start simulation")
    return result


@router.post("/projects/{project_id}/simulator/simulations/{simulation_id}/stop")
def stop_simulation(
    project_id: UUID = Path(...),
    simulation_id: str = Path(...),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    check_admin_access(user, project_id, database)
    # 1. Fetch current config to toggle
    things = SimulatorService.get_all_simulated_things(str(project_id), database, token)
    target = next(
        (thing_item for thing_item in things if thing_item["uuid"] == simulation_id or thing_item["id"] == simulation_id),
        None,
    )

    if not target:
        raise HTTPException(status_code=404, detail="Simulation Thing not found")

    config = target.get("config", {}) or {}
    config["is_running"] = False

    result = SimulatorService.update_simulation_config(target["id"], config, token)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to stop simulation")
    return result


@router.delete("/projects/{project_id}/simulator/things/{thing_id}")
def delete_simulated_thing(
    project_id: UUID = Path(...),
    thing_id: str = Path(...),
    database: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
    token: str = Depends(deps.oauth2_scheme),
):
    """
    Delete a simulated thing.
    """
    check_admin_access(user, project_id, database)
    success = SimulatorService.delete_simulated_thing(
        str(project_id), thing_id, database, token
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete thing")

    # Unlink from Project
    try:
        ProjectService.remove_sensor(database, project_id, str(thing_id), user)
    except Exception:
        pass

    return {"message": "Thing deleted successfully"}
