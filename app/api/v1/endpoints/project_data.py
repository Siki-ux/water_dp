import csv
import io
import json
from datetime import datetime, timedelta
from typing import Annotated, Any, List
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Path,
    Query,
    UploadFile,
)
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.schemas.time_series import (
    StationResponse,
    TimeSeriesDataCreate,
)
from app.services.project_service import ProjectService
from app.services.time_series_service import (
    ResourceNotFoundException,
    TimeSeriesService,
)

router = APIRouter()


@router.get("/{project_id}/things", response_model=List[StationResponse])
def list_project_things(
    project_id: UUID,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
    ts_service: TimeSeriesService = Depends(deps.get_time_series_service),
) -> Any:
    """List valid Things (Stations) linked to the project with Activity Status."""
    # Check access
    ProjectService._check_access(db, project_id, current_user, required_role="viewer")

    # Get List of Sensor IDs linked to project
    sensor_ids = ProjectService.list_sensors(db, project_id, current_user)

    results = []
    # Optimization: This could be slow for many sensors.
    # Ideal: Schema expansion or Batch API in FROST.
    # For now, we loop.
    # FIXME: Potential N+1 performance issue. Consider implementing batch retrieval.
    for sid in sensor_ids:
        try:
            station = ts_service.get_station(sid)
            if not station:
                continue

            # Check Activity
            # Definition: Active if data in last 24h
            is_active = False
            last_activity = None

            # We fetch latest data for *any* parameter.
            # This is expensive (N+1).
            # TODO: Optimize by checking a "last_update" property if we maintain one?
            latest_data = ts_service.get_latest_data(station["id"])  # pass integer ID
            if latest_data:
                # Find most recent
                last_activity = max([d["timestamp"] for d in latest_data])
                if datetime.now(last_activity.tzinfo) - last_activity < timedelta(
                    hours=24
                ):
                    is_active = True

            # Map to response
            resp = StationResponse(
                id=station["id"],
                station_id=station["station_id"],
                name=station["name"],
                description=station["description"],
                station_type=station["station_type"],
                status=station["status"],
                organization=station["organization"],
                latitude=station["latitude"],
                longitude=station["longitude"],
                elevation=station["elevation"],
                properties=station["properties"],
                created_at=station["created_at"],
                updated_at=station["updated_at"],
                is_active=is_active,
                last_activity=last_activity,
            )
            results.append(resp)

        except Exception:
            # If one fails, don't break whole list
            continue

    return results


@router.post("/{project_id}/sensors/{sensor_id}")
def link_project_sensor(
    project_id: UUID,
    sensor_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
) -> Any:
    """Link an existing TimeIO Sensor (@iot.id) to the Project."""
    # Check Editor access
    ProjectService._check_access(db, project_id, current_user, required_role="editor")

    # Link to Project
    try:
        ProjectService.add_sensor(db, project_id, sensor_id, current_user)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to link sensor to project: {e}"
        )

    return {"status": "success", "message": "Sensor linked successfully"}


@router.delete("/{project_id}/things/{thing_id}")
def unlink_project_thing(
    project_id: UUID,
    thing_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
    ts_service: TimeSeriesService = Depends(deps.get_time_series_service),
) -> Any:
    """Remove Thing from project (Unlink only). Does NOT delete from Source."""
    # Check Editor access
    ProjectService._check_access(db, project_id, current_user, required_role="editor")

    # --- Robust Permission Check ---
    # 1. Get list of allowed sensor IDs in project
    allowed_sensors = ProjectService.list_sensors(db, project_id, current_user)

    # 2. Resolve the incoming thing_id to its canonical @iot.id
    canonical_id = thing_id
    if thing_id not in allowed_sensors:
        station = ts_service.get_station(thing_id)
        if station:
            found_id = str(station.get("id"))
            if found_id in allowed_sensors:
                canonical_id = found_id
            else:
                found_sid = station.get("station_id")
                if found_sid and str(found_sid) in allowed_sensors:
                    canonical_id = str(found_sid)
                else:
                    raise HTTPException(
                        status_code=404,
                        detail="Thing not found in project (Permission Denied)",
                    )
        else:
            raise HTTPException(status_code=404, detail="Thing not found in system")

    # Remove link using canonical ID
    ProjectService.remove_sensor(db, project_id, canonical_id, current_user)

    return {"status": "success", "message": "Sensor unlinked"}


class SimpleDataPoint(BaseModel):
    timestamp: datetime
    value: float
    quality_flag: str = "good"


@router.post("/{project_id}/things/{thing_id}/import", status_code=200)
async def import_project_thing_file(
    project_id: Annotated[UUID, Path(description="Project ID")],
    thing_id: Annotated[
        str, Path(description="Thing ID (FROST @iot.id) or Station ID Property")
    ],
    file: Annotated[UploadFile, File(description="Data file (CSV or JSON)")],
    parameter: Annotated[
        str,
        Query(
            description="The parameter (ObservedProperty) to import data for (e.g. Level, Flow, Temperature)."
        ),
    ],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[dict, Depends(deps.get_current_user)],
) -> Any:
    """
    Import data for a Thing via **File Upload** (CSV or JSON).

    - **CSV**: Headers `timestamp`, `value`, `quality_flag`.
    - **JSON**: List of objects with `timestamp`, `value`, `quality_flag`.
    """
    ts_service = TimeSeriesService(db)

    # Convert file content to text/json
    # Validation logic here...

    ProjectService._check_access(db, project_id, current_user, required_role="editor")

    # --- Robust Permission Check ---
    # 1. Get list of allowed sensor IDs in project (These are strings, likely @iot.id)
    allowed_sensors = ProjectService.list_sensors(db, project_id, current_user)

    # 2. Resolve the incoming thing_id to its canonical @iot.id
    # Try direct match first
    canonical_id = thing_id
    if thing_id not in allowed_sensors:
        # If not direct match, try to look it up via Service
        station = ts_service.get_station(thing_id)
        if station:
            # TimeSeriesService returns 'id' as int usually, we need str for comparison with project_sensors
            found_id = str(station.get("id"))
            if found_id in allowed_sensors:
                canonical_id = found_id
            else:
                # Also try station_id property from the station object
                found_sid = station.get("station_id")
                if found_sid and str(found_sid) in allowed_sensors:
                    canonical_id = str(found_sid)
                else:
                    raise HTTPException(
                        status_code=404,
                        detail="Thing not found in project (Permission Denied)",
                    )
        else:
            raise HTTPException(status_code=404, detail="Thing not found in system")

    # Use canonical_id for datastream creation

    series_id = f"DS_{canonical_id}_{parameter}"
    data_points = []

    content = await file.read()

    try:
        filename = file.filename.lower()
        if filename.endswith(".csv"):
            text = content.decode("utf-8")
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                ts_val = row.get("timestamp") or row.get("time") or row.get("date")
                val = row.get("value") or row.get("val")
                qual = row.get("quality_flag") or row.get("quality") or "good"

                if not ts_val or val is None:
                    continue

                data_points.append(
                    TimeSeriesDataCreate(
                        timestamp=datetime.fromisoformat(ts_val.replace("Z", "+00:00")),
                        value=float(val),
                        quality_flag=qual,
                        series_id=series_id,
                    )
                )

        elif filename.endswith(".json"):
            data = json.loads(content)
            if not isinstance(data, list):
                raise ValueError("JSON must be a list of objects")
            for item in data:
                ts_val = item.get("timestamp")
                val = item.get("value")
                qual = item.get("quality_flag", "good")
                if not ts_val or val is None:
                    continue
                data_points.append(
                    TimeSeriesDataCreate(
                        timestamp=datetime.fromisoformat(ts_val.replace("Z", "+00:00")),
                        value=float(val),
                        quality_flag=qual,
                        series_id=series_id,
                    )
                )
        else:
            raise HTTPException(
                status_code=400, detail="Unsupported file format. Use CSV or JSON."
            )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")

    if not data_points:
        return {"status": "warning", "message": "No valid data points found to import."}

    try:
        ts_service.ensure_datastream(canonical_id, parameter)
        imported_count = ts_service.add_bulk_data(series_id, data_points)
    except ResourceNotFoundException:
        raise HTTPException(
            status_code=404,
            detail=f"Datastream {series_id} could not be created or found.",
        )

    return {"status": "success", "imported": imported_count, "series_id": series_id}


@router.post("/{project_id}/things/{thing_id}/import-json", status_code=200)
async def import_project_thing_json(
    project_id: Annotated[UUID, Path(description="Project ID")],
    thing_id: Annotated[
        str, Path(description="Thing ID (FROST @iot.id) or Station ID Property")
    ],
    body: List[SimpleDataPoint],
    parameter: Annotated[
        str, Query(description="The parameter (ObservedProperty) to import data for.")
    ],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[dict, Depends(deps.get_current_user)],
) -> Any:
    """
    Import data for a Thing via **JSON Body**.

    Body should be a list of objects:
    ```json
    [
      { "timestamp": "2026-01-01T10:00:00Z", "value": 12.5, "quality_flag": "good" }
    ]
    ```
    """

    ts_service = TimeSeriesService(db)

    ProjectService._check_access(db, project_id, current_user, required_role="editor")

    # --- Robust Permission Check ---
    allowed_sensors = ProjectService.list_sensors(db, project_id, current_user)

    canonical_id = thing_id
    if thing_id not in allowed_sensors:
        station = ts_service.get_station(thing_id)
        if station:
            found_id = str(station.get("id"))
            if found_id in allowed_sensors:
                canonical_id = found_id
            else:
                found_sid = station.get("station_id")
                if found_sid and str(found_sid) in allowed_sensors:
                    canonical_id = str(found_sid)
                else:
                    raise HTTPException(
                        status_code=404,
                        detail="Thing not found in project (Permission Denied)",
                    )
        else:
            raise HTTPException(status_code=404, detail="Thing not found in system")

    series_id = f"DS_{canonical_id}_{parameter}"
    data_points = []

    for item in body:
        data_points.append(
            TimeSeriesDataCreate(
                timestamp=item.timestamp,
                value=item.value,
                quality_flag=item.quality_flag,
                series_id=series_id,
            )
        )

    if not data_points:
        return {"status": "warning", "message": "No data points received."}

    try:
        ts_service.ensure_datastream(canonical_id, parameter)
        imported_count = ts_service.add_bulk_data(series_id, data_points)
    except ResourceNotFoundException:
        raise HTTPException(
            status_code=404,
            detail=f"Datastream {series_id} could not be created or found.",
        )

    return {"status": "success", "imported": imported_count, "series_id": series_id}
