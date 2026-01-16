"""
Water data API endpoints.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db
from app.schemas.water_data import (
    BulkDataPointCreate,
    DataPointListResponse,
    StationListResponse,
    StationStatistics,
    WaterDataPointCreate,
    WaterDataPointResponse,
    WaterQualityCreate,
    WaterQualityResponse,
    WaterStationResponse,
)
from app.services.time_series_service import TimeSeriesService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/stations", response_model=StationListResponse)
async def get_stations(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    station_type: Optional[str] = Query(None, description="Filter by station type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    organization: Optional[str] = Query(None, description="Filter by organization"),
    db: Session = Depends(get_db),
):
    """Get water stations with optional filtering."""
    service = TimeSeriesService(db)
    stations = service.get_stations(
        skip=skip, limit=limit, station_type=station_type, status=status
    )

    total = len(
        stations
    )  # This is a simplified count, in production you'd want a separate count query

    return StationListResponse(stations=stations, total=total, skip=skip, limit=limit)


@router.get("/stations/{station_id}", response_model=WaterStationResponse)
async def get_station(station_id: str, db: Session = Depends(get_db)):
    """Get a specific water station."""
    service = TimeSeriesService(db)
    return service.get_station(station_id)


@router.post(
    "/stations/{id}/data-points", response_model=WaterDataPointResponse, status_code=201
)
async def create_data_point(
    id: str,
    data_point: WaterDataPointCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Create a new water data point."""
    service = TimeSeriesService(db)
    return service.create_data_point(id, data_point)


@router.post(
    "/stations/{id}/data-points/bulk",
    response_model=List[WaterDataPointResponse],
    status_code=201,
)
async def create_bulk_data_points(
    id: str,
    bulk_data: BulkDataPointCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Create multiple water data points."""
    service = TimeSeriesService(db)
    created_points = []
    for data_point in bulk_data.data_points:
        point = service.create_data_point(id, data_point)
        created_points.append(point)
    return created_points


@router.get("/data-points", response_model=DataPointListResponse)
async def get_data_points(
    id: str = Query(..., description="Station ID"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    parameter: Optional[str] = Query(None, description="Filter by parameter"),
    quality_filter: Optional[str] = Query(None, description="Filter by quality flag"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order"),
    db: Session = Depends(get_db),
):
    """Get water data points with filtering."""
    try:
        from datetime import datetime

        from app.schemas.time_series import TimeSeriesQuery

        service = TimeSeriesService(db)

        try:
            start_dt = datetime.fromisoformat(start_time) if start_time else None
            end_dt = datetime.fromisoformat(end_time) if end_time else None
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")

        # 1. Fetch Datastreams for this station/parameter to get metadata (unit, etc.)
        datastreams_result = service.get_datastreams_for_station(id, parameter)

        mapped_points = []
        for ds in datastreams_result:
            ds_name = ds.get("name")
            op_name = ds.get("ObservedProperty", {}).get("name", "unknown")
            uom = ds.get("unitOfMeasurement", {}).get("name", "unknown")

            # 2. Fetch Observations for this datastream
            query = TimeSeriesQuery(
                series_id=ds_name,
                start_time=start_dt,
                end_time=end_dt,
                limit=limit,
                offset=offset,
                sort_order=sort_order,
            )
            data_points = service.get_time_series_data(query)

            # 3. Map to Response using metadata
            for dp in data_points:
                # Normalize parameter
                # FROST might return "Water Level", schema expects "water_level"
                param_slug = op_name.lower().replace(" ", "_").replace("-", "_")

                # Simple mapping if needed, otherwise rely on slug
                # Schema enum: water_level, flow_rate, temperature, etc.
                if param_slug == "water_temperature":
                    param_slug = "temperature"
                elif param_slug == "level":
                    param_slug = "water_level"

                mapped_data = {
                    "id": str(dp.id),
                    "timestamp": dp.timestamp,
                    "parameter": param_slug,
                    "value": dp.value,
                    "unit": uom,
                    "quality_flag": getattr(dp, "quality_flag", "good"),
                    "created_at": getattr(dp, "created_at", datetime.now()),
                    "updated_at": getattr(dp, "updated_at", datetime.now()),
                }
                mapped_points.append(mapped_data)

        if quality_filter:
            mapped_points = [
                dp for dp in mapped_points if dp["quality_flag"] == quality_filter
            ]

        return DataPointListResponse(
            data_points=mapped_points,
            total=len(mapped_points),
            id=id,
            parameter=parameter,
            time_range=(
                {"start": start_dt, "end": end_dt} if start_dt and end_dt else None
            ),
        )
    except Exception as e:
        logger.error(f"Error in get_data_points: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


@router.get("/data-points/latest", response_model=List[WaterDataPointResponse])
async def get_latest_data_points(
    id: str = Query(..., description="Station ID"),
    parameter: Optional[str] = Query(None, description="Filter by parameter"),
    db: Session = Depends(get_db),
):
    """Get latest data points for a station."""
    service = TimeSeriesService(db)
    data_points = service.get_latest_data(id, parameter)
    return data_points


@router.get("/stations/{station_id}/statistics", response_model=StationStatistics)
async def get_station_statistics(
    station_id: str,
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    db: Session = Depends(get_db),
):
    """Get statistical summary for a station."""
    try:
        from datetime import datetime

        service = TimeSeriesService(db)

        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        stats = service.get_station_statistics(
            station_id=station_id, start_time=start_dt, end_time=end_dt
        )

        return StationStatistics(**stats)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get station statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quality", response_model=WaterQualityResponse, status_code=201)
async def create_quality_data(
    quality_data: WaterQualityCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Create water quality data."""
    try:
        # Placeholder
        raise HTTPException(
            status_code=501, detail="Quality data creation not yet implemented"
        )
    except Exception as e:
        logger.error(f"Failed to create quality data: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/quality", response_model=List[WaterQualityResponse])
async def get_quality_data(
    id: str = Query(..., description="Station ID"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    db: Session = Depends(get_db),
):
    """Get water quality data."""
    try:
        # Placeholder
        raise HTTPException(
            status_code=501, detail="Quality data retrieval not yet implemented"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get quality data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
