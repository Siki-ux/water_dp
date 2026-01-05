"""
Water data API endpoints.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

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
    WaterStationCreate,
    WaterStationResponse,
    WaterStationUpdate,
)
from app.services.database_service import DatabaseService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/stations", response_model=WaterStationResponse, status_code=201)
async def create_station(station: WaterStationCreate, db: Session = Depends(get_db)):
    """Create a new water station."""
    try:
        db_service = DatabaseService(db)
        return db_service.create_station(station)
    except Exception as e:
        logger.error(f"Failed to create station: {e}")
        raise HTTPException(status_code=400, detail=str(e))


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
    try:
        db_service = DatabaseService(db)
        stations = db_service.get_stations(
            skip=skip, limit=limit, station_type=station_type, status=status
        )

        total = len(
            stations
        )  # This is a simplified count, in production you'd want a separate count query

        return StationListResponse(
            stations=stations, total=total, skip=skip, limit=limit
        )
    except Exception as e:
        logger.error(f"Failed to get stations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stations/{station_id}", response_model=WaterStationResponse)
async def get_station(station_id: str, db: Session = Depends(get_db)):
    """Get a specific water station."""
    try:
        db_service = DatabaseService(db)
        station = db_service.get_station(station_id)
        if not station:
            raise HTTPException(status_code=404, detail="Station not found")
        return station
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get station {station_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/stations/{station_id}", response_model=WaterStationResponse)
async def update_station(
    station_id: str, station_update: WaterStationUpdate, db: Session = Depends(get_db)
):
    """Update a water station."""
    try:
        db_service = DatabaseService(db)

        update_data = station_update.model_dump(exclude_unset=True)

        station = db_service.update_station(station_id, update_data)
        if not station:
            raise HTTPException(status_code=404, detail="Station not found")
        return station
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update station {station_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/stations/{station_id}", status_code=204)
async def delete_station(station_id: str, db: Session = Depends(get_db)):
    """Delete a water station."""
    try:
        db_service = DatabaseService(db)
        station = db_service.get_station(station_id)
        if not station:
            raise HTTPException(status_code=404, detail="Station not found")

        # In a real implementation, you'd want to handle cascading deletes
        # and soft deletes rather than hard deletes
        db.delete(station)
        db.commit()

        return {"message": "Station deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete station {station_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data-points", response_model=WaterDataPointResponse, status_code=201)
async def create_data_point(
    data_point: WaterDataPointCreate, db: Session = Depends(get_db)
):
    """Create a new water data point."""
    try:
        db_service = DatabaseService(db)
        return db_service.create_data_point(data_point)
    except Exception as e:
        logger.error(f"Failed to create data point: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post(
    "/data-points/bulk", response_model=List[WaterDataPointResponse], status_code=201
)
async def create_bulk_data_points(
    bulk_data: BulkDataPointCreate, db: Session = Depends(get_db)
):
    """Create multiple water data points."""
    try:
        db_service = DatabaseService(db)
        created_points = []

        for data_point in bulk_data.data_points:
            point = db_service.create_data_point(data_point)
            created_points.append(point)

        return created_points
    except Exception as e:
        logger.error(f"Failed to create bulk data points: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/data-points", response_model=DataPointListResponse)
async def get_data_points(
    station_id: int = Query(..., description="Station ID"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    parameter: Optional[str] = Query(None, description="Filter by parameter"),
    quality_filter: Optional[str] = Query(None, description="Filter by quality flag"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    db: Session = Depends(get_db),
):
    """Get water data points with filtering."""
    try:
        from datetime import datetime

        db_service = DatabaseService(db)

        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        data_points = db_service.get_data_points(
            station_id=station_id,
            start_time=start_dt,
            end_time=end_dt,
            parameter=parameter,
            limit=limit,
        )

        if quality_filter:
            data_points = [
                dp for dp in data_points if dp.quality_flag == quality_filter
            ]

        return DataPointListResponse(
            data_points=data_points,
            total=len(data_points),
            station_id=station_id,
            parameter=parameter,
            time_range=(
                {"start": start_dt, "end": end_dt} if start_dt and end_dt else None
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get data points: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-points/latest", response_model=List[WaterDataPointResponse])
async def get_latest_data_points(
    station_id: int = Query(..., description="Station ID"),
    parameter: Optional[str] = Query(None, description="Filter by parameter"),
    db: Session = Depends(get_db),
):
    """Get latest data points for a station."""
    try:
        db_service = DatabaseService(db)
        data_points = db_service.get_latest_data(station_id, parameter)
        return data_points
    except Exception as e:
        logger.error(f"Failed to get latest data points: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stations/{station_id}/statistics", response_model=StationStatistics)
async def get_station_statistics(
    station_id: int,
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    db: Session = Depends(get_db),
):
    """Get statistical summary for a station."""
    try:
        from datetime import datetime

        db_service = DatabaseService(db)

        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        stats = db_service.get_station_statistics(
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
    quality_data: WaterQualityCreate, db: Session = Depends(get_db)
):
    """Create water quality data."""
    try:
        # Note: You'll need to implement create_quality_data in DatabaseService
        # For now, this is a placeholder
        raise HTTPException(
            status_code=501, detail="Quality data creation not yet implemented"
        )
    except Exception as e:
        logger.error(f"Failed to create quality data: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/quality", response_model=List[WaterQualityResponse])
async def get_quality_data(
    station_id: int = Query(..., description="Station ID"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    db: Session = Depends(get_db),
):
    """Get water quality data."""
    try:

        # Note: You'll need to implement get_quality_data in DatabaseService
        # For now, this is a placeholder
        raise HTTPException(
            status_code=501, detail="Quality data retrieval not yet implemented"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get quality data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
