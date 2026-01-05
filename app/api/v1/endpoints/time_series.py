"""
Time series API endpoints.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.time_series import (
    AggregatedTimeSeriesResponse,
    BulkTimeSeriesDataCreate,
    InterpolationRequest,
    TimeSeriesAggregation,
    TimeSeriesDataCreate,
    TimeSeriesDataResponse,
    TimeSeriesListResponse,
    TimeSeriesMetadataCreate,
    TimeSeriesMetadataListResponse,
    TimeSeriesMetadataResponse,
    TimeSeriesMetadataUpdate,
    TimeSeriesQuery,
    TimeSeriesStatistics,
)
from app.services.database_service import DatabaseService
from app.services.time_series_service import TimeSeriesService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/metadata", response_model=TimeSeriesMetadataResponse, status_code=201)
async def create_time_series_metadata(
    metadata: TimeSeriesMetadataCreate, db: Session = Depends(get_db)
):
    """Create time series metadata."""
    try:
        db_service = DatabaseService(db)
        return db_service.create_time_series_metadata(metadata)
    except Exception as e:
        logger.error(f"Failed to create time series metadata: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/metadata", response_model=TimeSeriesMetadataListResponse)
async def get_time_series_metadata(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    parameter: Optional[str] = Query(None, description="Filter by parameter"),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    db: Session = Depends(get_db),
):
    """Get time series metadata with filtering."""
    try:
        db_service = DatabaseService(db)
        metadata_list = db_service.get_time_series_metadata(
            skip=skip,
            limit=limit,
            parameter=parameter,
            source_type=source_type,
            station_id=station_id,
        )

        return TimeSeriesMetadataListResponse(
            series=metadata_list,
            total=len(metadata_list),  # Todo: implement count
            skip=skip,
            limit=limit,
        )
    except Exception as e:
        logger.error(f"Failed to get time series metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metadata/{series_id}", response_model=TimeSeriesMetadataResponse)
async def get_time_series_metadata_by_id(series_id: str, db: Session = Depends(get_db)):
    """Get specific time series metadata."""
    try:
        db_service = DatabaseService(db)
        metadata = db_service.get_time_series_metadata_by_id(series_id)
        if not metadata:
            raise HTTPException(
                status_code=404, detail=f"Time series {series_id} not found"
            )
        return metadata
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get time series metadata {series_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/metadata/{series_id}", response_model=TimeSeriesMetadataResponse)
async def update_time_series_metadata(
    series_id: str,
    metadata_update: TimeSeriesMetadataUpdate,
    db: Session = Depends(get_db),
):
    """Update time series metadata."""
    try:
        # Note: You'll need to implement update_time_series_metadata in DatabaseService
        raise HTTPException(
            status_code=501, detail="Time series metadata update not yet implemented"
        )
    except Exception as e:
        logger.error(f"Failed to update time series metadata {series_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data", response_model=TimeSeriesDataResponse, status_code=201)
async def create_time_series_data(
    data_point: TimeSeriesDataCreate, db: Session = Depends(get_db)
):
    """Create a single time series data point."""
    try:
        # Note: You'll need to implement create_time_series_data in DatabaseService
        raise HTTPException(
            status_code=501, detail="Time series data creation not yet implemented"
        )
    except Exception as e:
        logger.error(f"Failed to create time series data: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/data/bulk", response_model=List[TimeSeriesDataResponse], status_code=201)
async def create_bulk_time_series_data(
    bulk_data: BulkTimeSeriesDataCreate, db: Session = Depends(get_db)
):
    """Create multiple time series data points."""
    try:
        db_service = DatabaseService(db)
        return db_service.add_time_series_data(bulk_data.data_points)
    except Exception as e:
        logger.error(f"Failed to create bulk time series data: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/data", response_model=TimeSeriesListResponse)
async def get_time_series_data(
    series_id: str = Query(..., description="Series ID"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    quality_filter: Optional[str] = Query(None, description="Filter by quality flag"),
    include_interpolated: bool = Query(True, description="Include interpolated values"),
    include_aggregated: bool = Query(True, description="Include aggregated values"),
    db: Session = Depends(get_db),
):
    """Get time series data with filtering."""
    try:
        from datetime import datetime

        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        query = TimeSeriesQuery(
            series_id=series_id,
            start_time=start_dt,
            end_time=end_dt,
            limit=limit,
            quality_filter=quality_filter,
            include_interpolated=include_interpolated,
            include_aggregated=include_aggregated,
        )

        ts_service = TimeSeriesService(db)
        data_points = ts_service.get_time_series_data(query)

        return TimeSeriesListResponse(
            data_points=data_points,
            total=len(data_points),
            series_id=series_id,
            time_range=(
                {"start": start_dt, "end": end_dt} if start_dt and end_dt else None
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get time series data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/aggregate", response_model=AggregatedTimeSeriesResponse)
async def aggregate_time_series(
    aggregation: TimeSeriesAggregation, db: Session = Depends(get_db)
):
    """Aggregate time series data."""
    try:
        ts_service = TimeSeriesService(db)
        aggregated_points = ts_service.aggregate_time_series(aggregation)

        return AggregatedTimeSeriesResponse(
            series_id=aggregation.series_id,
            aggregation_method=aggregation.aggregation_method,
            aggregation_interval=aggregation.aggregation_interval,
            time_range={"start": aggregation.start_time, "end": aggregation.end_time},
            data_points=aggregated_points,
            total_points=len(aggregated_points),
            metadata={
                "time_zone": aggregation.time_zone,
                "include_metadata": aggregation.include_metadata,
            },
        )
    except Exception as e:
        logger.error(f"Failed to aggregate time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/interpolate", response_model=List[TimeSeriesDataResponse])
async def interpolate_time_series(
    request: InterpolationRequest, db: Session = Depends(get_db)
):
    """Interpolate missing values in time series."""
    try:
        ts_service = TimeSeriesService(db)
        interpolated_points = ts_service.interpolate_time_series(request)
        return interpolated_points
    except Exception as e:
        logger.error(f"Failed to interpolate time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/{series_id}", response_model=TimeSeriesStatistics)
async def get_time_series_statistics(
    series_id: str,
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
    db: Session = Depends(get_db),
):
    """Get comprehensive statistics for time series."""
    try:
        from datetime import datetime

        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        ts_service = TimeSeriesService(db)
        statistics = ts_service.calculate_statistics(series_id, start_dt, end_dt)
        return statistics
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to get time series statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies/{series_id}")
async def detect_anomalies(
    series_id: str,
    start_time: str = Query(..., description="Start time (ISO format)"),
    end_time: str = Query(..., description="End time (ISO format)"),
    method: str = Query("statistical", description="Anomaly detection method"),
    threshold: float = Query(3.0, description="Detection threshold"),
    db: Session = Depends(get_db),
):
    """Detect anomalies in time series data."""
    try:
        from datetime import datetime

        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)

        ts_service = TimeSeriesService(db)
        anomalies = ts_service.detect_anomalies(
            series_id, start_dt, end_dt, method, threshold
        )

        return {
            "series_id": series_id,
            "time_range": {"start": start_dt, "end": end_dt},
            "method": method,
            "threshold": threshold,
            "anomalies": anomalies,
            "total_anomalies": len(anomalies),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to detect anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/{series_id}")
async def export_time_series(
    series_id: str,
    start_time: str = Query(..., description="Start time (ISO format)"),
    end_time: str = Query(..., description="End time (ISO format)"),
    format: str = Query("csv", description="Export format (csv, json, excel)"),
    db: Session = Depends(get_db),
):
    """Export time series data."""
    try:
        from datetime import datetime

        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)

        ts_service = TimeSeriesService(db)
        exported_data = ts_service.export_time_series(
            series_id, start_dt, end_dt, format
        )

        return {
            "series_id": series_id,
            "time_range": {"start": start_dt, "end": end_dt},
            "format": format,
            "data": exported_data,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    except Exception as e:
        logger.error(f"Failed to export time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))
