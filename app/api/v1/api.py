"""
API v1 router configuration.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth,
    dashboards,
    geospatial,
    projects,
    time_series,
    water_data,
)

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(water_data.router, prefix="/water-data", tags=["water-data"])

api_router.include_router(
    time_series.router, prefix="/time-series", tags=["time-series"]
)

api_router.include_router(geospatial.router, prefix="/geospatial", tags=["geospatial"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(dashboards.router, prefix="/dashboards", tags=["dashboards"])
