"""
API v1 router configuration.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    alerts,
    auth,
    bulk,
    computations,
    dashboards,
    datasources,
    geospatial,
    groups,
    projects,
    simulator,
    things,
    parsers,
)

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(groups.router, prefix="/groups", tags=["groups"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(things.router, prefix="/things", tags=["sensors"])
api_router.include_router(parsers.router, prefix="/parsers", tags=["parsers"])
api_router.include_router(geospatial.router, prefix="/geospatial", tags=["geospatial"])
api_router.include_router(
    computations.router, prefix="/computations", tags=["computations"]
)
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_router.include_router(simulator.router, tags=["simulator"])
api_router.include_router(dashboards.router, prefix="/dashboards", tags=["dashboards"])
api_router.include_router(bulk.router, prefix="/bulk", tags=["bulk"])
api_router.include_router(datasources.router, tags=["datasources"])
