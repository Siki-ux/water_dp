"""
Main FastAPI application.
"""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.api.v1.api import api_router
from app.core.config import settings
from app.core.constants import API_DESCRIPTION
from app.core.database import init_db
from app.core.logging_config import setup_logging
from app.core.middleware import ErrorHandlingMiddleware, LoggingMiddleware

# Setup Centralized Logging
setup_logging()

# Note: No need for BasicConfig or getLogger here, logging_config handles it.
# But we can still get a logger if we want.

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    logger.info("Starting Water Data Platform API...")

    # Initialize startup state
    app.state.startup_complete = False

    try:
        # init_db()
        # logger.info("Database initialized successfully")
        # Migrations are handled by the entrypoint script run_migrations.py
        # no longer calling init_db() here to avoid conflicts with Alembic
        logger.info("Application starting (migrations assumed complete)")

        # Always register system datasources (infra discovery)
        from app.core.database import SessionLocal
        from app.core.system_datasources import register_system_datasources

        database_session = SessionLocal()
        try:
            register_system_datasources(database_session)
        finally:
            database_session.close()

        app.state.startup_complete = True
        logger.info("Application is now fully healthy and ready.")

    except Exception as error:
        logger.error(f"Failed to initialize database: {error}")
        raise

    yield

    logger.info("Shutting down Water Data Platform API...")


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description=API_DESCRIPTION,
    openapi_url=f"{settings.api_prefix}/openapi.json" if settings.debug else None,
    docs_url=f"{settings.api_prefix}/docs" if settings.debug else None,
    redoc_url=f"{settings.api_prefix}/redoc" if settings.debug else None,
    lifespan=lifespan,
    contact={
        "name": "Water Data Platform Support",
        "email": "support@waterdataplatform.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    servers=[
        {"url": "http://localhost:8000", "description": "Development server"},
        {
            "url": "http://hydro-portal.westeurope.cloudapp.azure.com:8000",
            "description": "Azure VM server",
        },
        {
            "url": "https://api.waterdataplatform.com",
            "description": "Production server",
        },
    ],
)


@app.get("/health", tags=["General"])
async def health_check(response: Response):
    """
    ## Health Check

    Check the health status of the Water Data Platform API.

    Returns:
    - **200 OK**: Service is healthy and fully seeded.
    - **503 Service Unavailable**: Service is still initializing.
    """
    if not getattr(app.state, "startup_complete", False):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "initializing",
            "message": "Seeding data in progress...",
            "app_name": settings.app_name,
            "timestamp": time.time(),
        }

    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.version,
        "timestamp": time.time(),
    }


# Middleware Stack (Executed Top to Bottom)


app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ErrorHandlingMiddleware)
app.add_middleware(LoggingMiddleware)


@app.get("/docs", tags=["General"])
async def redirect_to_swagger():
    """
    ## API Documentation

    Redirects to the Swagger UI documentation.

    This endpoint provides easy access to the interactive API documentation.
    """
    from fastapi.responses import RedirectResponse

    return RedirectResponse(url=f"{settings.api_prefix}/docs")


@app.get("/", tags=["General"])
async def root():
    """
    ## Welcome to Water Data Platform API

    This is the main entry point for the Water Data Platform API.

    ### Quick Links:
    - 📚 **API Documentation**: [Swagger UI](/api/v1/docs)
    - 📖 **Alternative Docs**: [ReDoc](/api/v1/redoc)
    - 🔍 **OpenAPI Schema**: [JSON Schema](/api/v1/openapi.json)
    - ❤️ **Health Check**: [Health Status](/health)

    ### Available Endpoints:
    - **Sensors**: `/api/v1/things/` - Sensor management (replacement for water-data)
    - **Geospatial**: `/api/v1/geospatial/` - Layers, features, GeoServer integration
    - **Projects**: `/api/v1/projects/` - Project management
    """
    return {
        "message": "Water Data Platform API",
        "version": settings.version,
        "status": "running",
        "documentation": {
            "swagger_ui": f"{settings.api_prefix}/docs",
            "redoc": f"{settings.api_prefix}/redoc",
            "openapi_json": f"{settings.api_prefix}/openapi.json",
        },
        "endpoints": {
            "sensors": f"{settings.api_prefix}/things/",
            "geospatial": f"{settings.api_prefix}/geospatial/",
            "projects": f"{settings.api_prefix}/projects/",
            "health": "/health",
        },
        "health_url": "/health",
    }


app.include_router(api_router, prefix=settings.api_prefix)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
