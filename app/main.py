"""
Main FastAPI application.
"""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.api.v1.api import api_router
from app.core.config import settings
from app.core.database import init_db
from app.core.middleware import ErrorHandlingMiddleware

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    logger.info("Starting Water Data Platform API...")

    try:
        init_db()
        logger.info("Database initialized successfully")

        if settings.seeding:
            from app.core.database import SessionLocal
            from app.core.seeding import seed_data

            db = SessionLocal()
            try:
                seed_data(db)
            finally:
                db.close()

        # Always register system datasources (infra discovery)
        from app.core.database import SessionLocal
        from app.core.system_datasources import register_system_datasources

        db_sys = SessionLocal()
        try:
            register_system_datasources(db_sys)
        finally:
            db_sys.close()

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    yield

    logger.info("Shutting down Water Data Platform API...")


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="""
    ## Water Data Platform API

    A comprehensive backend for water data management with:

    * **Database Integration**: PostgreSQL with PostGIS for geospatial data
    * **GeoServer Integration**: Full geospatial services and layer management
    * **Time Series Processing**: Advanced analytics and data processing
    * **RESTful API**: Complete CRUD operations for all data types

    ### Features
    - 🗺️ **Geospatial Data**: Manage layers, features, and GeoServer integration
    - 📊 **Water Data**: Stations, measurements, and quality data
    - ⏰ **Time Series**: Advanced time series analysis and processing
    - 🔍 **Analytics**: Statistical analysis, anomaly detection, and aggregation

    ### Authentication
    Integrated with Keycloak. Use the **Authorize** button to authenticate.
    """,
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


app.add_middleware(ErrorHandlingMiddleware)

logger.info(f"CORS origins: {settings.cors_origins_list}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.add_middleware(
    TrustedHostMiddleware, allowed_hosts=["*"]  # Configure this properly for production
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests."""
    start_time = time.time()

    # Log request
    if request.url.path != "/health":
        logger.info(f"Request: {request.method} {request.url}")

    # Process request
    response = await call_next(request)

    # Log response
    process_time = time.time() - start_time
    if request.url.path != "/health":
        logger.info(f"Response: {response.status_code} - {process_time:.3f}s")

    return response


@app.get("/health", tags=["General"])
async def health_check():
    """
    ## Health Check

    Check the health status of the Water Data Platform API.

    Returns the current status and basic system information.
    """
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.version,
        "timestamp": time.time(),
    }


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
    - **Water Data**: `/api/v1/water-data/` - Stations, measurements, quality data
    - **Time Series**: `/api/v1/time-series/` - Time series data and analysis
    - **Geospatial**: `/api/v1/geospatial/` - Layers, features, GeoServer integration
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
            "water_data": f"{settings.api_prefix}/water-data/",
            "time_series": f"{settings.api_prefix}/time-series/",
            "geospatial": f"{settings.api_prefix}/geospatial/",
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
