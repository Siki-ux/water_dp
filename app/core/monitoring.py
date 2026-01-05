"""
Monitoring and metrics configuration for the Water Data Platform.
"""

import logging
import time
from typing import Any, Dict

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status_code"]
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)

ACTIVE_CONNECTIONS = Gauge("active_connections", "Number of active connections")

DATABASE_OPERATIONS = Counter(
    "database_operations_total", "Total database operations", ["operation", "table"]
)

GEOSERVER_OPERATIONS = Counter(
    "geoserver_operations_total", "Total GeoServer operations", ["operation", "status"]
)

TIME_SERIES_OPERATIONS = Counter(
    "time_series_operations_total",
    "Total time series operations",
    ["operation", "status"],
)

CACHE_OPERATIONS = Counter(
    "cache_operations_total", "Total cache operations", ["operation", "status"]
)


class MetricsMiddleware:
    """Middleware for collecting Prometheus metrics."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Start timing
            start_time = time.time()

            # Process request
            await self.app(scope, receive, send)

            # Calculate duration
            duration = time.time() - start_time

            # Extract request info
            method = scope.get("method", "UNKNOWN")
            path = scope.get("path", "/")

            # Record metrics
            REQUEST_DURATION.labels(method=method, endpoint=path).observe(duration)

        else:
            await self.app(scope, receive, send)


def record_database_operation(operation: str, table: str, success: bool = True):
    """Record database operation metrics."""
    DATABASE_OPERATIONS.labels(operation=operation, table=table).inc()


def record_geoserver_operation(operation: str, success: bool = True):
    """Record GeoServer operation metrics."""
    status = "success" if success else "error"
    GEOSERVER_OPERATIONS.labels(operation=operation, status=status).inc()


def record_time_series_operation(operation: str, success: bool = True):
    """Record time series operation metrics."""
    status = "success" if success else "error"
    TIME_SERIES_OPERATIONS.labels(operation=operation, status=status).inc()


def record_cache_operation(operation: str, success: bool = True):
    """Record cache operation metrics."""
    status = "success" if success else "error"
    CACHE_OPERATIONS.labels(operation=operation, status=status).inc()


def get_metrics() -> str:
    """Get Prometheus metrics in text format."""
    return generate_latest()


class HealthChecker:
    """Health check service for monitoring system components."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def check_database(self, db) -> Dict[str, Any]:
        """Check database connectivity."""
        try:
            # Simple query to test connection
            db.execute("SELECT 1")
            return {
                "status": "healthy",
                "response_time_ms": 0,  # You could measure this
                "details": "Database connection successful",
            }
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "details": "Database connection failed",
            }

    async def check_redis(self, redis_client) -> Dict[str, Any]:
        """Check Redis connectivity."""
        try:
            # Simple ping to test connection
            await redis_client.ping()
            return {"status": "healthy", "details": "Redis connection successful"}
        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "details": "Redis connection failed",
            }

    async def check_geoserver(self, geoserver_service) -> Dict[str, Any]:
        """Check GeoServer connectivity."""
        try:
            is_connected = geoserver_service.test_connection()
            if is_connected:
                return {
                    "status": "healthy",
                    "details": "GeoServer connection successful",
                }
            else:
                return {"status": "unhealthy", "details": "GeoServer connection failed"}
        except Exception as e:
            self.logger.error(f"GeoServer health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "details": "GeoServer connection failed",
            }

    async def get_system_health(
        self, db=None, redis_client=None, geoserver_service=None
    ) -> Dict[str, Any]:
        """Get overall system health."""
        health_status = {
            "overall_status": "healthy",
            "timestamp": time.time(),
            "components": {},
        }

        # Check database
        if db:
            db_health = await self.check_database(db)
            health_status["components"]["database"] = db_health
            if db_health["status"] != "healthy":
                health_status["overall_status"] = "degraded"

        # Check Redis
        if redis_client:
            redis_health = await self.check_redis(redis_client)
            health_status["components"]["redis"] = redis_health
            if redis_health["status"] != "healthy":
                health_status["overall_status"] = "degraded"

        # Check GeoServer
        if geoserver_service:
            geoserver_health = await self.check_geoserver(geoserver_service)
            health_status["components"]["geoserver"] = geoserver_health
            if geoserver_health["status"] != "healthy":
                health_status["overall_status"] = "degraded"

        return health_status


# Global health checker instance
health_checker = HealthChecker()
