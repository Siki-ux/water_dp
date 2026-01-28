"""
Custom exceptions for the Water Data Platform.
"""

from typing import Any, Dict, Optional

from fastapi import HTTPException, status


class AppException(Exception):
    """Base exception for Water Data Platform."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class DatabaseException(AppException):
    """Database operation exception."""

    pass


class GeoServerException(AppException):
    """GeoServer operation exception."""

    pass


class TimeSeriesException(AppException):
    """Time series processing exception."""

    pass


class ValidationException(AppException):
    """Data validation exception."""

    pass


class ConfigurationException(AppException):
    """Configuration exception."""

    pass


class AuthenticationException(AppException):
    """Authentication exception."""

    pass


class AuthorizationException(AppException):
    """Authorization exception."""

    pass


class ResourceNotFoundException(AppException):
    """Resource not found exception."""

    pass


class ConflictException(AppException):
    """Resource conflict exception."""

    pass


class RateLimitException(AppException):
    """Rate limit exceeded exception."""

    pass


# HTTP Exception mappings
def create_http_exception(exc: AppException) -> HTTPException:
    """Convert custom exceptions to HTTP exceptions."""

    if isinstance(exc, ResourceNotFoundException):
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, ValidationException):
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, ConflictException):
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, AuthenticationException):
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, AuthorizationException):
        return HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, RateLimitException):
        return HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, DatabaseException):
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=exc.message or "Database operation failed",
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, GeoServerException):
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GeoServer operation failed",
            headers={"X-Error-Details": str(exc.details)},
        )

    elif isinstance(exc, TimeSeriesException):
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=exc.message or "Time series processing failed",
            headers={"X-Error-Details": str(exc.details)},
        )

    else:
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=exc.message,
            headers={"X-Error-Details": str(exc.details)},
        )


# Exception handlers
def handle_water_data_platform_exception(
    exc: AppException,
) -> HTTPException:
    """Handle Water Data Platform exceptions."""
    return create_http_exception(exc)


def handle_validation_error(exc: ValueError) -> HTTPException:
    """Handle validation errors."""
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"Validation error: {str(exc)}",
    )


def handle_database_error(exc: Exception) -> HTTPException:
    """Handle database errors."""
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Database operation failed",
        headers={"X-Error-Type": "database_error"},
    )


def handle_geoserver_error(exc: Exception) -> HTTPException:
    """Handle GeoServer errors."""
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="GeoServer operation failed",
        headers={"X-Error-Type": "geoserver_error"},
    )
