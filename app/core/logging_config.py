"""
Logging configuration for the Water Data Platform.
"""

import logging
import logging.config
import sys
from typing import Any, Dict

import structlog

from app.core.config import settings


def configure_logging() -> None:
    """Configure application logging."""

    # Base logging configuration
    logging_config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {
                "()": "structlog.stdlib.ProcessorFormatter",
                "processor": structlog.dev.ConsoleRenderer(colors=True),
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": settings.log_level.upper(),
                "formatter": "default",
                "stream": sys.stdout,
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "detailed",
                "filename": "logs/water_dp.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "detailed",
                "filename": "logs/water_dp_errors.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            },
        },
        "loggers": {
            "app": {
                "level": settings.log_level.upper(),
                "handlers": ["console", "file"],
                "propagate": False,
            },
            "sqlalchemy": {
                "level": "WARNING",
                "handlers": ["console", "file"],
                "propagate": False,
            },
            "alembic": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
        },
        "root": {
            "level": settings.log_level.upper(),
            "handlers": ["console", "file", "error_file"],
        },
    }

    # Apply configuration
    logging.config.dictConfig(logging_config)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


# Request logging middleware
class RequestLoggingMiddleware:
    """Middleware for logging HTTP requests."""

    def __init__(self, app):
        self.app = app
        self.logger = get_logger("request")

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Log request
            self.logger.info(
                "Request started",
                method=scope["method"],
                path=scope["path"],
                query_string=scope.get("query_string", b"").decode(),
            )

            # Process request
            await self.app(scope, receive, send)

            # Log response (this is simplified - in practice you'd want to capture status)
            self.logger.info("Request completed")
        else:
            await self.app(scope, receive, send)
