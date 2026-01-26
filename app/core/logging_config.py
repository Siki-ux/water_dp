import logging
import logging.config
import sys

from app.core.config import settings


class RequestIdFilter(logging.Filter):
    """
    Filter to inject request_id into log records.
    Relies on contextvar set by middleware.
    """

    def filter(self, record):
        from app.core.middleware import request_id_context

        record.request_id = request_id_context.get() or "system"
        return True


class MockExternalHandler(logging.Handler):
    """
    Mock handler for external logging services (e.g., Azure Monitor, AWS CloudWatch).
    """

    def emit(self, record):
        # In a real implementation, this would push logs to an external service.
        # For now, we just formatted it but don't output to avoid duplicate console noise,
        # or we could print a specific prefix to show it's working.
        try:
            self.format(record)
            # print(f"[EXTERNAL HOOK] {msg}") # Uncomment to verify hook
        except Exception:
            self.handleError(record)


def setup_logging():
    """
    Configure logging using logging.dictConfig.
    """
    handlers = ["console"]
    if settings.enable_external_logging:
        handlers.append("external_mock")

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"request_id": {"()": RequestIdFilter}},
        "formatters": {
            "console": {
                "format": "%(asctime)s - %(levelname)s - [%(request_id)s] - %(name)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(levelname)s %(request_id)s %(name)s %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "console" if settings.log_format == "console" else "json",
                "filters": ["request_id"],
                "level": settings.log_level.upper(),
            },
            "external_mock": {
                "class": "app.core.logging_config.MockExternalHandler",
                "formatter": "json",
                "filters": ["request_id"],
                "level": "INFO",
            },
        },
        "loggers": {
            "root": {
                "handlers": handlers,
                "level": settings.log_level.upper(),
                "propagate": False,
            },
            "app": {
                "handlers": handlers,
                "level": settings.log_level.upper(),
                "propagate": False,
            },
            "uvicorn": {"handlers": handlers, "level": "INFO", "propagate": False},
            "uvicorn.access": {
                "handlers": handlers,
                "level": "INFO",
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "handlers": handlers,
                "level": settings.sqlalchemy_log_level.upper(),
                "propagate": False,
            },
            "fastapi": {
                "handlers": handlers,
                "level": settings.log_level.upper(),
                "propagate": False,
            },
        },
    }

    try:
        # If using json formatter, ensure library is installed or fallback
        if settings.log_format == "json":
            try:
                import pythonjsonlogger  # noqa: F401
            except ImportError:
                print(
                    "WARNING: python-json-logger not found. Falling back to console format."
                )
                logging_config["handlers"]["console"]["formatter"] = "console"
                if "json" in logging_config["formatters"]:
                    del logging_config["formatters"]["json"]

        logging.config.dictConfig(logging_config)
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        # Fallback basic config
        logging.basicConfig(level=logging.INFO)
