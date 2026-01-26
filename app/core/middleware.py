import logging
import time
import uuid
from contextvars import ContextVar

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.exceptions import AppException, create_http_exception

logger = logging.getLogger(__name__)

# Context Variable for Request ID (accessed by logging filter)
request_id_context = ContextVar("request_id", default=None)


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle request logging and context management.
    Should run BEFORE ErrorHandlingMiddleware to ensure context is set.
    """

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # 1. Generate or Extract Request ID
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request_id_context.set(request_id)

        # 2. Log Request Start
        # Skip health check noise usually, but let's keep it minimal
        if request.url.path != "/health":
            logger.info(f"Request: {request.method} {request.url.path}")

        try:
            response = await call_next(request)

            # 3. Log Response
            process_time = time.time() - start_time
            if request.url.path != "/health":
                logger.info(f"Response: {response.status_code} - {process_time:.3f}s")

            response.headers["X-Request-ID"] = request_id
            return response

        except Exception:
            # If an exception bubbles up here (missed by ErrorMiddleware?), logs are handled there.
            # We just ensure we don't crash.
            raise


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle exceptions globally and provide structured error responses.
    """

    async def dispatch(self, request: Request, call_next):
        # reuse context request_id if available, else new
        request_id = request_id_context.get() or str(uuid.uuid4())

        try:
            response = await call_next(request)
            return response

        except AppException as exc:
            # Handle Application-specific known exceptions
            http_exc = create_http_exception(exc)

            error_content = {
                "error": {
                    "code": exc.__class__.__name__,
                    "message": http_exc.detail,
                    "details": exc.details,
                    "request_id": request_id,
                    "status_code": http_exc.status_code,
                }
            }

            # Log warnings for expected app errors
            # Request ID is automatically properly stamped by Filter now!
            logger.warning(
                f"Application Error: {exc.message} ({exc.__class__.__name__})"
            )

            return JSONResponse(
                status_code=http_exc.status_code,
                content=error_content,
                headers={"X-Request-ID": request_id, **(http_exc.headers or {})},
            )

        except StarletteHTTPException as exc:
            # Handle standard HTTPExceptions (404, 401, etc. raised by FastAPI)
            error_content = {
                "error": {
                    "code": "HTTPException",
                    "message": exc.detail,
                    "details": None,
                    "request_id": request_id,
                    "status_code": exc.status_code,
                }
            }

            return JSONResponse(
                status_code=exc.status_code,
                content=error_content,
                headers={"X-Request-ID": request_id},
            )

        except RequestValidationError as exc:
            # Handle Pydantic Validation Errors (422)
            error_content = {
                "error": {
                    "code": "ValidationError",
                    "message": "Data validation failed",
                    "details": exc.errors(),
                    "request_id": request_id,
                    "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                }
            }

            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content=error_content,
                headers={"X-Request-ID": request_id},
            )

        except Exception as exc:
            # Handle unexpected exceptions
            logger.error(f"Unhandled exception: {exc}", exc_info=True)

            error_content = {
                "error": {
                    "code": "InternalServerException",
                    "message": "An unexpected error occurred.",
                    "details": (
                        str(exc)
                        if logging.getLogger().isEnabledFor(logging.DEBUG)
                        else None
                    ),
                    "request_id": request_id,
                    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                }
            }

            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=error_content,
                headers={"X-Request-ID": request_id},
            )
