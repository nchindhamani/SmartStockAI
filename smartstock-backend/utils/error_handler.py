"""
FastAPI Error Handler Middleware

Provides centralized error handling for FastAPI endpoints with
proper HTTP status codes and structured error responses.
"""

from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import logging
import traceback

from utils.errors import (
    SmartStockError,
    handle_exception,
    create_error_response,
    ErrorCode
)

logger = logging.getLogger("smartstock.api")


async def smartstock_exception_handler(request: Request, exc: SmartStockError) -> JSONResponse:
    """Handle SmartStock custom exceptions."""
    error_response = create_error_response(exc)
    
    logger.error(
        f"SmartStock Error: {exc.error_code.value} - {exc.message}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "error_code": exc.error_code.value,
            "status_code": exc.status_code,
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=error_response
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle FastAPI validation errors."""
    errors = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"])
        errors.append({
            "field": field,
            "message": error["msg"],
            "type": error["type"]
        })
    
    logger.warning(
        f"Validation Error: {len(errors)} field(s) invalid",
        extra={
            "path": request.url.path,
            "method": request.method,
            "errors": errors,
        }
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": ErrorCode.INVALID_REQUEST.value,
                "message": "Request validation failed",
                "status_code": 422,
                "details": {
                    "validation_errors": errors
                }
            }
        }
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Handle standard HTTP exceptions."""
    logger.warning(
        f"HTTP Exception: {exc.status_code} - {exc.detail}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "status_code": exc.status_code,
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": ErrorCode.INVALID_REQUEST.value,
                "message": exc.detail,
                "status_code": exc.status_code,
            }
        }
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions."""
    smartstock_error = handle_exception(exc, context=f"{request.method} {request.url.path}")
    
    logger.error(
        f"Unexpected Exception: {type(exc).__name__} - {str(exc)}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "exception_type": type(exc).__name__,
            "traceback": traceback.format_exc(),
        },
        exc_info=True
    )
    
    error_response = create_error_response(smartstock_error)
    
    # Don't expose internal error details in production
    if error_response["error"]["code"] == ErrorCode.INTERNAL_SERVER_ERROR.value:
        error_response["error"]["message"] = "An internal server error occurred"
        error_response["error"]["details"] = {}
    
    return JSONResponse(
        status_code=smartstock_error.status_code,
        content=error_response
    )

