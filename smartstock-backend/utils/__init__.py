"""
Utility modules for SmartStock AI.

This package contains utility modules for error handling, logging, and other
common functionality.
"""

from utils.errors import (
    SmartStockError,
    APIError,
    ValidationError,
    NotFoundError,
    AgentError,
    DatabaseError,
    FMPAPIError,
    ErrorCode,
    handle_exception,
    create_error_response
)

from utils.error_handler import (
    smartstock_exception_handler,
    validation_exception_handler,
    http_exception_handler,
    generic_exception_handler
)

__all__ = [
    "SmartStockError",
    "APIError",
    "ValidationError",
    "NotFoundError",
    "AgentError",
    "DatabaseError",
    "FMPAPIError",
    "ErrorCode",
    "handle_exception",
    "create_error_response",
    "smartstock_exception_handler",
    "validation_exception_handler",
    "http_exception_handler",
    "generic_exception_handler",
]

