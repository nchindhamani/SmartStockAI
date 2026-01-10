"""
Enhanced Error Handling Module

Provides centralized error handling with custom exception classes,
structured error responses, and comprehensive logging.
"""

from typing import Optional, Dict, Any
from enum import Enum
import traceback
import logging
from datetime import datetime

# Configure error logger
error_logger = logging.getLogger("smartstock.errors")
error_logger.setLevel(logging.ERROR)


class ErrorCode(str, Enum):
    """Standard error codes for SmartStock AI."""
    # API Errors (1000-1999)
    INVALID_REQUEST = "1000"
    MISSING_PARAMETER = "1001"
    INVALID_TICKER = "1002"
    TICKER_NOT_FOUND = "1003"
    
    # Agent Errors (2000-2999)
    AGENT_EXECUTION_FAILED = "2000"
    AGENT_TIMEOUT = "2001"
    AGENT_TOOL_ERROR = "2002"
    
    # Data Errors (3000-3999)
    DATABASE_CONNECTION_ERROR = "3000"
    DATABASE_QUERY_ERROR = "3001"
    DATA_NOT_FOUND = "3002"
    DATA_INGESTION_ERROR = "3003"
    DATA_VALIDATION_ERROR = "3004"
    
    # External API Errors (4000-4999)
    FMP_API_ERROR = "4000"
    FMP_API_RATE_LIMIT = "4001"
    FMP_API_TIMEOUT = "4002"
    FMP_API_AUTH_ERROR = "4003"
    
    # System Errors (5000-5999)
    INTERNAL_SERVER_ERROR = "5000"
    SERVICE_UNAVAILABLE = "5001"
    CONFIGURATION_ERROR = "5002"


class SmartStockError(Exception):
    """Base exception for all SmartStock AI errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        self.cause = cause
        self.timestamp = datetime.now().isoformat()
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for JSON response."""
        error_dict = {
            "error": {
                "code": self.error_code.value,
                "message": self.message,
                "status_code": self.status_code,
                "timestamp": self.timestamp,
            }
        }
        
        if self.details:
            error_dict["error"]["details"] = self.details
        
        if self.cause and isinstance(self.cause, Exception):
            error_dict["error"]["cause"] = str(self.cause)
        
        return error_dict
    
    def log(self, context: Optional[str] = None):
        """Log the error with context."""
        log_msg = f"[{self.error_code.value}] {self.message}"
        if context:
            log_msg = f"[{context}] {log_msg}"
        
        error_logger.error(
            log_msg,
            extra={
                "error_code": self.error_code.value,
                "status_code": self.status_code,
                "details": self.details,
                "traceback": traceback.format_exc() if self.cause else None,
            }
        )


class APIError(SmartStockError):
    """API-related errors."""
    pass


class ValidationError(APIError):
    """Request validation errors."""
    
    def __init__(self, message: str, field: Optional[str] = None, **kwargs):
        details = kwargs.pop("details", {})
        if field:
            details["field"] = field
        super().__init__(
            message,
            ErrorCode.INVALID_REQUEST,
            status_code=400,
            details=details,
            **kwargs
        )


class NotFoundError(APIError):
    """Resource not found errors."""
    
    def __init__(self, resource: str, identifier: Optional[str] = None, **kwargs):
        message = f"{resource} not found"
        if identifier:
            message += f": {identifier}"
        super().__init__(
            message,
            ErrorCode.DATA_NOT_FOUND,
            status_code=404,
            details={"resource": resource, "identifier": identifier},
            **kwargs
        )


class AgentError(SmartStockError):
    """Agent execution errors."""
    
    def __init__(self, message: str, tool: Optional[str] = None, **kwargs):
        details = kwargs.pop("details", {})
        if tool:
            details["tool"] = tool
        super().__init__(
            message,
            ErrorCode.AGENT_EXECUTION_FAILED,
            status_code=500,
            details=details,
            **kwargs
        )


class DatabaseError(SmartStockError):
    """Database-related errors."""
    
    def __init__(self, message: str, query: Optional[str] = None, **kwargs):
        details = kwargs.pop("details", {})
        if query:
            # Don't log full query to avoid exposing sensitive data
            details["query_hash"] = str(hash(query))[:8]
        super().__init__(
            message,
            ErrorCode.DATABASE_CONNECTION_ERROR,
            status_code=503,
            details=details,
            **kwargs
        )


class FMPAPIError(SmartStockError):
    """FMP API-related errors."""
    
    def __init__(self, message: str, status_code: int = 500, retry_after: Optional[int] = None, **kwargs):
        details = kwargs.pop("details", {})
        if retry_after:
            details["retry_after"] = retry_after
        
        # Determine error code based on status
        if status_code == 429:
            error_code = ErrorCode.FMP_API_RATE_LIMIT
            status_code = 429
        elif status_code == 401 or status_code == 403:
            error_code = ErrorCode.FMP_API_AUTH_ERROR
            status_code = 401
        elif status_code >= 500:
            error_code = ErrorCode.FMP_API_TIMEOUT
            status_code = 502
        else:
            error_code = ErrorCode.FMP_API_ERROR
            status_code = 502
        
        super().__init__(
            message,
            error_code,
            status_code=status_code,
            details=details,
            **kwargs
        )


def handle_exception(error: Exception, context: Optional[str] = None) -> SmartStockError:
    """
    Convert a generic exception to a SmartStockError.
    
    Args:
        error: The exception to handle
        context: Optional context for logging
        
    Returns:
        SmartStockError: A properly formatted SmartStock error
    """
    # If it's already a SmartStockError, just log and return
    if isinstance(error, SmartStockError):
        error.log(context)
        return error
    
    # Handle specific exception types
    if isinstance(error, ValueError):
        return ValidationError(str(error), cause=error)
    elif isinstance(error, KeyError):
        return ValidationError(f"Missing required field: {error}", cause=error)
    elif isinstance(error, AttributeError):
        return SmartStockError(
            f"Invalid attribute: {str(error)}",
            ErrorCode.INVALID_REQUEST,
            status_code=400,
            cause=error
        )
    else:
        # Generic internal server error
        return SmartStockError(
            f"Internal server error: {str(error)}",
            ErrorCode.INTERNAL_SERVER_ERROR,
            status_code=500,
            cause=error
        )


def create_error_response(error: SmartStockError) -> Dict[str, Any]:
    """
    Create a standardized error response.
    
    Args:
        error: The SmartStockError to convert
        
    Returns:
        dict: Standardized error response
    """
    error.log()
    return error.to_dict()

