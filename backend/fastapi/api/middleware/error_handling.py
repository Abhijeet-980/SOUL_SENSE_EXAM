"""
Error Handling Middleware

Provides centralized error handling for the API:
- Catches all exceptions
- Maps to standardized error responses
- Adds correlation IDs
- Logs errors appropriately
- Handles nested exceptions

Usage:
    from api.middleware.error_handling import setup_error_handling
    
    app = FastAPI()
    setup_error_handling(app)
"""

import uuid
import traceback
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime, timezone

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..utils.error_catalog import (
    DomainError,
    ErrorCatalog,
    ErrorSeverity,
    internal_error,
    validation_failed,
    not_found,
    catalog
)
from ..schemas.error_response import (
    ErrorResponse,
    ValidationErrorResponse,
    FieldError
)


class ErrorHandlingMiddleware:
    """
    Middleware for centralized error handling.
    
    Catches and standardizes all errors before they reach the client.
    """
    
    def __init__(
        self,
        app: FastAPI,
        debug: bool = False,
        log_errors: bool = True
    ):
        self.app = app
        self.debug = debug
        self.log_errors = log_errors
        self.catalog = catalog
        self._setup_exception_handlers()
    
    def _setup_exception_handlers(self) -> None:
        """Register exception handlers with FastAPI."""
        
        @self.app.exception_handler(DomainError)
        async def handle_domain_error(request: Request, error: DomainError):
            return self._handle_domain_error(request, error)
        
        @self.app.exception_handler(RequestValidationError)
        async def handle_validation_error(request: Request, error: RequestValidationError):
            return self._handle_validation_error(request, error)
        
        @self.app.exception_handler(StarletteHTTPException)
        async def handle_http_exception(request: Request, error: StarletteHTTPException):
            return self._handle_http_exception(request, error)
        
        @self.app.exception_handler(Exception)
        async def handle_generic_exception(request: Request, error: Exception):
            return self._handle_generic_exception(request, error)
    
    def _get_correlation_id(self, request: Request) -> str:
        """Extract or generate correlation ID from request."""
        # Check headers for existing correlation ID
        correlation_id = request.headers.get("X-Correlation-ID")
        if correlation_id:
            return correlation_id
        
        # Check if already set in request state
        if hasattr(request.state, "correlation_id"):
            return request.state.correlation_id
        
        # Generate new correlation ID
        correlation_id = str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        return correlation_id
    
    def _handle_domain_error(
        self,
        request: Request,
        error: DomainError
    ) -> JSONResponse:
        """Handle DomainError exceptions."""
        correlation_id = self._get_correlation_id(request)
        
        # Ensure error has correlation ID
        if not error.correlation_id:
            error.correlation_id = correlation_id
        
        # Log error if enabled
        if self.log_errors:
            self._log_error(request, error)
        
        # Build response
        response_data = error.to_dict(include_traceback=self.debug)
        
        # Add retry-after header for rate limits
        headers = {}
        if error.retryable and error.definition and error.definition.retry_after:
            headers["Retry-After"] = str(error.definition.retry_after)
        
        return JSONResponse(
            status_code=error.http_status,
            content=response_data,
            headers=headers
        )
    
    def _handle_validation_error(
        self,
        request: Request,
        error: RequestValidationError
    ) -> JSONResponse:
        """Handle FastAPI validation errors."""
        correlation_id = self._get_correlation_id(request)
        
        # Convert validation errors to field errors
        field_errors = []
        for err in error.errors():
            field_path = ".".join(str(loc) for loc in err["loc"])
            field_errors.append({
                "field": field_path,
                "message": err["msg"],
                "code": err.get("type", "VALIDATION_ERROR"),
                "value": str(err.get("input", ""))[:100]  # Limit value length
            })
        
        # Create domain error
        domain_error = validation_failed(
            message="Request validation failed",
            field_errors=field_errors,
            correlation_id=correlation_id
        )
        
        if self.log_errors:
            self._log_error(request, domain_error)
        
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=domain_error.to_dict()
        )
    
    def _handle_http_exception(
        self,
        request: Request,
        error: StarletteHTTPException
    ) -> JSONResponse:
        """Handle Starlette HTTP exceptions."""
        correlation_id = self._get_correlation_id(request)
        
        # Map common status codes to error codes
        code_mapping = {
            400: "GLB002",
            401: "AUTH005",
            403: "WFK005",
            404: "RES001",
            405: "GLB002",
            409: "RES002",
            422: "VAL001",
            429: "RAT001",
            500: "INT001",
            503: "INT003",
        }
        
        code = code_mapping.get(error.status_code, "GLB001")
        
        # Get error details if dict
        details = None
        message = error.detail
        if isinstance(error.detail, dict):
            details = error.detail.get("details")
            message = error.detail.get("message", str(error.detail))
        
        domain_error = DomainError(
            code=code,
            message=message,
            details=details,
            correlation_id=correlation_id
        )
        
        if self.log_errors:
            self._log_error(request, domain_error)
        
        return JSONResponse(
            status_code=error.status_code,
            content=domain_error.to_dict()
        )
    
    def _handle_generic_exception(
        self,
        request: Request,
        error: Exception
    ) -> JSONResponse:
        """Handle unexpected exceptions."""
        correlation_id = self._get_correlation_id(request)
        
        # Create internal error
        domain_error = internal_error(
            message="An unexpected error occurred",
            cause=error,
            correlation_id=correlation_id
        )
        
        # Always log unexpected errors
        self._log_error(request, domain_error, level="error")
        
        # In debug mode, include traceback
        response_data = domain_error.to_dict(include_traceback=self.debug)
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=response_data
        )
    
    def _log_error(
        self,
        request: Request,
        error: DomainError,
        level: str = "warning"
    ) -> None:
        """Log error with context."""
        import logging
        
        logger = logging.getLogger("api.errors")
        
        log_data = {
            "correlation_id": error.correlation_id,
            "error_code": error.code,
            "error_message": error.message,
            "error_category": error.category.value,
            "error_severity": error.severity.value,
            "http_status": error.http_status,
            "method": request.method,
            "path": request.url.path,
            "client_ip": request.client.host if request.client else None,
        }
        
        if error.cause:
            log_data["original_error"] = str(error.cause)
            log_data["original_error_type"] = type(error.cause).__name__
        
        # Log at appropriate level based on severity
        if error.severity == ErrorSeverity.CRITICAL:
            logger.critical(f"Critical error: {log_data}", extra=log_data)
        elif error.severity == ErrorSeverity.ERROR:
            logger.error(f"Error: {log_data}", extra=log_data)
        elif error.severity == ErrorSeverity.WARNING:
            logger.warning(f"Warning: {log_data}", extra=log_data)
        else:
            logger.info(f"Error: {log_data}", extra=log_data)


def setup_error_handling(
    app: FastAPI,
    debug: bool = False,
    log_errors: bool = True
) -> ErrorHandlingMiddleware:
    """
    Set up centralized error handling for a FastAPI application.
    
    Args:
        app: FastAPI application instance
        debug: Whether to include debug information (tracebacks) in responses
        log_errors: Whether to log errors
        
    Returns:
        Configured ErrorHandlingMiddleware instance
        
    Example:
        from fastapi import FastAPI
        from api.middleware.error_handling import setup_error_handling
        
        app = FastAPI()
        setup_error_handling(app, debug=False)
    """
    return ErrorHandlingMiddleware(app, debug=debug, log_errors=log_errors)


# Correlation ID middleware
class CorrelationIdMiddleware:
    """
    Middleware to ensure all requests have a correlation ID.
    
    Extracts from header or generates new one.
    Adds to response headers for client tracking.
    """
    
    def __init__(self, app: FastAPI, header_name: str = "X-Correlation-ID"):
        self.app = app
        self.header_name = header_name
        self._setup_middleware()
    
    def _setup_middleware(self) -> None:
        """Register middleware with FastAPI."""
        
        @self.app.middleware("http")
        async def add_correlation_id(request: Request, call_next):
            # Get or generate correlation ID
            correlation_id = request.headers.get(self.header_name)
            if not correlation_id:
                correlation_id = str(uuid.uuid4())
            
            # Store in request state
            request.state.correlation_id = correlation_id
            
            # Process request
            response = await call_next(request)
            
            # Add to response headers
            response.headers[self.header_name] = correlation_id
            
            return response


def setup_correlation_ids(app: FastAPI, header_name: str = "X-Correlation-ID") -> CorrelationIdMiddleware:
    """
    Set up correlation ID tracking for all requests.
    
    Args:
        app: FastAPI application instance
        header_name: Header name for correlation ID
        
    Returns:
        Configured CorrelationIdMiddleware instance
    """
    return CorrelationIdMiddleware(app, header_name=header_name)
