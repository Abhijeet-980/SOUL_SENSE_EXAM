"""
Unified Domain Error Catalog and Mapping

Provides a centralized registry for all domain errors with standardized:
- Error codes (machine-readable)
- HTTP status code mapping
- Error messages (human-readable)
- Structured JSON schema for responses
- Correlation ID support for request tracking

Example:
    from api.utils.error_catalog import ErrorCatalog, DomainError
    
    # Register errors
    catalog = ErrorCatalog()
    
    # Raise domain errors
    raise DomainError(
        code=ErrorCode.VALIDATION_ERROR,
        message="Invalid input data",
        details={"field": "email"}
    )
    
    # Convert to HTTP response
    response = catalog.to_http_response(error, correlation_id="req-123")
"""

import uuid
import traceback
from typing import Any, Dict, List, Optional, Type, Union
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field, asdict
from fastapi import status


class ErrorSeverity(str, Enum):
    """Error severity levels for monitoring and alerting."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    """Error categories for grouping and filtering."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    VALIDATION = "validation"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    RATE_LIMIT = "rate_limit"
    INTERNAL = "internal"
    EXTERNAL = "external"
    CONFIGURATION = "configuration"
    SECURITY = "security"


@dataclass
class ErrorDefinition:
    """
    Definition of an error type in the catalog.
    
    Attributes:
        code: Machine-readable error code (e.g., "AUTH001")
        message: Default human-readable message
        http_status: HTTP status code to return
        category: Error category for grouping
        severity: Severity level for monitoring
        description: Detailed description for documentation
        retryable: Whether client should retry the request
        retry_after: Suggested retry delay in seconds (if retryable)
    """
    code: str
    message: str
    http_status: int = status.HTTP_400_BAD_REQUEST
    category: ErrorCategory = ErrorCategory.INTERNAL
    severity: ErrorSeverity = ErrorSeverity.ERROR
    description: str = ""
    retryable: bool = False
    retry_after: Optional[int] = None


# Centralized Error Catalog Definitions
ERROR_CATALOG: Dict[str, ErrorDefinition] = {
    # ==========================================================================
    # Authentication Errors (AUTH)
    # ==========================================================================
    "AUTH001": ErrorDefinition(
        code="AUTH001",
        message="Invalid credentials provided",
        http_status=status.HTTP_401_UNAUTHORIZED,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.WARNING,
        description="Username or password is incorrect",
    ),
    "AUTH002": ErrorDefinition(
        code="AUTH002",
        message="Account is locked due to too many failed attempts",
        http_status=status.HTTP_403_FORBIDDEN,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.WARNING,
        description="Account temporarily locked for security",
        retryable=True,
        retry_after=3600,
    ),
    "AUTH003": ErrorDefinition(
        code="AUTH003",
        message="CAPTCHA verification failed",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.WARNING,
        description="CAPTCHA response was invalid or expired",
    ),
    "AUTH004": ErrorDefinition(
        code="AUTH004",
        message="Authentication token has expired",
        http_status=status.HTTP_401_UNAUTHORIZED,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.INFO,
        description="Token expired, please re-authenticate",
        retryable=True,
    ),
    "AUTH005": ErrorDefinition(
        code="AUTH005",
        message="Unauthorized access",
        http_status=status.HTTP_401_UNAUTHORIZED,
        category=ErrorCategory.AUTHORIZATION,
        severity=ErrorSeverity.WARNING,
        description="Valid authentication required",
    ),
    "AUTH006": ErrorDefinition(
        code="AUTH006",
        message="Account is inactive",
        http_status=status.HTTP_403_FORBIDDEN,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.WARNING,
        description="Account has been deactivated",
    ),
    "AUTH007": ErrorDefinition(
        code="AUTH007",
        message="Email address not verified",
        http_status=status.HTTP_403_FORBIDDEN,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.INFO,
        description="Please verify your email address",
    ),
    "AUTH008": ErrorDefinition(
        code="AUTH008",
        message="Invalid or malformed token",
        http_status=status.HTTP_401_UNAUTHORIZED,
        category=ErrorCategory.AUTHENTICATION,
        severity=ErrorSeverity.WARNING,
        description="Token format is invalid",
    ),
    "AUTH009": ErrorDefinition(
        code="AUTH009",
        message="Password reuse not allowed",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Choose a password you haven't used before",
    ),
    "AUTH010": ErrorDefinition(
        code="AUTH010",
        message="Token rotation failed",
        http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        category=ErrorCategory.INTERNAL,
        severity=ErrorSeverity.ERROR,
        description="Failed to rotate authentication token",
    ),
    
    # ==========================================================================
    # Registration Errors (REG)
    # ==========================================================================
    "REG001": ErrorDefinition(
        code="REG001",
        message="Username already exists",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.CONFLICT,
        severity=ErrorSeverity.INFO,
        description="Choose a different username",
    ),
    "REG002": ErrorDefinition(
        code="REG002",
        message="Email address already registered",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.CONFLICT,
        severity=ErrorSeverity.INFO,
        description="Use a different email or try logging in",
    ),
    "REG003": ErrorDefinition(
        code="REG003",
        message="Password does not meet requirements",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Password must meet complexity requirements",
    ),
    "REG004": ErrorDefinition(
        code="REG004",
        message="Invalid registration data",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Check your input and try again",
    ),
    "REG005": ErrorDefinition(
        code="REG005",
        message="Disposable email addresses are not allowed",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Use a permanent email address",
    ),
    "REG006": ErrorDefinition(
        code="REG006",
        message="Password is too weak",
        http_status=status.HTTP_400_BAD_REQUEST,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Use a stronger password with more variety",
    ),
    
    # ==========================================================================
    # Validation Errors (VAL)
    # ==========================================================================
    "VAL001": ErrorDefinition(
        code="VAL001",
        message="Validation failed",
        http_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Request data failed validation checks",
    ),
    "VAL002": ErrorDefinition(
        code="VAL002",
        message="Required field missing",
        http_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="A required field was not provided",
    ),
    "VAL003": ErrorDefinition(
        code="VAL003",
        message="Field value out of range",
        http_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Value is outside allowed range",
    ),
    "VAL004": ErrorDefinition(
        code="VAL004",
        message="Invalid field format",
        http_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.INFO,
        description="Field value format is invalid",
    ),
    
    # ==========================================================================
    # Resource Errors (RES)
    # ==========================================================================
    "RES001": ErrorDefinition(
        code="RES001",
        message="Resource not found",
        http_status=status.HTTP_404_NOT_FOUND,
        category=ErrorCategory.NOT_FOUND,
        severity=ErrorSeverity.INFO,
        description="The requested resource does not exist",
    ),
    "RES002": ErrorDefinition(
        code="RES002",
        message="Resource already exists",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.CONFLICT,
        severity=ErrorSeverity.INFO,
        description="A resource with this identifier already exists",
    ),
    "RES003": ErrorDefinition(
        code="RES003",
        message="Resource has been deleted",
        http_status=status.HTTP_410_GONE,
        category=ErrorCategory.NOT_FOUND,
        severity=ErrorSeverity.INFO,
        description="This resource has been permanently deleted",
    ),
    "RES004": ErrorDefinition(
        code="RES004",
        message="Resource version conflict",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.CONFLICT,
        severity=ErrorSeverity.WARNING,
        description="Resource was modified by another request",
        retryable=True,
    ),
    
    # ==========================================================================
    # Rate Limiting Errors (RAT)
    # ==========================================================================
    "RAT001": ErrorDefinition(
        code="RAT001",
        message="Rate limit exceeded",
        http_status=status.HTTP_429_TOO_MANY_REQUESTS,
        category=ErrorCategory.RATE_LIMIT,
        severity=ErrorSeverity.WARNING,
        description="Too many requests, please slow down",
        retryable=True,
        retry_after=60,
    ),
    "RAT002": ErrorDefinition(
        code="RAT002",
        message="Daily quota exceeded",
        http_status=status.HTTP_429_TOO_MANY_REQUESTS,
        category=ErrorCategory.RATE_LIMIT,
        severity=ErrorSeverity.INFO,
        description="Daily request quota has been reached",
        retryable=True,
        retry_after=3600,
    ),
    "RAT003": ErrorDefinition(
        code="RAT003",
        message="Concurrent request limit reached",
        http_status=status.HTTP_429_TOO_MANY_REQUESTS,
        category=ErrorCategory.RATE_LIMIT,
        severity=ErrorSeverity.WARNING,
        description="Too many concurrent requests",
        retryable=True,
        retry_after=5,
    ),
    
    # ==========================================================================
    # Internal Errors (INT)
    # ==========================================================================
    "INT001": ErrorDefinition(
        code="INT001",
        message="Internal server error",
        http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        category=ErrorCategory.INTERNAL,
        severity=ErrorSeverity.ERROR,
        description="An unexpected error occurred",
    ),
    "INT002": ErrorDefinition(
        code="INT002",
        message="Database connection failed",
        http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
        category=ErrorCategory.EXTERNAL,
        severity=ErrorSeverity.CRITICAL,
        description="Unable to connect to database",
        retryable=True,
        retry_after=5,
    ),
    "INT003": ErrorDefinition(
        code="INT003",
        message="External service unavailable",
        http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
        category=ErrorCategory.EXTERNAL,
        severity=ErrorSeverity.ERROR,
        description="A dependent service is not responding",
        retryable=True,
        retry_after=10,
    ),
    "INT004": ErrorDefinition(
        code="INT004",
        message="Configuration error",
        http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        category=ErrorCategory.CONFIGURATION,
        severity=ErrorSeverity.CRITICAL,
        description="Server configuration is invalid",
    ),
    
    # ==========================================================================
    # Workflow Errors (WFK)
    # ==========================================================================
    "WFK001": ErrorDefinition(
        code="WFK001",
        message="Invalid workflow state",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.CONFLICT,
        severity=ErrorSeverity.INFO,
        description="The requested action is not valid in current state",
    ),
    "WFK002": ErrorDefinition(
        code="WFK002",
        message="Session not found",
        http_status=status.HTTP_404_NOT_FOUND,
        category=ErrorCategory.NOT_FOUND,
        severity=ErrorSeverity.INFO,
        description="The session has expired or does not exist",
    ),
    "WFK003": ErrorDefinition(
        code="WFK003",
        message="Session expired",
        http_status=status.HTTP_410_GONE,
        category=ErrorCategory.NOT_FOUND,
        severity=ErrorSeverity.INFO,
        description="The session has expired",
        retryable=True,
    ),
    "WFK004": ErrorDefinition(
        code="WFK004",
        message="Attempt limit reached",
        http_status=status.HTTP_429_TOO_MANY_REQUESTS,
        category=ErrorCategory.RATE_LIMIT,
        severity=ErrorSeverity.WARNING,
        description="Maximum number of attempts reached",
        retryable=True,
        retry_after=3600,
    ),
    "WFK005": ErrorDefinition(
        code="WFK005",
        message="Access denied",
        http_status=status.HTTP_403_FORBIDDEN,
        category=ErrorCategory.AUTHORIZATION,
        severity=ErrorSeverity.WARNING,
        description="You don't have permission to perform this action",
    ),
    "WFK006": ErrorDefinition(
        code="WFK006",
        message="Potential replay attack detected",
        http_status=status.HTTP_409_CONFLICT,
        category=ErrorCategory.SECURITY,
        severity=ErrorSeverity.CRITICAL,
        description="Request appears to be a replay attack",
    ),
}


class DomainError(Exception):
    """
    Standardized domain error with full context.
    
    Attributes:
        code: Error code from catalog
        message: Human-readable error message
        details: Additional error context
        correlation_id: Request tracking ID
        cause: Original exception that caused this error
    """
    
    def __init__(
        self,
        code: str,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        cause: Optional[Exception] = None,
        field_errors: Optional[List[Dict[str, Any]]] = None
    ):
        self.code = code
        self.definition = ERROR_CATALOG.get(code)
        
        # Use default message from catalog if not provided
        if message is None and self.definition:
            self.message = self.definition.message
        elif message:
            self.message = message
        else:
            self.message = "An error occurred"
        
        self.details = details or {}
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.cause = cause
        self.field_errors = field_errors or []
        self.timestamp = datetime.now(timezone.utc).isoformat()
        
        super().__init__(self.message)
    
    @property
    def http_status(self) -> int:
        """Get HTTP status code for this error."""
        if self.definition:
            return self.definition.http_status
        return status.HTTP_500_INTERNAL_SERVER_ERROR
    
    @property
    def category(self) -> ErrorCategory:
        """Get error category."""
        if self.definition:
            return self.definition.category
        return ErrorCategory.INTERNAL
    
    @property
    def severity(self) -> ErrorSeverity:
        """Get error severity."""
        if self.definition:
            return self.definition.severity
        return ErrorSeverity.ERROR
    
    @property
    def retryable(self) -> bool:
        """Check if error is retryable."""
        if self.definition:
            return self.definition.retryable
        return False
    
    def to_dict(self, include_traceback: bool = False) -> Dict[str, Any]:
        """
        Convert error to dictionary for JSON serialization.
        
        Args:
            include_traceback: Whether to include stack trace (for debug)
        """
        result = {
            "error": {
                "code": self.code,
                "message": self.message,
                "correlation_id": self.correlation_id,
                "timestamp": self.timestamp,
            }
        }
        
        # Add details if present
        if self.details:
            result["error"]["details"] = self.details
        
        # Add field errors for validation errors
        if self.field_errors:
            result["error"]["fields"] = self.field_errors
        
        # Add retry information
        if self.retryable and self.definition and self.definition.retry_after:
            result["error"]["retry_after"] = self.definition.retry_after
        
        # Add cause information (without exposing internal details in production)
        if self.cause:
            result["error"]["cause"] = {
                "type": type(self.cause).__name__,
                "message": str(self.cause)
            }
        
        # Add traceback for debugging (should be disabled in production)
        if include_traceback:
            result["error"]["traceback"] = traceback.format_exception(
                type(self), self, self.__traceback__
            )
        
        return result


class ErrorCatalog:
    """
    Centralized error catalog for managing domain errors.
    
    Provides methods for:
    - Looking up error definitions
    - Converting errors to HTTP responses
    - Registering custom errors
    """
    
    def __init__(self):
        self._catalog = ERROR_CATALOG.copy()
    
    def get_definition(self, code: str) -> Optional[ErrorDefinition]:
        """Get error definition by code."""
        return self._catalog.get(code)
    
    def register(self, definition: ErrorDefinition) -> None:
        """Register a new error definition."""
        self._catalog[definition.code] = definition
    
    def create_error(
        self,
        code: str,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        cause: Optional[Exception] = None
    ) -> DomainError:
        """Create a new domain error."""
        return DomainError(
            code=code,
            message=message,
            details=details,
            correlation_id=correlation_id,
            cause=cause
        )
    
    def to_http_response(
        self,
        error: DomainError,
        include_traceback: bool = False
    ) -> Dict[str, Any]:
        """
        Convert domain error to HTTP response format.
        
        Returns a dict suitable for FastAPI JSONResponse.
        """
        return {
            "status_code": error.http_status,
            "content": error.to_dict(include_traceback=include_traceback)
        }
    
    def get_errors_by_category(self, category: ErrorCategory) -> List[ErrorDefinition]:
        """Get all errors in a category."""
        return [
            defn for defn in self._catalog.values()
            if defn.category == category
        ]
    
    def get_errors_by_severity(self, severity: ErrorSeverity) -> List[ErrorDefinition]:
        """Get all errors with a severity level."""
        return [
            defn for defn in self._catalog.values()
            if defn.severity == severity
        ]
    
    def list_all_codes(self) -> List[str]:
        """List all registered error codes."""
        return list(self._catalog.keys())
    
    def generate_documentation(self) -> Dict[str, Any]:
        """Generate documentation for all errors."""
        return {
            code: {
                "code": defn.code,
                "message": defn.message,
                "http_status": defn.http_status,
                "category": defn.category.value,
                "severity": defn.severity.value,
                "description": defn.description,
                "retryable": defn.retryable,
                "retry_after": defn.retry_after,
            }
            for code, defn in sorted(self._catalog.items())
        }


# Global error catalog instance
catalog = ErrorCatalog()


# Convenience functions for common errors
def not_found(resource: str, resource_id: str, correlation_id: Optional[str] = None) -> DomainError:
    """Create a not found error."""
    return DomainError(
        code="RES001",
        message=f"{resource} not found",
        details={"resource": resource, "resource_id": resource_id},
        correlation_id=correlation_id
    )


def validation_failed(
    message: str = "Validation failed",
    details: Optional[Dict[str, Any]] = None,
    field_errors: Optional[List[Dict[str, Any]]] = None,
    correlation_id: Optional[str] = None
) -> DomainError:
    """Create a validation error."""
    return DomainError(
        code="VAL001",
        message=message,
        details=details,
        field_errors=field_errors,
        correlation_id=correlation_id
    )


def unauthorized(
    message: str = "Unauthorized",
    correlation_id: Optional[str] = None
) -> DomainError:
    """Create an unauthorized error."""
    return DomainError(
        code="AUTH005",
        message=message,
        correlation_id=correlation_id
    )


def forbidden(
    message: str = "Access denied",
    correlation_id: Optional[str] = None
) -> DomainError:
    """Create a forbidden error."""
    return DomainError(
        code="WFK005",
        message=message,
        correlation_id=correlation_id
    )


def internal_error(
    message: str = "Internal server error",
    cause: Optional[Exception] = None,
    correlation_id: Optional[str] = None
) -> DomainError:
    """Create an internal error."""
    return DomainError(
        code="INT001",
        message=message,
        cause=cause,
        correlation_id=correlation_id
    )


def rate_limit_exceeded(
    wait_seconds: int = 60,
    correlation_id: Optional[str] = None
) -> DomainError:
    """Create a rate limit error."""
    return DomainError(
        code="RAT001",
        message="Rate limit exceeded",
        details={"wait_seconds": wait_seconds},
        correlation_id=correlation_id
    )
