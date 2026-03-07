"""
Error Response Schemas

Standardized error response schemas for consistent API error handling.
All error responses follow the same structure for easy client handling.

Example Response:
    {
        "error": {
            "code": "AUTH001",
            "message": "Invalid credentials provided",
            "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2026-03-07T12:00:00Z",
            "details": {"field": "password"},
            "fields": [
                {"field": "email", "message": "Invalid email format"}
            ],
            "retry_after": 60
        }
    }
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class FieldError(BaseModel):
    """
    Validation error for a specific field.
    
    Used in validation errors to indicate which fields failed validation.
    """
    field: str = Field(..., description="Field name that failed validation")
    message: str = Field(..., description="Human-readable error message")
    code: Optional[str] = Field(None, description="Field-specific error code")
    value: Optional[Any] = Field(None, description="The invalid value (sanitized)")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "field": "email",
                "message": "Invalid email format",
                "code": "FORMAT_ERROR",
                "value": "not-an-email"
            }
        }
    }


class ErrorDetails(BaseModel):
    """
    Detailed error information.
    
    Contains all information about an error for client handling.
    """
    code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    correlation_id: str = Field(..., description="Unique request identifier for support")
    timestamp: str = Field(..., description="ISO 8601 timestamp when error occurred")
    
    # Optional fields
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional error context"
    )
    fields: Optional[List[FieldError]] = Field(
        None,
        description="Field validation errors"
    )
    retry_after: Optional[int] = Field(
        None,
        description="Seconds to wait before retry (for rate limits)"
    )
    cause: Optional[Dict[str, str]] = Field(
        None,
        description="Original error information (if wrapped)"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "code": "VAL001",
                "message": "Validation failed",
                "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2026-03-07T12:00:00Z",
                "details": {"resource": "User"},
                "fields": [
                    {"field": "email", "message": "Invalid format"}
                ]
            }
        }
    }


class ErrorResponse(BaseModel):
    """
    Standard error response wrapper.
    
    All API errors return this structure for consistency.
    """
    error: ErrorDetails = Field(..., description="Error information")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "AUTH001",
                    "message": "Invalid credentials provided",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z"
                }
            }
        }
    }


class ValidationErrorResponse(ErrorResponse):
    """
    Validation error response with field details.
    
    Returned when request data fails validation.
    """
    error: ErrorDetails = Field(
        ...,
        description="Validation error with field details"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "VAL001",
                    "message": "Validation failed",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "fields": [
                        {
                            "field": "username",
                            "message": "Username must be at least 3 characters",
                            "code": "MIN_LENGTH"
                        },
                        {
                            "field": "email",
                            "message": "Invalid email format",
                            "code": "INVALID_FORMAT"
                        }
                    ]
                }
            }
        }
    }


class RateLimitErrorResponse(ErrorResponse):
    """
    Rate limit error response.
    
    Returned when rate limit is exceeded.
    """
    error: ErrorDetails = Field(
        ...,
        description="Rate limit error with retry information"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "RAT001",
                    "message": "Rate limit exceeded",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "details": {"limit": 100, "window": "60s"},
                    "retry_after": 60
                }
            }
        }
    }


class NotFoundErrorResponse(ErrorResponse):
    """
    Not found error response.
    
    Returned when a requested resource does not exist.
    """
    error: ErrorDetails = Field(
        ...,
        description="Not found error with resource information"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "RES001",
                    "message": "Resource not found",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "details": {
                        "resource": "User",
                        "resource_id": "123"
                    }
                }
            }
        }
    }


class ConflictErrorResponse(ErrorResponse):
    """
    Conflict error response.
    
    Returned when there's a resource conflict (e.g., duplicate).
    """
    error: ErrorDetails = Field(
        ...,
        description="Conflict error with resource information"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "RES002",
                    "message": "Resource already exists",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "details": {
                        "resource": "User",
                        "field": "email",
                        "value": "user@example.com"
                    }
                }
            }
        }
    }


class UnauthorizedErrorResponse(ErrorResponse):
    """
    Unauthorized error response.
    
    Returned when authentication is required or invalid.
    """
    error: ErrorDetails = Field(
        ...,
        description="Unauthorized error"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "AUTH005",
                    "message": "Unauthorized access",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z"
                }
            }
        }
    }


class ForbiddenErrorResponse(ErrorResponse):
    """
    Forbidden error response.
    
    Returned when user lacks permission for the action.
    """
    error: ErrorDetails = Field(
        ...,
        description="Forbidden error with permission details"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "WFK005",
                    "message": "Access denied",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "details": {
                        "required_permission": "admin",
                        "user_permission": "user"
                    }
                }
            }
        }
    }


class InternalErrorResponse(ErrorResponse):
    """
    Internal server error response.
    
    Returned when an unexpected error occurs.
    """
    error: ErrorDetails = Field(
        ...,
        description="Internal error"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "INT001",
                    "message": "Internal server error",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z"
                }
            }
        }
    }


class ServiceUnavailableErrorResponse(ErrorResponse):
    """
    Service unavailable error response.
    
    Returned when a dependent service is unavailable.
    """
    error: ErrorDetails = Field(
        ...,
        description="Service unavailable with retry information"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "error": {
                    "code": "INT002",
                    "message": "Database connection failed",
                    "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                    "timestamp": "2026-03-07T12:00:00Z",
                    "retry_after": 5
                }
            }
        }
    }


# Error code to response model mapping
ERROR_RESPONSE_MODELS = {
    400: ErrorResponse,
    401: UnauthorizedErrorResponse,
    403: ForbiddenErrorResponse,
    404: NotFoundErrorResponse,
    409: ConflictErrorResponse,
    422: ValidationErrorResponse,
    429: RateLimitErrorResponse,
    500: InternalErrorResponse,
    503: ServiceUnavailableErrorResponse,
}


def get_error_response_model(status_code: int) -> type:
    """
    Get the appropriate error response model for an HTTP status code.
    
    Args:
        status_code: HTTP status code
        
    Returns:
        Error response model class
    """
    return ERROR_RESPONSE_MODELS.get(status_code, ErrorResponse)
