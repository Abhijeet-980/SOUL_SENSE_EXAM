"""
Unit tests for Unified Domain Error Catalog (#1362).

Tests error catalog, error definitions, mappings, and response generation.
"""
import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from api.utils.error_catalog import (
    ErrorCatalog,
    DomainError,
    ErrorDefinition,
    ErrorCategory,
    ErrorSeverity,
    ERROR_CATALOG,
    not_found,
    validation_failed,
    unauthorized,
    forbidden,
    internal_error,
    rate_limit_exceeded,
    catalog
)


class TestErrorDefinition:
    """Test ErrorDefinition dataclass."""

    def test_basic_creation(self):
        """Test creating an error definition."""
        definition = ErrorDefinition(
            code="TEST001",
            message="Test error message",
            http_status=400,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.INFO
        )
        
        assert definition.code == "TEST001"
        assert definition.message == "Test error message"
        assert definition.http_status == 400
        assert definition.category == ErrorCategory.VALIDATION
        assert definition.severity == ErrorSeverity.INFO
        assert definition.retryable is False

    def test_with_retry(self):
        """Test error definition with retry information."""
        definition = ErrorDefinition(
            code="RAT001",
            message="Rate limit exceeded",
            http_status=429,
            category=ErrorCategory.RATE_LIMIT,
            severity=ErrorSeverity.WARNING,
            retryable=True,
            retry_after=60
        )
        
        assert definition.retryable is True
        assert definition.retry_after == 60

    def test_default_values(self):
        """Test default values in error definition."""
        definition = ErrorDefinition(
            code="TEST002",
            message="Simple error"
        )
        
        assert definition.http_status == 400
        assert definition.category == ErrorCategory.INTERNAL
        assert definition.severity == ErrorSeverity.ERROR
        assert definition.description == ""
        assert definition.retryable is False
        assert definition.retry_after is None


class TestDomainError:
    """Test DomainError exception class."""

    def test_basic_error(self):
        """Test creating a basic domain error."""
        error = DomainError(
            code="AUTH001",
            message="Invalid credentials"
        )
        
        assert error.code == "AUTH001"
        assert error.message == "Invalid credentials"
        assert error.correlation_id is not None
        assert error.timestamp is not None

    def test_error_with_details(self):
        """Test error with additional details."""
        error = DomainError(
            code="RES001",
            message="Resource not found",
            details={"resource": "User", "id": "123"}
        )
        
        assert error.details == {"resource": "User", "id": "123"}

    def test_error_with_correlation_id(self):
        """Test error with custom correlation ID."""
        custom_id = "custom-correlation-id"
        error = DomainError(
            code="TEST003",
            message="Test error",
            correlation_id=custom_id
        )
        
        assert error.correlation_id == custom_id

    def test_error_with_cause(self):
        """Test error with original cause."""
        original = ValueError("Original error")
        error = DomainError(
            code="INT001",
            message="Internal error",
            cause=original
        )
        
        assert error.cause == original
        assert "Original error" in str(error.cause)

    def test_error_with_field_errors(self):
        """Test error with field validation errors."""
        field_errors = [
            {"field": "email", "message": "Invalid format"},
            {"field": "password", "message": "Too short"}
        ]
        error = DomainError(
            code="VAL001",
            message="Validation failed",
            field_errors=field_errors
        )
        
        assert len(error.field_errors) == 2
        assert error.field_errors[0]["field"] == "email"

    def test_http_status_from_definition(self):
        """Test HTTP status from catalog definition."""
        error = DomainError(code="AUTH001")  # Should be 401
        assert error.http_status == 401
        
        error = DomainError(code="RES001")  # Should be 404
        assert error.http_status == 404

    def test_http_status_default(self):
        """Test default HTTP status for unknown code."""
        error = DomainError(code="UNKNOWN")
        assert error.http_status == 500

    def test_category_from_definition(self):
        """Test category from catalog definition."""
        error = DomainError(code="AUTH001")
        assert error.category == ErrorCategory.AUTHENTICATION

    def test_severity_from_definition(self):
        """Test severity from catalog definition."""
        error = DomainError(code="INT001")
        assert error.severity == ErrorSeverity.ERROR

    def test_retryable_from_definition(self):
        """Test retryable from catalog definition."""
        error = DomainError(code="RAT001")  # Should be retryable
        assert error.retryable is True
        
        error = DomainError(code="AUTH001")  # Should not be retryable
        assert error.retryable is False

    def test_to_dict_basic(self):
        """Test basic conversion to dictionary."""
        error = DomainError(
            code="TEST004",
            message="Test error"
        )
        
        result = error.to_dict()
        
        assert result["error"]["code"] == "TEST004"
        assert result["error"]["message"] == "Test error"
        assert "correlation_id" in result["error"]
        assert "timestamp" in result["error"]

    def test_to_dict_with_details(self):
        """Test conversion with details."""
        error = DomainError(
            code="TEST005",
            message="Test error",
            details={"key": "value"}
        )
        
        result = error.to_dict()
        assert result["error"]["details"] == {"key": "value"}

    def test_to_dict_with_field_errors(self):
        """Test conversion with field errors."""
        error = DomainError(
            code="VAL001",
            message="Validation failed",
            field_errors=[{"field": "email", "message": "Invalid"}]
        )
        
        result = error.to_dict()
        assert "fields" in result["error"]
        assert len(result["error"]["fields"]) == 1

    def test_to_dict_with_retry(self):
        """Test conversion includes retry_after."""
        error = DomainError(code="RAT001")  # Has retry_after
        result = error.to_dict()
        assert "retry_after" in result["error"]
        assert result["error"]["retry_after"] == 60

    def test_to_dict_with_cause(self):
        """Test conversion includes cause."""
        original = ValueError("Original")
        error = DomainError(
            code="INT001",
            message="Internal",
            cause=original
        )
        
        result = error.to_dict()
        assert "cause" in result["error"]
        assert result["error"]["cause"]["type"] == "ValueError"

    def test_to_dict_with_traceback(self):
        """Test conversion with traceback (debug mode)."""
        try:
            raise ValueError("Test")
        except ValueError as e:
            error = DomainError(code="TEST006", message="Test", cause=e)
            result = error.to_dict(include_traceback=True)
            assert "traceback" in result["error"]


class TestErrorCatalog:
    """Test ErrorCatalog class."""

    def test_get_definition_existing(self):
        """Test getting existing error definition."""
        definition = catalog.get_definition("AUTH001")
        
        assert definition is not None
        assert definition.code == "AUTH001"
        assert definition.message == "Invalid credentials provided"

    def test_get_definition_nonexistent(self):
        """Test getting non-existent error definition."""
        definition = catalog.get_definition("NONEXISTENT")
        assert definition is None

    def test_register_new_error(self):
        """Test registering a new error definition."""
        new_catalog = ErrorCatalog()
        
        definition = ErrorDefinition(
            code="CUST001",
            message="Custom error",
            http_status=418  # I'm a teapot
        )
        
        new_catalog.register(definition)
        
        retrieved = new_catalog.get_definition("CUST001")
        assert retrieved is not None
        assert retrieved.code == "CUST001"
        assert retrieved.http_status == 418

    def test_create_error(self):
        """Test creating error through catalog."""
        error = catalog.create_error(
            code="AUTH001",
            message="Custom message",
            details={"extra": "info"}
        )
        
        assert error.code == "AUTH001"
        assert error.message == "Custom message"
        assert error.details == {"extra": "info"}

    def test_to_http_response(self):
        """Test converting error to HTTP response."""
        error = DomainError(code="RES001", message="Not found")
        
        response = catalog.to_http_response(error)
        
        assert response["status_code"] == 404
        assert "content" in response
        assert response["content"]["error"]["code"] == "RES001"

    def test_get_errors_by_category(self):
        """Test filtering errors by category."""
        auth_errors = catalog.get_errors_by_category(ErrorCategory.AUTHENTICATION)
        
        assert len(auth_errors) > 0
        for error in auth_errors:
            assert error.category == ErrorCategory.AUTHENTICATION

    def test_get_errors_by_severity(self):
        """Test filtering errors by severity."""
        critical_errors = catalog.get_errors_by_severity(ErrorSeverity.CRITICAL)
        
        for error in critical_errors:
            assert error.severity == ErrorSeverity.CRITICAL

    def test_list_all_codes(self):
        """Test listing all error codes."""
        codes = catalog.list_all_codes()
        
        assert "AUTH001" in codes
        assert "RES001" in codes
        assert "INT001" in codes
        assert len(codes) > 20  # Should have many errors

    def test_generate_documentation(self):
        """Test generating error documentation."""
        docs = catalog.generate_documentation()
        
        assert "AUTH001" in docs
        assert "message" in docs["AUTH001"]
        assert "http_status" in docs["AUTH001"]
        assert "category" in docs["AUTH001"]
        assert "severity" in docs["AUTH001"]


class TestConvenienceFunctions:
    """Test convenience error creation functions."""

    def test_not_found(self):
        """Test not_found convenience function."""
        error = not_found("User", "123", correlation_id="test-id")
        
        assert error.code == "RES001"
        assert "User" in error.message
        assert error.details["resource"] == "User"
        assert error.details["resource_id"] == "123"
        assert error.correlation_id == "test-id"

    def test_validation_failed(self):
        """Test validation_failed convenience function."""
        field_errors = [{"field": "email", "message": "Invalid"}]
        error = validation_failed(
            message="Validation error",
            details={"context": "test"},
            field_errors=field_errors
        )
        
        assert error.code == "VAL001"
        assert error.message == "Validation error"
        assert error.details == {"context": "test"}
        assert error.field_errors == field_errors

    def test_unauthorized(self):
        """Test unauthorized convenience function."""
        error = unauthorized("Access denied", correlation_id="auth-id")
        
        assert error.code == "AUTH005"
        assert error.message == "Access denied"
        assert error.correlation_id == "auth-id"

    def test_forbidden(self):
        """Test forbidden convenience function."""
        error = forbidden("Admin required", correlation_id="forbid-id")
        
        assert error.code == "WFK005"
        assert error.message == "Admin required"
        assert error.correlation_id == "forbid-id"

    def test_internal_error(self):
        """Test internal_error convenience function."""
        cause = RuntimeError("Database down")
        error = internal_error(
            message="System failure",
            cause=cause,
            correlation_id="internal-id"
        )
        
        assert error.code == "INT001"
        assert error.message == "System failure"
        assert error.cause == cause
        assert error.correlation_id == "internal-id"

    def test_rate_limit_exceeded(self):
        """Test rate_limit_exceeded convenience function."""
        error = rate_limit_exceeded(
            wait_seconds=120,
            correlation_id="rate-id"
        )
        
        assert error.code == "RAT001"
        assert error.details["wait_seconds"] == 120
        assert error.correlation_id == "rate-id"


class TestErrorCatalogContents:
    """Test the predefined error catalog contents."""

    def test_all_auth_errors_exist(self):
        """Test all authentication errors are defined."""
        auth_codes = [f"AUTH{i:03d}" for i in range(1, 11)]
        for code in auth_codes:
            assert code in ERROR_CATALOG, f"{code} not found in catalog"

    def test_all_reg_errors_exist(self):
        """Test all registration errors are defined."""
        reg_codes = [f"REG{i:03d}" for i in range(1, 7)]
        for code in reg_codes:
            assert code in ERROR_CATALOG, f"{code} not found in catalog"

    def test_http_status_consistency(self):
        """Test HTTP status codes are appropriate."""
        # Auth errors should be 401 or 403
        for code in ["AUTH001", "AUTH004", "AUTH005", "AUTH008"]:
            assert ERROR_CATALOG[code].http_status == 401
        
        for code in ["AUTH002", "AUTH006", "AUTH007"]:
            assert ERROR_CATALOG[code].http_status == 403
        
        # Not found should be 404
        assert ERROR_CATALOG["RES001"].http_status == 404
        
        # Rate limit should be 429
        assert ERROR_CATALOG["RAT001"].http_status == 429

    def test_error_code_format(self):
        """Test error codes follow format XXXNNNN."""
        for code in ERROR_CATALOG.keys():
            # Should be 3-4 letters followed by 3 digits
            assert len(code) >= 6 and len(code) <= 7
            prefix = ''.join(c for c in code if c.isalpha())
            suffix = ''.join(c for c in code if c.isdigit())
            assert len(prefix) >= 3 and len(prefix) <= 4
            assert len(suffix) == 3

    def test_all_errors_have_message(self):
        """Test all errors have non-empty messages."""
        for code, definition in ERROR_CATALOG.items():
            assert definition.message, f"{code} has empty message"
            assert len(definition.message) > 5, f"{code} message too short"

    def test_retryable_errors_have_retry_after(self):
        """Test retryable errors with specific retry windows specify retry_after."""
        for code, definition in ERROR_CATALOG.items():
            # Only check retryable errors that should have a specific retry window
            # Some errors like AUTH004 (token expired), RES004 (version conflict),
            # or WFK003 (session expired) are retryable conceptually but don't have
            # a specific retry_after since the action needed is not time-based
            if definition.retryable and definition.category not in [
                ErrorCategory.AUTHENTICATION,
                ErrorCategory.CONFLICT,
                ErrorCategory.NOT_FOUND
            ]:
                assert definition.retry_after is not None, f"{code} is retryable but no retry_after"
                assert definition.retry_after > 0, f"{code} retry_after must be positive"


class TestErrorResponseFormats:
    """Test error response format generation."""

    def test_nested_error_handling(self):
        """Test handling of nested/wrapped errors."""
        inner = DomainError(code="DB001", message="Database error")
        outer = DomainError(
            code="INT001",
            message="Internal error",
            cause=inner
        )
        
        result = outer.to_dict()
        assert "cause" in result["error"]

    def test_unknown_error_code_handling(self):
        """Test handling of unknown error codes."""
        error = DomainError(code="UNKNOWN999", message="Unknown error")
        
        assert error.http_status == 500  # Default
        assert error.category == ErrorCategory.INTERNAL  # Default
        assert error.severity == ErrorSeverity.ERROR  # Default

    def test_field_error_format(self):
        """Test field error format in response."""
        field_errors = [
            {"field": "username", "message": "Too short", "code": "MIN_LENGTH"},
            {"field": "email", "message": "Invalid", "code": "INVALID_EMAIL"}
        ]
        
        error = DomainError(
            code="VAL001",
            message="Validation failed",
            field_errors=field_errors
        )
        
        result = error.to_dict()
        fields = result["error"]["fields"]
        
        assert len(fields) == 2
        assert fields[0]["field"] == "username"
        assert fields[1]["code"] == "INVALID_EMAIL"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
