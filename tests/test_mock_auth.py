"""
Test suite for Mock Authentication Service

This test suite verifies that the mock authentication service works correctly
for testing and development purposes.
"""

import pytest
import os
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Set mock auth mode before importing the app
os.environ["MOCK_AUTH_MODE"] = "true"
os.environ["APP_ENV"] = "development"

from backend.fastapi.api.services.mock_auth_service import MockAuthService, MOCK_USERS, MOCK_OTP_CODES
from backend.fastapi.api.root_models import User


class TestMockAuthService:
    """Test the MockAuthService class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.auth_service = MockAuthService()

    def test_authenticate_user_with_email(self):
        """Test authentication with email."""
        user = self.auth_service.authenticate_user(
            "test@example.com", 
            "any_password",  # Any password works in mock mode
            ip_address="127.0.0.1",
            user_agent="TestAgent"
        )
        
        assert user is not None
        assert user.username == "testuser"
        assert user.id == 1
        assert user.is_active is True

    def test_authenticate_user_with_username(self):
        """Test authentication with username."""
        user = self.auth_service.authenticate_user(
            "testuser",
            "any_password",
            ip_address="127.0.0.1",
            user_agent="TestAgent"
        )
        
        assert user is not None
        assert user.username == "testuser"
        assert user.id == 1

    def test_authenticate_user_case_insensitive(self):
        """Test that authentication is case-insensitive."""
        user = self.auth_service.authenticate_user(
            "TEST@EXAMPLE.COM",
            "any_password"
        )
        
        assert user is not None
        assert user.username == "testuser"

    def test_authenticate_user_invalid(self):
        """Test authentication with invalid credentials."""
        user = self.auth_service.authenticate_user(
            "nonexistent@example.com",
            "any_password"
        )
        
        assert user is None

    def test_create_access_token(self):
        """Test access token creation."""
        token = self.auth_service.create_access_token(
            data={"sub": "testuser"}
        )
        
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0

    def test_create_pre_auth_token(self):
        """Test pre-auth token creation for 2FA."""
        token = self.auth_service.create_pre_auth_token(user_id=1)
        
        assert token is not None
        assert isinstance(token, str)

    def test_2fa_flow(self):
        """Test complete 2FA flow."""
        # Get user with 2FA enabled
        user = self.auth_service.authenticate_user("2fa@example.com", "any_password")
        assert user is not None
        assert user.is_2fa_enabled is True
        
        # Initiate 2FA
        pre_auth_token, otp_code = self.auth_service.initiate_2fa_login(user)
        assert pre_auth_token is not None
        assert otp_code == MOCK_OTP_CODES["2fa@example.com"]
        
        # Verify 2FA
        verified_user = self.auth_service.verify_2fa_login(pre_auth_token, otp_code)
        assert verified_user is not None
        assert verified_user.id == user.id

    def test_2fa_verification_invalid_code(self):
        """Test 2FA verification with invalid code."""
        user = self.auth_service.authenticate_user("2fa@example.com", "any_password")
        pre_auth_token, _ = self.auth_service.initiate_2fa_login(user)
        
        # Try with wrong code
        verified_user = self.auth_service.verify_2fa_login(pre_auth_token, "000000")
        assert verified_user is None

    def test_refresh_token_flow(self):
        """Test refresh token creation and validation."""
        # Create refresh token
        refresh_token = self.auth_service.create_refresh_token(user_id=1)
        assert refresh_token is not None
        
        # Refresh access token
        access_token, new_refresh_token = self.auth_service.refresh_access_token(refresh_token)
        assert access_token is not None
        assert new_refresh_token is not None
        assert new_refresh_token != refresh_token  # Token rotation

    def test_refresh_token_invalid(self):
        """Test refresh with invalid token."""
        with pytest.raises(ValueError, match="Invalid refresh token"):
            self.auth_service.refresh_access_token("invalid_token")

    def test_revoke_refresh_token(self):
        """Test refresh token revocation."""
        refresh_token = self.auth_service.create_refresh_token(user_id=1)
        
        # Revoke token
        self.auth_service.revoke_refresh_token(refresh_token)
        
        # Try to use revoked token
        with pytest.raises(ValueError):
            self.auth_service.refresh_access_token(refresh_token)

    def test_password_reset_flow(self):
        """Test password reset flow."""
        # Initiate reset
        otp_code = self.auth_service.initiate_password_reset("test@example.com")
        assert otp_code == MOCK_OTP_CODES["test@example.com"]
        
        # Complete reset
        success = self.auth_service.complete_password_reset(
            "test@example.com",
            otp_code,
            "new_password"
        )
        assert success is True

    def test_password_reset_invalid_code(self):
        """Test password reset with invalid code."""
        self.auth_service.initiate_password_reset("test@example.com")
        
        success = self.auth_service.complete_password_reset(
            "test@example.com",
            "wrong_code",
            "new_password"
        )
        assert success is False

    def test_2fa_setup_flow(self):
        """Test 2FA setup flow."""
        user = self.auth_service.authenticate_user("test@example.com", "any_password")
        
        # Send setup OTP
        otp_code = self.auth_service.send_2fa_setup_otp(user)
        assert otp_code == "888888"
        
        # Enable 2FA
        success = self.auth_service.enable_2fa(user.id, "888888")
        assert success is True

    def test_2fa_setup_invalid_code(self):
        """Test 2FA setup with invalid code."""
        success = self.auth_service.enable_2fa(1, "wrong_code")
        assert success is False

    def test_2fa_disable(self):
        """Test 2FA disablement."""
        # This should not raise any exceptions
        self.auth_service.disable_2fa(1)

    def test_update_last_login(self):
        """Test last login update."""
        # This should not raise any exceptions
        self.auth_service.update_last_login(1)

    def test_mock_users_available(self):
        """Test that all mock users are available."""
        assert "test@example.com" in MOCK_USERS
        assert "admin@example.com" in MOCK_USERS
        assert "2fa@example.com" in MOCK_USERS
        
        # Verify user data structure
        test_user = MOCK_USERS["test@example.com"]
        assert test_user["id"] == 1
        assert test_user["username"] == "testuser"
        assert test_user["is_active"] is True


class TestMockAuthIntegration:
    """Integration tests for mock authentication with FastAPI."""

    @pytest.fixture
    def client(self):
        """Create a test client with mock auth enabled."""
        # This would require the actual FastAPI app setup
        # For now, we'll skip this as it requires more infrastructure
        pytest.skip("Integration tests require full app setup")

    def test_login_endpoint_mock(self, client):
        """Test login endpoint with mock authentication."""
        response = client.post(
            "/api/v1/auth/login",
            data={
                "username": "test@example.com",
                "password": "any_password"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_register_endpoint_mock(self, client):
        """Test register endpoint with mock authentication."""
        response = client.post(
            "/api/v1/auth/register",
            json={
                "username": "newuser",
                "email": "newuser@example.com",
                "first_name": "New",
                "last_name": "User",
                "age": 25,
                "gender": "M",
                "password": "any_password"
            }
        )
        
        assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
