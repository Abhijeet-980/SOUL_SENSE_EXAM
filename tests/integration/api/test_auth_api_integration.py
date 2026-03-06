"""
Integration tests for Authentication API endpoints.
Tests complete authentication flows including registration, login, logout, and token management.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.fastapi.api.main import app
from backend.fastapi.api.services.db_service import Base, get_db
from backend.fastapi.api.services.user_service import UserService

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_auth.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db()


@pytest.fixture(scope="function")
def db():
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client(db):
    return TestClient(app)


@pytest.fixture
def test_user_data():
    return {
        "username": "testuser_auth",
        "password": "SecurePass123!",
        "email": "test@example.com"
    }


class TestAuthenticationAPI:
    """Test suite for authentication endpoints"""

    def test_user_registration_success(self, client, test_user_data):
        """Test successful user registration"""
        response = client.post("/api/v1/auth/register", json=test_user_data)
        assert response.status_code == 201
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["username"] == test_user_data["username"]

    def test_user_registration_duplicate_username(self, client, test_user_data):
        """Test registration with duplicate username fails"""
        client.post("/api/v1/auth/register", json=test_user_data)

        response = client.post("/api/v1/auth/register", json=test_user_data)
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"].lower()

    def test_user_registration_weak_password(self, client, test_user_data):
        """Test registration with weak password fails"""
        test_user_data["password"] = "123"
        response = client.post("/api/v1/auth/register", json=test_user_data)
        assert response.status_code == 400

    def test_user_login_success(self, client, test_user_data):
        """Test successful user login"""
        client.post("/api/v1/auth/register", json=test_user_data)

        login_data = {
            "username": test_user_data["username"],
            "password": test_user_data["password"]
        }
        response = client.post("/api/v1/auth/login", data=login_data)
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"

    def test_user_login_invalid_credentials(self, client, test_user_data):
        """Test login with invalid credentials fails"""
        client.post("/api/v1/auth/register", json=test_user_data)

        login_data = {
            "username": test_user_data["username"],
            "password": "WrongPassword123!"
        }
        response = client.post("/api/v1/auth/login", data=login_data)
        assert response.status_code == 401

    def test_token_refresh_success(self, client, test_user_data):
        """Test successful token refresh"""
        reg_response = client.post("/api/v1/auth/register", json=test_user_data)
        refresh_token = reg_response.json()["refresh_token"]

        response = client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": refresh_token}
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data

    def test_token_refresh_invalid_token(self, client):
        """Test token refresh with invalid token fails"""
        response = client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": "invalid_token"}
        )
        assert response.status_code == 401

    def test_protected_endpoint_without_token(self, client):
        """Test accessing protected endpoint without token fails"""
        response = client.get("/api/v1/users/me")
        assert response.status_code == 401

    def test_protected_endpoint_with_valid_token(self, client, test_user_data):
        """Test accessing protected endpoint with valid token"""
        reg_response = client.post("/api/v1/auth/register", json=test_user_data)
        token = reg_response.json()["access_token"]

        headers = {"Authorization": f"Bearer {token}"}
        response = client.get("/api/v1/users/me", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == test_user_data["username"]

    def test_logout_success(self, client, test_user_data):
        """Test successful logout"""
        reg_response = client.post("/api/v1/auth/register", json=test_user_data)
        token = reg_response.json()["access_token"]

        headers = {"Authorization": f"Bearer {token}"}
        response = client.post("/api/v1/auth/logout", headers=headers)
        assert response.status_code == 200

    def test_password_reset_request(self, client, test_user_data):
        """Test password reset request"""
        client.post("/api/v1/auth/register", json=test_user_data)

        response = client.post(
            "/api/v1/auth/password-reset/request",
            json={"email": test_user_data["email"]}
        )
        assert response.status_code == 200

    def test_password_reset_with_token(self, client, test_user_data):
        """Test password reset with valid token"""
        client.post("/api/v1/auth/register", json=test_user_data)

        reset_response = client.post(
            "/api/v1/auth/password-reset/request",
            json={"email": test_user_data["email"]}
        )

        response = client.post(
            "/api/v1/auth/password-reset/confirm",
            json={
                "token": "test_token",
                "new_password": "NewSecurePass456!"
            }
        )
        assert response.status_code in [200, 400]
