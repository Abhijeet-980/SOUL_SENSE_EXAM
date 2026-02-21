"""
Integration tests for Profile API endpoints.
Tests profile management, settings, and account operations.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.fastapi.api.main import app
from backend.fastapi.api.services.db_service import Base, get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_profile.db"

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
def auth_headers(client):
    """Create a test user and return auth headers"""
    user_data = {
        "username": "profile_user",
        "password": "TestPass123!",
        "email": "profile@example.com"
    }
    response = client.post("/api/v1/auth/register", json=user_data)
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


class TestProfileAPI:
    """Test suite for profile endpoints"""

    def test_get_profile(self, client, auth_headers):
        """Test retrieving user profile"""
        response = client.get("/api/v1/users/me", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert "username" in data
        assert "email" in data
        assert data["username"] == "profile_user"

    def test_update_profile(self, client, auth_headers):
        """Test updating user profile"""
        update_data = {
            "email": "updated@example.com",
            "full_name": "Test User"
        }
        response = client.put(
            "/api/v1/users/me",
            headers=auth_headers,
            json=update_data
        )
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == update_data["email"]

    def test_change_password(self, client, auth_headers):
        """Test changing user password"""
        password_data = {
            "current_password": "TestPass123!",
            "new_password": "NewSecurePass456!"
        }
        response = client.post(
            "/api/v1/users/me/change-password",
            headers=auth_headers,
            json=password_data
        )
        assert response.status_code == 200

    def test_change_password_wrong_current_password(self, client, auth_headers):
        """Test password change with wrong current password"""
        password_data = {
            "current_password": "WrongPassword123!",
            "new_password": "NewSecurePass456!"
        }
        response = client.post(
            "/api/v1/users/me/change-password",
            headers=auth_headers,
            json=password_data
        )
        assert response.status_code == 400

    def test_update_privacy_settings(self, client, auth_headers):
        """Test updating privacy settings"""
        settings_data = {
            "share_analytics": True,
            "data_collection": False
        }
        response = client.put(
            "/api/v1/users/me/privacy",
            headers=auth_headers,
            json=settings_data
        )
        assert response.status_code == 200
        data = response.json()
        assert data["share_analytics"] == settings_data["share_analytics"]

    def test_export_user_data(self, client, auth_headers):
        """Test exporting user data"""
        response = client.get(
            "/api/v1/users/me/export",
            headers=auth_headers
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"

    def test_delete_account(self, client):
        """Test account deletion"""
        user_data = {
            "username": "delete_user",
            "password": "TestPass123!",
            "email": "delete@example.com"
        }
        reg_response = client.post("/api/v1/auth/register", json=user_data)
        token = reg_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        response = client.delete("/api/v1/users/me", headers=headers)
        assert response.status_code == 200

    def test_get_user_settings(self, client, auth_headers):
        """Test retrieving user settings"""
        response = client.get(
            "/api/v1/users/me/settings",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "language" in data
        assert "theme" in data

    def test_update_user_settings(self, client, auth_headers):
        """Test updating user settings"""
        settings_data = {
            "language": "es",
            "theme": "dark"
        }
        response = client.put(
            "/api/v1/users/me/settings",
            headers=auth_headers,
            json=settings_data
        )
        assert response.status_code == 200
        data = response.json()
        assert data["language"] == settings_data["language"]

    def test_upload_profile_picture(self, client, auth_headers):
        """Test uploading profile picture"""
        from io import BytesIO

        image_content = b"fake_image_content"
        files = {"file": ("profile.jpg", BytesIO(image_content), "image/jpeg")}

        response = client.post(
            "/api/v1/users/me/picture",
            headers=auth_headers,
            files=files
        )
        assert response.status_code in [200, 201]

    def test_get_user_statistics(self, client, auth_headers):
        """Test retrieving user statistics"""
        response = client.get(
            "/api/v1/users/me/statistics",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "assessments_taken" in data
        assert "journal_entries" in data

    def test_deactivate_account(self, client, auth_headers):
        """Test account deactivation"""
        response = client.post(
            "/api/v1/users/me/deactivate",
            headers=auth_headers
        )
        assert response.status_code == 200

    def test_reactivate_account(self, client):
        """Test account reactivation"""
        user_data = {
            "username": "reactivate_user",
            "password": "TestPass123!",
            "email": "reactivate@example.com"
        }
        client.post("/api/v1/auth/register", json=user_data)

        reactivate_data = {
            "username": "reactivate_user",
            "email": "reactivate@example.com"
        }
        response = client.post(
            "/api/v1/auth/reactivate",
            json=reactivate_data
        )
        assert response.status_code in [200, 404]
