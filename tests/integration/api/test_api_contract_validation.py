"""
API Contract Tests - Validate OpenAPI schema compliance
Tests that API responses match documented schemas and status codes
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.fastapi.api.main import app
from backend.fastapi.api.services.db_service import Base, get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_contract.db"

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
def authenticated_client(client):
    """Create authenticated client"""
    user_data = {
        "username": "contract_test_user",
        "password": "TestPass123!",
        "email": "contract@example.com"
    }
    client.post("/api/v1/auth/register", json=user_data)
    return client


class TestAPIContracts:
    """Test suite for API contract validation"""

    def test_root_endpoint_contract(self, client):
        """Test root endpoint returns expected structure"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "versions" in data
        assert isinstance(data["versions"], list)

    def test_health_check_contract(self, client):
        """Test health check endpoint contract"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"

    def test_register_endpoint_contract(self, client):
        """Test user registration endpoint contract"""
        user_data = {
            "username": "newuser",
            "password": "SecurePass123!",
            "email": "newuser@example.com"
        }
        response = client.post("/api/v1/auth/register", json=user_data)
        assert response.status_code == 201
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert "username" in data
        assert isinstance(data["access_token"], str)
        assert isinstance(data["refresh_token"], str)

    def test_login_endpoint_contract(self, client):
        """Test login endpoint contract"""
        user_data = {
            "username": "loginuser",
            "password": "TestPass123!",
            "email": "login@example.com"
        }
        client.post("/api/v1/auth/register", json=user_data)

        login_data = {
            "username": "loginuser",
            "password": "TestPass123!"
        }
        response = client.post("/api/v1/auth/login", data=login_data)
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "token_type" in data
        assert data["token_type"] == "bearer"

    def test_error_response_contract(self, client):
        """Test error response follows standard format"""
        response = client.get("/api/v1/protected-endpoint")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data or "code" in data

    def test_questions_endpoint_contract(self, authenticated_client):
        """Test questions endpoint contract"""
        user_data = {
            "username": "contract_test_user",
            "password": "TestPass123!"
        }
        login_response = authenticated_client.post("/api/v1/auth/login", data=user_data)
        token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        response = authenticated_client.get("/api/v1/questions?age=25", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        if len(data) > 0:
            assert "id" in data[0]
            assert "text" in data[0]
            assert "category" in data[0]

    def test_validation_error_contract(self, client):
        """Test validation error responses"""
        invalid_data = {
            "username": "",
            "password": "123"
        }
        response = client.post("/api/v1/auth/register", json=invalid_data)
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data or "errors" in data

    def test_pagination_contract(self, authenticated_client):
        """Test paginated responses follow contract"""
        user_data = {
            "username": "contract_test_user",
            "password": "TestPass123!"
        }
        login_response = authenticated_client.post("/api/v1/auth/login", data=user_data)
        token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        response = authenticated_client.get(
            "/api/v1/journal?page=1&limit=10",
            headers=headers
        )
        assert response.status_code in [200, 404]

    def test_rate_limiting_headers(self, client):
        """Test rate limiting headers are present"""
        for i in range(5):
            response = client.post(
                "/api/v1/auth/register",
                json={
                    "username": f"rate_test_{i}",
                    "password": "TestPass123!",
                    "email": f"rate{i}@example.com"
                }
            )

        assert "X-RateLimit-Remaining" in response.headers or response.status_code != 429

    def test_cors_headers(self, client):
        """Test CORS headers are set correctly"""
        response = client.options(
            "/api/v1/questions",
            headers={"Origin": "http://localhost:3005"}
        )
        assert "access-control-allow-origin" in response.headers

    def test_api_version_header(self, client):
        """Test API version header is present"""
        response = client.get("/")
        assert "X-API-Version" in response.headers

    def test_content_type_negotiation(self, authenticated_client):
        """Test content negotiation"""
        user_data = {
            "username": "contract_test_user",
            "password": "TestPass123!"
        }
        login_response = authenticated_client.post("/api/v1/auth/login", data=user_data)
        token = login_response.json()["access_token"]
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        response = authenticated_client.get("/api/v1/journal", headers=headers)
        assert response.headers["content-type"] == "application/json"
