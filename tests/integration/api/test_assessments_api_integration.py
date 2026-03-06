"""
Integration tests for Assessment API endpoints.
Tests assessment creation, submission, and retrieval flows.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.fastapi.api.main import app
from backend.fastapi.api.services.db_service import Base, get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_assessments.db"

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
        "username": "assessment_user",
        "password": "TestPass123!",
        "email": "assessment@example.com"
    }
    response = client.post("/api/v1/auth/register", json=user_data)
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


class TestAssessmentsAPI:
    """Test suite for assessment endpoints"""

    def test_get_questions_for_age(self, client, auth_headers):
        """Test retrieving questions filtered by age"""
        response = client.get(
            "/api/v1/questions?age=25&limit=10",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) <= 10

    def test_start_assessment(self, client, auth_headers):
        """Test starting a new assessment"""
        response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assert response.status_code == 201
        data = response.json()
        assert "assessment_id" in data
        assert data["status"] == "in_progress"

    def test_submit_assessment_response(self, client, auth_headers):
        """Test submitting answers to assessment"""
        start_response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assessment_id = start_response.json()["assessment_id"]

        response = client.post(
            f"/api/v1/assessments/{assessment_id}/responses",
            headers=auth_headers,
            json={
                "responses": [
                    {"question_id": 1, "answer": 3},
                    {"question_id": 2, "answer": 4}
                ]
            }
        )
        assert response.status_code == 200

    def test_submit_complete_assessment(self, client, auth_headers):
        """Test submitting a complete assessment"""
        start_response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assessment_id = start_response.json()["assessment_id"]

        response = client.post(
            f"/api/v1/assessments/{assessment_id}/submit",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "score" in data
        assert "results" in data

    def test_get_assessment_results(self, client, auth_headers):
        """Test retrieving assessment results"""
        start_response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assessment_id = start_response.json()["assessment_id"]

        client.post(
            f"/api/v1/assessments/{assessment_id}/submit",
            headers=auth_headers
        )

        response = client.get(
            f"/api/v1/assessments/{assessment_id}/results",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert "score" in data
        assert "recommendations" in data

    def test_get_user_assessment_history(self, client, auth_headers):
        """Test retrieving user's assessment history"""
        client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )

        response = client.get(
            "/api/v1/assessments/history",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_assessment_validation_invalid_answer(self, client, auth_headers):
        """Test assessment validation with invalid answer"""
        start_response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assessment_id = start_response.json()["assessment_id"]

        response = client.post(
            f"/api/v1/assessments/{assessment_id}/responses",
            headers=auth_headers,
            json={
                "responses": [
                    {"question_id": 1, "answer": 10}
                ]
            }
        )
        assert response.status_code == 400

    def test_assessment_progress_saving(self, client, auth_headers):
        """Test saving and resuming assessment progress"""
        start_response = client.post(
            "/api/v1/assessments/start",
            headers=auth_headers,
            json={"age": 25}
        )
        assessment_id = start_response.json()["assessment_id"]

        client.post(
            f"/api/v1/assessments/{assessment_id}/responses",
            headers=auth_headers,
            json={
                "responses": [
                    {"question_id": 1, "answer": 3}
                ]
            }
        )

        response = client.get(
            f"/api/v1/assessments/{assessment_id}/progress",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["current_question"] > 0
