import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime

# Adjust import based on your actual structure
from backend.fastapi.api.main import app
from backend.fastapi.api.schemas import ExamResponseCreate, ExamResultCreate
from app.models import User, Score, Response
from app.auth.auth import AuthManager

client = TestClient(app)

@pytest.fixture
def auth_headers(temp_db: Session):
    """Creates a test user and returns auth headers."""
    username = "exam_test_user"
    password = "SafePassword123!"
    email = "exam_test@example.com"
    
    # Clean up
    temp_db.query(User).filter_by(username=username).delete()
    temp_db.commit()
    
    # Create user
    auth = AuthManager()
    auth.register_user(username, email, "Test", "User", 25, "M", password)
    
    # Login to get token
    response = client.post("/api/v1/auth/login", data={"username": username, "password": password})
    assert response.status_code == 200
    token = response.json()["access_token"]
    
    return {"Authorization": f"Bearer {token}"}

def test_start_exam(auth_headers):
    """Test initiating an exam session."""
    response = client.post("/api/v1/exams/start", headers=auth_headers)
    assert response.status_code == 201
    assert "session_id" in response.json()

def test_save_response_api(auth_headers, temp_db):
    """Test saving a single response via session path."""
    session_id = "test-session-123"
    payload = {
        "question_id": 101,
        "value": 3,
        "age_group": "18-25"
    }
    
    response = client.post(
        f"/api/v1/exams/{session_id}/responses",
        json=payload,
        headers=auth_headers
    )
    
    assert response.status_code == 201
    assert response.json() == {"status": "success"}

    # Verify DB
    saved = temp_db.query(Response).filter_by(question_id=101, response_value=3, session_id=session_id).first()
    assert saved is not None
    assert saved.username == "exam_test_user"

def test_complete_exam_api(auth_headers, temp_db):
    """Test submitting a full exam score via session path."""
    from app.auth.crypto import EncryptionManager
    session_id = "test-session-123"
    
    payload = {
        "total_score": 85,
        "sentiment_score": 75.5,
        "reflection_text": "I feel great about this test.",
        "is_rushed": False,
        "is_inconsistent": False,
        "age": 25,
        "age_group": "18-25",
        "detailed_age_group": "Young Adult"
    }
    
    response = client.post(
        f"/api/v1/exams/{session_id}/complete",
        json=payload,
        headers=auth_headers
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["total_score"] == 85
    
    # Verify DB and Encryption
    score_entry = temp_db.query(Score).filter_by(
        total_score=85, 
        username="exam_test_user",
        session_id=session_id
    ).first()
    assert score_entry is not None

def test_invalid_response_value(auth_headers):
    """Test validation for invalid inputs."""
    session_id = "test-session"
    payload = {
        "question_id": 101,
        "value": 5, # Invalid (must be 1-4)
        "age_group": "18-25"
    }
    response = client.post(
        f"/api/v1/exams/{session_id}/responses",
        json=payload,
        headers=auth_headers
    )
    assert response.status_code == 422 # Validation Error

def test_unauthenticated_access():
    """Test access without token."""
    session_id = "test-session"
    payload = {"question_id": 1, "value": 1}
    response = client.post(f"/api/v1/exams/{session_id}/responses", json=payload)
    assert response.status_code == 401

def test_get_exam_history_api(auth_headers, temp_db):
    """Test retrieving exam history."""
    # Ensure at least one result exists (from previous tests or create explicitly)
    session_id = "history-session-999"
    payload = {
        "total_score": 90,
        "sentiment_score": 80.0,
        "reflection_text": "History check",
        "is_rushed": False,
        "is_inconsistent": False,
        "age": 25,
        "age_group": "18-25",
        "detailed_age_group": "Young Adult"
    }
    client.post(f"/api/v1/exams/{session_id}/complete", json=payload, headers=auth_headers)
    
    # Check history
    response = client.get("/api/v1/exams/history", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any(a["total_score"] == 90 for a in data["assessments"])
