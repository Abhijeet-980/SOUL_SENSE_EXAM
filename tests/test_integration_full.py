import pytest
import time
from datetime import datetime
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from backend.fastapi.api.main import app
from backend.fastapi.api.root_models import User, Score, AssessmentResult
from app.auth.auth import AuthManager
from backend.fastapi.api.services.deep_dive_service import DeepDiveService

client = TestClient(app)

@pytest.fixture
def clean_db(temp_db: Session):
    """Clean specific tables for integration test."""
    temp_db.query(AssessmentResult).delete()
    temp_db.query(Score).delete()
    temp_db.query(User).filter(User.username.like("int_%")).delete()
    temp_db.commit()

@pytest.fixture
def user_setup(clean_db, temp_db: Session):
    """Create a user and return token."""
    username = "int_user"
    email = "int_user@example.com"
    password = "IntPassword123!"
    
    auth = AuthManager()
    auth.register_user(username, email, "Inte", "Gration", 25, "F", password)
    
    response = client.post("/api/v1/auth/login", data={"username": username, "password": password})
    assert response.status_code == 200
    token = response.json()["access_token"]
    
    return {
        "headers": {"Authorization": f"Bearer {token}"},
        "username": username,
        "password": password
    }

def test_golden_path_full_journey(user_setup, temp_db):
    """
    Verify the complete user journey:
    1. Check initial empty analytics
    2. Take an Exam
    3. Check updated analytics & recommendations
    4. Take a Recommended Deep Dive
    5. Check History
    6. Export Data
    """
    headers = user_setup["headers"]
    
    # -------------------------------------------------------------------------
    # 1. Initial State Check
    # -------------------------------------------------------------------------
    start = time.time()
    
    resp = client.get("/api/v1/analytics/me/summary", headers=headers)
    assert resp.status_code == 200
    stats = resp.json()
    assert stats["total_exams"] == 0
    
    # Recommendations should fallback to 'strengths'
    resp = client.get("/api/v1/deep-dive/recommendations", headers=headers)
    assert resp.status_code == 200
    recs = resp.json()
    assert "strengths_deep_dive" in recs
    
    # -------------------------------------------------------------------------
    # 2. Core Exam Flow
    # -------------------------------------------------------------------------
    # Start
    resp = client.post("/api/v1/exams/start", headers=headers)
    assert resp.status_code == 201
    session_id = resp.json()["session_id"]
    
    # Save Response
    resp = client.post(f"/api/v1/exams/{session_id}/responses", 
                       json={"question_id": 1, "value": 4}, 
                       headers=headers)
    assert resp.status_code == 201
    
    # Complete
    complete_payload = {
        "total_score": 80,
        "sentiment_score": 0.5,
        "reflection_text": "Integration Test",
        "age": 25,
        "age_group": "Young Adult",
        "detailed_age_group": "25-29",
        "is_rushed": False,
        "is_inconsistent": False
    }
    resp = client.post(f"/api/v1/exams/{session_id}/complete", json=complete_payload, headers=headers)
    assert resp.status_code == 200
    result = resp.json()
    assert "total_score" in result
    
    # -------------------------------------------------------------------------
    # 3. Analytics Update
    # -------------------------------------------------------------------------
    resp = client.get("/api/v1/analytics/me/summary", headers=headers)
    assert resp.status_code == 200
    stats = resp.json()
    assert stats["total_exams"] == 1
    
    # -------------------------------------------------------------------------
    # 4. Deep Dive Flow
    # -------------------------------------------------------------------------
    # Fetch questions for "career_clarity"
    target_type = "career_clarity"
    resp = client.get(f"/api/v1/deep-dive/{target_type}/questions?count=5", headers=headers)
    assert resp.status_code == 200
    questions = resp.json()
    assert len(questions) == 5
    
    # Construct submission
    responses = {
        q["text"]: 5  # Give full marks
        for q in questions
    }
    
    # Submit
    payload = {
        "assessment_type": target_type,
        "responses": responses
    }
    resp = client.post("/api/v1/deep-dive/submit", json=payload, headers=headers)
    assert resp.status_code == 200
    dd_result = resp.json()
    assert dd_result["normalized_score"] == 100
    
    # Check History
    resp = client.get("/api/v1/deep-dive/history", headers=headers)
    assert resp.status_code == 200
    history = resp.json()
    assert len(history) >= 1
    assert history[0]["assessment_type"] == target_type

    # -------------------------------------------------------------------------
    # 5. Export Data
    # -------------------------------------------------------------------------
    resp = client.post("/api/v1/export", json={"format": "json"}, headers=headers)
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]
    download_url = resp.json()["download_url"]
    
    # Verify download access
    resp = client.get(download_url, headers=headers)
    assert resp.status_code == 200
    content_disp = resp.headers["content-disposition"]
    assert "attachment" in content_disp or "filename=" in content_disp

def test_deep_dive_security_edge_cases(user_setup, temp_db):
    """Verify security controls and edge cases."""
    headers = user_setup["headers"]
    
    # 1. Invalid Assessment Type
    resp = client.get("/api/v1/deep-dive/fake_test/questions", headers=headers)
    assert resp.status_code == 404
    
    # 2. Ghost Question Submission (Schema Consistency)
    payload = {
        "assessment_type": "career_clarity",
        "responses": {
            "I am a ghost question": 5
        }
    }
    resp = client.post("/api/v1/deep-dive/submit", json=payload, headers=headers)
    assert resp.status_code == 400
    assert "Invalid question" in resp.json()["detail"]
    
    # 3. Invalid Score Range (<1 or >5)
    questions = DeepDiveService.get_questions("career_clarity", 1)
    q_text = questions[0].text
    
    payload = {
        "assessment_type": "career_clarity",
        "responses": {
            q_text: 6 # Invalid
        }
    }
    resp = client.post("/api/v1/deep-dive/submit", json=payload, headers=headers)
    assert resp.status_code == 400
    
    # 4. Data Isolation
    # Create User B
    auth = AuthManager()
    auth.register_user("int_user_b", "b@example.com", "B", "User", 30, "M", "Pass1234!")
    login_resp = client.post("/api/v1/auth/login", data={"username": "int_user_b", "password": "Pass1234!"})
    token_b = login_resp.json()["access_token"]
    headers_b = {"Authorization": f"Bearer {token_b}"}
    
    # User B checks history (should be empty, despite User A's activity)
    resp = client.get("/api/v1/deep-dive/history", headers=headers_b)
    assert resp.status_code == 200
    assert len(resp.json()) == 0
