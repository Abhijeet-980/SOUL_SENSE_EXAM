import pytest
from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from backend.fastapi.api.main import app
from backend.fastapi.api.root_models import User, Score, JournalEntry
from backend.fastapi.api.services.auth_service import AuthService

client = TestClient(app)

@pytest.fixture
def auth_headers(temp_db: Session):
    """Creates a test user and returns auth headers."""
    username = "analytics_user"
    password = "TestPass123!"
    email = "analytics@example.com"
    
    # Cleanup
    temp_db.query(User).filter_by(username=username).delete()
    temp_db.commit()
    
    # Create user
    auth = AuthManager()
    auth.register_user(username, email, "Analytics", "User", 30, "M", password)
    
    # Login
    response = client.post("/api/v1/auth/login", data={"username": username, "password": password})
    assert response.status_code == 200
    token = response.json()["access_token"]
    
    return {"Authorization": f"Bearer {token}"}

@pytest.fixture
def user_id(temp_db: Session):
    return temp_db.query(User).filter_by(username="analytics_user").first().id

def test_zero_data_user(auth_headers, temp_db):
    """Test analytics for a fresh user with no data."""
    response = client.get("/api/v1/analytics/me/summary", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    
    assert data["total_exams"] == 0
    assert data["average_score"] == 0.0
    assert data["best_score"] == 0
    assert data["sentiment_trend"] == "stable"
    assert data["consistency_score"] is None

def test_summary_flow(auth_headers, temp_db, user_id):
    """Test summary calculations with real data."""
    # Add scores: 10, 20, 30
    scores = [10, 20, 30]
    for s in scores:
        score = Score(
            user_id=user_id,
            username="analytics_user",
            total_score=s,
            sentiment_score=50.0,
            timestamp=datetime.utcnow().isoformat(),
            age=30,
            detailed_age_group="Adult"
        )
        temp_db.add(score)
    temp_db.commit()
    
    response = client.get("/api/v1/analytics/me/summary", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    
    assert data["total_exams"] == 3
    assert data["average_score"] == 20.0
    assert data["best_score"] == 30
    assert data["latest_score"] == 30
    # Consitency: SD of [10, 20, 30] is 10. Mean is 20. CV = (10/20)*100 = 50.0
    assert data["consistency_score"] == 50.0

def test_data_isolation(auth_headers, temp_db, user_id):
    """Ensure User A cannot see User B's data via analytics."""
    # Add data for "analytics_user" (User A)
    score = Score(user_id=user_id, username="analytics_user", total_score=100, age=30, detailed_age_group="A", timestamp=datetime.utcnow().isoformat())
    temp_db.add(score)
    temp_db.commit()
    
    # Create User B
    auth = AuthManager()
    auth.register_user("intruder", "intruder@example.com", "Bad", "Guy", 25, "M", "Pass1234!")
    login_resp = client.post("/api/v1/auth/login", data={"username": "intruder", "password": "Pass1234!"})
    intruder_token = login_resp.json()["access_token"]
    intruder_headers = {"Authorization": f"Bearer {intruder_token}"}
    
    # User B calls summary
    response = client.get("/api/v1/analytics/me/summary", headers=intruder_headers)
    assert response.status_code == 200
    data = response.json()
    
    # Should be empty/zero, ignoring User A's score
    assert data["total_exams"] == 0
    assert data["best_score"] == 0

def test_wellbeing_trends_sparse(auth_headers, temp_db, user_id):
    """Test handling of journal entries with missing metrics."""
    # 1. Full entry
    j1 = JournalEntry(
        user_id=user_id, 
        username="analytics_user", 
        content="Full", 
        entry_date=(datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d"),
        sleep_hours=8.0,
        energy_level=8
    )
    # 2. Sparse entry
    j2 = JournalEntry(
        user_id=user_id, 
        username="analytics_user", 
        content="Sparse", 
        entry_date=datetime.utcnow().strftime("%Y-%m-%d"),
        sleep_hours=None, # Missing
        energy_level=5
    )
    temp_db.add_all([j1, j2])
    temp_db.commit()
    
    response = client.get("/api/v1/analytics/me/trends?days=7", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    wellbeing = data["wellbeing"]
    
    assert len(wellbeing) == 2
    # Check J1
    assert wellbeing[0]["sleep_hours"] == 8.0
    # Check J2
    assert wellbeing[1]["sleep_hours"] is None
    assert wellbeing[1]["energy_level"] == 5
