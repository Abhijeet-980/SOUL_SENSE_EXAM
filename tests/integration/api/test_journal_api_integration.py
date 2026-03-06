"""
Integration tests for Journal API endpoints.
Tests journal entry creation, retrieval, update, and deletion flows.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.fastapi.api.main import app
from backend.fastapi.api.services.db_service import Base, get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///./test_journal.db"

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
        "username": "journal_user",
        "password": "TestPass123!",
        "email": "journal@example.com"
    }
    response = client.post("/api/v1/auth/register", json=user_data)
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


class TestJournalAPI:
    """Test suite for journal endpoints"""

    def test_create_journal_entry(self, client, auth_headers):
        """Test creating a new journal entry"""
        entry_data = {
            "content": "Today was a productive day",
            "mood": "happy",
            "tags": ["work", "productivity"]
        }
        response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json=entry_data
        )
        assert response.status_code == 201
        data = response.json()
        assert data["content"] == entry_data["content"]
        assert data["mood"] == entry_data["mood"]
        assert "id" in data
        assert "created_at" in data

    def test_create_journal_entry_with_sentiment(self, client, auth_headers):
        """Test creating journal entry with AI sentiment analysis"""
        entry_data = {
            "content": "I am very happy and excited about my progress!",
            "mood": "happy"
        }
        response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json=entry_data
        )
        assert response.status_code == 201
        data = response.json()
        assert "sentiment" in data
        assert "sentiment_score" in data

    def test_get_journal_entries(self, client, auth_headers):
        """Test retrieving journal entries"""
        entry_data = {
            "content": "First journal entry",
            "mood": "neutral"
        }
        client.post("/api/v1/journal", headers=auth_headers, json=entry_data)

        response = client.get("/api/v1/journal", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_get_journal_entry_by_id(self, client, auth_headers):
        """Test retrieving a specific journal entry"""
        create_response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Test entry", "mood": "happy"}
        )
        entry_id = create_response.json()["id"]

        response = client.get(f"/api/v1/journal/{entry_id}", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == entry_id

    def test_update_journal_entry(self, client, auth_headers):
        """Test updating a journal entry"""
        create_response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Original content", "mood": "neutral"}
        )
        entry_id = create_response.json()["id"]

        update_data = {
            "content": "Updated content",
            "mood": "happy"
        }
        response = client.put(
            f"/api/v1/journal/{entry_id}",
            headers=auth_headers,
            json=update_data
        )
        assert response.status_code == 200
        data = response.json()
        assert data["content"] == update_data["content"]

    def test_delete_journal_entry(self, client, auth_headers):
        """Test deleting a journal entry"""
        create_response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Entry to delete", "mood": "sad"}
        )
        entry_id = create_response.json()["id"]

        response = client.delete(f"/api/v1/journal/{entry_id}", headers=auth_headers)
        assert response.status_code == 200

        get_response = client.get(f"/api/v1/journal/{entry_id}", headers=auth_headers)
        assert get_response.status_code == 404

    def test_filter_journal_by_mood(self, client, auth_headers):
        """Test filtering journal entries by mood"""
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Happy day", "mood": "happy"}
        )
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Sad moment", "mood": "sad"}
        )

        response = client.get(
            "/api/v1/journal?mood=happy",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert all(entry["mood"] == "happy" for entry in data)

    def test_filter_journal_by_date_range(self, client, auth_headers):
        """Test filtering journal entries by date range"""
        from datetime import datetime, timedelta

        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Recent entry", "mood": "neutral"}
        )

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        response = client.get(
            f"/api/v1/journal?start_date={yesterday}&end_date={today}",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_journal_entry_validation_empty_content(self, client, auth_headers):
        """Test journal entry validation with empty content"""
        response = client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "", "mood": "neutral"}
        )
        assert response.status_code == 400

    def test_get_journal_analytics(self, client, auth_headers):
        """Test retrieving journal analytics"""
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Happy day", "mood": "happy"}
        )
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Another happy day", "mood": "happy"}
        )

        response = client.get("/api/v1/journal/analytics", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert "mood_distribution" in data
        assert "entry_count" in data

    def test_search_journal_entries(self, client, auth_headers):
        """Test searching journal entries by keyword"""
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Important meeting with the team", "mood": "neutral"}
        )
        client.post(
            "/api/v1/journal",
            headers=auth_headers,
            json={"content": "Regular day", "mood": "neutral"}
        )

        response = client.get(
            "/api/v1/journal/search?q=meeting",
            headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert "meeting" in data[0]["content"].lower()
