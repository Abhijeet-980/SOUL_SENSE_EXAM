
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_security_headers_present():
    """Verify security headers are added to responses."""
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    
    headers = response.headers
    assert headers["X-Frame-Options"] == "DENY"
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["X-XSS-Protection"] == "1; mode=block"
    assert headers["Referrer-Policy"] == "strict-origin-when-cross-origin"

def test_cors_allowed_origin():
    """Verify CORS headers for allowed origin."""
    response = client.options(
        "/api/v1/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET"
        }
    )
    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:3000"
    assert "GET" in response.headers["Access-Control-Allow-Methods"]

def test_cors_disallowed_origin():
    """Verify disallowed origin does not receive CORS headers."""
    response = client.options(
        "/api/v1/health",
        headers={
            "Origin": "http://evil.com",
            "Access-Control-Request-Method": "GET"
        }
    )
    # Be default, FastAPI CORSMiddleware returns 400 for disallowed origins on preflight
    # or just doesn't send the allow-origin header for simple requests.
    # For OPTIONS (preflight), it usually returns 200 but without Access-Control-Allow-Origin
    # or 400 depending on implementation. 
    # Let's check that the Allow-Origin header strictly matches the request origin if allowed,
    # or is missing if disallowed.
    
    assert "Access-Control-Allow-Origin" not in response.headers

def test_cors_credentials():
    """Verify credentials support."""
    response = client.options(
        "/api/v1/health",
        headers={
            "Origin": "http://localhost:8000",
            "Access-Control-Request-Method": "GET"
        }
    )
    assert response.headers["Access-Control-Allow-Credentials"] == "true"
