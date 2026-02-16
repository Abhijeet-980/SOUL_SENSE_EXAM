
import unittest
from fastapi.testclient import TestClient
from typing import List
from backend.fastapi.api.main import app
from app.db import get_session, engine
from backend.fastapi.api.root_models import User, AuditLog, Base

class TestAuditAPI(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(bind=engine)
        self.client = TestClient(app)
        # Create unique user for test
        self.username = "audit_api_user"
        self.password = "Password123!"
        self.email = "audit_api@test.com"
        
        # Cleanup
        session = get_session()
        existing = session.query(User).filter_by(username=self.username).first()
        if existing:
            session.delete(existing)
            session.commit()
        session.close()

    def test_get_audit_logs(self):
        # 1. Register
        reg_payload = {
            "username": self.username,
            "email": self.email,
            "password": self.password,
            "first_name": "Audit",
            "age": 25,
            "gender": "M"
        }
        resp = self.client.post("/api/v1/auth/register", json=reg_payload)
        self.assertEqual(resp.status_code, 201, f"Registration failed: {resp.text}")

        # 2. Login (This should generate a LOGIN audit log)
        login_payload = {
            "username": self.username,
            "password": self.password
        }
        # Login endpoint might be urlencoded or json. 
        # Looking at login page calls: application/x-www-form-urlencoded
        resp = self.client.post("/api/v1/auth/login", data=login_payload)
        self.assertEqual(resp.status_code, 200, f"Login failed: {resp.text}")
        token = resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 3. Get Audit Logs
        resp = self.client.get("/api/v1/users/me/audit-logs", headers=headers)
        self.assertEqual(resp.status_code, 200, f"Get logs failed: {resp.text}")
        
        logs = resp.json()
        self.assertTrue(isinstance(logs, list))
        self.assertGreater(len(logs), 0)
        
        # Verify log content
        actions = [log["action"] for log in logs]
        print(f"DEBUG: Found audit actions: {actions}")
        
        # Should have REGISTER and LOGIN
        self.assertIn("REGISTER", actions)
        self.assertIn("LOGIN", actions)
        
        # Check structure
        first_log = logs[0]
        self.assertIn("id", first_log)
        self.assertIn("timestamp", first_log)
        self.assertIn("action", first_log)
        self.assertIn("ip_address", first_log)

if __name__ == "__main__":
    unittest.main()
