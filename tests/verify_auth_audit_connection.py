
import unittest
from datetime import datetime
from backend.fastapi.api.services.auth_service import AuthService
from app.db import get_session, engine
from backend.fastapi.api.root_models import User, AuditLog, PersonalProfile, UserSettings, Base
import backend.fastapi.api.root_models as root_models

class TestAuthAuditConnection(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(bind=engine)
        self.session = get_session()
        self.auth_service = AuthService(db=self.session)
        
        # Create test user
        self.username = "audit_test_user"
        self.password = "Password123!"
        self.email = "audit_test@example.com"
        
        # Clean previous
        existing = self.session.query(User).filter_by(username=self.username).first()
        if existing:
            self.session.delete(existing)
            self.session.commit()
            
        # Register manually or via service (using service is better but manual is faster/safer for setup)
        from backend.fastapi.api.schemas import UserCreate
        try:
             self.auth_service.register_user(UserCreate(
                 username=self.username,
                 email=self.email,
                 password=self.password,
                 first_name="Test",
                 last_name="User", 
                 age=30,
                 gender="M"
             ))
        except Exception as e:
            print(f"User registration error (might already exist): {e}")
            self.session.rollback()

    def tearDown(self):
        self.session.close()

    def test_login_creates_audit_log(self):
        # Perform Login via AuthService
        user_agent = "TestUserAgent/1.0"
        ip = "127.0.0.1"
        
        user = self.auth_service.authenticate_user(
            self.username, 
            self.password, 
            ip_address=ip, 
            user_agent=user_agent
        )
        
        self.assertIsNotNone(user, "Login failed")
        
        # Verify Audit Log
        # We need a new session or refresh to see changes committed by AuthService?
        # AuthService uses the passed db session (self.session). 
        # But log_event uses its own session unless passed. 
        # In AuthService.authenticate_user, we call AuditService.log_event(self.db, ...)
        # So it uses the SAME session.
        
        audit_log = self.session.query(AuditLog).filter_by(user_id=user.id).order_by(AuditLog.timestamp.desc()).first()
        
        self.assertIsNotNone(audit_log, "Audit log not created")
        self.assertEqual(audit_log.action, "LOGIN")
        self.assertEqual(audit_log.ip_address, ip)
        self.assertEqual(audit_log.user_agent, user_agent)
        print(f"Verified Audit Log: {audit_log.action} - {audit_log.user_agent}")

if __name__ == "__main__":
    unittest.main()
