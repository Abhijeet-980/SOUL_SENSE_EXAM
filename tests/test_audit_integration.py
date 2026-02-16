import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.fastapi.api.services.auth_service import AuthService
from backend.fastapi.api.root_models import Base, User, AuditLog, PersonalProfile
from app.services.audit_service import AuditService

class TestAuditIntegration(unittest.TestCase):
    
    def setUp(self):
        # Create in-memory DB
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        
        # Patcher for get_session in auth and audit_service
        self.patcher1 = patch('app.auth.auth.get_session', return_value=self.session)
        self.patcher2 = patch('app.services.audit_service.get_session', return_value=self.session)
        self.patcher1.start()
        self.patcher2.start()
        
        self.auth = AuthManager()

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()
        self.session.close()
        Base.metadata.drop_all(self.engine)

    def test_register_and_login_audit(self):
        # 1. Register
        # register_user commits/closes session internally. 
        # Since we return the SAME session object (self.session), 
        # we need to be careful about session.close() or use scoped_session if logic re-opens.
        # But our simple patch returns the same instance. 
        # Ideally, we should mock get_session to return a NEW session attached to same engine, 
        # or just ensure close doesn't break it. 
        # SQLAlchemy session.close() returns connection to pool, so it's fine for reusing if we don't query same obj?
        # Actually session.close() clears the session. 
        # If we reuse the same session object, we might have issues if code calls close().
        
        # Better approach: 
        # Define get_session side_effect to return new session from same engine?
        # Or just prevent close?
        
        self.session.close = MagicMock() # Prevent closing
        
        success, msg, _ = self.auth.register_user(
            "audit_test_user", "audit@test.com", "Test", "User", 25, "M", "Password123!"
        )
        self.assertTrue(success, f"Registration failed: {msg}")

        # Check Audit Log for REGISTER
        user = self.session.query(User).filter_by(username="audit_test_user").first()
        self.assertIsNotNone(user)
        
        logs = self.session.query(AuditLog).filter_by(user_id=user.id, action="REGISTER").all()
        self.assertEqual(len(logs), 1, "Should have 1 REGISTER log")
        
        # 2. Login
        success, msg, token = self.auth.login_user("audit_test_user", "Password123!")
        self.assertTrue(success, f"Login failed: {msg}")
        
        # Check Audit Log for LOGIN
        logs = self.session.query(AuditLog).filter_by(user_id=user.id, action="LOGIN").all()
        self.assertEqual(len(logs), 1, "Should have 1 LOGIN log")
        
        # 3. Logout
        self.auth.current_user = "audit_test_user"
        self.auth.logout_user()
        
        # Check Audit Log for LOGOUT
        logs = self.session.query(AuditLog).filter_by(user_id=user.id, action="LOGOUT").all()
        self.assertEqual(len(logs), 1, "Should have 1 LOGOUT log")

if __name__ == '__main__':
    unittest.main()
