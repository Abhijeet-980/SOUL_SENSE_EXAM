import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from app.services.audit_service import AuditService
from backend.fastapi.api.root_models import AuditLog

class TestAuditService(unittest.TestCase):

    @patch('app.services.audit_service.get_session')
    def test_log_event_success(self, mock_get_session):
        # Mock Session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        
        # Call
        result = AuditService.log_event(
            user_id=1,
            action="TEST_ACTION",
            ip_address="127.0.0.1",
            user_agent="TestAgent",
            details={"status": "success", "secret": "HIDDEN"}
        )
        
        # Verify
        self.assertTrue(result)
        mock_session.add.assert_called_once()
        saved_log = mock_session.add.call_args[0][0]
        
        self.assertIsInstance(saved_log, AuditLog)
        self.assertEqual(saved_log.user_id, 1)
        self.assertEqual(saved_log.action, "TEST_ACTION")
        
        # Check Details Filtering
        self.assertIn('"status": "success"', saved_log.details)
        self.assertNotIn('secret', saved_log.details) # Should be filtered out

    @patch('app.services.audit_service.get_session')
    def test_log_event_truncation(self, mock_get_session):
        # Mock Session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        
        long_ua = "A" * 300
        
        # Call
        AuditService.log_event(
            user_id=1,
            action="TEST_TRUNCATE",
            user_agent=long_ua
        )
        
        # Verify
        saved_log = mock_session.add.call_args[0][0]
        self.assertTrue(len(saved_log.user_agent) <= 255)
        self.assertTrue(saved_log.user_agent.endswith("..."))

if __name__ == '__main__':
    unittest.main()
