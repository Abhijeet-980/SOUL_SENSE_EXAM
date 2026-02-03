import pytest
from unittest.mock import MagicMock, patch
from app.auth.auth import AuthManager
from app.auth.otp_manager import OTPManager
from app.models import User, PersonalProfile, OTP
from datetime import datetime, timedelta

@pytest.fixture
def auth_manager(temp_db):
    return AuthManager()

def test_password_reset_flow(auth_manager, temp_db):
    """
    Verify the full password reset flow.
    """
    # 1. Setup User
    username = "reset_test"
    email = "reset@test.com"
    password = "OldPassword1!"
    
    # Create user manually or via register
    # We'll use register to ensure profile is linked
    auth_manager.register_user(username, email, "First", "Last", 20, "M", password)
    
    # 2. Initiate Reset with Known Code
    # We patch secrets.choice to return "1" -> Code will be "111111"
    with patch("app.auth.otp_manager.secrets.choice", return_value="1"): 
        with patch("app.services.email_service.EmailService.send_otp", return_value=True) as mock_send:
            success, msg = auth_manager.initiate_password_reset(email)
            assert success
            assert "sent" in msg
            assert mock_send.called
            
    # 3. Verify DB state (OTP stored)
    user = temp_db.query(User).filter_by(username=username).first()
    otp_record = temp_db.query(OTP).filter_by(user_id=user.id, type="RESET_PASSWORD").first()
    assert otp_record is not None
    assert not otp_record.is_used
    
    # 4. Verify & Reset
    code = "111111"
    new_password = "NewPassword1!"
    success, msg = auth_manager.complete_password_reset(email, code, new_password)
    assert success
    assert "success" in msg
    
    # 5. Verify DB state
    temp_db.refresh(otp_record)
    assert otp_record.is_used
        
    # 6. Verify Login
    temp_db.refresh(user) # Refresh to get new hash
    
    # Old password should fail
    assert not auth_manager.verify_password(password, user.password_hash)
    # New password should pass
    # auth_manager.verify_password checks hash validity, return True/False
    assert auth_manager.verify_password(new_password, user.password_hash)

def test_rate_limiting(auth_manager, temp_db):
    """Verify rate limiting for OTP generation"""
    username = "rate_test"
    email = "rate@test.com"
    auth_manager.register_user(username, email, "Rate", "Test", 25, "F", "Password123!")
    
    # 1. First Request
    success, _ = auth_manager.initiate_password_reset(email)
    assert success
    
    # 2. Immediate Second Request (Should fail)
    success, msg = auth_manager.initiate_password_reset(email)
    assert not success
    assert "wait" in msg.lower()

def test_otp_expiry(auth_manager, temp_db):
    """Verify OTP expiration"""
    username = "expire_test"
    email = "expire@test.com"
    auth_manager.register_user(username, email, "Exp", "Test", 30, "M", "Password123!")
    
    # Generate OTP (Force code 111111)
    with patch("app.auth.otp_manager.secrets.choice", return_value="1"):
        auth_manager.initiate_password_reset(email)
        code = "111111"
        
        # Advance time by 6 minutes
        with patch("app.auth.otp_manager.datetime") as mock_dt:
            from datetime import timedelta
            # We need to mock datetime.utcnow() to return FUTURE time
            # But stored OTP has REAL creation time (from auth_manager call above)
            # So checking it against FUTURE mocked time works.
            
            future = datetime.utcnow() + timedelta(minutes=6)
            mock_dt.utcnow.return_value = future
            
            success, msg = auth_manager.complete_password_reset(email, code, "NewPass1!")
            assert not success
            assert "expired" in msg.lower()
