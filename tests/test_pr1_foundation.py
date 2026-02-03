
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base, User, LoginAttempt, OTP
from app.auth.auth import AuthManager
from datetime import datetime, timedelta

# Create in-memory DB for testing
@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

@pytest.fixture
def auth_manager(mocker):
    # Mock get_session to return the fixture session
    # but AuthManager calls get_session() internally which imports from app.db
    # We need to mock app.db.get_session
    manager = AuthManager()
    return manager

def test_user_schema_extensions(session):
    """Verify new columns exist on User model"""
    user = User(username="test_user", password_hash="hash")
    session.add(user)
    session.commit()
    
    # Check default values
    assert user.is_active is True
    assert user.is_2fa_enabled is False
    assert user.otp_secret is None
    
    # Check modification
    user.is_active = False
    user.otp_secret = "SECRET123"
    session.commit()
    
    assert session.query(User).filter_by(is_active=False).first() is not None

def test_otp_table(session):
    """Verify OTP table works"""
    user = User(username="otp_user", password_hash="hash")
    session.add(user)
    session.commit()
    
    otp = OTP(
        user_id=user.id,
        code_hash="hashed_code",
        type="RESET",
        expires_at=datetime.utcnow() + timedelta(minutes=10)
    )
    session.add(otp)
    session.commit()
    
    saved_otp = session.query(OTP).first()
    assert saved_otp.code_hash == "hashed_code"
    assert saved_otp.user_id == user.id

def test_login_history_schema(session):
    """Verify LoginAttempt accepts failure_reason"""
    attempt = LoginAttempt(
        username="test",
        is_successful=False,
        failure_reason="invalid_password",
        user_agent="pytest"
    )
    session.add(attempt)
    session.commit()
    
    saved = session.query(LoginAttempt).first()
    assert saved.failure_reason == "invalid_password"
    assert saved.user_agent == "pytest"

def test_auth_logic_inactive_user(session, mocker):
    """Test that inactive users cannot login"""
    
    # Setup
    am = AuthManager()
    
    # Mock session
    mocker.patch('app.auth.auth.get_session', return_value=session)
    
    # Create inactive user
    pwd_hash = am.hash_password("Password123!")
    user = User(
        username="inactive_user",
        password_hash=pwd_hash,
        is_active=False
    )
    session.add(user)
    session.commit()
    
    # Attempt login
    success, msg, code = am.login_user("inactive_user", "Password123!")
    
    assert success is False
    assert code == "AUTH003" # Account deactivated
    
    # Verify audit log
    log = session.query(LoginAttempt).filter_by(username="inactive_user").order_by(LoginAttempt.id.desc()).first()
    assert log is not None
    assert log.is_successful is False
    assert log.failure_reason == "account_deactivated"

def test_auth_logic_active_user(session, mocker):
    """Test that active users can login"""
    
    am = AuthManager()
    mocker.patch('app.auth.auth.get_session', return_value=session)
    
    pwd_hash = am.hash_password("Password123!")
    user = User(
        username="active_user",
        password_hash=pwd_hash,
        is_active=True
    )
    session.add(user)
    session.commit()
    
    success, msg, code = am.login_user("active_user", "Password123!")
    assert success is True
    
    # Verify audit log
    log = session.query(LoginAttempt).filter_by(username="active_user").order_by(LoginAttempt.id.desc()).first()
    assert log.is_successful is True
    # Reason might be None for success, or we didn't set it in logic?
    # Logic: self._record_login_attempt(session, id_lower, True) -> reason default None
    assert log.failure_reason is None
