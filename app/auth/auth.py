import bcrypt
import secrets
import time
from datetime import datetime, timedelta
from app.db import get_session
from app.models import User
from app.security_config import PASSWORD_HASH_ROUNDS, LOCKOUT_DURATION_MINUTES
import logging

class AuthManager:
    def __init__(self):
        self.current_user = None
        self.session_token = None
        self.session_expiry = None
        self.session_expiry = None
        self.failed_attempts = {}
        self.lockout_duration = LOCKOUT_DURATION_MINUTES * 60

    def hash_password(self, password):
        """Hash password using bcrypt with configurable rounds."""
        salt = bcrypt.gensalt(rounds=PASSWORD_HASH_ROUNDS)
        return bcrypt.hashpw(password.encode(), salt).decode()

    def verify_password(self, password, password_hash):
        """Verify password against bcrypt hash."""
        try:
            return bcrypt.checkpw(password.encode(), password_hash.encode())
        except Exception as e:
            logging.error(f"Password verification failed: {e}")
            return False

    def register_user(self, name, email, age, gender, password):
        # Enhanced validation
        if len(name) < 2:
            return False, "Name must be at least 2 characters"
        if len(password) < 8:
            return False, "Password must be at least 8 characters"
        if not self._validate_password_strength(password):
            return False, "Password must contain uppercase, lowercase, number and special character"
        if age < 13 or age > 120:
            return False, "Age must be between 13 and 120"
        if gender not in ["Male", "Female", "Other", "Prefer not to say"]:
            return False, "Invalid gender selection"

        session = get_session()
        try:
            # Check if username (name) already exists
            existing_user = session.query(User).filter_by(username=name).first()
            if existing_user:
                return False, "Username already exists"

            # Check if email already exists
            from app.models import PersonalProfile
            existing_email = session.query(PersonalProfile).filter_by(email=email).first()
            if existing_email:
                return False, "Email already exists"

            password_hash = self.hash_password(password)
            
            # Calculate date_of_birth from age
            from datetime import datetime
            current_year = datetime.utcnow().year
            birth_year = current_year - age
            date_of_birth = f"{birth_year}-01-01"  # Approximate
            
            new_user = User(
                username=name,
                password_hash=password_hash,
                created_at=datetime.utcnow().isoformat()
            )
            session.add(new_user)
            session.flush()  # Get the user id
            
            # Create personal profile
            profile = PersonalProfile(
                user_id=new_user.id,
                email=email,
                date_of_birth=date_of_birth,
                gender=gender,
                last_updated=datetime.utcnow().isoformat()
            )
            session.add(profile)
            
            session.commit()
            return True, "Registration successful"

        except Exception as e:
            session.rollback()
            logging.error(f"Registration failed: {e}")
            return False, "Registration failed"
        finally:
            session.close()

    def login_user(self, username, password):
        # Check rate limiting
        if self._is_locked_out(username):
            return False, "Account temporarily locked due to failed attempts"

        session = get_session()
        try:
            user = session.query(User).filter_by(username=username).first()

            if user and self.verify_password(password, user.password_hash):
                user.last_login = datetime.utcnow().isoformat()
                session.commit()
                self.current_user = username
                self._generate_session_token()
                self._reset_failed_attempts(username)
                return True, "Login successful"
            else:
                self._record_failed_attempt(username)
                return False, "Invalid username or password"

        except Exception as e:
            logging.error(f"Login failed: {e}")
            return False, "Login failed"
        finally:
            session.close()

    def logout_user(self):
        self.current_user = None
        self.session_token = None
        self.session_expiry = None

    def is_logged_in(self):
        if self.current_user is None:
            return False
        if self.session_expiry and datetime.utcnow() > self.session_expiry:
            self.logout_user()
            return False
        return True

    def _validate_password_strength(self, password):
        """Validate password contains required character types"""
        import re
        if len(password) < 8:
            return False
        if not re.search(r'[A-Z]', password):
            return False
        if not re.search(r'[a-z]', password):
            return False
        if not re.search(r'\d', password):
            return False
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False
        return True

    def _generate_session_token(self):
        """Generate secure session token"""
        self.session_token = secrets.token_urlsafe(32)
        self.session_expiry = datetime.utcnow() + timedelta(hours=24)

    def _is_locked_out(self, username):
        """Check if user is locked out due to failed attempts"""
        if username not in self.failed_attempts:
            return False
        attempts, last_attempt = self.failed_attempts[username]
        if attempts >= 5 and (time.time() - last_attempt) < self.lockout_duration:
            return True
        return False

    def _record_failed_attempt(self, username):
        """Record failed login attempt"""
        current_time = time.time()
        if username in self.failed_attempts:
            attempts, _ = self.failed_attempts[username]
            self.failed_attempts[username] = (attempts + 1, current_time)
        else:
            self.failed_attempts[username] = (1, current_time)

    def _reset_failed_attempts(self, username):
        """Reset failed attempts on successful login"""
        if username in self.failed_attempts:
            del self.failed_attempts[username]
