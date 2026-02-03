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

    def register_user(self, username, email, first_name, last_name, age, gender, password):
        # Enhanced validation
        if len(username) < 3:
            return False, "Username must be at least 3 characters", "REG003"
        if len(first_name) < 1:
            return False, "First name is required", "REG004"
        if len(password) < 8:
            return False, "Password must be at least 8 characters", "REG005"
        if not self._validate_password_strength(password):
            return False, "Password must contain uppercase, lowercase, number and special character", "REG006"
        if age < 13 or age > 120:
            return False, "Age must be between 13 and 120", "REG007"
        if gender not in ["M", "F", "Other", "Prefer not to say"]:
            return False, "Invalid gender selection", "REG008"

        session = get_session()
        try:
            # 1. Normalize
            username_lower = username.lower().strip()
            email_lower = email.lower().strip()

            # 2. Check if username already exists
            if session.query(User).filter(User.username == username_lower).first():
                return False, "Username already taken", "REG001"

            # 3. Check if email already exists
            from app.models import PersonalProfile
            if session.query(PersonalProfile).filter(PersonalProfile.email == email_lower).first():
                return False, "Email already registered", "REG002"

            password_hash = self.hash_password(password)
            
            # 4. Create User
            new_user = User(
                username=username_lower,
                password_hash=password_hash,
                created_at=datetime.utcnow().isoformat()
            )
            session.add(new_user)
            session.flush()  # Get the user id
            
            # 5. Create personal profile
            profile = PersonalProfile(
                user_id=new_user.id,
                email=email_lower,
                first_name=first_name,
                last_name=last_name,
                age=age,
                gender=gender,
                last_updated=datetime.utcnow().isoformat()
            )
            session.add(profile)
            
            session.commit()
            return True, "Registration successful", None

        except Exception as e:
            session.rollback()
            logging.error(f"Registration failed: {e}")
            return False, "Registration failed", "REG009"
        finally:
            session.close()

    def login_user(self, identifier, password):
        # Check rate limiting
        if self._is_locked_out(identifier):
            return False, "Account temporarily locked due to failed attempts", "AUTH002"

        session = get_session()
        try:
            # Normalize
            id_lower = identifier.lower().strip()

            # 1. Try fetching by username
            user = session.query(User).filter(User.username == id_lower).first()

            # 2. If not found, try fetching by email
            if not user:
                from app.models import PersonalProfile
                profile = session.query(PersonalProfile).filter(PersonalProfile.email == id_lower).first()
                if profile:
                    user = session.query(User).filter(User.id == profile.user_id).first()

            if user and self.verify_password(password, user.password_hash):
                # PR 1: Check if account is active
                if hasattr(user, 'is_active') and not user.is_active:
                    self._record_login_attempt(session, id_lower, False, reason="account_deactivated")
                    session.commit()
                    return False, "Account is deactivated. Please contact support.", "AUTH003"

                # Update last login
                try:
                    user.last_login = datetime.utcnow().isoformat()
                    # Audit success
                    self._record_login_attempt(session, id_lower, True)
                    session.commit()
                except Exception as e:
                    logging.error(f"Failed to update login metadata: {e}")
                    # Allow login even if metadata update fails
                    
                self.current_user = user.username # Return canonical username
                self._generate_session_token()
                return True, "Login successful", None
            else:
                self._record_login_attempt(session, id_lower, False, reason="invalid_credentials")
                session.commit()
                return False, "Incorrect username or password", "AUTH001"

        except Exception as e:
            logging.error(f"Login failed: {e}")
            return False, "Internal error occurred", "GLB001"
        finally:
            session.close()

    def logout_user(self):
        self.current_user = None
        self.session_token = None
        self.session_expiry = None
        # Clear saved Remember Me session
        from app.auth import session_storage
        session_storage.clear_session()

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
        """Check if user is locked out based on recent failed attempts in DB."""
        session = get_session()
        try:
            from app.models import LoginAttempt
            
            # Count recent failed attempts
            since_time = datetime.utcnow() - timedelta(seconds=self.lockout_duration)
            
            recent_failures = session.query(LoginAttempt).filter(
                LoginAttempt.username == username,
                LoginAttempt.is_successful == False,
                LoginAttempt.timestamp >= since_time
            ).count()
            
            return recent_failures >= 5
        except Exception as e:
            logging.error(f"Lockout check failed: {e}")
            return False
        finally:
            session.close()

    def _record_login_attempt(self, session, username, success, reason=None):
        """Record login attempt to DB."""
        try:
            from app.models import LoginAttempt
            attempt = LoginAttempt(
                username=username,
                is_successful=success,
                timestamp=datetime.utcnow(),
                ip_address="desktop",
                failure_reason=reason
            )
            session.add(attempt)
        except Exception as e:
            logging.error(f"Failed to record attempt: {e}")

    # PR 3: Password Reset Flow
    def initiate_password_reset(self, email):
        """
        Trigger the password reset flow.
        1. Find user by email.
        2. Generate OTP.
        3. Send OTP via EmailService.
        Privacy: Always returns success message to prevent enumeration.
        """
        from app.auth.otp_manager import OTPManager
        from app.services.email_service import EmailService
        from app.models import PersonalProfile, User

        session = get_session()
        try:
            # Normalize email
            email_lower = email.lower().strip()
            
            # Find user via profile
            profile = session.query(PersonalProfile).filter(PersonalProfile.email == email_lower).first()
            user = None
            if profile:
                user = session.query(User).filter(User.id == profile.user_id).first()
            
            # Privacy: If user not found, we still return success-like message,
            # but we don't send anything (or maybe send a generic 'account not found' to that email if we wanted)
            # For now, just logging internal check.
            if not user:
                logging.info(f"Password reset requested for unknown email: {email_lower}")
                return True, "If an account exists with this email, a reset code has been sent."

            # Generate OTP
            code, error = OTPManager.generate_otp(user.id, "RESET_PASSWORD")
            
            if not code:
                # Rate limit hit or error
                return False, error or "Too many requests. Please wait."
                
            # Send Email
            if EmailService.send_otp(email_lower, code, "Password Reset"):
                return True, "If an account exists with this email, a reset code has been sent."
            else:
                return False, "Failed to send email. Please try again later."
                
        except Exception as e:
            logging.error(f"Error in initiate_password_reset: {e}")
            return False, "An error occurred. Please try again."
        finally:
            session.close()

    def complete_password_reset(self, email, otp_code, new_password):
        """
        Verify OTP and update password.
        """
        from app.auth.otp_manager import OTPManager
        from app.models import PersonalProfile, User
        
        # Validation
        if not self._validate_password_strength(new_password):
            return False, "Password does not meet complexity requirements."
            
        session = get_session()
        try:
            email_lower = email.lower().strip()
            
            # Find User
            profile = session.query(PersonalProfile).filter(PersonalProfile.email == email_lower).first()
            if not profile:
                return False, "Invalid request."
            
            user = session.query(User).filter(User.id == profile.user_id).first()
            if not user:
                return False, "Invalid request."
                
            # Verify OTP
            if not OTPManager.verify_otp(user.id, otp_code, "RESET_PASSWORD"):
                return False, "Invalid or expired code."
            
            # Update Password
            user.password_hash = self.hash_password(new_password)
            session.commit()
            
            logging.info(f"Password reset successfully for user {user.username}")
            return True, "Password reset successfully. You can now login."
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error in complete_password_reset: {e}")
            return False, "Internal error."
        finally:
            session.close()
