from datetime import timedelta
from typing import Annotated
from fastapi import APIRouter, Depends, status, Request, Response, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

from ..config import get_settings
from ..schemas import UserCreate, Token, UserResponse, ErrorResponse, PasswordResetRequest, PasswordResetComplete, TwoFactorLoginRequest, TwoFactorAuthRequiredResponse, TwoFactorConfirmRequest, UsernameAvailabilityResponse, CaptchaResponse, LoginRequest
from ..services.db_service import get_db
from ..services.auth_service import AuthService
from ..services.captcha_service import captcha_service
from ..utils.network import get_real_ip
from ..constants.security_constants import REFRESH_TOKEN_EXPIRE_DAYS
from ..models import User
from ..utils.limiter import limiter
from backend.fastapi.app.core import (
    AuthenticationError,
    AuthorizationError,
    InvalidCredentialsError,
    TokenExpiredError,
    ValidationError,
    NotFoundError,
    RateLimitError,
    ResourceAlreadyExistsError,
    BusinessLogicError
)
# Rate limiters imported inline within routes to avoid potential circular/timing issues
from sqlalchemy.orm import Session
from cachetools import TTLCache
import secrets

router = APIRouter()
settings = get_settings()

@router.get("/captcha", response_model=CaptchaResponse)
@limiter.limit("100/minute")
async def get_captcha(request: Request):
    """Generate a new CAPTCHA."""
    session_id = secrets.token_urlsafe(16)
    code = captcha_service.generate_captcha(session_id)
    return CaptchaResponse(captcha_code=code, session_id=session_id)

@router.get("/server-id")
async def get_server_id(request: Request):
    """Return the current server instance ID. Changes on every server restart."""
    return {"server_id": getattr(request.app.state, "server_instance_id", None)}

# oauth2_scheme is still needed for other endpoints but login now uses custom JSON
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

async def get_current_user(request: Request, token: Annotated[str, Depends(oauth2_scheme)], db: Session = Depends(get_db)):
    """
    Get current user from JWT token.
    This remains in the router/dependency layer as it couples HTTP security with logic.
    """
    try:
        # Pydantic schema validation for TokenData could be used here
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.jwt_algorithm])
        
        # Check if token is revoked
        from ..root_models import TokenRevocation
        revoked = db.query(TokenRevocation).filter(TokenRevocation.token_str == token).first()
        if revoked:
            raise TokenExpiredError("Token has been revoked")

        username: str = payload.get("sub")
        if not username:
            raise InvalidCredentialsError()
    except JWTError:
        raise InvalidCredentialsError()

    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise InvalidCredentialsError()
    
    # Set user_id in request state for rate limiting and auditing
    request.state.user_id = user.id
    # Check if user is active
    if not getattr(user, 'is_active', True):
        raise BusinessLogicError(
            message="User account is inactive",
            code="INACTIVE_ACCOUNT"
        )
    
    # Check if user is deleted
    if getattr(user, 'is_deleted', False) or getattr(user, 'deleted_at', None) is not None:
        raise AuthorizationError(message="User account is deleted")
    
    return user


# Rate limiter cache for username checks (20 per minute per IP)
availability_limiter_cache = TTLCache(maxsize=1000, ttl=60)

@router.get("/check-username", response_model=UsernameAvailabilityResponse)
@limiter.limit("20/minute")
async def check_username_availability(
    username: str,
    request: Request,
    auth_service: AuthService = Depends()
):
    """
    Check if a username is available.
    Rate limited to 20 requests per minute per IP.
    """
    available, message = auth_service.check_username_available(username)
    return UsernameAvailabilityResponse(available=available, message=message)


@router.post("/register", response_model=None, responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
@limiter.limit("10/minute")
async def register(
    request: Request,
    user: UserCreate, 
    auth_service: AuthService = Depends()
):
    """Register a new user. Rate limited to 10 requests per minute per IP/user."""
    success, new_user, message = auth_service.register_user(user)
    
    if not success:
        raise BusinessLogicError(
            message=message,
            code="REGISTRATION_FAILED"
        )
         
    # Always return a generic success message to prevent enumeration
    return {"message": message}


@router.post("/login", response_model=Token, responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 202: {"model": TwoFactorAuthRequiredResponse}})
@limiter.limit("10/minute")
async def login(
    response: Response,
    login_request: LoginRequest, 
    request: Request,
    auth_service: AuthService = Depends()
):
    """Login endpoint. Rate limited to 10 requests per minute per IP/user."""
    ip = get_real_ip(request)
    user_agent = request.headers.get("user-agent", "Unknown")

    # 1. Start with CAPTCHA Validation (Before Rate Limiting to prevent spam cheapness)
    if not captcha_service.validate_captcha(login_request.session_id, login_request.captcha_input):
        raise ValidationError(
            message="The CAPTCHA validation failed. Please refresh the CAPTCHA and try again.",
            details=[{"field": "captcha_input", "error": "Invalid CAPTCHA"}]
        )

    user = auth_service.authenticate_user(login_request.identifier, login_request.password, ip_address=ip, user_agent=user_agent)
    
    # PR 4: 2FA Check
    if user.is_2fa_enabled:
        pre_auth_token = auth_service.initiate_2fa_login(user)
        response.status_code = status.HTTP_202_ACCEPTED
        return TwoFactorAuthRequiredResponse(
            pre_auth_token=pre_auth_token
        )

    # Standard Login
    access_token = auth_service.create_access_token(
        data={"sub": user.username}
    )
    
    refresh_token = auth_service.create_refresh_token(user.id)
    has_multiple_sessions = auth_service.has_multiple_active_sessions(user.id)

    
    # Set refresh token in HttpOnly cookie
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=settings.is_production, 
        samesite="lax",
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    )
    
    # return Token(access_token=access_token, token_type="bearer", refresh_token=refresh_token)
    return Token(
        access_token=access_token,
        token_type="bearer",
        refresh_token=refresh_token,
        username=user.username,
        email=user.personal_profile.email if user.personal_profile else None,
        id=user.id,
        created_at=user.created_at,
        warnings=(
            [{
                "code": "MULTIPLE_SESSIONS_ACTIVE",
                "message": "Your account is active on another device or browser."
            }] if has_multiple_sessions else []
        ),
        onboarding_completed=user.onboarding_completed or False
    )



@router.post("/login/2fa", response_model=Token, responses={401: {"model": ErrorResponse}})
@limiter.limit("5/minute")
async def verify_2fa(
    login_request: TwoFactorLoginRequest,
    response: Response,
    request: Request,
    auth_service: AuthService = Depends()
):
    """
    Verify 2FA code and issue tokens.
    """
    ip = get_real_ip(request)
    user = auth_service.verify_2fa_login(login_request.pre_auth_token, login_request.code, ip_address=ip)
    
    # Issue Tokens
    access_token = auth_service.create_access_token(
        data={"sub": user.username}
    )
    
    refresh_token = auth_service.create_refresh_token(user.id)
    has_multiple_sessions = auth_service.has_multiple_active_sessions(user.id)

    
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=settings.is_production,
        samesite="lax",
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    )
    
    # return Token(access_token=access_token, token_type="bearer", refresh_token=refresh_token)
    return Token(
        access_token=access_token,
        token_type="bearer",
        refresh_token=refresh_token,
        username=user.username,
        email=user.personal_profile.email if user.personal_profile else None,
        id=user.id,
        created_at=user.created_at,
        warnings=(
            [{
                "code": "MULTIPLE_SESSIONS_ACTIVE",
                "message": "Your account is active on another device or browser."
            }] if has_multiple_sessions else []
        ),
        onboarding_completed=user.onboarding_completed or False
    )



@router.post("/refresh", response_model=Token)
async def refresh(
    request: Request,
    response: Response,
    auth_service: AuthService = Depends()
):
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise AuthenticationError(
            message="Refresh token missing",
            code="REFRESH_TOKEN_MISSING"
        )
        
    access_token, new_refresh_token = auth_service.refresh_access_token(refresh_token)
    
    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True,
        secure=settings.is_production,
        samesite="lax",
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    )
    
    return Token(access_token=access_token, token_type="bearer", refresh_token=new_refresh_token)


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    current_user: Annotated[User, Depends(get_current_user)],
    token: Annotated[str, Depends(oauth2_scheme)],
    auth_service: AuthService = Depends()
):
    """ Logout the current user by revoking tokens and clearing cookies. """
    # 1. Revoke Refresh Token (from cookie)
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        auth_service.revoke_refresh_token(refresh_token)
    
    # 2. Revoke Access Token (from header)
    auth_service.revoke_access_token(token)
    
    # 3. Audit Logout
    from app.services.audit_service import AuditService
    AuditService.log_event(
        current_user.id,
        "LOGOUT",
        ip_address=request.client.host,
        user_agent=request.headers.get("user-agent", "Unknown"),
        db_session=auth_service.db
    )
        
    response.delete_cookie("refresh_token")
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: Annotated[User, Depends(get_current_user)]):
    return UserResponse(id=current_user.id, username=current_user.username, created_at=current_user.created_at)


@router.post("/password-reset/initiate")
async def initiate_password_reset(
    request: Request,
    reset_data: PasswordResetRequest, # Renamed to avoid name conflict with Request
    background_tasks: BackgroundTasks,
    auth_service: AuthService = Depends()
):
    from ..middleware.rate_limiter import password_reset_limiter
    """
    Initiate the password reset flow.
    ALWAYS returns success message to prevent user enumeration.
    """
    # Rate limit by IP
    real_ip = get_real_ip(request)
    is_limited, wait_time = password_reset_limiter.is_rate_limited(real_ip)
    if is_limited:
        raise RateLimitError(
            message=f"Too many reset requests. Please try again in {wait_time}s.",
            wait_seconds=wait_time
        )

    # Rate limit by Email
    is_limited, wait_time = password_reset_limiter.is_rate_limited(f"reset_{reset_data.email}")
    if is_limited:
        raise RateLimitError(
            message=f"Multiple requests for this email. Please try again in {wait_time}s.",
            wait_seconds=wait_time
        )

    success, message = await auth_service.initiate_password_reset(reset_data.email, background_tasks)
    if not success:
        # Rate limit or server error
        raise BusinessLogicError(
            message=message,
            code="PASSWORD_RESET_FAILED"
        )
    return {"message": message}


@router.post("/password-reset/complete")
async def complete_password_reset(
    request: PasswordResetComplete,
    req_obj: Request, # Need Request object for IP
    auth_service: AuthService = Depends()
):
    from ..middleware.rate_limiter import password_reset_limiter
    """
    Verify OTP and set new password.
    """
    # Rate limit by IP for OTP attempts
    real_ip = get_real_ip(req_obj)
    is_limited, wait_time = password_reset_limiter.is_rate_limited(real_ip)
    if is_limited:
        raise RateLimitError(
            message=f"Too many attempts. Please try again in {wait_time}s.",
            wait_seconds=wait_time
        )

    success, message = auth_service.complete_password_reset(
        request.email, 
        request.otp_code, 
        request.new_password
    )
    if not success:
        raise ValidationError(
            message=message,
            details=[{"field": "otp_code", "error": "Invalid or expired OTP"}]
        )
    return {"message": message}


@router.post("/2fa/setup/initiate")
@limiter.limit("5/minute")
async def initiate_2fa_setup(
    request: Request,
    current_user: Annotated[User, Depends(get_current_user)],
    auth_service: AuthService = Depends()
):
    """Send OTP to verify email before enabling 2FA."""
    if auth_service.send_2fa_setup_otp(current_user):
        return {"message": "OTP sent to your email"}
    raise BusinessLogicError(
        message="Could not send OTP. Check email configuration.",
        code="OTP_SEND_FAILED"
    )


@router.post("/2fa/enable")
@limiter.limit("5/minute")
async def enable_2fa(
    request: Request,
    confirm_request: TwoFactorConfirmRequest,
    current_user: Annotated[User, Depends(get_current_user)],
    auth_service: AuthService = Depends()
):
    """Enable 2FA after verifying OTP."""
    if auth_service.enable_2fa(current_user.id, confirm_request.code):
        return {"message": "2FA enabled successfully"}
    raise ValidationError(
        message="Invalid verification code",
        details=[{"field": "code", "error": "Invalid or expired verification code"}]
    )


@router.post("/2fa/disable")
@limiter.limit("5/minute")
async def disable_2fa(
    request: Request,
    current_user: Annotated[User, Depends(get_current_user)],
    auth_service: AuthService = Depends()
):
    """Disable 2FA."""
    if auth_service.disable_2fa(current_user.id):
        return {"message": "2FA disabled"}
    raise BusinessLogicError(
        message="Failed to disable 2FA",
        code="2FA_DISABLE_FAILED"
    )
