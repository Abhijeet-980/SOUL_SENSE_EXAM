# --------------------------------------------------------------
# File: c:\Users\ayaan shaikh\Documents\EWOC\SOULSENSE2\backend\fastapi\api\middleware\rbac_middleware.py
# --------------------------------------------------------------
"""RBAC Enforcement Middleware

Ensures that the role information present in the JWT token matches the trusted
source (the database). If a mismatch is detected, the request is rejected with
HTTP 403 – Forbidden. The middleware also populates ``request.state.is_admin``
so downstream dependencies (e.g., ``require_admin``) can rely on a verified
value.

Why a middleware?
* Centralised validation – every endpoint, including those that may have been
  missed during development, is protected.
* Prevents front‑end only role checks – the JWT claim is treated as *untrusted*.
* Works with existing ``require_admin`` dependency which now simply checks the
  pre‑validated ``request.state.is_admin`` flag.
"""

import logging
from typing import Callable

from fastapi import Request, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from ..config import get_settings_instance
from ..services.db_service import get_db
from ..models import User
from sqlalchemy import select

log = logging.getLogger(__name__)

# Re‑use the same token scheme as the auth router
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

async def rbac_middleware(request: Request, call_next: Callable):
    """FastAPI middleware that validates the user's role against the DB.

    - Extracts the JWT from the ``Authorization`` header.
    - Decodes it using the secret key.
    - Retrieves the user record from the database.
    - Compares the ``is_admin`` claim (if present) with the DB value.
    - Populates ``request.state.is_admin`` and ``request.state.user_id``.
    - Raises ``HTTPException(status_code=403)`` on any mismatch.
    """
    settings = get_settings_instance()

    # Default values for non‑authenticated routes (e.g., health checks)
    request.state.is_admin = False
    request.state.user_id = None

    # Only protect routes that require authentication – skip static files, docs, etc.
    if request.url.path.startswith("/api/v1"):
        token: str = await oauth2_scheme(request)
        if not token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authentication token")
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.jwt_algorithm])
            username: str = payload.get("sub")
            token_is_admin: bool = payload.get("is_admin", False)
        except JWTError as exc:
            log.warning("JWT decode error: %s", exc)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        # Fetch the authoritative user record from the DB
        async for db in get_db(request):
            stmt = select(User).filter(User.username == username)
            result = await db.execute(stmt)
            user: User | None = result.scalar_one_or_none()
            if not user:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
            # Store user ID for downstream use (e.g., audit logs)
            request.state.user_id = user.id
            # Compare DB admin flag with token claim
            db_is_admin = getattr(user, "is_admin", False)
            if token_is_admin != db_is_admin:
                log.warning(
                    "RBAC mismatch for user %s: token_is_admin=%s, db_is_admin=%s",
                    username,
                    token_is_admin,
                    db_is_admin,
                )
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role tampering detected")
            # Populate the verified flag for downstream dependencies
            request.state.is_admin = db_is_admin
            break

    response = await call_next(request)
    return response

# End of rbac_middleware.py
