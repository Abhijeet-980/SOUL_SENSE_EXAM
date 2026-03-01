from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)


def get_real_ip(request: Request) -> str:
    """
    Extract the real client IP address from request headers.
    
    Handles proxy scenarios by checking X-Forwarded-For and X-Real-IP headers.
    This is critical for rate limiting behind proxies (ALB, Nginx, Cloudflare).
    
    Priority:
    1. X-Forwarded-For (first IP in chain - actual client)
    2. X-Real-IP
    3. request.client.host (direct connection)
    
    Args:
        request: FastAPI Request object
        
    Returns:
        Client IP address as string
    """
    # Check X-Forwarded-For header (standard for proxies)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # X-Forwarded-For contains comma-separated IPs: client, proxy1, proxy2
        # The first IP is the actual client
        client_ip = forwarded_for.split(",")[0].strip()
        if client_ip:
            logger.debug(f"Using IP from X-Forwarded-For: {client_ip}")
            return client_ip
    
    # Check X-Real-IP header (Nginx standard)
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        logger.debug(f"Using IP from X-Real-IP: {real_ip}")
        return real_ip
    
    # Fallback to direct client host
    client_host = request.client.host if request.client else "unknown"
    logger.debug(f"Using direct client host: {client_host}")
    return client_host


def get_user_id(request: Request):
    """
    Key function for slowapi to identify users for rate limiting.
    
    Prioritizes authenticated user ID/username, falls back to real IP address.
    Uses get_real_ip() to properly extract client IP behind proxies.
    """
    # 1. Check if user_id was already set in request.state (by some middleware)
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        return f"user_id:{user_id}"

    # 2. Extract from JWT manually if limiter runs before dependency injection
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            from ..config import get_settings_instance
            settings = get_settings_instance()
            from jose import jwt
            
            # Use jwt_secret_key if available (dev), otherwise SECRET_KEY
            secret = getattr(settings, "jwt_secret_key", settings.SECRET_KEY)
            payload = jwt.decode(token, secret, algorithms=[settings.jwt_algorithm])
            username = payload.get("sub")
            if username:
                return f"user:{username}"
        except Exception:
            # Token might be invalid, expired, or for a different scope
            pass
            
    # 3. Fallback to real IP address (handles proxy scenarios)
    return get_real_ip(request)


# Initialize Redis connection for rate limiting storage
# This will be initialized in the application startup
_redis_connection = None


def get_redis_connection():
    """Get or create Redis connection for rate limiting."""
    global _redis_connection
    if _redis_connection is None:
        from ..config import get_settings_instance
        settings = get_settings_instance()
        _redis_connection = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        logger.info(f"Redis connection initialized for rate limiting: {settings.redis_host}:{settings.redis_port}")
    return _redis_connection


# Initialize limiter with Redis storage backend
limiter = Limiter(
    key_func=get_user_id,
    storage_uri=None  # Will be set dynamically on first use via get_redis_connection
)
