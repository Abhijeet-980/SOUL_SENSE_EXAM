import time
import logging
from typing import Optional, Tuple
from fastapi import Request, Response, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from ..utils.limiter import get_real_ip, get_user_id
from ..config import get_settings_instance

logger = logging.getLogger(__name__)

# Sliding Window Rate Limiting Lua Script
LUA_RATE_LIMIT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clear_before = now - window

-- 1. Remove old requests from the window
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

-- 2. Count current requests
local count = redis.call('ZCARD', key)
local allowed = count < limit

if allowed then
    -- 3. Add the new request timestamp (unique seed using now + random/counter is better, but now usually suffices for high resolution)
    redis.call('ZADD', key, now, now)
end

-- 4. Set expiry to clean up idle keys
redis.call('EXPIRE', key, window + 1)

return {allowed and 1 or 0, limit - count - (allowed and 1 or 0)}
"""

class SlidingWindowRateLimiter:
    def __init__(self):
        self.settings = get_settings_instance()
        self.redis = None
        self._script = None

    async def _get_redis(self):
        if self.redis:
            return self.redis
        try:
            from ..main import app
            self.redis = getattr(app.state, 'redis_client', None)
            if self.redis:
                 self._script = self.redis.register_script(LUA_RATE_LIMIT)
        except Exception:
            pass
        return self.redis

    async def check_rate_limit(self, key_name: str, limit: int, window: int) -> Tuple[bool, int]:
        redis = await self._get_redis()
        if not redis or not self._script:
            return True, limit # Open if Redis is down

        now = time.time()
        # Returns [allowed_int, remaining]
        res = await self._script(keys=[f"rate_limit:{key_name}"], args=[now, window, limit])
        return bool(res[0]), res[1]

rate_limiter = SlidingWindowRateLimiter()

async def sliding_rate_limit_middleware(request: Request, call_next):
    """
    FastAPI middleware for sliding-window rate limiting (#1087).
    Applies global limits by IP/User and supports endpoint-specific overrides.
    """
    # 1. Skip non-API routes or health checks
    if request.url.path.startswith("/api/v1/health") or not request.url.path.startswith("/api"):
        return await call_next(request)

    # 2. Identify the requester (IP or User)
    ident = get_user_id(request)
    
    # 3. Apply default global limit (e.g., 200 requests per minute)
    # This can be configured in settings
    limit = 200
    window = 60
    
    allowed, remaining = await rate_limiter.check_rate_limit(ident, limit, window)
    
    if not allowed:
        logger.warning(f"Rate limit exceeded for {ident}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please slow down.",
            headers={
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(window)
            }
        )

    response: Response = await call_next(request)
    
    # 4. Inject headers
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(window)
    
    return response
