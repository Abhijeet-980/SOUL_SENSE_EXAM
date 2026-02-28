from ..config import get_settings_instance
import time
import logging
from typing import Tuple
from fastapi import Request, HTTPException, status
import redis.asyncio as redis

from api.config import get_settings_instance

logger = logging.getLogger(__name__)

class RedisRateLimiter:
    """
    Distributed rate limiter using Redis sorted sets for sliding window logic.
    """
    def __init__(self, key_prefix: str, max_requests: int = 5, window_seconds: int = 600):
        self.key_prefix = key_prefix
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.settings = get_settings_instance()
        self.redis = redis.from_url(self.settings.redis_url, decode_responses=True)

    async def is_rate_limited(self, identifier: str) -> Tuple[bool, int]:
        """
        Check if the key is rate limited.
        Returns (is_limited, remaining_seconds)
        """
        key = f"rate_limit:{self.key_prefix}:{identifier}"
        now = time.time()
        window_start = now - self.window_seconds

        async with self.redis.pipeline(transaction=True) as pipe:
            try:
                # Remove timestamps older than the window
                pipe.zremrangebyscore(key, 0, window_start)
                
                # Add the current timestamp
                # Using timestamp+counter or just timestamp since we need unique elements in sorted set
                import uuid
                unique_val = f"{now}-{uuid.uuid4().hex[:6]}"
                
                pipe.zadd(key, {unique_val: now})
                
                # Count the number of requests in the current window
                pipe.zcard(key)
                
                # Get the oldest timestamp to calculate wait time if needed
                pipe.zrange(key, 0, 0, withscores=True)
                
                # Set TTL on the key so we don't leak memory
                pipe.expire(key, self.window_seconds)
                
                results = await pipe.execute()
                
                request_count = results[2]
                
                if request_count > self.max_requests:
                    # Revert this recent insertion
                    await self.redis.zrem(key, unique_val)
                    
                    oldest_data = results[3]
                    if oldest_data:
                        oldest_ts = oldest_data[0][1]
                        wait_time = int(self.window_seconds - (now - oldest_ts))
                        return True, max(0, wait_time)
                    return True, self.window_seconds
                    
                return False, 0
            except Exception as e:
                logger.error(f"Redis rate limiting error: {e}")
                # Fail open to not block users on Redis failure
                return False, 0

# Global limiters
login_limiter = RedisRateLimiter("login", max_requests=10, window_seconds=60)
registration_limiter = RedisRateLimiter("register", max_requests=10, window_seconds=60)
password_reset_limiter = RedisRateLimiter("pw_reset", max_requests=10, window_seconds=60)
analytics_limiter = RedisRateLimiter("analytics", max_requests=30, window_seconds=60)

async def rate_limit_analytics(request: Request):
    client_ip = request.client.host if request.client else "unknown"
    is_limited, wait_time = await analytics_limiter.is_rate_limited(client_ip)
    if is_limited:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many analytics requests. Please wait {wait_time}s."
        )
