import uuid
import logging
from functools import wraps
from typing import Optional, Callable, Any

import redis.asyncio as redis
from ..config import get_settings_instance

logger = logging.getLogger(__name__)

# Lua script to safely release a lock
RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_redis_pool = None

async def get_redis():
    global _redis_pool
    if _redis_pool is None:
        settings = get_settings_instance()
        _redis_pool = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_pool

class DistributedLock:
    """
    Context manager for a distributed lock using Redis SET NX PX.
    Implements a variation of the Redlock algorithm for single-node Redis.
    """
    def __init__(self, name: str, timeout: int = 60, redis_client: Optional[redis.Redis] = None):
        self.name = f"lock:{name}"
        self.timeout = timeout
        self.lock_value = str(uuid.uuid4())
        self.redis = redis_client
        self._acquired = False

    async def __aenter__(self):
        if self.redis is None:
            self.redis = await get_redis()
            
        acquired = await self.redis.set(
            self.name,
            self.lock_value,
            nx=True,
            px=self.timeout * 1000
        )
        
        if not acquired:
            raise RuntimeError(f"Could not acquire lock for {self.name}")
            
        self._acquired = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._acquired and self.redis:
            try:
                await self.redis.eval(RELEASE_LOCK_SCRIPT, 1, self.name, self.lock_value)
            except Exception as e:
                logger.error(f"Failed to release lock {self.name}: {e}")
            finally:
                self._acquired = False

def require_lock(name: str, timeout: int = 60):
    """
    Decorator to prevent concurrent execution of the same job across worker nodes.
    `name` can be a format string using kwargs from the decorated function.
    """
    import inspect
    
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Attempt to interpolate the name with kwargs
            try:
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                lock_name = name.format(**bound_args.arguments)
            except (KeyError, IndexError, AttributeError):
                lock_name = name
                
            try:
                async with DistributedLock(name=lock_name, timeout=timeout):
                    return await func(*args, **kwargs)
            except RuntimeError as e:
                logger.warning(f"Task skipped due to active lock: {e}")
                raise
        return wrapper
    return decorator
