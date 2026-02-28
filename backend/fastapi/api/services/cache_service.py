from ..config import get_settings_instance
import json
import logging
from typing import Any, Optional
import redis.asyncio as redis

from api.config import get_settings_instance

logger = logging.getLogger(__name__)

class CacheService:
    def __init__(self):
        self.settings = get_settings_instance()
        self.redis: Optional[redis.Redis] = None

    async def connect(self):
        if not self.redis:
            self.redis = redis.from_url(self.settings.redis_url, decode_responses=True)

    async def get(self, key: str) -> Optional[Any]:
        await self.connect()
        try:
            val = await self.redis.get(key)
            if val:
                return json.loads(val)
            return None
        except Exception as e:
            logger.error(f"Redis get error for {key}: {e}")
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int = 3600):
        await self.connect()
        try:
            await self.redis.setex(key, ttl_seconds, json.dumps(value))
        except Exception as e:
            logger.error(f"Redis set error for {key}: {e}")

    async def delete(self, key: str):
        await self.connect()
        try:
            await self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis delete error for {key}: {e}")

    async def invalidate_prefix(self, prefix: str):
        await self.connect()
        try:
            # Note: keys is not recommended for very huge datasets but since this is targeted caches, it's fine. 
            # Better approach is SCAN
            cursor = '0'
            while cursor != 0:
                cursor, keys = await self.redis.scan(cursor=cursor, match=f"{prefix}*", count=100)
                if keys:
                    await self.redis.delete(*keys)
        except Exception as e:
            logger.error(f"Redis invalidate_prefix error for {prefix}: {e}")

    def sync_invalidate(self, key: str):
        try:
            import redis
            r = redis.from_url(self.settings.redis_url)
            r.delete(key)
        except Exception as e:
            logger.error(f"Redis sync delete error for {key}: {e}")
            
    def sync_invalidate_prefix(self, prefix: str):
        try:
            import redis
            r = redis.from_url(self.settings.redis_url)
            cursor = '0'
            while cursor != 0:
                cursor, keys = r.scan(cursor=cursor, match=f"{prefix}*", count=100)
                if keys:
                    r.delete(*keys)
        except Exception as e:
            logger.error(f"Redis sync invalidate_prefix error for {prefix}: {e}")

cache_service = CacheService()
