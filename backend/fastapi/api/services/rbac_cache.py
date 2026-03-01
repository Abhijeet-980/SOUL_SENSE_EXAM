"""
RBAC Permission Cache (Sidecar Cache Pattern) — #1145

Stores verified DB-sourced permission flags in Redis with a short TTL.
The RBAC middleware reads from this cache first, falling back to a DB query
only on a cache miss. This decouples the RBAC check from the primary
database session, eliminating deadlock risk under high concurrency.
"""

import logging
from typing import Optional
from datetime import timedelta

logger = logging.getLogger(__name__)

RBAC_CACHE_TTL_SECONDS = 60  # 1 minute; short enough to catch revocations


class RBACPermissionCache:
    """
    Redis-backed permission sidecar cache.
    Key format: `rbac:user:{username}`
    Value: "1" for admin, "0" for non-admin.
    """

    def __init__(self):
        self._redis = None

    def _get_key(self, username: str) -> str:
        return f"rbac:user:{username}"

    async def _get_redis(self):
        """Lazily get Redis client from app state or fallback."""
        if self._redis is not None:
            return self._redis
        try:
            import redis.asyncio as aioredis
            from ..config import get_settings_instance
            settings = get_settings_instance()
            self._redis = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            return self._redis
        except Exception as e:
            logger.warning(f"[RBAC Cache] Redis unavailable: {e}")
            return None

    async def get(self, username: str) -> Optional[bool]:
        """Return cached is_admin value or None on miss/error."""
        try:
            redis = await self._get_redis()
            if redis is None:
                return None
            val = await redis.get(self._get_key(username))
            if val is None:
                return None
            return val == "1"
        except Exception as e:
            logger.debug(f"[RBAC Cache] get miss for {username}: {e}")
            return None

    async def set(self, username: str, is_admin: bool) -> None:
        """Store the permission flag in Redis with a TTL."""
        try:
            redis = await self._get_redis()
            if redis is None:
                return
            await redis.setex(self._get_key(username), RBAC_CACHE_TTL_SECONDS, "1" if is_admin else "0")
        except Exception as e:
            logger.debug(f"[RBAC Cache] set failed for {username}: {e}")

    async def invalidate(self, username: str) -> None:
        """Force invalidate a user's cached permissions (e.g. after role change)."""
        try:
            redis = await self._get_redis()
            if redis is None:
                return
            await redis.delete(self._get_key(username))
            logger.info(f"[RBAC Cache] Invalidated cache for {username}")
        except Exception as e:
            logger.debug(f"[RBAC Cache] invalidate failed for {username}: {e}")


# Module-level singleton
rbac_permission_cache = RBACPermissionCache()
