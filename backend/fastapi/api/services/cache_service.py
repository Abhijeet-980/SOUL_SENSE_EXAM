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

    # ==========================================
    # Distributed Cache Invalidation (ISSUE-1123)
    # ==========================================
    
    async def broadcast_invalidation(self, key_or_prefix: str, is_prefix: bool = False):
        """
        Broadcasts an invalidation message across the Redis Pub/Sub channel.
        Use this when modifying entities that might be cached in local memory
        across multiple uncoordinated uvicorn workers.
        """
        await self.connect()
        try:
            message = json.dumps({
                "type": "invalidate_prefix" if is_prefix else "invalidate_key",
                "target": key_or_prefix
            })
            await self.redis.publish("soulsense_cache_invalidation", message)
            logger.info(f"Broadcasted cache invalidation -> {message}")
        except Exception as e:
            logger.error(f"Failed to broadcast cache invalidation: {e}")

    async def start_invalidation_listener(self):
        """
        Background task that subscribes to the Redis Pub/Sub channel.
        When an invalidation message is received, it purges the local FastAPICache
        and local CacheService storage to ensure distributed consistency.
        """
        await self.connect()
        pubsub = self.redis.pubsub()
        try:
            await pubsub.subscribe("soulsense_cache_invalidation")
            logger.info("Subscribed to distributed cache invalidation channel")
            
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    data = json.loads(message['data'])
                    action = data.get('type')
                    target = data.get('target')
                    
                    if not action or not target:
                        continue
                        
                    logger.info(f"Received cache invalidation event: {action} -> {target}")
                    
                    # 1. Clear from our custom CacheService Redis wrapper
                    if action == "invalidate_key":
                        await self.delete(target)
                    elif action == "invalidate_prefix":
                        await self.invalidate_prefix(target)
                        
                    # 2. Clear from FastAPICache (which might be using MemoryBackend locally in some setups)
                    from fastapi_cache import FastAPICache
                    backend = FastAPICache.get_backend()
                    if backend:
                        if action == "invalidate_key":
                            # FastAPICache doesn't have a direct delete API cleanly exposed in all versions,
                            # but cache.clear(namespace=...) or direct backend calls can be used.
                            try:
                                await backend.clear(namespace=target) # Approximate deletion if supported
                            except Exception:
                                pass
                        elif action == "invalidate_prefix":
                            try:
                                await backend.clear(namespace=target)
                            except Exception:
                                pass
                            
        except Exception as e:
            logger.error(f"Cache invalidation listener crashed: {e}")
        finally:
            await pubsub.unsubscribe("soulsense_cache_invalidation")
            await pubsub.close()

    # ==========================================
    # Generation-based Versioning (ISSUE-1143)
    # ==========================================

    async def update_version(self, entity_type: str, entity_id: Any, version: int):
        """Update the authoritative version for an entity in Redis."""
        await self.connect()
        try:
            key = f"version:{entity_type}:{entity_id}"
            await self.redis.set(key, version) # No TTL, this is the persistent truth
            logger.debug(f"[GenVersion] Updated {key} -> {version}")
        except Exception as e:
            logger.error(f"[GenVersion] Update failed for {entity_type}:{entity_id}: {e}")

    async def get_latest_version(self, entity_type: str, entity_id: Any) -> int:
        """Get the authoritative version for an entity from Redis."""
        await self.connect()
        try:
            key = f"version:{entity_type}:{entity_id}"
            val = await self.redis.get(key)
            return int(val) if val else 0
        except Exception as e:
            logger.error(f"[GenVersion] Get failed for {entity_type}:{entity_id}: {e}")
            return 0

    async def get_with_version_check(self, key: str, entity_type: str, entity_id: Any) -> Optional[Any]:
        """
        Retrieves data from cache and verifies it against the 
        global authoritative generation/version from Redis.
        Ensures nodes that missed invalidation eventually catch up.
        """
        cached_data = await self.get(key)
        if not cached_data:
            return None
        
        # Verify version
        cached_version = cached_data.get("version", 0)
        latest_version = await self.get_latest_version(entity_type, entity_id)

        if cached_version < latest_version:
            logger.info(f"[GenVersion] Cache stale for {key}: cached={cached_version} latest={latest_version}. Purging.")
            await self.delete(key)
            return None
        
        return cached_data

cache_service = CacheService()
