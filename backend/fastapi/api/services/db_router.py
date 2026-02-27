# --------------------------------------------------------------
# File: c:\Users\ayaan shaikh\Documents\EWOC\SOULSENSE2\backend\fastapi\api\services\db_router.py
# --------------------------------------------------------------
"""
Read/Write‑splitting with primary‑replica support.

- POST / PUT / PATCH / DELETE → primary engine
- GET / HEAD / OPTIONS      → replica engine (if configured)

To avoid “read‑your‑own‑writes” we store a short‑lived Redis key
(`recent_write:{user_id}`) whenever a write succeeds.  Subsequent
GET requests that see this key will be forced onto the primary DB for a
configurable lag window (default 5 seconds).

All routers should depend on `get_db(request: Request)` instead of the
old `api.services.db_service.get_db`.
"""

import logging
from datetime import timedelta
from typing import AsyncGenerator, Optional

import redis.asyncio as redis
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..config import get_settings_instance

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# 1️⃣ Engine / Session creation
# ------------------------------------------------------------------
settings = get_settings_instance()

# Primary (write) engine – always present
_primary_engine = create_async_engine(
    settings.async_database_url,
    echo=settings.debug,
    future=True,
    connect_args={"check_same_thread": False} if settings.database_type == "sqlite" else {},
)
PrimarySessionLocal = async_sessionmaker(
    _primary_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

# Replica (read) engine – optional
_ReplicaSessionLocal: Optional[async_sessionmaker] = None
if getattr(settings, "replica_database_url", None):
    _replica_engine = create_async_engine(
        settings.replica_database_url,
        echo=settings.debug,
        future=True,
        connect_args={"check_same_thread": False} if settings.database_type == "sqlite" else {},
    )
    _ReplicaSessionLocal = async_sessionmaker(
        _replica_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    log.info("Read‑replica engine initialised.")
else:
    log.warning("No replica_database_url configured – all reads will hit primary.")

# ------------------------------------------------------------------
# 2️⃣ Redis helper – recent‑write guard
# ------------------------------------------------------------------
_REDIS_TTL_SECONDS = 5  # how long we consider a write “fresh”

async def _redis_client() -> redis.Redis:
    """Lazy‑init a Redis connection (same URL used by CacheService)."""
    return redis.from_url(settings.redis_url, decode_responses=True)

async def mark_write(user_id: int) -> None:
    """Called after a successful write (POST/PUT/PATCH/DELETE).
    Stores a short‑lived key so subsequent reads for the same user
    are forced onto the primary DB.
    """
    r = await _redis_client()
    key = f"recent_write:{user_id}"
    await r.set(key, "1", ex=_REDIS_TTL_SECONDS)

async def _has_recent_write(user_id: int) -> bool:
    """Check if the user performed a write within the lag window."""
    r = await _redis_client()
    return bool(await r.get(f"recent_write:{user_id}"))

# ------------------------------------------------------------------
# 3️⃣ Dependency – get_db
# ------------------------------------------------------------------
async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an AsyncSession bound to the correct
    engine based on the HTTP method and recent‑write guard.

    Usage in routers:
        async def my_endpoint(..., db: AsyncSession = Depends(get_db)):
            ...
    """
    method = request.method.upper()
    use_primary = method in {"POST", "PUT", "PATCH", "DELETE"}

    # If a recent write exists for this authenticated user, force primary.
    if not use_primary and hasattr(request.state, "user_id"):
        if await _has_recent_write(request.state.user_id):
            use_primary = True
            log.debug(
                f"Read‑your‑own‑writes guard: routing GET for user {request.state.user_id} to primary."
            )

    SessionMaker = (
        PrimarySessionLocal
        if use_primary
        else _ReplicaSessionLocal or PrimarySessionLocal
    )

    async with SessionMaker() as db:
        try:
            yield db
        finally:
            await db.close()

# ------------------------------------------------------------------
# 4️⃣ Helper – write_guard decorator (optional convenience)
# ------------------------------------------------------------------
def write_guard(func):
    """Decorator for service methods that perform writes.
    It automatically calls `mark_write(user_id)` after a successful commit.
    The wrapped function must accept `request: Request` (or have it in scope)
    and must expose the affected `user_id` as the second positional argument
    after `db`.
    """
    async def wrapper(*args, **kwargs):
        if len(args) < 2:
            raise ValueError("write_guard expects at least (db, user_id, ...) args")
        db = args[0]
        user_id = args[1]
        result = await func(*args, **kwargs)
        await mark_write(user_id)
        return result
    return wrapper

# End of db_router.py
