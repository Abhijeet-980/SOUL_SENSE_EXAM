"""
Background Scheduler Single-Leader Election Service (Issue #1366)

This module implements distributed leader election for background job schedulers
to prevent duplicate job execution in distributed deployments.

Features:
- Distributed locking using Redis with automatic failover
- Heartbeat monitoring with configurable intervals
- Clock skew detection and tolerance
- Network partition handling
- Graceful leader resignation and failover
"""

import asyncio
import logging
import uuid
import time
from datetime import datetime, timedelta, UTC
from enum import Enum
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import json

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, insert, delete, and_, or_, func

from ..config import get_settings_instance
from ..services.db_service import AsyncSessionLocal
from ..utils.timestamps import utc_now

logger = logging.getLogger(__name__)


class LeaderStatus(str, Enum):
    """Leader instance status."""
    ACTIVE = "active"           # Currently the leader
    STANDBY = "standby"         # Waiting to become leader
    FAILED = "failed"           # Failed heartbeat, needs recovery
    RESIGNED = "resigned"       # Gracefully resigned


class FailoverReason(str, Enum):
    """Reason for leader failover."""
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"
    LOCK_EXPIRED = "lock_expired"
    GRACEFUL_RESIGNATION = "graceful_resignation"
    NETWORK_PARTITION = "network_partition"
    CLOCK_SKEW_DETECTED = "clock_skew_detected"


@dataclass
class LeaderInfo:
    """Information about the current leader."""
    instance_id: str
    node_id: str
    acquired_at: datetime
    last_heartbeat: datetime
    heartbeat_interval_sec: int
    lock_ttl_sec: int
    status: LeaderStatus
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ElectionConfig:
    """Configuration for leader election."""
    lock_key: str = "scheduler:leader:lock"
    leader_info_key: str = "scheduler:leader:info"
    heartbeat_interval_sec: int = 10
    lock_ttl_sec: int = 30
    clock_skew_threshold_sec: float = 5.0
    max_missed_heartbeats: int = 3
    failover_delay_sec: float = 2.0
    enable_clock_skew_detection: bool = True


# Lua scripts for atomic operations
ACQUIRE_LEADER_LOCK_SCRIPT = """
-- Try to acquire leader lock
-- Returns: 1 if acquired, 0 if already held by someone else

local lock_key = KEYS[1]
local leader_info_key = KEYS[2]
local instance_id = ARGV[1]
local node_id = ARGV[2]
local lock_ttl = tonumber(ARGV[3])
local current_time = ARGV[4]
local metadata = ARGV[5]

-- Check if lock is already held
local current = redis.call("get", lock_key)
if current and current ~= instance_id then
    return 0
end

-- Set the lock
redis.call("set", lock_key, instance_id, "PX", lock_ttl)

-- Store leader info
local leader_info = {
    instance_id = instance_id,
    node_id = node_id,
    acquired_at = current_time,
    last_heartbeat = current_time,
    lock_ttl_sec = lock_ttl,
    status = "active"
}
if metadata then
    leader_info.metadata = metadata
end

redis.call("set", leader_info_key, cjson.encode(leader_info), "PX", lock_ttl * 2)
return 1
"""

RENEW_LEADER_LOCK_SCRIPT = """
-- Renew leader lock and heartbeat
-- Returns: 1 if renewed, 0 if not leader

local lock_key = KEYS[1]
local leader_info_key = KEYS[2]
local instance_id = ARGV[1]
local current_time = ARGV[2]
local lock_ttl = tonumber(ARGV[3])

-- Verify we're still the leader
local current = redis.call("get", lock_key)
if current ~= instance_id then
    return 0
end

-- Renew the lock
redis.call("pexpire", lock_key, lock_ttl)

-- Update leader info
local info_json = redis.call("get", leader_info_key)
if info_json then
    local info = cjson.decode(info_json)
    info.last_heartbeat = current_time
    redis.call("set", leader_info_key, cjson.encode(info), "PX", lock_ttl * 2)
end

return 1
"""

RELEASE_LEADER_LOCK_SCRIPT = """
-- Release leader lock gracefully
-- Returns: 1 if released, 0 if not leader

local lock_key = KEYS[1]
local leader_info_key = KEYS[2]
local instance_id = ARGV[1]

-- Verify we're the leader before releasing
local current = redis.call("get", lock_key)
if current ~= instance_id then
    return 0
end

-- Delete both lock and info
redis.call("del", lock_key)
redis.call("del", leader_info_key)
return 1
"""


class SchedulerLeaderElection:
    """
    Distributed leader election service for background schedulers.
    
    Ensures only one scheduler instance acts as the leader at any time,
    with automatic failover and heartbeat monitoring.
    """
    
    def __init__(self, config: Optional[ElectionConfig] = None):
        self.config = config or ElectionConfig()
        self.instance_id: str = str(uuid.uuid4())
        self.node_id: str = self._generate_node_id()
        self._redis: Optional[redis.Redis] = None
        self._is_leader: bool = False
        self._leader_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._clock_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._lock_renewal_lock: asyncio.Lock = asyncio.Lock()
        self._last_clock_check: Optional[datetime] = None
        self._clock_drift_ms: float = 0.0
        self._failover_callbacks: List[Callable[[FailoverReason, Optional[str]], None]] = []
        self._leadership_callbacks: List[Callable[[], None]] = []
        
    def _generate_node_id(self) -> str:
        """Generate a unique node identifier."""
        import socket
        import os
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"{hostname}:{pid}"
    
    async def _get_redis(self) -> redis.Redis:
        """Get or create Redis connection."""
        if self._redis is None:
            settings = get_settings_instance()
            self._redis = redis.from_url(
                settings.redis_url, 
                decode_responses=True
            )
        return self._redis
    
    async def _register_lua_scripts(self) -> None:
        """Register Lua scripts with Redis."""
        redis_conn = await self._get_redis()
        
        # Register scripts
        self._acquire_script = await redis_conn.script_load(ACQUIRE_LEADER_LOCK_SCRIPT)
        self._renew_script = await redis_conn.script_load(RENEW_LEADER_LOCK_SCRIPT)
        self._release_script = await redis_conn.script_load(RELEASE_LEADER_LOCK_SCRIPT)
    
    async def try_acquire_leadership(self) -> bool:
        """
        Attempt to acquire leadership.
        
        Returns:
            True if leadership acquired, False otherwise.
        """
        try:
            redis_conn = await self._get_redis()
            
            current_time = datetime.now(UTC).isoformat()
            metadata = json.dumps({
                "version": "1.0",
                "python_version": self._get_python_version(),
            })
            
            # Try to acquire using Lua script for atomicity
            result = await redis_conn.eval(
                ACQUIRE_LEADER_LOCK_SCRIPT,
                2,  # Number of keys
                self.config.lock_key,
                self.config.leader_info_key,
                self.instance_id,
                self.node_id,
                self.config.lock_ttl_sec * 1000,  # Convert to milliseconds
                current_time,
                metadata
            )
            
            if result == 1:
                self._is_leader = True
                logger.info(
                    f"[LeaderElection] Leadership acquired: instance={self.instance_id}, "
                    f"node={self.node_id}"
                )
                # Trigger leadership callbacks
                for callback in self._leadership_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback())
                        else:
                            callback()
                    except Exception as e:
                        logger.error(f"[LeaderElection] Leadership callback error: {e}")
                return True
            else:
                logger.debug(f"[LeaderElection] Leadership not acquired, already held")
                return False
                
        except Exception as e:
            logger.error(f"[LeaderElection] Error acquiring leadership: {e}")
            return False
    
    async def renew_leadership(self) -> bool:
        """
        Renew leadership lock (heartbeat).
        
        Returns:
            True if renewal successful, False if leadership lost.
        """
        if not self._is_leader:
            return False
            
        try:
            redis_conn = await self._get_redis()
            
            current_time = datetime.now(UTC).isoformat()
            
            async with self._lock_renewal_lock:
                result = await redis_conn.eval(
                    RENEW_LEADER_LOCK_SCRIPT,
                    2,
                    self.config.lock_key,
                    self.config.leader_info_key,
                    self.instance_id,
                    current_time,
                    self.config.lock_ttl_sec * 1000
                )
                
                if result == 0:
                    # Lost leadership
                    logger.warning(
                        f"[LeaderElection] Leadership lost during renewal: "
                        f"instance={self.instance_id}"
                    )
                    self._is_leader = False
                    await self._trigger_failover(FailoverReason.LOCK_EXPIRED)
                    return False
                
                logger.debug(f"[LeaderElection] Heartbeat renewed successfully")
                return True
                
        except Exception as e:
            logger.error(f"[LeaderElection] Error renewing leadership: {e}")
            return False
    
    async def release_leadership(self) -> bool:
        """
        Gracefully release leadership.
        
        Returns:
            True if released successfully, False otherwise.
        """
        try:
            redis_conn = await self._get_redis()
            
            async with self._lock_renewal_lock:
                result = await redis_conn.eval(
                    RELEASE_LEADER_LOCK_SCRIPT,
                    2,
                    self.config.lock_key,
                    self.config.leader_info_key,
                    self.instance_id
                )
                
                was_leader = self._is_leader
                self._is_leader = False
                
                if result == 1:
                    logger.info(
                        f"[LeaderElection] Leadership released gracefully: "
                        f"instance={self.instance_id}"
                    )
                    if was_leader:
                        await self._trigger_failover(FailoverReason.GRACEFUL_RESIGNATION)
                    return True
                else:
                    logger.warning(
                        f"[LeaderElection] Leadership release failed (not leader): "
                        f"instance={self.instance_id}"
                    )
                    return False
                    
        except Exception as e:
            logger.error(f"[LeaderElection] Error releasing leadership: {e}")
            self._is_leader = False
            return False
    
    async def get_current_leader(self) -> Optional[LeaderInfo]:
        """
        Get information about the current leader.
        
        Returns:
            LeaderInfo if leader exists, None otherwise.
        """
        try:
            redis_conn = await self._get_redis()
            
            info_json = await redis_conn.get(self.config.leader_info_key)
            if not info_json:
                return None
            
            data = json.loads(info_json)
            return LeaderInfo(
                instance_id=data.get("instance_id", ""),
                node_id=data.get("node_id", ""),
                acquired_at=datetime.fromisoformat(data.get("acquired_at", "")),
                last_heartbeat=datetime.fromisoformat(data.get("last_heartbeat", "")),
                heartbeat_interval_sec=data.get("heartbeat_interval_sec", self.config.heartbeat_interval_sec),
                lock_ttl_sec=data.get("lock_ttl_sec", self.config.lock_ttl_sec),
                status=LeaderStatus(data.get("status", "active")),
                metadata=data.get("metadata", {})
            )
            
        except Exception as e:
            logger.error(f"[LeaderElection] Error getting current leader: {e}")
            return None
    
    async def is_leader(self) -> bool:
        """Check if this instance is currently the leader."""
        if not self._is_leader:
            return False
        
        # Double-check with Redis to handle network partitions
        try:
            redis_conn = await self._get_redis()
            current = await redis_conn.get(self.config.lock_key)
            if current != self.instance_id:
                logger.warning(
                    f"[LeaderElection] Leadership inconsistency detected, "
                    f"correcting: instance={self.instance_id}"
                )
                self._is_leader = False
                await self._trigger_failover(FailoverReason.NETWORK_PARTITION)
                return False
            return True
        except Exception as e:
            logger.error(f"[LeaderElection] Error checking leadership: {e}")
            # Conservative approach: assume not leader on error
            return False
    
    async def _heartbeat_loop(self) -> None:
        """Background task for heartbeat renewal."""
        logger.info("[LeaderElection] Starting heartbeat loop")
        
        while not self._shutdown_event.is_set():
            try:
                if self._is_leader:
                    success = await self.renew_leadership()
                    if not success:
                        logger.error("[LeaderElection] Failed to renew leadership")
                        break
                
                # Wait for next heartbeat interval
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.heartbeat_interval_sec
                )
            except asyncio.TimeoutError:
                # Normal heartbeat interval
                continue
            except Exception as e:
                logger.error(f"[LeaderElection] Heartbeat loop error: {e}")
                await asyncio.sleep(1)
        
        logger.info("[LeaderElection] Heartbeat loop stopped")
    
    async def _clock_skew_monitor(self) -> None:
        """Monitor for clock skew issues."""
        if not self.config.enable_clock_skew_detection:
            return
            
        logger.info("[LeaderElection] Starting clock skew monitor")
        
        check_interval = min(self.config.heartbeat_interval_sec * 2, 30)
        
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=check_interval
                )
            except asyncio.TimeoutError:
                await self._check_clock_skew()
                continue
        
        logger.info("[LeaderElection] Clock skew monitor stopped")
    
    async def _check_clock_skew(self) -> None:
        """Check for clock skew between local time and Redis time."""
        try:
            redis_conn = await self._get_redis()
            
            # Get Redis time
            redis_time = await redis_conn.time()
            redis_timestamp = redis_time[0] + redis_time[1] / 1000000
            
            # Get local time
            local_timestamp = time.time()
            
            # Calculate drift
            drift = abs(local_timestamp - redis_timestamp)
            self._clock_drift_ms = drift * 1000
            
            if drift > self.config.clock_skew_threshold_sec:
                logger.warning(
                    f"[LeaderElection] Clock skew detected: {drift:.3f}s, "
                    f"instance={self.instance_id}"
                )
                
                if self._is_leader:
                    logger.error(
                        f"[LeaderElection] Stepping down due to clock skew: "
                        f"instance={self.instance_id}"
                    )
                    await self.release_leadership()
                    await self._trigger_failover(FailoverReason.CLOCK_SKEW_DETECTED)
            
            self._last_clock_check = datetime.now(UTC)
            
        except Exception as e:
            logger.error(f"[LeaderElection] Clock skew check error: {e}")
    
    async def _trigger_failover(self, reason: FailoverReason) -> None:
        """Trigger failover callbacks."""
        for callback in self._failover_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(reason, self.instance_id)
                else:
                    callback(reason, self.instance_id)
            except Exception as e:
                logger.error(f"[LeaderElection] Failover callback error: {e}")
    
    def on_leadership_change(self, callback: Callable[[], None]) -> None:
        """Register a callback for when leadership is acquired."""
        self._leadership_callbacks.append(callback)
    
    def on_failover(self, callback: Callable[[FailoverReason, Optional[str]], None]) -> None:
        """Register a callback for failover events."""
        self._failover_callbacks.append(callback)
    
    async def start(self) -> bool:
        """
        Start the leader election service.
        
        Returns:
            True if this instance became leader, False otherwise.
        """
        logger.info(
            f"[LeaderElection] Starting election service: "
            f"instance={self.instance_id}, node={self.node_id}"
        )
        
        # Try to acquire leadership
        is_leader = await self.try_acquire_leadership()
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"leader-heartbeat-{self.instance_id[:8]}"
        )
        
        self._clock_monitor_task = asyncio.create_task(
            self._clock_skew_monitor(),
            name=f"leader-clock-monitor-{self.instance_id[:8]}"
        )
        
        return is_leader
    
    async def stop(self) -> None:
        """Stop the leader election service gracefully."""
        logger.info(
            f"[LeaderElection] Stopping election service: "
            f"instance={self.instance_id}"
        )
        
        self._shutdown_event.set()
        
        # Release leadership if we have it
        if self._is_leader:
            await self.release_leadership()
        
        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._clock_monitor_task:
            self._clock_monitor_task.cancel()
            try:
                await self._clock_monitor_task
            except asyncio.CancelledError:
                pass
        
        # Close Redis connection
        if self._redis:
            await self._redis.close()
            self._redis = None
        
        logger.info(f"[LeaderElection] Election service stopped")
    
    def _get_python_version(self) -> str:
        """Get Python version string."""
        import sys
        return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    
    @asynccontextmanager
    async def leadership_context(self):
        """
        Context manager for leadership lifecycle.
        
        Example:
            async with election.leadership_context():
                # Run as leader
                await run_scheduler()
        """
        try:
            await self.start()
            yield self
        finally:
            await self.stop()


class SchedulerLeaderElectionManager:
    """
    Manager for coordinating multiple scheduler leader election instances.
    Handles automatic failover and leader monitoring.
    """
    
    def __init__(self, config: Optional[ElectionConfig] = None):
        self.config = config or ElectionConfig()
        self._election: Optional[SchedulerLeaderElection] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._leader_change_callbacks: List[Callable[[bool, Optional[str]], None]] = []
        
    async def start(self) -> None:
        """Start the leader election manager."""
        logger.info("[LeaderElectionManager] Starting manager")
        
        # Create and start election instance
        self._election = SchedulerLeaderElection(self.config)
        self._election.on_leadership_change(lambda: self._notify_leader_change(True))
        self._election.on_failover(lambda r, i: self._notify_leader_change(False, i))
        
        await self._election.start()
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(
            self._monitor_loop(),
            name="leader-election-manager"
        )
    
    async def stop(self) -> None:
        """Stop the manager."""
        logger.info("[LeaderElectionManager] Stopping manager")
        
        self._shutdown_event.set()
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        if self._election:
            await self._election.stop()
    
    async def _monitor_loop(self) -> None:
        """Monitor leader status and attempt failover if needed."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.heartbeat_interval_sec * 2
                )
            except asyncio.TimeoutError:
                # Check if we need to attempt leadership
                if self._election and not await self._election.is_leader():
                    # Check if there's a current leader
                    current_leader = await self._election.get_current_leader()
                    
                    if not current_leader:
                        # No leader, try to acquire
                        logger.info(
                            "[LeaderElectionManager] No leader detected, "
                            "attempting to acquire leadership"
                        )
                        await self._election.try_acquire_leadership()
                    else:
                        # Check if current leader is healthy
                        last_heartbeat = current_leader.last_heartbeat
                        timeout = timedelta(
                            seconds=self.config.heartbeat_interval_sec * 
                            self.config.max_missed_heartbeats
                        )
                        
                        if datetime.now(UTC) - last_heartbeat > timeout:
                            logger.warning(
                                f"[LeaderElectionManager] Leader heartbeat timeout, "
                                f"last heartbeat: {last_heartbeat}"
                            )
                            # Wait failover delay before attempting
                            await asyncio.sleep(self.config.failover_delay_sec)
                            await self._election.try_acquire_leadership()
    
    def _notify_leader_change(self, is_leader: bool, instance_id: Optional[str] = None) -> None:
        """Notify callbacks of leader change."""
        for callback in self._leader_change_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(is_leader, instance_id))
                else:
                    callback(is_leader, instance_id)
            except Exception as e:
                logger.error(f"[LeaderElectionManager] Callback error: {e}")
    
    def on_leader_change(self, callback: Callable[[bool, Optional[str]], None]) -> None:
        """Register callback for leader status changes."""
        self._leader_change_callbacks.append(callback)
    
    @property
    def is_leader(self) -> bool:
        """Check if current instance is leader."""
        return self._election.is_leader() if self._election else False


# Global instance for convenience
election_manager: Optional[SchedulerLeaderElectionManager] = None


async def get_election_manager(config: Optional[ElectionConfig] = None) -> SchedulerLeaderElectionManager:
    """Get or create the global election manager instance."""
    global election_manager
    if election_manager is None:
        election_manager = SchedulerLeaderElectionManager(config)
    return election_manager


async def start_leader_election(config: Optional[ElectionConfig] = None) -> SchedulerLeaderElectionManager:
    """
    Start the leader election service.
    
    Args:
        config: Optional custom configuration
        
    Returns:
        The election manager instance
    """
    manager = await get_election_manager(config)
    await manager.start()
    return manager


async def stop_leader_election() -> None:
    """Stop the leader election service."""
    global election_manager
    if election_manager:
        await election_manager.stop()
        election_manager = None
