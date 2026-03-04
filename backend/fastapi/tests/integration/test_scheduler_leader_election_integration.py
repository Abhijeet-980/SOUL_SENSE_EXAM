"""
Integration tests for Scheduler Leader Election (Issue #1366)

These tests simulate real-world distributed scenarios:
- Multiple scheduler instances competing for leadership
- Leader failure and automatic failover
- Network partition detection
- Clock skew scenarios
"""

import pytest
import asyncio
import time
import json
import sys
import os
from datetime import datetime, timedelta, UTC
from typing import List, Optional, Dict, Any
from unittest.mock import Mock, patch, AsyncMock, call
from dataclasses import dataclass

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Mock the dependencies before importing the target module
mock_db_service = Mock()
mock_db_service.AsyncSessionLocal = AsyncMock()
sys.modules['api.services.db_service'] = mock_db_service

mock_config = Mock()
mock_config.get_settings_instance = Mock(return_value=Mock(redis_url="redis://localhost:6379"))
sys.modules['api.config'] = mock_config

mock_timestamps = Mock()
mock_timestamps.utc_now = Mock(return_value=datetime.now(UTC))
sys.modules['api.utils.timestamps'] = mock_timestamps

# Now import the module under test
from api.services.scheduler_leader_election import (
    SchedulerLeaderElection,
    SchedulerLeaderElectionManager,
    ElectionConfig,
    LeaderStatus,
    FailoverReason,
)


@dataclass
class SimulatedRedisState:
    """In-memory Redis state for testing."""
    data: Dict[str, Any] = None
    ttls: Dict[str, float] = None
    
    def __post_init__(self):
        if self.data is None:
            self.data = {}
        if self.ttls is None:
            self.ttls = {}
    
    def is_expired(self, key: str) -> bool:
        if key in self.ttls:
            return time.time() > self.ttls[key]
        return False
    
    def cleanup_expired(self, key: str):
        if self.is_expired(key):
            if key in self.data:
                del self.data[key]
            if key in self.ttls:
                del self.ttls[key]


def create_mock_redis(state: SimulatedRedisState, instance_id: str):
    """Create a mock Redis connection with simulated state."""
    mock = AsyncMock()
    
    async def get(key: str) -> Optional[str]:
        state.cleanup_expired(key)
        return state.data.get(key)
    
    async def set_key(key: str, value: str, **kwargs) -> bool:
        state.data[key] = value
        if 'px' in kwargs:
            state.ttls[key] = time.time() + (kwargs['px'] / 1000)
        elif 'ex' in kwargs:
            state.ttls[key] = time.time() + kwargs['ex']
        return True
    
    async def pexpire(key: str, milliseconds: int) -> bool:
        if key in state.data:
            state.ttls[key] = time.time() + (milliseconds / 1000)
            return True
        return False
    
    async def delete(*keys) -> int:
        count = 0
        for key in keys:
            if key in state.data:
                del state.data[key]
                if key in state.ttls:
                    del self.ttls[key]
                count += 1
        return count
    
    async def eval_script(script: str, num_keys: int, *args) -> int:
        keys = args[:num_keys]
        argv = args[num_keys:]
        
        lock_key = keys[0]
        info_key = keys[1] if len(keys) > 1 else None
        
        # Determine script type by argument count and content patterns
        # ACQUIRE: 5 args (instance_id, node_id, ttl, current_time, metadata)
        # RENEW: 3 args (instance_id, current_time, ttl)
        # RELEASE: 1 arg (instance_id)
        
        is_acquire = len(argv) == 5
        is_renew = len(argv) == 3
        is_release = len(argv) == 1 and "del" in script and "KEYS" in script
        
        # ACQUIRE script
        if is_acquire:
            inst_id = argv[0]
            state.cleanup_expired(lock_key)
            
            if lock_key not in state.data or state.data.get(lock_key) == inst_id:
                state.data[lock_key] = inst_id
                ttl_ms = int(argv[2])
                state.ttls[lock_key] = time.time() + (ttl_ms / 1000)
                
                if info_key:
                    leader_info = {
                        "instance_id": argv[0],
                        "node_id": argv[1],
                        "acquired_at": argv[3],
                        "last_heartbeat": argv[3],
                        "lock_ttl_sec": ttl_ms // 1000,
                        "status": "active"
                    }
                    state.data[info_key] = json.dumps(leader_info)
                    state.ttls[info_key] = time.time() + (ttl_ms * 2 / 1000)
                return 1
            return 0
        
        # RENEW script
        if is_renew:
            inst_id = argv[0]
            if state.data.get(lock_key) == inst_id:
                ttl_ms = int(argv[2])
                state.ttls[lock_key] = time.time() + (ttl_ms / 1000)
                
                if info_key and info_key in state.data:
                    info = json.loads(state.data[info_key])
                    info["last_heartbeat"] = argv[1]
                    state.data[info_key] = json.dumps(info)
                    state.ttls[info_key] = time.time() + (ttl_ms * 2 / 1000)
                return 1
            return 0
        
        # RELEASE script
        if is_release:
            inst_id = argv[0]
            if state.data.get(lock_key) == inst_id:
                if lock_key in state.data:
                    del state.data[lock_key]
                if lock_key in state.ttls:
                    del state.ttls[lock_key]
                if info_key:
                    if info_key in state.data:
                        del state.data[info_key]
                    if info_key in state.ttls:
                        del state.ttls[info_key]
                return 1
            return 0
        
        return 0
    
    async def redis_time() -> tuple:
        now = time.time()
        return (int(now), int((now - int(now)) * 1000000))
    
    async def close():
        pass
    
    mock.get = get
    mock.set = set_key
    mock.pexpire = pexpire
    mock.delete = delete
    mock.eval = eval_script
    mock.time = redis_time
    mock.close = close
    
    return mock


class TestLeaderElectionRealScenarios:
    """Real-world scenario tests using simulated Redis."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ElectionConfig(
            lock_key="test:scheduler:lock",
            leader_info_key="test:scheduler:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3,
            clock_skew_threshold_sec=2.0,
            max_missed_heartbeats=2,
            failover_delay_sec=0.5
        )
    
    @pytest.mark.asyncio
    async def test_single_leader_scenario(self, config):
        """Test that only one instance can be leader at a time."""
        state = SimulatedRedisState()
        
        # Create multiple election instances
        elections = []
        for _ in range(3):
            election = SchedulerLeaderElection(config)
            election._redis = create_mock_redis(state, election.instance_id)
            elections.append(election)
        
        # All try to acquire leadership
        results = []
        for election in elections:
            result = await election.try_acquire_leadership()
            results.append(result)
        
        # Only one should succeed
        assert sum(results) == 1, f"Expected 1 leader, got {sum(results)}"
        
        # Verify the leader
        leaders = [e for e in elections if e._is_leader]
        assert len(leaders) == 1
        
        # Verify Redis state
        assert config.lock_key in state.data
        assert config.leader_info_key in state.data
        
        # Cleanup
        for e in elections:
            if e._is_leader:
                await e.release_leadership()
    
    @pytest.mark.asyncio
    async def test_leader_failover_scenario(self, config):
        """Test automatic failover when leader fails."""
        state = SimulatedRedisState()
        
        # Instance 1 becomes leader
        election1 = SchedulerLeaderElection(config)
        election1._redis = create_mock_redis(state, election1.instance_id)
        assert await election1.try_acquire_leadership() is True
        
        # Instance 2 is standby
        election2 = SchedulerLeaderElection(config)
        election2._redis = create_mock_redis(state, election2.instance_id)
        assert await election2.try_acquire_leadership() is False
        
        # Simulate leader failure (expire the lock)
        state.ttls[config.lock_key] = time.time() - 1
        state.ttls[config.leader_info_key] = time.time() - 1
        
        # Election1 should detect it's no longer leader
        assert await election1.is_leader() is False
        
        # Election2 can now acquire
        assert await election2.try_acquire_leadership() is True
        
        # Cleanup
        await election2.release_leadership()
    
    @pytest.mark.asyncio
    async def test_heartbeat_maintains_leadership(self, config):
        """Test that heartbeat renews keep leadership alive."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Become leader
        assert await election.try_acquire_leadership() is True
        
        # Simulate multiple heartbeats
        for i in range(5):
            success = await election.renew_leadership()
            assert success is True, f"Heartbeat {i+1} failed"
            assert election._is_leader is True
        
        # Cleanup
        await election.release_leadership()
    
    @pytest.mark.asyncio
    async def test_graceful_handover(self, config):
        """Test graceful leadership handover."""
        state = SimulatedRedisState()
        
        # Instance 1 becomes leader
        election1 = SchedulerLeaderElection(config)
        election1._redis = create_mock_redis(state, election1.instance_id)
        assert await election1.try_acquire_leadership() is True
        
        # Instance 2 is waiting
        election2 = SchedulerLeaderElection(config)
        election2._redis = create_mock_redis(state, election2.instance_id)
        assert await election2.try_acquire_leadership() is False
        
        # Instance 1 gracefully releases
        assert await election1.release_leadership() is True
        assert election1._is_leader is False
        
        # Verify lock is released
        assert config.lock_key not in state.data
        
        # Instance 2 can now acquire
        assert await election2.try_acquire_leadership() is True
        
        # Cleanup
        await election2.release_leadership()
    
    @pytest.mark.asyncio
    async def test_leader_info_persistence(self, config):
        """Test that leader info is properly stored and retrievable."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Before acquiring
        info = await election.get_current_leader()
        assert info is None
        
        # Acquire leadership
        assert await election.try_acquire_leadership() is True
        
        # Get leader info
        info = await election.get_current_leader()
        assert info is not None
        assert info.instance_id == election.instance_id
        assert info.node_id == election.node_id
        assert info.status == LeaderStatus.ACTIVE
        
        # Cleanup
        await election.release_leadership()
    
    @pytest.mark.asyncio
    async def test_network_partition_detection(self, config):
        """Test detection of network partition scenario."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Become leader
        assert await election.try_acquire_leadership() is True
        
        # Simulate network partition by changing the lock holder
        state.data[config.lock_key] = "impostor-instance"
        
        # Should detect partition and step down
        is_leader = await election.is_leader()
        assert is_leader is False
        assert election._is_leader is False


class TestLeaderElectionStress:
    """Stress tests for leader election."""
    
    @pytest.fixture
    def config(self):
        return ElectionConfig(
            lock_key="test:stress:lock",
            leader_info_key="test:stress:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3,
            max_missed_heartbeats=2
        )
    
    @pytest.mark.asyncio
    async def test_rapid_acquisition_attempts(self, config):
        """Test rapid leadership acquisition attempts."""
        state = SimulatedRedisState()
        
        # Create many instances
        num_instances = 10
        elections = []
        for _ in range(num_instances):
            election = SchedulerLeaderElection(config)
            election._redis = create_mock_redis(state, election.instance_id)
            elections.append(election)
        
        # All try to acquire simultaneously
        results = await asyncio.gather(*[
            e.try_acquire_leadership() for e in elections
        ])
        
        # Only one should succeed
        assert sum(results) == 1
        
        # Cleanup
        for e in elections:
            if e._is_leader:
                await e.release_leadership()
    
    @pytest.mark.asyncio
    async def test_rapid_failover_sequence(self, config):
        """Test rapid succession of failovers."""
        state = SimulatedRedisState()
        
        elections = [SchedulerLeaderElection(config) for _ in range(5)]
        
        for i, election in enumerate(elections):
            election._redis = create_mock_redis(state, election.instance_id)
            
            # Acquire
            assert await election.try_acquire_leadership() is True
            
            # Verify
            assert await election.is_leader() is True
            
            # Release
            assert await election.release_leadership() is True
            
            # Verify released
            assert election._is_leader is False
    
    @pytest.mark.asyncio
    async def test_heartbeat_under_load(self, config):
        """Test heartbeat behavior under load."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Become leader
        assert await election.try_acquire_leadership() is True
        
        # Simulate many rapid heartbeats
        success_count = 0
        for _ in range(50):
            if await election.renew_leadership():
                success_count += 1
        
        # All should succeed
        assert success_count == 50
        assert election._is_leader is True
        
        # Cleanup
        await election.release_leadership()


class TestLeaderElectionEdgeCases:
    """Edge case tests."""
    
    @pytest.fixture
    def config(self):
        return ElectionConfig(
            lock_key="test:edge:lock",
            leader_info_key="test:edge:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3
        )
    
    @pytest.mark.asyncio
    async def test_release_without_being_leader(self, config):
        """Test release when not leader."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Try to release without being leader
        result = await election.release_leadership()
        
        # Should return False but not crash
        assert result is False
    
    @pytest.mark.asyncio
    async def test_double_acquisition(self, config):
        """Test acquiring leadership twice."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # First acquisition
        assert await election.try_acquire_leadership() is True
        
        # Second acquisition should still succeed (idempotent)
        assert await election.try_acquire_leadership() is True
        
        # Cleanup
        await election.release_leadership()
    
    @pytest.mark.asyncio
    async def test_expired_lock_detection(self, config):
        """Test detection of expired lock."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Become leader
        assert await election.try_acquire_leadership() is True
        
        # Manually expire the lock
        state.ttls[config.lock_key] = time.time() - 1
        
        # get should return None for expired key
        result = await election._redis.get(config.lock_key)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_corrupted_lock_value(self, config):
        """Test handling of corrupted lock value."""
        state = SimulatedRedisState()
        
        election = SchedulerLeaderElection(config)
        election._redis = create_mock_redis(state, election.instance_id)
        
        # Become leader
        assert await election.try_acquire_leadership() is True
        
        # Corrupt the lock value
        state.data[config.lock_key] = "corrupted-value"
        
        # Should detect and step down
        is_leader = await election.is_leader()
        assert is_leader is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
