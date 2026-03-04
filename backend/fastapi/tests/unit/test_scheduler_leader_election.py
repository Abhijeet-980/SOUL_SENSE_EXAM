"""
Unit tests for Scheduler Leader Election Service (Issue #1366)

Tests cover:
- Leader acquisition and release
- Heartbeat monitoring
- Automatic failover
- Clock skew detection
- Network partition handling
- Edge cases (lock expiration, duplicate prevention)
"""

import pytest
import asyncio
import uuid
import time
import json
import sys
import os
from datetime import datetime, timedelta, UTC
from unittest.mock import Mock, patch, AsyncMock, MagicMock, call
from typing import Optional

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


# Mock the db_service and other imports before importing the target module
mock_db_service = MagicMock()
mock_db_service.AsyncSessionLocal = AsyncMock()

sys.modules['api.services.db_service'] = mock_db_service

# Mock config
mock_config = MagicMock()
mock_config.get_settings_instance = MagicMock(return_value=MagicMock(redis_url="redis://localhost:6379"))
sys.modules['api.config'] = mock_config

# Mock timestamps
mock_timestamps = MagicMock()
mock_timestamps.utc_now = MagicMock(return_value=datetime.now(UTC))
sys.modules['api.utils.timestamps'] = mock_timestamps

# Now import the module under test
from api.services.scheduler_leader_election import (
    SchedulerLeaderElection,
    SchedulerLeaderElectionManager,
    ElectionConfig,
    LeaderStatus,
    LeaderInfo,
    FailoverReason,
    start_leader_election,
    stop_leader_election,
    get_election_manager,
    ACQUIRE_LEADER_LOCK_SCRIPT,
    RENEW_LEADER_LOCK_SCRIPT,
    RELEASE_LEADER_LOCK_SCRIPT,
)


class TestElectionConfig:
    """Tests for ElectionConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = ElectionConfig()
        assert config.lock_key == "scheduler:leader:lock"
        assert config.leader_info_key == "scheduler:leader:info"
        assert config.heartbeat_interval_sec == 10
        assert config.lock_ttl_sec == 30
        assert config.clock_skew_threshold_sec == 5.0
        assert config.max_missed_heartbeats == 3
        assert config.failover_delay_sec == 2.0
        assert config.enable_clock_skew_detection is True
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = ElectionConfig(
            lock_key="custom:lock",
            heartbeat_interval_sec=5,
            lock_ttl_sec=15,
            max_missed_heartbeats=2
        )
        assert config.lock_key == "custom:lock"
        assert config.heartbeat_interval_sec == 5
        assert config.lock_ttl_sec == 15
        assert config.max_missed_heartbeats == 2


class TestSchedulerLeaderElection:
    """Tests for SchedulerLeaderElection class."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration with short intervals."""
        return ElectionConfig(
            lock_key="test:scheduler:lock",
            leader_info_key="test:scheduler:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3,
            clock_skew_threshold_sec=1.0,
            max_missed_heartbeats=2,
            failover_delay_sec=0.5,
            enable_clock_skew_detection=True
        )
    
    @pytest.fixture
    def election(self, config):
        """Create an election instance."""
        return SchedulerLeaderElection(config)
    
    @pytest.mark.asyncio
    async def test_initialization(self, config):
        """Test election instance initialization."""
        election = SchedulerLeaderElection(config)
        
        assert election.config == config
        assert election.instance_id is not None
        assert len(election.instance_id) > 0
        assert election.node_id is not None
        assert ":" in election.node_id  # hostname:pid format
        assert election._is_leader is False
        assert election._shutdown_event.is_set() is False
    
    @pytest.mark.asyncio
    async def test_generate_node_id(self, config):
        """Test node ID generation."""
        election = SchedulerLeaderElection(config)
        node_id = election._generate_node_id()
        
        assert ":" in node_id
        parts = node_id.split(":")
        assert len(parts) == 2
        assert parts[1].isdigit()  # PID should be numeric
    
    @pytest.mark.asyncio
    async def test_try_acquire_leadership_success(self, config):
        """Test successful leadership acquisition."""
        # Setup mock Redis
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        result = await election.try_acquire_leadership()
        
        assert result is True
        assert election._is_leader is True
        mock_redis_conn.eval.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_try_acquire_leadership_already_held(self, config):
        """Test leadership acquisition when already held."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 0  # Already held
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        result = await election.try_acquire_leadership()
        
        assert result is False
        assert election._is_leader is False
    
    @pytest.mark.asyncio
    async def test_try_acquire_leadership_redis_error(self, config):
        """Test leadership acquisition with Redis error."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.side_effect = Exception("Redis connection failed")
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        result = await election.try_acquire_leadership()
        
        assert result is False
        assert election._is_leader is False
    
    @pytest.mark.asyncio
    async def test_renew_leadership_success(self, config):
        """Test successful leadership renewal."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        result = await election.renew_leadership()
        
        assert result is True
        assert election._is_leader is True
    
    @pytest.mark.asyncio
    async def test_renew_leadership_lost(self, config):
        """Test leadership renewal when lock lost."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 0  # Lock lost
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        # Track failover callback
        failover_called = []
        election.on_failover(lambda r, i: failover_called.append(r))
        
        result = await election.renew_leadership()
        
        assert result is False
        assert election._is_leader is False
        assert len(failover_called) == 1
        assert failover_called[0] == FailoverReason.LOCK_EXPIRED
    
    @pytest.mark.asyncio
    async def test_renew_not_leader(self, config):
        """Test renewal when not leader."""
        election = SchedulerLeaderElection(config)
        election._is_leader = False
        
        result = await election.renew_leadership()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_release_leadership_success(self, config):
        """Test successful leadership release."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        result = await election.release_leadership()
        
        assert result is True
        assert election._is_leader is False
    
    @pytest.mark.asyncio
    async def test_release_leadership_not_leader(self, config):
        """Test release when not leader."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 0  # Not leader
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True  # Believes is leader
        
        result = await election.release_leadership()
        
        assert result is False
        assert election._is_leader is False  # Corrected to False
    
    @pytest.mark.asyncio
    async def test_get_current_leader_exists(self, config):
        """Test getting current leader when one exists."""
        mock_redis_conn = AsyncMock()
        leader_data = {
            "instance_id": "test-instance-123",
            "node_id": "test-node:1234",
            "acquired_at": datetime.now(UTC).isoformat(),
            "last_heartbeat": datetime.now(UTC).isoformat(),
            "lock_ttl_sec": 30,
            "status": "active",
            "metadata": {}
        }
        mock_redis_conn.get.return_value = json.dumps(leader_data)
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        leader = await election.get_current_leader()
        
        assert leader is not None
        assert leader.instance_id == "test-instance-123"
        assert leader.node_id == "test-node:1234"
    
    @pytest.mark.asyncio
    async def test_get_current_leader_none(self, config):
        """Test getting current leader when none exists."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.get.return_value = None
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        leader = await election.get_current_leader()
        
        assert leader is None
    
    @pytest.mark.asyncio
    async def test_is_leader_true(self, config):
        """Test is_leader when actually leader."""
        mock_redis_conn = AsyncMock()
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        # Mock Redis to confirm leadership
        mock_redis_conn.get.return_value = election.instance_id
        
        result = await election.is_leader()
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_is_leader_false_different_holder(self, config):
        """Test is_leader when lock held by different instance."""
        mock_redis_conn = AsyncMock()
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True  # Believes is leader
        
        # Mock Redis to show different leader
        mock_redis_conn.get.return_value = "different-instance-id"
        
        # Track failover callback
        failover_called = []
        election.on_failover(lambda r, i: failover_called.append(r))
        
        result = await election.is_leader()
        
        assert result is False
        assert election._is_leader is False
        assert len(failover_called) == 1
        assert failover_called[0] == FailoverReason.NETWORK_PARTITION
    
    @pytest.mark.asyncio
    async def test_is_leader_not_leader(self, config):
        """Test is_leader when not leader."""
        election = SchedulerLeaderElection(config)
        election._is_leader = False
        
        result = await election.is_leader()
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_clock_skew_detection(self, config):
        """Test clock skew detection."""
        mock_redis_conn = AsyncMock()
        # Redis time is 10 seconds ahead (large skew)
        mock_redis_conn.time.return_value = (int(time.time()) + 10, 0)
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        # Track failover callback
        failover_called = []
        election.on_failover(lambda r, i: failover_called.append(r))
        
        # Mock release_leadership to succeed
        with patch.object(election, 'release_leadership', return_value=True):
            await election._check_clock_skew()
        
        # Should detect skew and trigger failover
        assert election._clock_drift_ms > 5000  # > 5 seconds
        assert len(failover_called) == 1
        assert failover_called[0] == FailoverReason.CLOCK_SKEW_DETECTED
    
    @pytest.mark.asyncio
    async def test_clock_skew_within_threshold(self, config):
        """Test clock skew detection within threshold."""
        mock_redis_conn = AsyncMock()
        # Redis time is close to local time
        mock_redis_conn.time.return_value = (int(time.time()), 0)
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        await election._check_clock_skew()
        
        # Should not trigger failover
        assert election._clock_drift_ms < 1000  # < 1 second
    
    @pytest.mark.asyncio
    async def test_callbacks_registration(self, config):
        """Test callback registration."""
        election = SchedulerLeaderElection(config)
        
        leadership_called = []
        failover_called = []
        
        election.on_leadership_change(lambda: leadership_called.append(True))
        election.on_failover(lambda r, i: failover_called.append((r, i)))
        
        # Verify callbacks are registered
        assert len(election._leadership_callbacks) == 1
        assert len(election._failover_callbacks) == 1
    
    @pytest.mark.asyncio
    async def test_start_becomes_leader(self, config):
        """Test start when becoming leader."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        result = await election.start()
        
        assert result is True
        assert election._is_leader is True
        assert election._heartbeat_task is not None
        assert election._clock_monitor_task is not None
        
        # Cleanup
        await election.stop()
    
    @pytest.mark.asyncio
    async def test_start_standby(self, config):
        """Test start when not becoming leader."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 0  # Already held
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        result = await election.start()
        
        assert result is False
        assert election._is_leader is False
        assert election._heartbeat_task is not None  # Still starts monitoring
        
        # Cleanup
        await election.stop()
    
    @pytest.mark.asyncio
    async def test_stop_releases_leadership(self, config):
        """Test stop releases leadership."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        await election.stop()
        
        assert election._is_leader is False
        assert election._shutdown_event.is_set() is True
        # Verify release was called
        mock_redis_conn.eval.assert_called()
    
    @pytest.mark.asyncio
    async def test_leadership_context_manager(self, config):
        """Test leadership context manager."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        async with election.leadership_context() as e:
            assert e._is_leader is True
        
        # After exit, should release leadership
        assert election._is_leader is False


class TestSchedulerLeaderElectionManager:
    """Tests for SchedulerLeaderElectionManager class."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ElectionConfig(
            lock_key="test:manager:lock",
            leader_info_key="test:manager:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3,
            max_missed_heartbeats=2,
            failover_delay_sec=0.5
        )
    
    @pytest.mark.asyncio
    async def test_manager_start(self, config):
        """Test manager start."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1  # Success
        
        manager = SchedulerLeaderElectionManager(config)
        
        # Mock the election's Redis
        with patch.object(manager, '_election') as mock_election:
            mock_election._redis = mock_redis_conn
            mock_election._is_leader = True
            mock_election.instance_id = "test-instance"
            mock_election.try_acquire_leadership = AsyncMock(return_value=True)
            mock_election.is_leader = AsyncMock(return_value=True)
            
            # Track leader change callback
            leader_changes = []
            manager.on_leader_change(lambda is_leader, _: leader_changes.append(is_leader))
            
            await manager.start()
            
            assert manager._monitor_task is not None
            
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_manager_is_leader_property(self, config):
        """Test manager is_leader property."""
        manager = SchedulerLeaderElectionManager(config)
        
        # Before start
        assert manager.is_leader is False
        
        # Mock election
        with patch.object(manager, '_election') as mock_election:
            mock_election.is_leader = AsyncMock(return_value=True)
            
            # After start
            is_leader = await manager.is_leader
            assert is_leader is True


class TestLeaderElectionIntegration:
    """Integration tests simulating real scenarios."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration with fast intervals."""
        return ElectionConfig(
            lock_key="test:integration:lock",
            leader_info_key="test:integration:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3,
            clock_skew_threshold_sec=2.0,
            max_missed_heartbeats=2,
            failover_delay_sec=0.5
        )
    
    @pytest.mark.asyncio
    async def test_leader_election_competition(self, config):
        """Test two instances competing for leadership."""
        # Track which instance holds the lock
        lock_holder = None
        
        def create_mock_redis(instance_id):
            mock = AsyncMock()
            
            async def eval_side_effect(*args, **kwargs):
                nonlocal lock_holder
                script = args[0] if args else ""
                
                # ACQUIRE
                if "ACQUIRED" in script or "cjson" in script:
                    if lock_holder is None:
                        lock_holder = instance_id
                        return 1
                    return 0
                
                # RELEASE
                if "del" in script:
                    if lock_holder == instance_id:
                        lock_holder = None
                        return 1
                    return 0
                
                # RENEW
                if "pexpire" in script:
                    if lock_holder == instance_id:
                        return 1
                    return 0
                
                return 0
            
            mock.eval.side_effect = eval_side_effect
            mock.get.return_value = None
            return mock
        
        # Create two election instances
        election1 = SchedulerLeaderElection(config)
        election1._redis = create_mock_redis(election1.instance_id)
        
        election2 = SchedulerLeaderElection(config)
        election2._redis = create_mock_redis(election2.instance_id)
        
        # Both try to acquire
        result1 = await election1.try_acquire_leadership()
        result2 = await election2.try_acquire_leadership()
        
        # Only one should succeed
        assert result1 is True
        assert result2 is False
        assert election1._is_leader is True
        assert election2._is_leader is False
        
        # Cleanup
        await election1.release_leadership()
    
    @pytest.mark.asyncio
    async def test_no_duplicate_leadership(self, config):
        """Test that duplicate leadership cannot occur."""
        acquired = False
        
        def create_mock_redis():
            mock = AsyncMock()
            
            async def eval_side_effect(*args, **kwargs):
                nonlocal acquired
                script = args[0] if args else ""
                
                if "ACQUIRED" in script or "cjson" in script:
                    if not acquired:
                        acquired = True
                        return 1
                    return 0
                return 0
            
            mock.eval.side_effect = eval_side_effect
            return mock
        
        # Try multiple simultaneous acquisitions
        elections = [SchedulerLeaderElection(config) for _ in range(5)]
        for e in elections:
            e._redis = create_mock_redis()
        
        results = await asyncio.gather(*[
            e.try_acquire_leadership() for e in elections
        ])
        
        # Only one should succeed
        assert sum(results) == 1
        assert acquired is True
        
        # Cleanup
        for e in elections:
            if e._is_leader:
                await e.release_leadership()


class TestGlobalFunctions:
    """Tests for global helper functions."""
    
    @pytest.mark.asyncio
    async def test_get_election_manager(self):
        """Test get_election_manager creates singleton."""
        # Reset global state
        import api.services.scheduler_leader_election as le_module
        le_module.election_manager = None
        
        with patch('redis.asyncio.from_url', AsyncMock()):
            manager1 = await get_election_manager()
            manager2 = await get_election_manager()
            
            assert manager1 is manager2  # Same instance
    
    @pytest.mark.asyncio
    async def test_start_and_stop_leader_election(self):
        """Test start_leader_election and stop_leader_election."""
        # Reset global state
        import api.services.scheduler_leader_election as le_module
        le_module.election_manager = None
        
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1
        
        with patch('redis.asyncio.from_url', return_value=mock_redis_conn):
            # Start
            manager = await start_leader_election()
            assert manager is not None
            assert le_module.election_manager is manager
            
            # Stop
            await stop_leader_election()
            assert le_module.election_manager is None


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""
    
    @pytest.fixture
    def config(self):
        return ElectionConfig(
            lock_key="test:edge:lock",
            leader_info_key="test:edge:info",
            heartbeat_interval_sec=1,
            lock_ttl_sec=3
        )
    
    @pytest.mark.asyncio
    async def test_redis_connection_failure(self, config):
        """Test behavior when Redis connection fails."""
        election = SchedulerLeaderElection(config)
        election._redis = None
        
        # Mock get_redis to fail
        with patch.object(election, '_get_redis', side_effect=Exception("Connection refused")):
            result = await election.try_acquire_leadership()
        
        # Should gracefully handle error
        assert result is False
        assert election._is_leader is False
    
    @pytest.mark.asyncio
    async def test_corrupted_leader_info(self, config):
        """Test handling of corrupted leader info."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.get.return_value = "invalid json"  # Corrupted data
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        # Should handle gracefully
        leader = await election.get_current_leader()
        assert leader is None  # Returns None on error
    
    @pytest.mark.asyncio
    async def test_concurrent_heartbeat_renewal(self, config):
        """Test concurrent heartbeat renewal attempts."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        election._is_leader = True
        
        # Multiple concurrent renewals
        results = await asyncio.gather(*[
            election.renew_leadership() for _ in range(5)
        ])
        
        # All should succeed (lock protects against race conditions)
        assert all(results)
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown_during_heartbeat(self, config):
        """Test graceful shutdown during heartbeat cycle."""
        mock_redis_conn = AsyncMock()
        mock_redis_conn.eval.return_value = 1
        
        election = SchedulerLeaderElection(config)
        election._redis = mock_redis_conn
        
        await election.start()
        
        # Stop should interrupt heartbeat and cleanup
        await election.stop()
        
        assert election._is_leader is False
        assert election._shutdown_event.is_set() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
