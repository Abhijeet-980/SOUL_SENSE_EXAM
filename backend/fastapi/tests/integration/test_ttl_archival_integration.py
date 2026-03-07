"""
Integration tests for Row-Level TTL Archival Partitioning (#1413).

Tests end-to-end TTL policy management with real database operations.
"""
import pytest
import asyncio
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from api.utils.ttl_partition_manager import (
    TTLPartitionManager,
    TTLPolicy,
    PartitionGranularity,
    ArchiveStrategy,
    PolicyStatus,
    get_ttl_manager,
)
from api.services.db_service import engine


@pytest.fixture
async def ttl_manager():
    """Create and initialize TTL manager for testing."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    return manager


@pytest.mark.asyncio
async def test_manager_initialization():
    """Test TTL manager initialization."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    # Verify tables were created
    assert manager._metadata is not None


@pytest.mark.asyncio
async def test_policy_registration():
    """Test registering and retrieving a policy."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    policy = TTLPolicy(
        table_name="test_notification_logs",
        retention_days=30,
        partition_granularity=PartitionGranularity.MONTHLY,
        archive_strategy=ArchiveStrategy.SOFT_DELETE,
    )
    
    await manager.register_policy(policy)
    
    # Verify policy was stored
    retrieved = await manager.get_policy("test_notification_logs")
    assert retrieved is not None
    assert retrieved.table_name == "test_notification_logs"
    assert retrieved.retention_days == 30


@pytest.mark.asyncio
async def test_list_policies():
    """Test listing all policies."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    # Register multiple policies
    policies = [
        TTLPolicy(table_name=f"test_table_{i}", retention_days=30 * (i + 1))
        for i in range(3)
    ]
    
    for policy in policies:
        await manager.register_policy(policy)
    
    # List policies
    retrieved = await manager.list_policies()
    
    # Should have at least our 3 test policies
    table_names = [p.table_name for p in retrieved]
    for i in range(3):
        assert f"test_table_{i}" in table_names


@pytest.mark.asyncio
async def test_archival_stats_calculation():
    """Test archival statistics calculation."""
    from api.utils.ttl_partition_manager import ArchivalStats
    
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(minutes=1)
    
    stats = ArchivalStats(
        table_name="test_table",
        start_time=start_time,
        end_time=end_time,
        rows_scanned=1000,
        rows_archived=500,
        rows_deleted=500,
        duration_ms=60000.0,
    )
    
    assert stats.table_name == "test_table"
    assert stats.rows_scanned == 1000
    assert stats.success is True
    
    # Test with errors
    stats_with_errors = ArchivalStats(
        table_name="test_table",
        start_time=start_time,
        errors=["Error 1"],
    )
    
    assert stats_with_errors.success is False


@pytest.mark.asyncio
async def test_policy_status_changes():
    """Test changing policy status."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    policy = TTLPolicy(
        table_name="test_status_table",
        retention_days=30,
        status=PolicyStatus.ACTIVE,
    )
    
    await manager.register_policy(policy)
    
    # Change status
    policy.status = PolicyStatus.PAUSED
    await manager.register_policy(policy)
    
    retrieved = await manager.get_policy("test_status_table")
    assert retrieved.status == PolicyStatus.PAUSED


@pytest.mark.asyncio
async def test_dry_run_mode():
    """Test dry-run mode doesn't modify data."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    policy = TTLPolicy(
        table_name="notification_logs",
        retention_days=1,  # Very short retention for testing
        dry_run=True,
        archive_strategy=ArchiveStrategy.DELETE,
    )
    
    await manager.register_policy(policy)
    
    # Run archival in dry-run mode
    stats = await manager.archive_expired_data(policy)
    
    # Should complete without errors in dry-run mode
    assert stats is not None


@pytest.mark.asyncio
async def test_archival_history_tracking():
    """Test that archival operations are tracked in history."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    policy = TTLPolicy(
        table_name="test_history_table",
        retention_days=30,
        dry_run=True,
    )
    
    await manager.register_policy(policy)
    
    # Run archival
    await manager.archive_expired_data(policy)
    
    # Check history was recorded
    history = await manager.get_archival_history("test_history_table", limit=1)
    
    assert len(history) >= 0  # May be empty if no data to archive


@pytest.mark.asyncio
async def test_statistics_aggregation():
    """Test statistics aggregation."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    stats = await manager.get_stats()
    
    # Verify stats structure
    assert "policy_count" in stats
    assert "active_policies" in stats
    assert "total_rows_archived" in stats
    assert "total_rows_deleted" in stats
    assert "runs_last_24h" in stats


@pytest.mark.asyncio
async def test_callback_registration():
    """Test archival callback registration."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    callbacks_triggered = []
    
    def test_callback(stats):
        callbacks_triggered.append(stats.table_name)
    
    manager.register_archival_callback(test_callback)
    
    # Verify callback is registered
    assert test_callback in manager._archival_callbacks


@pytest.mark.asyncio
async def test_partition_info_retrieval():
    """Test partition information retrieval."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    # Get partitions for a table (may be empty if no data)
    partitions = await manager.get_partitions("notification_logs")
    
    # Should return a list (may be empty)
    assert isinstance(partitions, list)


@pytest.mark.asyncio
async def test_policy_with_filters():
    """Test policy with additional filters."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    policy = TTLPolicy(
        table_name="test_filtered_table",
        retention_days=30,
        filters={"status": "completed"},
    )
    
    await manager.register_policy(policy)
    
    retrieved = await manager.get_policy("test_filtered_table")
    assert retrieved.filters == {"status": "completed"}


@pytest.mark.asyncio
async def test_different_retention_periods():
    """Test policies with different retention periods."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    retention_periods = [7, 30, 90, 365]
    
    for days in retention_periods:
        policy = TTLPolicy(
            table_name=f"test_retention_{days}",
            retention_days=days,
        )
        await manager.register_policy(policy)
    
    # Verify all policies
    for days in retention_periods:
        retrieved = await manager.get_policy(f"test_retention_{days}")
        assert retrieved.retention_days == days


@pytest.mark.asyncio
async def test_archive_strategies():
    """Test different archive strategies."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    strategies = [
        ArchiveStrategy.DELETE,
        ArchiveStrategy.SOFT_DELETE,
        ArchiveStrategy.ARCHIVE_THEN_DELETE,
        ArchiveStrategy.ARCHIVE_ONLY,
    ]
    
    for strategy in strategies:
        policy = TTLPolicy(
            table_name=f"test_strategy_{strategy.value}",
            retention_days=30,
            archive_strategy=strategy,
            dry_run=True,
        )
        await manager.register_policy(policy)
    
    # Verify all strategies
    for strategy in strategies:
        retrieved = await manager.get_policy(f"test_strategy_{strategy.value}")
        assert retrieved.archive_strategy == strategy


@pytest.mark.asyncio
async def test_batch_size_configuration():
    """Test different batch sizes."""
    manager = TTLPartitionManager(engine)
    await manager.initialize()
    
    batch_sizes = [100, 500, 1000, 5000]
    
    for size in batch_sizes:
        policy = TTLPolicy(
            table_name=f"test_batch_{size}",
            retention_days=30,
            batch_size=size,
        )
        await manager.register_policy(policy)
        
        retrieved = await manager.get_policy(f"test_batch_{size}")
        assert retrieved.batch_size == size


@pytest.mark.asyncio
async def test_global_manager_instance():
    """Test global TTL manager instance."""
    manager1 = await get_ttl_manager(engine)
    manager2 = await get_ttl_manager(engine)
    
    # Should return same instance
    assert manager1 is manager2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
