"""
Unit tests for Row-Level TTL Archival Partitioning (#1413).

Tests TTL policy management, archival operations, partition management,
and background task functionality.
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from typing import Dict, Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from api.utils.ttl_partition_manager import (
    TTLPartitionManager,
    TTLPolicy,
    ArchivalStats,
    PartitionInfo,
    PartitionGranularity,
    ArchiveStrategy,
    PolicyStatus,
    get_ttl_manager,
)


class TestTTLPolicy:
    """Test TTLPolicy dataclass."""

    def test_basic_creation(self):
        """Test creating a TTL policy."""
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
        )
        
        assert policy.table_name == "test_table"
        assert policy.retention_days == 30
        assert policy.partition_granularity == PartitionGranularity.MONTHLY
        assert policy.archive_strategy == ArchiveStrategy.ARCHIVE_THEN_DELETE
        assert policy.status == PolicyStatus.ACTIVE

    def test_policy_to_dict(self):
        """Test converting policy to dictionary."""
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=90,
            partition_granularity=PartitionGranularity.DAILY,
            archive_strategy=ArchiveStrategy.SOFT_DELETE,
            dry_run=True,
            batch_size=500,
            filters={"type": "log"},
        )
        
        result = policy.to_dict()
        
        assert result["table_name"] == "test_table"
        assert result["retention_days"] == 90
        assert result["partition_granularity"] == "daily"
        assert result["archive_strategy"] == "soft_delete"
        assert result["dry_run"] is True
        assert result["batch_size"] == 500
        assert result["filters"] == {"type": "log"}


class TestArchivalStats:
    """Test ArchivalStats dataclass."""

    def test_basic_creation(self):
        """Test creating archival stats."""
        stats = ArchivalStats(
            table_name="test_table",
            start_time=datetime.utcnow(),
        )
        
        assert stats.table_name == "test_table"
        assert stats.rows_scanned == 0
        assert stats.success is True

    def test_stats_with_errors(self):
        """Test stats with errors."""
        stats = ArchivalStats(
            table_name="test_table",
            start_time=datetime.utcnow(),
            errors=["Error 1", "Error 2"],
        )
        
        assert stats.success is False
        assert len(stats.errors) == 2

    def test_stats_to_dict(self):
        """Test converting stats to dictionary."""
        stats = ArchivalStats(
            table_name="test_table",
            start_time=datetime(2026, 3, 7, 12, 0, 0),
            end_time=datetime(2026, 3, 7, 12, 1, 0),
            rows_scanned=1000,
            rows_archived=500,
            rows_deleted=500,
            duration_ms=60000.0,
        )
        
        result = stats.to_dict()
        
        assert result["table_name"] == "test_table"
        assert result["rows_scanned"] == 1000
        assert result["rows_archived"] == 500
        assert result["rows_deleted"] == 500
        assert result["duration_ms"] == 60000.0
        assert result["success"] is True


class TestPartitionInfo:
    """Test PartitionInfo dataclass."""

    def test_partition_info_creation(self):
        """Test creating partition info."""
        info = PartitionInfo(
            name="test_table_2026_03",
            table_name="test_table",
            start_date=datetime(2026, 3, 1),
            end_date=datetime(2026, 4, 1),
            row_count=10000,
        )
        
        assert info.name == "test_table_2026_03"
        assert info.row_count == 10000
        assert info.is_archived is False


class TestTTLPartitionManagerInitialization:
    """Test TTLPartitionManager initialization."""

    def test_init_with_engine(self):
        """Test initialization with engine."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        assert manager.engine == mock_engine
        assert len(manager._policies) == 0

    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self):
        """Test initialize creates TTL tables."""
        mock_engine = Mock()
        mock_conn = AsyncMock()
        mock_engine.begin.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_engine.begin.return_value.__aexit__ = AsyncMock(return_value=None)
        
        manager = TTLPartitionManager(mock_engine)
        
        # Mock _create_ttl_tables
        with patch.object(manager, '_create_ttl_tables') as mock_create:
            await manager.initialize()
            mock_create.assert_called_once()


class TestTTLPolicyManagement:
    """Test policy registration and retrieval."""

    @pytest.mark.asyncio
    async def test_register_policy(self):
        """Test registering a policy."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
        )
        
        # Mock database operations
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            await manager.register_policy(policy)
            
            assert manager._policies["test_table"] == policy

    @pytest.mark.asyncio
    async def test_get_policy_from_memory(self):
        """Test getting policy from memory cache."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
        )
        manager._policies["test_table"] = policy
        
        result = await manager.get_policy("test_table")
        
        assert result == policy

    @pytest.mark.asyncio
    async def test_get_policy_not_found(self):
        """Test getting non-existent policy."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        # Mock database returning no results
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session_instance.execute.return_value.fetchone.return_value = None
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            result = await manager.get_policy("nonexistent")
            
            assert result is None

    @pytest.mark.asyncio
    async def test_list_policies(self):
        """Test listing policies."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        # Mock database returning policies
        mock_row = Mock()
        mock_row.table_name = "test_table"
        mock_row.retention_days = 30
        mock_row.partition_granularity = "monthly"
        mock_row.archive_strategy = "delete"
        mock_row.archive_table = None
        mock_row.status = "active"
        mock_row.dry_run = False
        mock_row.batch_size = 1000
        mock_row.id_column = "id"
        mock_row.timestamp_column = "created_at"
        mock_row.filters = None
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session_instance.execute.return_value.fetchall.return_value = [mock_row]
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            policies = await manager.list_policies()
            
            assert len(policies) == 1
            assert policies[0].table_name == "test_table"


class TestArchivalStrategies:
    """Test different archival strategies."""

    @pytest.mark.asyncio
    async def test_archive_expired_data_disabled_policy(self):
        """Test that disabled policies are skipped."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
            status=PolicyStatus.DISABLED,
        )
        
        stats = await manager.archive_expired_data(policy)
        
        assert stats.rows_scanned == 0
        assert stats.rows_archived == 0

    @pytest.mark.asyncio
    async def test_archive_expired_data_dry_run(self):
        """Test dry-run mode."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
            dry_run=True,
        )
        
        # Mock database operations
        mock_result = Mock()
        mock_result.scalar.return_value = 100  # 100 rows to process
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session_instance.execute.return_value = mock_result
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            stats = await manager.archive_expired_data(policy)
            
            assert stats.rows_scanned == 100

    @pytest.mark.asyncio
    async def test_hard_delete_strategy(self):
        """Test hard delete archival strategy."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        policy = TTLPolicy(
            table_name="test_table",
            retention_days=30,
            archive_strategy=ArchiveStrategy.DELETE,
            dry_run=False,
        )
        
        mock_result = Mock()
        mock_result.scalar.return_value = 100  # Count query
        mock_result.rowcount = 50  # Delete query
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session_instance.execute.return_value = mock_result
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Patch _record_archival_history to avoid DB calls
            with patch.object(manager, '_record_archival_history'):
                stats = await manager.archive_expired_data(policy)
                
                assert stats.rows_scanned == 100


class TestStatisticsAndHistory:
    """Test statistics and history tracking."""

    @pytest.mark.asyncio
    async def test_get_stats(self):
        """Test getting statistics."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        # Mock database results
        mock_results = [
            Mock(scalar=lambda: 5),   # policy_count
            Mock(scalar=lambda: 3),   # active_count
            Mock(scalar=lambda: 1000), # total_archived
            Mock(scalar=lambda: 500),  # total_deleted
            Mock(scalar=lambda: 2),   # recent_runs
        ]
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session_instance.execute.side_effect = mock_results
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            stats = await manager.get_stats()
            
            assert stats["policy_count"] == 5
            assert stats["active_policies"] == 3
            assert stats["total_rows_archived"] == 1000
            assert stats["total_rows_deleted"] == 500
            assert stats["runs_last_24h"] == 2

    @pytest.mark.asyncio
    async def test_get_archival_history(self):
        """Test getting archival history."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        mock_row = Mock()
        mock_row.id = 1
        mock_row.table_name = "test_table"
        mock_row.start_time = datetime.utcnow()
        mock_row.end_time = datetime.utcnow()
        mock_row.rows_scanned = 100
        mock_row.rows_archived = 50
        mock_row.rows_deleted = 50
        mock_row.duration_ms = 1000
        mock_row.dry_run = False
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_result = AsyncMock()
            mock_result.fetchall.return_value = [mock_row]
            mock_session_instance.execute.return_value = mock_result
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            history = await manager.get_archival_history(limit=10)
            
            assert len(history) == 1
            assert history[0]["table_name"] == "test_table"


class TestRunAllPolicies:
    """Test running all policies."""

    @pytest.mark.asyncio
    async def test_run_all_policies(self):
        """Test running all active policies."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        # Create test policies
        policy1 = TTLPolicy(
            table_name="table1",
            retention_days=30,
            status=PolicyStatus.ACTIVE,
        )
        policy2 = TTLPolicy(
            table_name="table2",
            retention_days=60,
            status=PolicyStatus.PAUSED,  # Should be skipped
        )
        
        manager._policies = {
            "table1": policy1,
            "table2": policy2,
        }
        
        # Mock archive_expired_data
        mock_stats = ArchivalStats(
            table_name="table1",
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
            rows_scanned=100,
        )
        
        with patch.object(manager, 'archive_expired_data', return_value=mock_stats):
            results = await manager.run_all_policies()
            
            assert "table1" in results
            assert results["table1"].rows_scanned == 100


class TestCallbackRegistration:
    """Test callback registration."""

    def test_register_callback(self):
        """Test registering archival callback."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        def callback(stats):
            pass
        
        manager.register_archival_callback(callback)
        
        assert callback in manager._archival_callbacks


class TestPartitionInfoRetrieval:
    """Test partition information retrieval."""

    @pytest.mark.asyncio
    async def test_get_partitions(self):
        """Test getting partition information."""
        mock_engine = Mock()
        manager = TTLPartitionManager(mock_engine)
        
        mock_row = Mock()
        mock_row.partition_date = datetime(2026, 3, 1)
        mock_row.row_count = 5000
        
        with patch('api.utils.ttl_partition_manager.AsyncSessionLocal') as mock_session:
            mock_session_instance = AsyncMock()
            mock_result = AsyncMock()
            mock_result.__iter__ = Mock(return_value=iter([mock_row]))
            mock_session_instance.execute.return_value = mock_result
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            partitions = await manager.get_partitions("test_table")
            
            assert len(partitions) == 1
            assert partitions[0].row_count == 5000


class TestEnums:
    """Test enum classes."""

    def test_partition_granularity_values(self):
        """Test partition granularity enum values."""
        assert PartitionGranularity.DAILY.value == "daily"
        assert PartitionGranularity.WEEKLY.value == "weekly"
        assert PartitionGranularity.MONTHLY.value == "monthly"
        assert PartitionGranularity.QUARTERLY.value == "quarterly"
        assert PartitionGranularity.YEARLY.value == "yearly"

    def test_archive_strategy_values(self):
        """Test archive strategy enum values."""
        assert ArchiveStrategy.DELETE.value == "delete"
        assert ArchiveStrategy.SOFT_DELETE.value == "soft_delete"
        assert ArchiveStrategy.ARCHIVE_THEN_DELETE.value == "archive_then_delete"
        assert ArchiveStrategy.ARCHIVE_ONLY.value == "archive_only"

    def test_policy_status_values(self):
        """Test policy status enum values."""
        assert PolicyStatus.ACTIVE.value == "active"
        assert PolicyStatus.PAUSED.value == "paused"
        assert PolicyStatus.DRY_RUN.value == "dry_run"
        assert PolicyStatus.DISABLED.value == "disabled"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
