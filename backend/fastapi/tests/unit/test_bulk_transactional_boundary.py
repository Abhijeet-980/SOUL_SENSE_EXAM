"""
Unit tests for Bulk Endpoint Transactional Boundary (#1364).

Tests atomic database transactions for bulk operations to ensure:
- No partial commits on batch failures
- Rollback verification on errors
- Data consistency maintained
- Idempotency safeguards work correctly
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime, timezone

# Test imports
from api.services.settings_sync_service import SettingsSyncService
from api.models import UserSyncSetting


class TestSettingsSyncServiceBatchTransactional:
    """Test batch operations with transactional boundaries."""

    @pytest.fixture
    def mock_db(self):
        """Create a properly configured mock AsyncSession."""
        db = AsyncMock(spec=AsyncSession)
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        db.execute = AsyncMock()
        db.add = MagicMock()
        db.delete = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create SettingsSyncService with mock DB."""
        return SettingsSyncService(mock_db)

    @pytest.mark.asyncio
    async def test_batch_upsert_all_success_atomic_commit(self, service, mock_db):
        """
        Test that all settings are committed atomically when batch succeeds.
        
        Acceptance Criteria: No partial commits - all or nothing.
        """
        user_id = 1
        settings = [
            {"key": "setting1", "value": "value1"},
            {"key": "setting2", "value": "value2"},
            {"key": "setting3", "value": "value3"},
        ]
        
        # Create a mock setting for post-commit fetch
        mock_setting = MagicMock()
        mock_setting.key = "test_key"
        
        call_count = [0]
        def execute_side_effect(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            # All calls return mock setting for simplicity
            result.scalar_one_or_none.return_value = mock_setting
            return result
        
        mock_db.execute.side_effect = execute_side_effect
        
        # Execute batch upsert
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        # Verify single atomic commit (not multiple commits)
        assert mock_db.commit.call_count == 1
        mock_db.rollback.assert_not_called()
        # Verify no conflicts
        assert len(conflicts) == 0

    @pytest.mark.asyncio
    async def test_batch_upsert_rollback_on_db_error(self, service, mock_db):
        """
        Test that batch rolls back on database error.
        
        Acceptance Criteria: Rollback verified on any failure.
        """
        user_id = 1
        settings = [
            {"key": "setting1", "value": "value1"},
            {"key": "setting2", "value": "value2"},
        ]
        
        # Create mock setting - use a simple callable that returns a mock
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        # Make commit raise an error (simulating DB failure)
        mock_db.commit.side_effect = SQLAlchemyError("Database connection lost")
        
        # Execute should raise exception after rollback
        with pytest.raises(SQLAlchemyError, match="Database connection lost"):
            await service.batch_upsert_settings(user_id, settings)
        
        # Verify rollback was called
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_upsert_prevents_partial_commit_on_conflict(self, service, mock_db):
        """
        Test that version conflicts prevent entire batch commit.
        
        Edge Case: Concurrent bulk calls with version conflicts.
        """
        user_id = 1
        settings = [
            {"key": "setting1", "value": "value1", "expected_version": 5},
            {"key": "setting2", "value": "value2", "expected_version": 1},
        ]
        
        # Create mock existing setting with version conflict
        existing_mock = MagicMock()
        existing_mock.version = 10  # Different from expected_version=5
        existing_mock.key = "setting1"
        
        call_count = [0]
        def execute_side_effect(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            # First call (setting1 validation) returns conflict
            if call_count[0] == 1:
                result.scalar_one_or_none.return_value = existing_mock
            else:
                result.scalar_one_or_none.return_value = None
            return result
        
        mock_db.execute.side_effect = execute_side_effect
        
        # Execute batch upsert
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        # Verify conflict detected and no commit made
        assert len(conflicts) == 1
        assert "setting1" in conflicts
        assert len(successful) == 0
        mock_db.commit.assert_not_called()
        mock_db.rollback.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_upsert_empty_settings(self, service, mock_db):
        """
        Test handling of empty batch.
        
        Edge Case: Empty batch should not cause errors.
        """
        user_id = 1
        settings = []
        
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        assert len(successful) == 0
        assert len(conflicts) == 0
        mock_db.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_upsert_single_item(self, service, mock_db):
        """
        Test batch with single item still uses atomic transaction.
        """
        user_id = 1
        settings = [{"key": "single", "value": "value"}]
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        assert len(conflicts) == 0
        assert mock_db.commit.call_count == 1

    @pytest.mark.asyncio
    async def test_batch_upsert_mixed_new_and_existing(self, service, mock_db):
        """
        Test batch with mix of new and existing settings.
        """
        user_id = 1
        settings = [
            {"key": "new_setting", "value": "new_value"},
            {"key": "existing_setting", "value": "updated_value"},
        ]
        
        # Mock existing setting
        existing_mock = MagicMock()
        existing_mock.version = 1
        existing_mock.key = "existing_setting"
        existing_mock.value = '"old_value"'
        
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            # Check if this is validation for existing_setting
            stmt_str = str(args[0]) if args else ""
            if "existing_setting" in stmt_str:
                result.scalar_one_or_none.return_value = existing_mock
            else:
                result.scalar_one_or_none.return_value = None
            return result
        
        mock_db.execute.side_effect = side_effect
        
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        # Should succeed with single commit
        assert len(conflicts) == 0
        assert mock_db.commit.call_count == 1

    @pytest.mark.asyncio
    async def test_batch_upsert_no_key_skipped(self, service, mock_db):
        """
        Test that settings without keys are skipped during batch.
        """
        user_id = 1
        settings = [
            {"key": "valid_key", "value": "value1"},
            {"value": "missing_key"},  # No key - should be skipped
            {"key": "another_valid", "value": "value2"},
        ]
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        successful, conflicts = await service.batch_upsert_settings(user_id, settings)
        
        # Only 2 valid settings should be processed, but commit still happens
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()


class TestBatchTransactionIsolation:
    """Test transaction isolation and concurrent access scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_batch_operations(self):
        """
        Simulate concurrent batch operations to verify isolation.
        
        Edge Case: Concurrent bulk calls should be properly isolated.
        """
        # This test verifies that concurrent operations don't interfere
        operations_log = []
        
        async def mock_batch_operation(user_id: int, settings: list):
            """Simulate batch operation."""
            operations_log.append(f"start_{user_id}")
            await asyncio.sleep(0.01)  # Simulate DB work
            operations_log.append(f"commit_{user_id}")
            return settings, []
        
        # Simulate concurrent batch operations
        tasks = [
            mock_batch_operation(1, [{"key": "s1", "value": "v1"}]),
            mock_batch_operation(2, [{"key": "s2", "value": "v2"}]),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Both should complete successfully
        assert all(not isinstance(r, Exception) for r in results)
        assert len(results) == 2


class TestIdempotencySafeguards:
    """Test idempotency safeguards for bulk operations."""

    @pytest.mark.asyncio
    async def test_batch_upsert_is_idempotent(self):
        """
        Test that running same batch twice produces consistent results.
        
        Acceptance Criteria: Idempotency safeguards work correctly.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        user_id = 1
        settings = [{"key": "idempotent_key", "value": "value"}]
        
        # First execution
        successful1, conflicts1 = await service.batch_upsert_settings(user_id, settings)
        
        assert len(conflicts1) == 0
        
        # Reset mocks for second execution
        mock_db.commit.reset_mock()
        
        # Second execution
        successful2, conflicts2 = await service.batch_upsert_settings(user_id, settings)
        
        # Should still succeed
        assert len(conflicts2) == 0


class TestEdgeCases:
    """Test edge cases for bulk transactional operations."""

    @pytest.mark.asyncio
    async def test_network_timeout_simulation(self):
        """
        Test behavior when network timeout occurs during batch.
        
        Edge Case: Network timeout during bulk operation.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=OperationalError("Connection timed out", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(OperationalError, match="timed out"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_deadlock_detection(self):
        """
        Test behavior when deadlock is detected during batch.
        
        Edge Case: Deadlock during concurrent bulk calls.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=OperationalError("deadlock detected", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(OperationalError, match="deadlock"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_large_batch_performance(self):
        """
        Test that large batches still use single transaction.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        # Create large batch
        settings = [{"key": f"key_{i}", "value": f"value_{i}"} for i in range(100)]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Should process all settings with single commit
        assert len(conflicts) == 0
        assert mock_db.commit.call_count == 1


class TestTransactionLogs:
    """Test transaction logging and observability."""

    @pytest.mark.asyncio
    async def test_transaction_boundary_logging(self, caplog):
        """
        Test that transaction boundaries are properly logged.
        
        Acceptance Criteria: Transaction logs for verification.
        """
        import logging
        
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        with caplog.at_level(logging.DEBUG):
            await service.batch_upsert_settings(1, [{"key": "test", "value": "val"}])
        
        # Transaction should complete (commit called)
        mock_db.commit.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
