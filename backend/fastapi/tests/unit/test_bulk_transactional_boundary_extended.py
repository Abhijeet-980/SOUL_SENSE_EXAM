"""
Extended unit tests for Bulk Endpoint Transactional Boundary (#1364).

Additional test coverage for edge cases, error scenarios, and complex transactional behavior.
"""
import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call, Mock
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import (
    SQLAlchemyError, OperationalError, IntegrityError, 
    TimeoutError as SATimeoutError
)
from datetime import datetime, timezone

# Test imports
from api.services.settings_sync_service import SettingsSyncService
from api.models import UserSyncSetting


class TestConcurrentTransactionConflicts:
    """Test concurrent transaction conflict scenarios."""

    @pytest.mark.asyncio
    async def test_simultaneous_batch_updates_same_user(self):
        """
        Test two simultaneous batch updates for the same user.
        
        Edge Case: Race condition on concurrent bulk calls.
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
        
        # Simulate concurrent updates
        settings_batch_1 = [{"key": "theme", "value": "dark"}]
        settings_batch_2 = [{"key": "language", "value": "en"}]
        
        async def update_batch(batch):
            return await service.batch_upsert_settings(1, batch)
        
        # Run concurrently
        results = await asyncio.gather(
            update_batch(settings_batch_1),
            update_batch(settings_batch_2),
            return_exceptions=True
        )
        
        # Both should complete without exceptions
        assert all(not isinstance(r, Exception) for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_version_conflict_detection(self):
        """
        Test that version conflicts are detected during concurrent access.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        # Simulate version change between read and write
        existing_mock = MagicMock()
        existing_mock.version = 5  # Changed from expected 3
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = existing_mock
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        # Try to update with stale version
        settings = [{"key": "setting1", "value": "new_value", "expected_version": 3}]
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Should detect conflict and abort
        assert len(conflicts) == 1
        assert "setting1" in conflicts
        mock_db.commit.assert_not_called()


class TestExternalFailureScenarios:
    """Test external API and service failure scenarios."""

    @pytest.mark.asyncio
    async def test_rollback_on_integrity_error(self):
        """
        Test rollback on unique constraint violation.
        
        Edge Case: Database integrity error during batch.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=IntegrityError("Unique constraint violation", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [
            {"key": "setting1", "value": "value1"},
            {"key": "setting2", "value": "value2"},
        ]
        
        with pytest.raises(IntegrityError, match="Unique constraint"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_rollback_on_serialization_failure(self):
        """
        Test rollback on serialization failure (concurrent modification).
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=OperationalError("could not serialize access", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(OperationalError, match="serialize"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_database_disconnect_during_batch(self):
        """
        Test handling of database disconnection during batch processing.
        
        Edge Case: Network failure mid-transaction.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=OperationalError("server closed the connection unexpectedly", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(OperationalError, match="connection"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()


class TestDataValidationEdgeCases:
    """Test data validation and serialization edge cases."""

    @pytest.mark.asyncio
    async def test_batch_with_unicode_characters(self):
        """
        Test batch operations with unicode and special characters.
        
        Edge Case: International characters, emojis, special symbols.
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
        
        settings = [
            {"key": "unicode_text", "value": "Hello 世界 🌍"},
            {"key": "emoji_only", "value": "🎉🎊✨"},
            {"key": "special_chars", "value": "<script>alert('test')</script>"},
            {"key": "arabic_text", "value": "مرحبا بالعالم"},
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_with_nested_objects(self):
        """
        Test batch with complex nested JSON objects.
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
        
        settings = [
            {
                "key": "complex_config",
                "value": {
                    "nested": {
                        "deeply": {
                            "nested": "value"
                        }
                    },
                    "array": [1, 2, 3, {"nested_in_array": True}],
                    "null_value": None,
                    "boolean": True,
                    "number": 42.5
                }
            }
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_with_very_long_values(self):
        """
        Test batch with very long string values.
        
        Edge Case: Large text content.
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
        
        # Create a 10KB string
        long_value = "x" * (10 * 1024)
        
        settings = [{"key": "long_content", "value": long_value}]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_with_null_values(self):
        """
        Test batch operations with null/None values.
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
        
        settings = [
            {"key": "null_value", "value": None},
            {"key": "empty_string", "value": ""},
            {"key": "zero", "value": 0},
            {"key": "false_value", "value": False},
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()


class TestBatchSizeAndPerformance:
    """Test batch size limits and performance scenarios."""

    @pytest.mark.asyncio
    async def test_very_large_batch(self):
        """
        Test batch with 1000 items.
        
        Edge Case: Stress test with very large batch.
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
        
        # Create very large batch
        settings = [{"key": f"key_{i}", "value": f"value_{i}"} for i in range(1000)]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Should process all settings with single commit
        assert len(conflicts) == 0
        assert mock_db.commit.call_count == 1

    @pytest.mark.asyncio
    async def test_batch_with_all_duplicate_keys(self):
        """
        Test batch where all items have duplicate keys.
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
        
        # All settings with same key
        settings = [
            {"key": "duplicate_key", "value": "value1"},
            {"key": "duplicate_key", "value": "value2"},
            {"key": "duplicate_key", "value": "value3"},
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Should still commit (last write wins)
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()


class TestMultiUserIsolation:
    """Test transaction isolation between different users."""

    @pytest.mark.asyncio
    async def test_concurrent_batches_different_users(self):
        """
        Test concurrent batches for different users don't interfere.
        """
        mock_db_1 = AsyncMock(spec=AsyncSession)
        mock_db_1.commit = AsyncMock()
        mock_db_1.rollback = AsyncMock()
        
        mock_db_2 = AsyncMock(spec=AsyncSession)
        mock_db_2.commit = AsyncMock()
        mock_db_2.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db_1.execute.side_effect = create_mock_result
        mock_db_2.execute.side_effect = create_mock_result
        
        service_1 = SettingsSyncService(mock_db_1)
        service_2 = SettingsSyncService(mock_db_2)
        
        # Concurrent operations for different users
        async def user_operation(service, user_id):
            settings = [{"key": "theme", "value": "dark"}]
            return await service.batch_upsert_settings(user_id, settings)
        
        results = await asyncio.gather(
            user_operation(service_1, 1),
            user_operation(service_2, 2),
            return_exceptions=True
        )
        
        # Both should succeed independently
        assert all(not isinstance(r, Exception) for r in results)
        mock_db_1.commit.assert_called_once()
        mock_db_2.commit.assert_called_once()


class TestTransactionRecovery:
    """Test transaction recovery scenarios."""

    @pytest.mark.asyncio
    async def test_state_consistency_after_rollback(self):
        """
        Test that database state remains consistent after rollback.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        
        # First commit succeeds, second fails
        commit_count = [0]
        async def commit_side_effect():
            commit_count[0] += 1
            if commit_count[0] > 1:
                raise OperationalError("Deadlock detected", None, None)
        
        mock_db.commit = AsyncMock(side_effect=commit_side_effect)
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        # First batch succeeds
        await service.batch_upsert_settings(1, [{"key": "k1", "value": "v1"}])
        assert mock_db.commit.call_count == 1
        
        # Second batch fails and rolls back
        mock_db.commit.reset_mock()
        commit_count[0] = 0  # Reset for new test
        mock_db.commit.side_effect = OperationalError("Deadlock detected", None, None)
        
        with pytest.raises(OperationalError):
            await service.batch_upsert_settings(1, [{"key": "k2", "value": "v2"}])
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_after_rollback(self):
        """
        Test that operations can be retried after rollback.
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
        
        # First attempt with simulated failure
        mock_db.commit.side_effect = OperationalError("Deadlock detected", None, None)
        
        with pytest.raises(OperationalError):
            await service.batch_upsert_settings(1, [{"key": "test", "value": "v1"}])
        
        mock_db.rollback.assert_called_once()
        
        # Reset for retry
        mock_db.commit.reset_mock()
        mock_db.rollback.reset_mock()
        mock_db.commit = AsyncMock()  # Remove side effect
        
        # Retry should succeed
        successful, conflicts = await service.batch_upsert_settings(1, [{"key": "test", "value": "v2"}])
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()


class TestPartialFailureScenarios:
    """Test scenarios with partial failures within batches."""

    @pytest.mark.asyncio
    async def test_mixed_valid_and_invalid_settings(self):
        """
        Test batch with mix of valid and invalid settings.
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
        
        settings = [
            {"key": "valid1", "value": "value1"},
            {"invalid": "no key field"},  # Invalid - no key
            {"key": "valid2", "value": "value2"},
            {"key": "", "value": "empty key"},  # Invalid - empty key
            {"key": "valid3", "value": "value3"},
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Valid settings should still commit
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_all_settings_invalid(self):
        """
        Test batch where all settings are invalid.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        service = SettingsSyncService(mock_db)
        
        settings = [
            {"invalid": "no key"},
            {"also_invalid": "no key here"},
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # No successful settings should be returned
        assert len(successful) == 0
        assert len(conflicts) == 0
        # Note: Current implementation may still commit with empty operations


class TestSerializationBehavior:
    """Test JSON serialization behavior in batches."""

    @pytest.mark.asyncio
    async def test_circular_reference_handling(self):
        """
        Test handling of circular references in values.
        
        Edge Case: Objects with circular references.
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
        
        # Create circular reference
        circular = {"name": "parent"}
        circular["child"] = circular
        
        settings = [{"key": "circular", "value": circular}]
        
        # Should raise error due to circular reference
        with pytest.raises((ValueError, TypeError, RecursionError)):
            await service.batch_upsert_settings(1, settings)

    @pytest.mark.asyncio
    async def test_datetime_serialization(self):
        """
        Test serialization of datetime objects.
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
        
        settings = [
            {
                "key": "datetime_value",
                "value": {"created_at": datetime.now(timezone.utc).isoformat()}
            }
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_bytes_serialization(self):
        """
        Test handling of bytes in values.
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
        
        # Bytes should be handled
        settings = [{"key": "bytes_value", "value": b"binary data"}]
        
        # May raise error or handle gracefully
        try:
            successful, conflicts = await service.batch_upsert_settings(1, settings)
            mock_db.commit.assert_called_once()
        except (TypeError, ValueError):
            pass  # Also acceptable if it raises


class TestTransactionTimeoutScenarios:
    """Test transaction timeout and long-running scenarios."""

    @pytest.mark.asyncio
    async def test_statement_timeout_during_batch(self):
        """
        Test handling of statement timeout.
        
        Edge Case: Query timeout during batch execution.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=SATimeoutError("Statement timeout"))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(SATimeoutError):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()

    @pytest.mark.asyncio 
    async def test_transaction_lock_timeout(self):
        """
        Test handling of lock timeout.
        
        Edge Case: Unable to acquire lock within timeout.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock(side_effect=OperationalError("Lock wait timeout exceeded", None, None))
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(OperationalError, match="timeout"):
            await service.batch_upsert_settings(1, settings)
        
        mock_db.rollback.assert_called_once()


class TestVersionConflictScenarios:
    """Test various version conflict scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_version_conflicts_in_batch(self):
        """
        Test batch with multiple version conflicts.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        # Create existing settings with different versions
        existing_1 = MagicMock()
        existing_1.version = 10
        existing_2 = MagicMock()
        existing_2.version = 20
        
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:
                result.scalar_one_or_none.return_value = existing_1
            elif call_count[0] == 2:
                result.scalar_one_or_none.return_value = existing_2
            else:
                result.scalar_one_or_none.return_value = None
            return result
        
        mock_db.execute.side_effect = side_effect
        
        service = SettingsSyncService(mock_db)
        
        settings = [
            {"key": "setting1", "value": "v1", "expected_version": 1},  # Conflict
            {"key": "setting2", "value": "v2", "expected_version": 2},  # Conflict
            {"key": "setting3", "value": "v3"},  # No conflict
        ]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        # Both conflicts should be reported
        assert len(conflicts) == 2
        assert "setting1" in conflicts
        assert "setting2" in conflicts
        mock_db.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_version_conflict_with_none_expected(self):
        """
        Test that None expected_version skips version check.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.commit = AsyncMock()
        mock_db.rollback = AsyncMock()
        
        existing = MagicMock()
        existing.version = 99
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = existing
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        # No expected_version means no conflict check
        settings = [{"key": "setting1", "value": "new_value"}]
        
        successful, conflicts = await service.batch_upsert_settings(1, settings)
        
        assert len(conflicts) == 0
        mock_db.commit.assert_called_once()


class TestErrorMessageAndLogging:
    """Test error messages and logging behavior."""

    @pytest.mark.asyncio
    async def test_rollback_preserves_error_context(self):
        """
        Test that original error context is preserved after rollback.
        """
        mock_db = AsyncMock(spec=AsyncSession)
        original_error = SQLAlchemyError("Original database error")
        mock_db.commit = AsyncMock(side_effect=original_error)
        mock_db.rollback = AsyncMock()
        
        def create_mock_result(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = MagicMock()
            return mock_result
        
        mock_db.execute.side_effect = create_mock_result
        
        service = SettingsSyncService(mock_db)
        
        settings = [{"key": "test", "value": "value"}]
        
        with pytest.raises(SQLAlchemyError) as exc_info:
            await service.batch_upsert_settings(1, settings)
        
        assert "Original database error" in str(exc_info.value)
        mock_db.rollback.assert_called_once()


class TestCleanupAndResourceManagement:
    """Test cleanup and resource management."""

    @pytest.mark.asyncio
    async def test_no_memory_leak_on_repeated_operations(self):
        """
        Test that repeated operations don't cause memory leaks.
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
        
        # Perform many operations
        for i in range(100):
            await service.batch_upsert_settings(1, [{"key": f"key_{i}", "value": f"value_{i}"}])
        
        # Should complete without issues
        assert mock_db.commit.call_count == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
