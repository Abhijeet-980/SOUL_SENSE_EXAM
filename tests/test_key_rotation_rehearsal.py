"""
Tests for Key Rotation Rehearsal System (#1425)

Comprehensive tests for encryption-at-rest key rotation rehearsals.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, patch, AsyncMock

from sqlalchemy import text, Column, Integer, String, Text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base

# Import the module under test
import sys
sys.path.insert(0, 'backend/fastapi')

from api.utils.key_rotation_rehearsal import (
    KeyRotationRehearsalOrchestrator,
    RotationStrategy,
    RehearsalStatus,
    KeyStatus,
    EncryptionKey,
    RehearsalSchedule,
    DataValidationResult,
    RotationRehearsalResult,
    get_key_rotation_orchestrator,
)


Base = declarative_base()


# Test Fixtures

@pytest.fixture
async def async_engine():
    """Create test async engine."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def orchestrator(async_engine):
    """Create initialized orchestrator."""
    orch = KeyRotationRehearsalOrchestrator(async_engine)
    
    # Create test tables
    async with async_engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_encrypted_data (
                id INTEGER PRIMARY KEY,
                encrypted_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS key_rotation_rehearsal_history (
                id INTEGER PRIMARY KEY,
                rehearsal_id TEXT UNIQUE NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                strategy TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                source_key_id TEXT,
                target_key_id TEXT,
                total_rows INTEGER DEFAULT 0,
                rows_processed INTEGER DEFAULT 0,
                rows_failed INTEGER DEFAULT 0,
                pre_validation TEXT,
                post_validation TEXT,
                rotation_duration_ms INTEGER,
                validation_duration_ms INTEGER,
                rollback_duration_ms INTEGER,
                rollback_performed BOOLEAN DEFAULT 0,
                errors TEXT,
                result_details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS encryption_keys (
                id INTEGER PRIMARY KEY,
                key_id TEXT UNIQUE NOT NULL,
                key_version INTEGER DEFAULT 1,
                key_status TEXT DEFAULT 'active',
                algorithm TEXT DEFAULT 'AES-256-GCM',
                key_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rotated_at TIMESTAMP,
                retired_at TIMESTAMP
            )
        """))
    
    # Insert test data
    async with AsyncSession(async_engine) as session:
        for i in range(100):
            await session.execute(text("""
                INSERT INTO test_encrypted_data (encrypted_value)
                VALUES (:value)
            """), {"value": f"encrypted_data_{i}"})
        await session.commit()
    
    await orch.initialize()
    yield orch


@pytest.fixture
def sample_key():
    """Create sample encryption key."""
    return EncryptionKey(
        key_id="test-key-001",
        key_version=1,
        key_status=KeyStatus.ACTIVE,
        created_at=datetime.utcnow(),
        algorithm="AES-256-GCM",
        key_hash="abc123hash",
    )


# --- Test Classes ---

class TestEncryptionKey:
    """Test encryption key model."""
    
    def test_key_creation(self):
        """Test creating an encryption key."""
        key = EncryptionKey(
            key_id="key-001",
            key_version=1,
            key_status=KeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
        )
        
        assert key.key_id == "key-001"
        assert key.key_version == 1
        assert key.key_status == KeyStatus.ACTIVE
        assert key.algorithm == "AES-256-GCM"
    
    def test_key_to_dict(self):
        """Test key serialization."""
        key = EncryptionKey(
            key_id="key-001",
            key_version=1,
            key_status=KeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
            key_hash="hash123",
        )
        
        data = key.to_dict()
        assert data["key_id"] == "key-001"
        assert data["key_version"] == 1
        assert data["key_status"] == "active"
        assert data["algorithm"] == "AES-256-GCM"
        assert data["key_hash"] == "hash123"


class TestDataValidationResult:
    """Test data validation result model."""
    
    def test_validation_result_creation(self):
        """Test creating validation result."""
        result = DataValidationResult(
            table_name="test_table",
            column_name="encrypted_col",
            rows_checked=100,
            rows_valid=95,
            rows_invalid=5,
        )
        
        assert result.table_name == "test_table"
        assert result.rows_checked == 100
        assert result.rows_valid == 95
        assert result.rows_invalid == 5
    
    def test_validation_is_valid(self):
        """Test is_valid property."""
        valid = DataValidationResult(
            table_name="t", column_name="c",
            rows_checked=100, rows_valid=100, rows_invalid=0,
        )
        assert valid.is_valid
        
        invalid = DataValidationResult(
            table_name="t", column_name="c",
            rows_checked=100, rows_valid=95, rows_invalid=5,
            validation_errors=["Found NULL values"],
        )
        assert not invalid.is_valid


class TestRotationRehearsalResult:
    """Test rotation rehearsal result model."""
    
    def test_result_creation(self):
        """Test creating rehearsal result."""
        result = RotationRehearsalResult(
            rehearsal_id="r001",
            table_name="test_table",
            column_name="encrypted_col",
            strategy=RotationStrategy.SHADOW_ROTATION,
            status=RehearsalStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
        )
        
        assert result.rehearsal_id == "r001"
        assert result.total_rows == 0
        assert result.progress_percentage == 0.0
    
    def test_progress_calculation(self):
        """Test progress percentage calculation."""
        result = RotationRehearsalResult(
            rehearsal_id="r001",
            table_name="test_table",
            column_name="encrypted_col",
            strategy=RotationStrategy.SHADOW_ROTATION,
            status=RehearsalStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
            total_rows=100,
            rows_processed=50,
        )
        
        assert result.progress_percentage == 50.0
    
    def test_success_property(self):
        """Test success property."""
        success = RotationRehearsalResult(
            rehearsal_id="r001",
            table_name="test_table",
            column_name="encrypted_col",
            strategy=RotationStrategy.SHADOW_ROTATION,
            status=RehearsalStatus.COMPLETED,
            started_at=datetime.utcnow(),
        )
        assert success.success
        
        failed = RotationRehearsalResult(
            rehearsal_id="r002",
            table_name="test_table",
            column_name="encrypted_col",
            strategy=RotationStrategy.SHADOW_ROTATION,
            status=RehearsalStatus.FAILED,
            started_at=datetime.utcnow(),
            errors=["Validation failed"],
        )
        assert not failed.success


class TestRehearsalSchedule:
    """Test rehearsal schedule model."""
    
    def test_default_schedule(self):
        """Test default schedule configuration."""
        schedule = RehearsalSchedule()
        
        assert not schedule.enabled
        assert schedule.frequency_days == 90
        assert schedule.preferred_hour == 3
        assert schedule.auto_rollback
        assert schedule.notify_on_failure
    
    def test_custom_schedule(self):
        """Test custom schedule configuration."""
        schedule = RehearsalSchedule(
            enabled=True,
            frequency_days=30,
            preferred_hour=2,
            tables_to_rotate=["users", "payments"],
        )
        
        assert schedule.enabled
        assert schedule.frequency_days == 30
        assert schedule.preferred_hour == 2
        assert len(schedule.tables_to_rotate) == 2


class TestKeyRotationOrchestrator:
    """Test key rotation orchestrator."""
    
    @pytest.mark.asyncio
    async def test_orchestrator_initialization(self, async_engine):
        """Test orchestrator initialization."""
        orch = KeyRotationRehearsalOrchestrator(async_engine)
        await orch.initialize()
        
        assert orch._rehearsal_history == []
        assert orch._schedule.enabled == False
    
    @pytest.mark.asyncio
    async def test_register_key(self, orchestrator, sample_key):
        """Test key registration."""
        await orchestrator.register_key(sample_key)
        
        # Verify key is stored
        key = await orchestrator.get_key("test-key-001")
        assert key is not None
        assert key.key_id == "test-key-001"
        assert key.key_version == 1
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self, orchestrator):
        """Test getting non-existent key."""
        key = await orchestrator.get_key("nonexistent-key")
        assert key is None
    
    @pytest.mark.asyncio
    async def test_run_dry_run_rehearsal(self, orchestrator, sample_key):
        """Test dry-run rehearsal."""
        await orchestrator.register_key(sample_key)
        
        result = await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            strategy=RotationStrategy.SHADOW_ROTATION,
            source_key_id="test-key-001",
            auto_rollback=True,
            dry_run=True,
        )
        
        assert result.success
        assert result.status == RehearsalStatus.COMPLETED
        assert result.rows_processed == 100
        assert result.pre_validation is not None
        assert result.post_validation is not None
    
    @pytest.mark.asyncio
    async def test_rehearsal_with_rollback(self, orchestrator):
        """Test rehearsal with automatic rollback."""
        result = await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            strategy=RotationStrategy.ONLINE_ROTATION,
            auto_rollback=True,
            dry_run=False,
        )
        
        assert result.rollback_performed
        assert result.status in (RehearsalStatus.ROLLED_BACK, RehearsalStatus.COMPLETED)
    
    @pytest.mark.asyncio
    async def test_rehearsal_without_rollback(self, orchestrator):
        """Test rehearsal without rollback."""
        result = await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            strategy=RotationStrategy.SHADOW_ROTATION,
            auto_rollback=False,
            dry_run=True,
        )
        
        assert not result.rollback_performed
        assert result.status == RehearsalStatus.COMPLETED
    
    @pytest.mark.asyncio
    async def test_rehearsal_history(self, orchestrator):
        """Test rehearsal history tracking."""
        # Run a rehearsal
        await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            strategy=RotationStrategy.SHADOW_ROTATION,
            dry_run=True,
        )
        
        # Get history
        history = await orchestrator.get_rehearsal_history()
        assert len(history) >= 1
        assert history[0]["table_name"] == "test_encrypted_data"
    
    @pytest.mark.asyncio
    async def test_get_statistics(self, orchestrator):
        """Test getting statistics."""
        stats = await orchestrator.get_statistics()
        
        assert "total_rehearsals" in stats
        assert "success_rate" in stats
        assert "active_keys" in stats
    
    @pytest.mark.asyncio
    async def test_configure_schedule(self, orchestrator):
        """Test schedule configuration."""
        schedule = RehearsalSchedule(
            enabled=True,
            frequency_days=60,
            preferred_hour=4,
        )
        
        orchestrator.configure_schedule(schedule)
        
        retrieved = orchestrator.get_schedule()
        assert retrieved.enabled
        assert retrieved.frequency_days == 60
        assert retrieved.preferred_hour == 4
    
    @pytest.mark.asyncio
    async def test_rehearsal_callback(self, orchestrator):
        """Test rehearsal callback registration."""
        callback_called = False
        
        def callback(result):
            nonlocal callback_called
            callback_called = True
        
        orchestrator.register_rehearsal_callback(callback)
        
        await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            dry_run=True,
        )
        
        assert callback_called
    
    @pytest.mark.asyncio
    async def test_data_validation(self, orchestrator):
        """Test data validation."""
        validation = await orchestrator._validate_data(
            "test_encrypted_data",
            "encrypted_value",
            "test"
        )
        
        assert validation.rows_checked == 100
        assert validation.rows_valid == 100
        assert validation.is_valid
    
    @pytest.mark.asyncio
    async def test_all_rotation_strategies(self, orchestrator):
        """Test all rotation strategies."""
        strategies = [
            RotationStrategy.SHADOW_ROTATION,
            RotationStrategy.ONLINE_ROTATION,
            RotationStrategy.OFFLINE_ROTATION,
            RotationStrategy.BATCH_ROTATION,
            RotationStrategy.ROLLING_ROTATION,
        ]
        
        for strategy in strategies:
            result = await orchestrator.run_rehearsal(
                table_name="test_encrypted_data",
                column_name="encrypted_value",
                strategy=strategy,
                dry_run=True,
            )
            
            assert result.success, f"Strategy {strategy.value} failed"
            assert result.status in (RehearsalStatus.COMPLETED, RehearsalStatus.ROLLED_BACK)


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.mark.asyncio
    async def test_empty_table_rehearsal(self, async_engine):
        """Test rehearsal on empty table."""
        orch = KeyRotationRehearsalOrchestrator(async_engine)
        
        async with async_engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE empty_table (
                    id INTEGER PRIMARY KEY,
                    encrypted_value TEXT
                )
            """))
            
            # Create history table for initialize
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS key_rotation_rehearsal_history (
                    id INTEGER PRIMARY KEY,
                    rehearsal_id TEXT UNIQUE NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    source_key_id TEXT,
                    target_key_id TEXT,
                    total_rows INTEGER DEFAULT 0,
                    rows_processed INTEGER DEFAULT 0,
                    rows_failed INTEGER DEFAULT 0,
                    pre_validation TEXT,
                    post_validation TEXT,
                    rotation_duration_ms INTEGER,
                    validation_duration_ms INTEGER,
                    rollback_duration_ms INTEGER,
                    rollback_performed BOOLEAN DEFAULT 0,
                    errors TEXT,
                    result_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS encryption_keys (
                    id INTEGER PRIMARY KEY,
                    key_id TEXT UNIQUE NOT NULL,
                    key_version INTEGER DEFAULT 1,
                    key_status TEXT DEFAULT 'active',
                    algorithm TEXT DEFAULT 'AES-256-GCM',
                    key_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rotated_at TIMESTAMP,
                    retired_at TIMESTAMP
                )
            """))
        
        await orch.initialize()
        
        result = await orch.run_rehearsal(
            table_name="empty_table",
            column_name="encrypted_value",
            dry_run=True,
        )
        
        assert result.success
        assert result.total_rows == 0
    
    @pytest.mark.asyncio
    async def test_rehearsal_on_nonexistent_table(self, orchestrator):
        """Test rehearsal on non-existent table."""
        result = await orchestrator.run_rehearsal(
            table_name="nonexistent_table_xyz",
            column_name="encrypted_value",
            dry_run=True,
        )
        
        assert not result.success
        assert result.status == RehearsalStatus.FAILED
    
    @pytest.mark.asyncio
    async def test_scheduled_rehearsal_skip(self, orchestrator):
        """Test scheduled rehearsal skipping when not due."""
        # Disable schedule
        schedule = RehearsalSchedule(enabled=False)
        orchestrator.configure_schedule(schedule)
        
        result = await orchestrator.run_scheduled_rehearsal()
        assert result is None
    
    @pytest.mark.asyncio
    async def test_key_version_increment(self, orchestrator):
        """Test key version auto-increment."""
        key1 = EncryptionKey(
            key_id="rotating-key",
            key_version=1,
            key_status=KeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
        )
        await orchestrator.register_key(key1)
        
        # Run rehearsal
        result = await orchestrator.run_rehearsal(
            table_name="test_encrypted_data",
            column_name="encrypted_value",
            source_key_id="rotating-key",
            dry_run=True,
        )
        
        assert result.target_key is not None
        assert result.target_key.key_version == 2


class TestIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.mark.asyncio
    async def test_full_key_lifecycle(self, async_engine):
        """Test complete key lifecycle with rotation."""
        orch = KeyRotationRehearsalOrchestrator(async_engine)
        
        async with async_engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_data (
                    id INTEGER PRIMARY KEY,
                    encrypted_value TEXT
                )
            """))
            for i in range(50):
                await conn.execute(text("""
                    INSERT INTO test_data (encrypted_value) VALUES (:val)
                """), {"val": f"data_{i}"})
            
            # Create history and keys tables
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS key_rotation_rehearsal_history (
                    id INTEGER PRIMARY KEY,
                    rehearsal_id TEXT UNIQUE NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    source_key_id TEXT,
                    target_key_id TEXT,
                    total_rows INTEGER DEFAULT 0,
                    rows_processed INTEGER DEFAULT 0,
                    rows_failed INTEGER DEFAULT 0,
                    pre_validation TEXT,
                    post_validation TEXT,
                    rotation_duration_ms INTEGER,
                    validation_duration_ms INTEGER,
                    rollback_duration_ms INTEGER,
                    rollback_performed BOOLEAN DEFAULT 0,
                    errors TEXT,
                    result_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS encryption_keys (
                    id INTEGER PRIMARY KEY,
                    key_id TEXT UNIQUE NOT NULL,
                    key_version INTEGER DEFAULT 1,
                    key_status TEXT DEFAULT 'active',
                    algorithm TEXT DEFAULT 'AES-256-GCM',
                    key_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rotated_at TIMESTAMP,
                    retired_at TIMESTAMP
                )
            """))
        
        await orch.initialize()
        
        # 1. Create initial key
        initial_key = EncryptionKey(
            key_id="lifecycle-key",
            key_version=1,
            key_status=KeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
        )
        await orch.register_key(initial_key)
        
        # 2. Run multiple rehearsals
        for i in range(3):
            result = await orch.run_rehearsal(
                table_name="test_data",
                column_name="encrypted_value",
                strategy=RotationStrategy.SHADOW_ROTATION,
                source_key_id="lifecycle-key",
                dry_run=True,
            )
            assert result.success
        
        # 3. Verify history
        history = await orch.get_rehearsal_history(table_name="test_data")
        assert len(history) == 3
        
        # 4. Check statistics
        stats = await orch.get_statistics()
        assert stats["total_rehearsals"] == 3
        assert stats["success_rate"] == 100.0
    
    @pytest.mark.asyncio
    async def test_batch_rotation(self, async_engine):
        """Test batch rotation on multiple tables."""
        orch = KeyRotationRehearsalOrchestrator(async_engine)
        
        async with async_engine.begin() as conn:
            for table in ["table_a", "table_b", "table_c"]:
                await conn.execute(text(f"""
                    CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY,
                        encrypted_value TEXT
                    )
                """))
                for i in range(20):
                    await conn.execute(text(f"""
                        INSERT INTO {table} (encrypted_value) VALUES (:val)
                    """), {"val": f"{table}_data_{i}"})
            
            # Create history and keys tables
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS key_rotation_rehearsal_history (
                    id INTEGER PRIMARY KEY,
                    rehearsal_id TEXT UNIQUE NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    source_key_id TEXT,
                    target_key_id TEXT,
                    total_rows INTEGER DEFAULT 0,
                    rows_processed INTEGER DEFAULT 0,
                    rows_failed INTEGER DEFAULT 0,
                    pre_validation TEXT,
                    post_validation TEXT,
                    rotation_duration_ms INTEGER,
                    validation_duration_ms INTEGER,
                    rollback_duration_ms INTEGER,
                    rollback_performed BOOLEAN DEFAULT 0,
                    errors TEXT,
                    result_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS encryption_keys (
                    id INTEGER PRIMARY KEY,
                    key_id TEXT UNIQUE NOT NULL,
                    key_version INTEGER DEFAULT 1,
                    key_status TEXT DEFAULT 'active',
                    algorithm TEXT DEFAULT 'AES-256-GCM',
                    key_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rotated_at TIMESTAMP,
                    retired_at TIMESTAMP
                )
            """))
        
        await orch.initialize()
        
        # Run rehearsals on all tables
        for table in ["table_a", "table_b", "table_c"]:
            result = await orch.run_rehearsal(
                table_name=table,
                column_name="encrypted_value",
                strategy=RotationStrategy.BATCH_ROTATION,
                dry_run=True,
            )
            assert result.success
            assert result.total_rows == 20


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
