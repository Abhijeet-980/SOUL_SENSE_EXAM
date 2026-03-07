"""
Row-Level TTL Archival Partitioning Manager (#1413)

Provides time-based data partitioning and automatic archival/purging
based on configurable TTL (Time To Live) policies. This system helps
manage database growth, improve query performance, and ensure compliance
with data retention policies.

Features:
- Configurable TTL policies per table/entity type
- Automatic partition creation and management
- Background archival of expired data
- Safe rollout controls with dry-run mode
- Observability metrics and health checks
- Support for soft-delete and hard-delete strategies

Example:
    # Configure TTL policy
    policy = TTLPolicy(
        table_name="notification_logs",
        retention_days=90,
        archive_before_delete=True,
        partition_granularity="monthly"
    )
    
    # Apply TTL management
    manager = TTLPartitionManager(engine)
    await manager.apply_policy(policy)
    
    # Run archival
    stats = await manager.archive_expired_data()
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import json

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy import text, select, delete, inspect, Table, MetaData, Column, DateTime, Integer, String, Boolean, JSON, Index
from sqlalchemy.sql import func

from ..services.db_service import AsyncSessionLocal


logger = logging.getLogger("api.ttl_partitioning")


class PartitionGranularity(str, Enum):
    """Time granularity for partitioning."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class ArchiveStrategy(str, Enum):
    """Strategy for handling expired data."""
    DELETE = "delete"  # Hard delete
    SOFT_DELETE = "soft_delete"  # Mark as deleted
    ARCHIVE_THEN_DELETE = "archive_then_delete"  # Archive to cold storage, then delete
    ARCHIVE_ONLY = "archive_only"  # Archive without deleting


class PolicyStatus(str, Enum):
    """Status of a TTL policy."""
    ACTIVE = "active"
    PAUSED = "paused"
    DRY_RUN = "dry_run"
    DISABLED = "disabled"


@dataclass
class TTLPolicy:
    """
    Time-To-Live policy configuration.
    
    Attributes:
        table_name: Target database table
        retention_days: Number of days to retain data
        partition_granularity: Time granularity for partitions
        archive_strategy: How to handle expired data
        archive_table: Optional destination table for archived data
        status: Policy execution status
        dry_run: If True, only simulate operations
        batch_size: Number of rows to process per batch
        id_column: Column name for row identification
        timestamp_column: Column used for TTL calculation
        filters: Additional SQL filters for selective archiving
    """
    table_name: str
    retention_days: int
    partition_granularity: PartitionGranularity = PartitionGranularity.MONTHLY
    archive_strategy: ArchiveStrategy = ArchiveStrategy.ARCHIVE_THEN_DELETE
    archive_table: Optional[str] = None
    status: PolicyStatus = PolicyStatus.ACTIVE
    dry_run: bool = False
    batch_size: int = 1000
    id_column: str = "id"
    timestamp_column: str = "created_at"
    filters: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert policy to dictionary."""
        return {
            "table_name": self.table_name,
            "retention_days": self.retention_days,
            "partition_granularity": self.partition_granularity.value,
            "archive_strategy": self.archive_strategy.value,
            "archive_table": self.archive_table,
            "status": self.status.value,
            "dry_run": self.dry_run,
            "batch_size": self.batch_size,
            "id_column": self.id_column,
            "timestamp_column": self.timestamp_column,
            "filters": self.filters,
        }


@dataclass
class ArchivalStats:
    """Statistics from an archival operation."""
    table_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_scanned: int = 0
    rows_archived: int = 0
    rows_deleted: int = 0
    rows_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    
    @property
    def success(self) -> bool:
        """Check if archival completed without errors."""
        return len(self.errors) == 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            "table_name": self.table_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "rows_scanned": self.rows_scanned,
            "rows_archived": self.rows_archived,
            "rows_deleted": self.rows_deleted,
            "rows_skipped": self.rows_skipped,
            "errors": self.errors,
            "duration_ms": round(self.duration_ms, 2),
            "success": self.success,
        }


@dataclass
class PartitionInfo:
    """Information about a database partition."""
    name: str
    table_name: str
    start_date: datetime
    end_date: datetime
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    is_archived: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "table_name": self.table_name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "is_archived": self.is_archived,
        }


class TTLPartitionManager:
    """
    Manages row-level TTL archival and partitioning.
    
    Provides automated data lifecycle management with configurable
    retention policies, partitioning strategies, and archival options.
    
    Example:
        manager = TTLPartitionManager(engine)
        
        # Define policy
        policy = TTLPolicy(
            table_name="audit_logs",
            retention_days=365,
            archive_strategy=ArchiveStrategy.ARCHIVE_THEN_DELETE
        )
        
        # Register and apply
        await manager.register_policy(policy)
        stats = await manager.archive_expired_data(policy)
    """
    
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self._policies: Dict[str, TTLPolicy] = {}
        self._archival_callbacks: List[Callable[[ArchivalStats], None]] = []
        self._metadata = MetaData()
        
    async def initialize(self) -> None:
        """
        Initialize TTL management tables and schema.
        
        Creates the necessary tables for tracking policies and
        archival history if they don't exist.
        """
        async with self.engine.begin() as conn:
            # Check if TTL policy table exists
            await conn.run_sync(self._create_ttl_tables)
        
        logger.info("TTL Partition Manager initialized")
    
    def _create_ttl_tables(self, conn) -> None:
        """Create TTL management tables (synchronous helper)."""
        # TTL Policies table
        if not inspect(conn).has_table("ttl_policies"):
            Table(
                "ttl_policies",
                self._metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("table_name", String, unique=True, nullable=False, index=True),
                Column("retention_days", Integer, nullable=False),
                Column("partition_granularity", String, default="monthly"),
                Column("archive_strategy", String, default="archive_then_delete"),
                Column("archive_table", String, nullable=True),
                Column("status", String, default="active"),
                Column("dry_run", Boolean, default=False),
                Column("batch_size", Integer, default=1000),
                Column("id_column", String, default="id"),
                Column("timestamp_column", String, default="created_at"),
                Column("filters", JSON, nullable=True),
                Column("created_at", DateTime, default=datetime.utcnow),
                Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
                Column("last_run_at", DateTime, nullable=True),
                Column("last_run_stats", JSON, nullable=True),
            ).create(conn)
            logger.info("Created ttl_policies table")
        
        # Archival history table
        if not inspect(conn).has_table("ttl_archival_history"):
            Table(
                "ttl_archival_history",
                self._metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("table_name", String, nullable=False, index=True),
                Column("policy_id", Integer, nullable=True),
                Column("start_time", DateTime, nullable=False),
                Column("end_time", DateTime, nullable=True),
                Column("rows_scanned", Integer, default=0),
                Column("rows_archived", Integer, default=0),
                Column("rows_deleted", Integer, default=0),
                Column("rows_skipped", Integer, default=0),
                Column("errors", JSON, nullable=True),
                Column("duration_ms", Integer, default=0),
                Column("dry_run", Boolean, default=False),
            ).create(conn)
            logger.info("Created ttl_archival_history table")
    
    async def register_policy(self, policy: TTLPolicy) -> None:
        """
        Register or update a TTL policy.
        
        Args:
            policy: The TTL policy to register
        """
        self._policies[policy.table_name] = policy
        
        # Persist to database
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO ttl_policies (
                        table_name, retention_days, partition_granularity,
                        archive_strategy, archive_table, status, dry_run,
                        batch_size, id_column, timestamp_column, filters, updated_at
                    ) VALUES (
                        :table_name, :retention_days, :partition_granularity,
                        :archive_strategy, :archive_table, :status, :dry_run,
                        :batch_size, :id_column, :timestamp_column, :filters, :updated_at
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        retention_days = EXCLUDED.retention_days,
                        partition_granularity = EXCLUDED.partition_granularity,
                        archive_strategy = EXCLUDED.archive_strategy,
                        archive_table = EXCLUDED.archive_table,
                        status = EXCLUDED.status,
                        dry_run = EXCLUDED.dry_run,
                        batch_size = EXCLUDED.batch_size,
                        id_column = EXCLUDED.id_column,
                        timestamp_column = EXCLUDED.timestamp_column,
                        filters = EXCLUDED.filters,
                        updated_at = EXCLUDED.updated_at
                """),
                {
                    "table_name": policy.table_name,
                    "retention_days": policy.retention_days,
                    "partition_granularity": policy.partition_granularity.value,
                    "archive_strategy": policy.archive_strategy.value,
                    "archive_table": policy.archive_table,
                    "status": policy.status.value,
                    "dry_run": policy.dry_run,
                    "batch_size": policy.batch_size,
                    "id_column": policy.id_column,
                    "timestamp_column": policy.timestamp_column,
                    "filters": json.dumps(policy.filters) if policy.filters else None,
                    "updated_at": datetime.utcnow(),
                }
            )
            await session.commit()
        
        logger.info(f"Registered TTL policy for table: {policy.table_name}")
    
    async def get_policy(self, table_name: str) -> Optional[TTLPolicy]:
        """Get a registered policy by table name."""
        if table_name in self._policies:
            return self._policies[table_name]
        
        # Load from database
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT * FROM ttl_policies WHERE table_name = :table_name"),
                {"table_name": table_name}
            )
            row = result.fetchone()
            
            if row:
                policy = TTLPolicy(
                    table_name=row.table_name,
                    retention_days=row.retention_days,
                    partition_granularity=PartitionGranularity(row.partition_granularity),
                    archive_strategy=ArchiveStrategy(row.archive_strategy),
                    archive_table=row.archive_table,
                    status=PolicyStatus(row.status),
                    dry_run=row.dry_run,
                    batch_size=row.batch_size,
                    id_column=row.id_column,
                    timestamp_column=row.timestamp_column,
                    filters=json.loads(row.filters) if row.filters else None,
                )
                self._policies[table_name] = policy
                return policy
        
        return None
    
    async def list_policies(self) -> List[TTLPolicy]:
        """List all registered TTL policies."""
        policies = []
        
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT * FROM ttl_policies ORDER BY table_name")
            )
            rows = result.fetchall()
            
            for row in rows:
                policy = TTLPolicy(
                    table_name=row.table_name,
                    retention_days=row.retention_days,
                    partition_granularity=PartitionGranularity(row.partition_granularity),
                    archive_strategy=ArchiveStrategy(row.archive_strategy),
                    archive_table=row.archive_table,
                    status=PolicyStatus(row.status),
                    dry_run=row.dry_run,
                    batch_size=row.batch_size,
                    id_column=row.id_column,
                    timestamp_column=row.timestamp_column,
                    filters=json.loads(row.filters) if row.filters else None,
                )
                policies.append(policy)
                self._policies[row.table_name] = policy
        
        return policies
    
    async def archive_expired_data(
        self,
        policy: Optional[TTLPolicy] = None,
        table_name: Optional[str] = None
    ) -> ArchivalStats:
        """
        Archive or delete expired data based on TTL policy.
        
        Args:
            policy: Specific policy to apply (optional)
            table_name: Table to process (uses registered policy if policy not provided)
            
        Returns:
            ArchivalStats with operation results
        """
        if policy is None:
            if table_name is None:
                raise ValueError("Either policy or table_name must be provided")
            policy = await self.get_policy(table_name)
            if policy is None:
                raise ValueError(f"No policy found for table: {table_name}")
        
        # Check policy status
        if policy.status == PolicyStatus.DISABLED:
            logger.info(f"Policy for {policy.table_name} is disabled, skipping")
            return ArchivalStats(
                table_name=policy.table_name,
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow(),
                rows_skipped=0,
            )
        
        is_dry_run = policy.dry_run or policy.status == PolicyStatus.DRY_RUN
        
        stats = ArchivalStats(
            table_name=policy.table_name,
            start_time=datetime.utcnow(),
        )
        
        try:
            # Calculate cutoff date
            cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)
            
            logger.info(
                f"Starting archival for {policy.table_name}: "
                f"cutoff={cutoff_date.isoformat()}, "
                f"strategy={policy.archive_strategy.value}, "
                f"dry_run={is_dry_run}"
            )
            
            # Build filter condition
            filter_condition = f"{policy.timestamp_column} < :cutoff_date"
            params = {"cutoff_date": cutoff_date}
            
            if policy.filters:
                for key, value in policy.filters.items():
                    filter_condition += f" AND {key} = :{key}"
                    params[key] = value
            
            async with AsyncSessionLocal() as session:
                # Count rows to process
                count_result = await session.execute(
                    text(f"""
                        SELECT COUNT(*) as count 
                        FROM {policy.table_name} 
                        WHERE {filter_condition}
                    """),
                    params
                )
                stats.rows_scanned = count_result.scalar()
                
                if stats.rows_scanned == 0:
                    logger.info(f"No expired data found in {policy.table_name}")
                    stats.end_time = datetime.utcnow()
                    stats.duration_ms = (stats.end_time - stats.start_time).total_seconds() * 1000
                    return stats
                
                # Process based on strategy
                if policy.archive_strategy == ArchiveStrategy.ARCHIVE_THEN_DELETE:
                    stats = await self._archive_then_delete(
                        session, policy, filter_condition, params, stats, is_dry_run
                    )
                elif policy.archive_strategy == ArchiveStrategy.ARCHIVE_ONLY:
                    stats = await self._archive_only(
                        session, policy, filter_condition, params, stats, is_dry_run
                    )
                elif policy.archive_strategy == ArchiveStrategy.SOFT_DELETE:
                    stats = await self._soft_delete(
                        session, policy, filter_condition, params, stats, is_dry_run
                    )
                elif policy.archive_strategy == ArchiveStrategy.DELETE:
                    stats = await self._hard_delete(
                        session, policy, filter_condition, params, stats, is_dry_run
                    )
                
                if not is_dry_run:
                    await session.commit()
                else:
                    await session.rollback()
                    logger.info(f"Dry run completed for {policy.table_name}")
            
            stats.end_time = datetime.utcnow()
            stats.duration_ms = (stats.end_time - stats.start_time).total_seconds() * 1000
            
            # Record history
            await self._record_archival_history(policy, stats, is_dry_run)
            
            # Trigger callbacks
            for callback in self._archival_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(stats)
                    else:
                        callback(stats)
                except Exception as e:
                    logger.error(f"Archival callback failed: {e}")
            
            logger.info(
                f"Archival completed for {policy.table_name}: "
                f"scanned={stats.rows_scanned}, "
                f"archived={stats.rows_archived}, "
                f"deleted={stats.rows_deleted}, "
                f"duration={stats.duration_ms:.2f}ms"
            )
            
        except Exception as e:
            stats.end_time = datetime.utcnow()
            stats.duration_ms = (stats.end_time - stats.start_time).total_seconds() * 1000
            stats.errors.append(str(e))
            logger.error(f"Archival failed for {policy.table_name}: {e}")
            raise
        
        return stats
    
    async def _archive_then_delete(
        self,
        session: AsyncSession,
        policy: TTLPolicy,
        filter_condition: str,
        params: Dict[str, Any],
        stats: ArchivalStats,
        dry_run: bool
    ) -> ArchivalStats:
        """Archive data to archive table then delete from source."""
        archive_table = policy.archive_table or f"{policy.table_name}_archive"
        
        # Ensure archive table exists
        if not dry_run:
            await self._ensure_archive_table(session, policy.table_name, archive_table)
        
        # Copy to archive
        batch_count = 0
        while True:
            if dry_run:
                # Just count in dry run
                result = await session.execute(
                    text(f"""
                        SELECT COUNT(*) FROM {policy.table_name}
                        WHERE {filter_condition}
                        LIMIT {policy.batch_size}
                    """),
                    params
                )
                batch_rows = result.scalar()
                if batch_rows == 0:
                    break
                stats.rows_archived += batch_rows
                batch_count += 1
                if batch_count >= 10:  # Limit dry run
                    break
            else:
                # Insert into archive
                result = await session.execute(
                    text(f"""
                        INSERT INTO {archive_table}
                        SELECT * FROM {policy.table_name}
                        WHERE {filter_condition}
                        LIMIT {policy.batch_size}
                    """),
                    params
                )
                
                # Delete from source
                await session.execute(
                    text(f"""
                        DELETE FROM {policy.table_name}
                        WHERE {policy.id_column} IN (
                            SELECT {policy.id_column} FROM {archive_table}
                            WHERE archived_at > NOW() - INTERVAL '1 minute'
                        )
                        AND {filter_condition}
                    """),
                    params
                )
                
                batch_rows = result.rowcount
                stats.rows_archived += batch_rows
                stats.rows_deleted += batch_rows
                
                if batch_rows < policy.batch_size:
                    break
                
                # Commit batch
                await session.commit()
        
        return stats
    
    async def _archive_only(
        self,
        session: AsyncSession,
        policy: TTLPolicy,
        filter_condition: str,
        params: Dict[str, Any],
        stats: ArchivalStats,
        dry_run: bool
    ) -> ArchivalStats:
        """Archive data without deleting from source."""
        archive_table = policy.archive_table or f"{policy.table_name}_archive"
        
        if not dry_run:
            await self._ensure_archive_table(session, policy.table_name, archive_table)
        
        if dry_run:
            result = await session.execute(
                text(f"""
                    SELECT COUNT(*) FROM {policy.table_name}
                    WHERE {filter_condition}
                """),
                params
            )
            stats.rows_archived = result.scalar()
        else:
            result = await session.execute(
                text(f"""
                    INSERT INTO {archive_table}
                    SELECT * FROM {policy.table_name}
                    WHERE {filter_condition}
                """),
                params
            )
            stats.rows_archived = result.rowcount
        
        return stats
    
    async def _soft_delete(
        self,
        session: AsyncSession,
        policy: TTLPolicy,
        filter_condition: str,
        params: Dict[str, Any],
        stats: ArchivalStats,
        dry_run: bool
    ) -> ArchivalStats:
        """Mark data as deleted without removing."""
        if dry_run:
            result = await session.execute(
                text(f"""
                    SELECT COUNT(*) FROM {policy.table_name}
                    WHERE {filter_condition}
                    AND (is_deleted = FALSE OR is_deleted IS NULL)
                """),
                params
            )
            stats.rows_deleted = result.scalar()
        else:
            result = await session.execute(
                text(f"""
                    UPDATE {policy.table_name}
                    SET is_deleted = TRUE, deleted_at = NOW()
                    WHERE {filter_condition}
                    AND (is_deleted = FALSE OR is_deleted IS NULL)
                """),
                params
            )
            stats.rows_deleted = result.rowcount
        
        return stats
    
    async def _hard_delete(
        self,
        session: AsyncSession,
        policy: TTLPolicy,
        filter_condition: str,
        params: Dict[str, Any],
        stats: ArchivalStats,
        dry_run: bool
    ) -> ArchivalStats:
        """Permanently delete data."""
        batch_count = 0
        while True:
            if dry_run:
                result = await session.execute(
                    text(f"""
                        SELECT COUNT(*) FROM {policy.table_name}
                        WHERE {filter_condition}
                        LIMIT {policy.batch_size}
                    """),
                    params
                )
                batch_rows = result.scalar()
                stats.rows_deleted += batch_rows
                batch_count += 1
                if batch_count >= 10 or batch_rows == 0:
                    break
            else:
                result = await session.execute(
                    text(f"""
                        DELETE FROM {policy.table_name}
                        WHERE {policy.id_column} IN (
                            SELECT {policy.id_column} FROM {policy.table_name}
                            WHERE {filter_condition}
                            LIMIT {policy.batch_size}
                        )
                    """),
                    params
                )
                batch_rows = result.rowcount
                stats.rows_deleted += batch_rows
                
                if batch_rows < policy.batch_size:
                    break
                
                await session.commit()
        
        return stats
    
    async def _ensure_archive_table(
        self,
        session: AsyncSession,
        source_table: str,
        archive_table: str
    ) -> None:
        """Ensure archive table exists with same schema as source."""
        # Check if archive table exists
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = :archive_table
                )
            """),
            {"archive_table": archive_table}
        )
        
        if result.scalar():
            return
        
        # Create archive table with same structure
        await session.execute(
            text(f"""
                CREATE TABLE {archive_table} (LIKE {source_table} INCLUDING ALL)
            """)
        )
        
        # Add archival timestamp
        await session.execute(
            text(f"""
                ALTER TABLE {archive_table} 
                ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT NOW()
            """)
        )
        
        logger.info(f"Created archive table: {archive_table}")
    
    async def _record_archival_history(
        self,
        policy: TTLPolicy,
        stats: ArchivalStats,
        dry_run: bool
    ) -> None:
        """Record archival operation in history."""
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO ttl_archival_history (
                        table_name, start_time, end_time, rows_scanned,
                        rows_archived, rows_deleted, rows_skipped, errors,
                        duration_ms, dry_run
                    ) VALUES (
                        :table_name, :start_time, :end_time, :rows_scanned,
                        :rows_archived, :rows_deleted, :rows_skipped, :errors,
                        :duration_ms, :dry_run
                    )
                """),
                {
                    "table_name": stats.table_name,
                    "start_time": stats.start_time,
                    "end_time": stats.end_time,
                    "rows_scanned": stats.rows_scanned,
                    "rows_archived": stats.rows_archived,
                    "rows_deleted": stats.rows_deleted,
                    "rows_skipped": stats.rows_skipped,
                    "errors": json.dumps(stats.errors) if stats.errors else None,
                    "duration_ms": int(stats.duration_ms),
                    "dry_run": dry_run,
                }
            )
            
            # Update policy last run
            await session.execute(
                text("""
                    UPDATE ttl_policies
                    SET last_run_at = NOW(),
                        last_run_stats = :stats
                    WHERE table_name = :table_name
                """),
                {
                    "table_name": policy.table_name,
                    "stats": json.dumps(stats.to_dict()),
                }
            )
            
            await session.commit()
    
    async def get_partitions(self, table_name: str) -> List[PartitionInfo]:
        """
        Get partition information for a table.
        
        Note: This is a simplified implementation. Full partition support
        would require database-specific implementations.
        """
        partitions = []
        
        async with AsyncSessionLocal() as session:
            # Query for date-based data distribution
            result = await session.execute(
                text(f"""
                    SELECT 
                        DATE_TRUNC('month', created_at) as partition_date,
                        COUNT(*) as row_count
                    FROM {table_name}
                    GROUP BY DATE_TRUNC('month', created_at)
                    ORDER BY partition_date DESC
                    LIMIT 24
                """)
            )
            
            for row in result:
                partition_date = row.partition_date
                partitions.append(PartitionInfo(
                    name=f"{table_name}_{partition_date.strftime('%Y_%m')}",
                    table_name=table_name,
                    start_date=partition_date,
                    end_date=(partition_date + timedelta(days=32)).replace(day=1),
                    row_count=row.row_count,
                ))
        
        return partitions
    
    async def get_archival_history(
        self,
        table_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get archival operation history."""
        async with AsyncSessionLocal() as session:
            if table_name:
                result = await session.execute(
                    text("""
                        SELECT * FROM ttl_archival_history
                        WHERE table_name = :table_name
                        ORDER BY start_time DESC
                        LIMIT :limit
                    """),
                    {"table_name": table_name, "limit": limit}
                )
            else:
                result = await session.execute(
                    text("""
                        SELECT * FROM ttl_archival_history
                        ORDER BY start_time DESC
                        LIMIT :limit
                    """),
                    {"limit": limit}
                )
            
            history = []
            for row in result:
                history.append({
                    "id": row.id,
                    "table_name": row.table_name,
                    "start_time": row.start_time.isoformat() if row.start_time else None,
                    "end_time": row.end_time.isoformat() if row.end_time else None,
                    "rows_scanned": row.rows_scanned,
                    "rows_archived": row.rows_archived,
                    "rows_deleted": row.rows_deleted,
                    "duration_ms": row.duration_ms,
                    "dry_run": row.dry_run,
                })
            
            return history
    
    def register_archival_callback(
        self,
        callback: Callable[[ArchivalStats], None]
    ) -> None:
        """Register a callback for archival completion."""
        self._archival_callbacks.append(callback)
    
    async def run_all_policies(self) -> Dict[str, ArchivalStats]:
        """Run all active policies."""
        policies = await self.list_policies()
        results = {}
        
        for policy in policies:
            if policy.status in (PolicyStatus.ACTIVE, PolicyStatus.DRY_RUN):
                try:
                    stats = await self.archive_expired_data(policy)
                    results[policy.table_name] = stats
                except Exception as e:
                    logger.error(f"Failed to run policy for {policy.table_name}: {e}")
                    results[policy.table_name] = ArchivalStats(
                        table_name=policy.table_name,
                        start_time=datetime.utcnow(),
                        end_time=datetime.utcnow(),
                        errors=[str(e)],
                    )
        
        return results
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get overall TTL management statistics."""
        async with AsyncSessionLocal() as session:
            # Policy count
            result = await session.execute(
                text("SELECT COUNT(*) FROM ttl_policies")
            )
            policy_count = result.scalar()
            
            # Active policy count
            result = await session.execute(
                text("SELECT COUNT(*) FROM ttl_policies WHERE status = 'active'")
            )
            active_count = result.scalar()
            
            # Total rows archived
            result = await session.execute(
                text("SELECT COALESCE(SUM(rows_archived), 0) FROM ttl_archival_history")
            )
            total_archived = result.scalar()
            
            # Total rows deleted
            result = await session.execute(
                text("SELECT COALESCE(SUM(rows_deleted), 0) FROM ttl_archival_history")
            )
            total_deleted = result.scalar()
            
            # Recent runs
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM ttl_archival_history
                    WHERE start_time > NOW() - INTERVAL '24 hours'
                """)
            )
            recent_runs = result.scalar()
            
            return {
                "policy_count": policy_count,
                "active_policies": active_count,
                "total_rows_archived": total_archived,
                "total_rows_deleted": total_deleted,
                "runs_last_24h": recent_runs,
            }


# Global instance
_ttl_manager: Optional[TTLPartitionManager] = None


async def get_ttl_manager(engine: Optional[AsyncEngine] = None) -> TTLPartitionManager:
    """Get or create the global TTL partition manager."""
    global _ttl_manager
    
    if _ttl_manager is None:
        if engine is None:
            from ..services.db_service import engine
        _ttl_manager = TTLPartitionManager(engine)
        await _ttl_manager.initialize()
    
    return _ttl_manager
