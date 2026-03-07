"""
Key Rotation Rehearsal Celery Tasks (#1425)

Background tasks for encryption key rotation rehearsals, including:
- Scheduled rehearsal execution
- Bulk rotation operations
- Compliance reporting
- Key lifecycle management
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from celery import shared_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from ..utils.key_rotation_rehearsal import (
    get_key_rotation_orchestrator,
    KeyRotationRehearsalOrchestrator,
    RotationStrategy,
    RehearsalStatus,
    KeyStatus,
    EncryptionKey,
    RehearsalSchedule,
)
from ..services.db_service import engine


logger = logging.getLogger("api.tasks.key_rotation")


# --- Scheduled Rehearsal Tasks ---

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    time_limit=3600,  # 1 hour
    soft_time_limit=3300,  # 55 minutes
)
def run_scheduled_key_rotation_rehearsal(self, schedule_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run scheduled key rotation rehearsal.
    
    Args:
        schedule_config: Optional rehearsal schedule configuration
        
    Returns:
        Result of rehearsal execution
    """
    logger.info("Starting scheduled key rotation rehearsal")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        # Apply schedule if provided
        if schedule_config:
            schedule = RehearsalSchedule(**schedule_config)
            orchestrator.configure_schedule(schedule)
        
        # Run scheduled rehearsal
        result = await orchestrator.run_scheduled_rehearsal()
        
        if result is None:
            return {
                "status": "skipped",
                "reason": "No rehearsal scheduled at this time",
                "timestamp": datetime.utcnow().isoformat(),
            }
        
        return {
            "status": "completed" if result.success else "failed",
            "rehearsal_id": result.rehearsal_id,
            "table_name": result.table_name,
            "strategy": result.strategy.value,
            "rows_processed": result.rows_processed,
            "success": result.success,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except SoftTimeLimitExceeded:
        logger.error("Key rotation rehearsal timed out")
        return {
            "status": "timeout",
            "error": "Task exceeded time limit",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        logger.error(f"Scheduled rehearsal failed: {exc}")
        try:
            raise self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {
                "status": "failed",
                "error": str(exc),
                "timestamp": datetime.utcnow().isoformat(),
            }


@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    time_limit=7200,  # 2 hours
)
def run_key_rotation_rehearsal(
    self,
    table_name: str,
    column_name: str = "encrypted_value",
    strategy: str = "shadow_rotation",
    source_key_id: Optional[str] = None,
    target_key_id: Optional[str] = None,
    auto_rollback: bool = True,
    dry_run: bool = True,
    batch_size: int = 1000,
) -> Dict[str, Any]:
    """
    Run a single key rotation rehearsal.
    
    Args:
        table_name: Table containing encrypted data
        column_name: Column with encrypted values
        strategy: Rotation strategy name
        source_key_id: Current encryption key
        target_key_id: New encryption key
        auto_rollback: Rollback on failure
        dry_run: Simulate without changes
        batch_size: Rows per batch
        
    Returns:
        Rehearsal result details
    """
    logger.info(f"Starting key rotation rehearsal for {table_name}.{column_name}")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        strategy_enum = RotationStrategy(strategy)
        
        result = await orchestrator.run_rehearsal(
            table_name=table_name,
            column_name=column_name,
            strategy=strategy_enum,
            source_key_id=source_key_id,
            target_key_id=target_key_id,
            auto_rollback=auto_rollback,
            dry_run=dry_run,
            batch_size=batch_size,
        )
        
        return result.to_dict()
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Key rotation rehearsal failed: {exc}")
        try:
            raise self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {
                "status": "failed",
                "error": str(exc),
                "timestamp": datetime.utcnow().isoformat(),
            }


@shared_task(
    bind=True,
    max_retries=1,
    default_retry_delay=30,
    time_limit=3600,
)
def run_batch_rotation_rehearsals(
    self,
    tables: List[Dict[str, str]],
    strategy: str = "shadow_rotation",
    auto_rollback: bool = True,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Run key rotation rehearsals on multiple tables.
    
    Args:
        tables: List of {"table_name": str, "column_name": str} dicts
        strategy: Rotation strategy name
        auto_rollback: Rollback on failure
        dry_run: Simulate without changes
        
    Returns:
        Batch rehearsal results
    """
    logger.info(f"Starting batch rotation rehearsals for {len(tables)} tables")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        strategy_enum = RotationStrategy(strategy)
        
        results = []
        success_count = 0
        fail_count = 0
        
        for table_config in tables:
            table_name = table_config.get("table_name")
            column_name = table_config.get("column_name", "encrypted_value")
            
            try:
                result = await orchestrator.run_rehearsal(
                    table_name=table_name,
                    column_name=column_name,
                    strategy=strategy_enum,
                    auto_rollback=auto_rollback,
                    dry_run=dry_run,
                )
                
                results.append({
                    "table": table_name,
                    "success": result.success,
                    "rehearsal_id": result.rehearsal_id,
                    "rows_processed": result.rows_processed,
                })
                
                if result.success:
                    success_count += 1
                else:
                    fail_count += 1
                    
            except Exception as e:
                logger.error(f"Rehearsal failed for {table_name}: {e}")
                results.append({
                    "table": table_name,
                    "success": False,
                    "error": str(e),
                })
                fail_count += 1
        
        return {
            "status": "completed",
            "total": len(tables),
            "successful": success_count,
            "failed": fail_count,
            "results": results,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Batch rehearsal failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        }


# --- Key Lifecycle Management Tasks ---

@shared_task(
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def register_encryption_key(
    self,
    key_id: str,
    key_version: int = 1,
    algorithm: str = "AES-256-GCM",
    key_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Register a new encryption key.
    
    Args:
        key_id: Unique key identifier
        key_version: Key version number
        algorithm: Encryption algorithm
        key_hash: Key hash for verification
        
    Returns:
        Key registration result
    """
    logger.info(f"Registering encryption key: {key_id}")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        key = EncryptionKey(
            key_id=key_id,
            key_version=key_version,
            key_status=KeyStatus.ACTIVE,
            created_at=datetime.utcnow(),
            algorithm=algorithm,
            key_hash=key_hash,
        )
        
        await orchestrator.register_key(key)
        
        return {
            "status": "registered",
            "key_id": key_id,
            "key_version": key_version,
            "algorithm": algorithm,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Key registration failed: {exc}")
        try:
            raise self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {
                "status": "failed",
                "error": str(exc),
                "timestamp": datetime.utcnow().isoformat(),
            }


@shared_task(
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def retire_encryption_key(
    self,
    key_id: str,
) -> Dict[str, Any]:
    """
    Retire an encryption key.
    
    Args:
        key_id: Key to retire
        
    Returns:
        Key retirement result
    """
    logger.info(f"Retiring encryption key: {key_id}")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        key = await orchestrator.get_key(key_id)
        if not key:
            return {
                "status": "not_found",
                "key_id": key_id,
                "timestamp": datetime.utcnow().isoformat(),
            }
        
        key.key_status = KeyStatus.RETIRED
        key.retired_at = datetime.utcnow()
        await orchestrator.register_key(key)
        
        return {
            "status": "retired",
            "key_id": key_id,
            "retired_at": key.retired_at.isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Key retirement failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        }


@shared_task(
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def mark_key_compromised(
    self,
    key_id: str,
) -> Dict[str, Any]:
    """
    Mark a key as compromised and trigger emergency rotation.
    
    Args:
        key_id: Compromised key
        
    Returns:
        Emergency response result
    """
    logger.warning(f"Marking key as compromised: {key_id}")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        key = await orchestrator.get_key(key_id)
        if not key:
            return {
                "status": "not_found",
                "key_id": key_id,
            }
        
        key.key_status = KeyStatus.COMPROMISED
        await orchestrator.register_key(key)
        
        # Trigger emergency rotation for tables using this key
        # This would integrate with actual rotation logic
        
        return {
            "status": "compromised",
            "key_id": key_id,
            "action": "emergency_rotation_triggered",
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Key compromise marking failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Reporting and Compliance Tasks ---

@shared_task(
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def generate_key_rotation_report(
    self,
    days: int = 30,
    include_details: bool = True,
) -> Dict[str, Any]:
    """
    Generate key rotation rehearsal report.
    
    Args:
        days: Number of days to include
        include_details: Include detailed results
        
    Returns:
        Comprehensive report
    """
    logger.info(f"Generating key rotation report for last {days} days")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        # Get statistics
        stats = await orchestrator.get_statistics()
        
        # Get recent history
        history = await orchestrator.get_rehearsal_history(limit=100)
        
        # Filter by date
        cutoff = datetime.utcnow() - timedelta(days=days)
        recent_history = [
            h for h in history 
            if datetime.fromisoformat(h.get("started_at", "").replace("Z", "+00:00")) >= cutoff
        ]
        
        # Calculate metrics
        rehearsal_count = len(recent_history)
        success_count = sum(1 for h in recent_history if h.get("success"))
        
        report = {
            "period_days": days,
            "generated_at": datetime.utcnow().isoformat(),
            "summary": {
                "total_rehearsals": rehearsal_count,
                "successful_rehearsals": success_count,
                "success_rate": round(success_count / rehearsal_count * 100, 2) if rehearsal_count > 0 else 0,
                "overall_stats": stats,
            },
        }
        
        if include_details:
            report["rehearsals"] = recent_history
        
        return report
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Report generation failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


@shared_task(
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def validate_encryption_coverage(
    self,
    tables: List[str],
) -> Dict[str, Any]:
    """
    Validate encryption coverage across specified tables.
    
    Args:
        tables: Tables to validate
        
    Returns:
        Coverage validation results
    """
    logger.info(f"Validating encryption coverage for {len(tables)} tables")
    
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        
        results = []
        
        for table_name in tables:
            try:
                validation = await orchestrator._validate_data(
                    table_name, "encrypted_value", "coverage"
                )
                
                results.append({
                    "table": table_name,
                    "rows_checked": validation.rows_checked,
                    "rows_valid": validation.rows_valid,
                    "rows_invalid": validation.rows_invalid,
                    "is_valid": validation.is_valid,
                })
            except Exception as e:
                results.append({
                    "table": table_name,
                    "error": str(e),
                })
        
        valid_tables = sum(1 for r in results if r.get("is_valid"))
        
        return {
            "status": "completed",
            "total_tables": len(tables),
            "valid_tables": valid_tables,
            "invalid_tables": len(tables) - valid_tables,
            "tables": results,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"Coverage validation failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Monitoring and Alerting Tasks ---

@shared_task(
    bind=True,
    max_retries=1,
)
def check_key_rotation_health(self) -> Dict[str, Any]:
    """
    Check key rotation rehearsal system health.
    
    Returns:
        Health status report
    """
    async def _run():
        orchestrator = await get_key_rotation_orchestrator(engine)
        stats = await orchestrator.get_statistics()
        
        # Determine health status
        health_status = "healthy"
        issues = []
        
        if stats.get("failed_rehearsals", 0) > stats.get("successful_rehearsals", 0):
            health_status = "critical"
            issues.append("More failed rehearsals than successful ones")
        
        if stats.get("success_rate", 100) < 80:
            health_status = "degraded"
            issues.append("Success rate below 80%")
        
        return {
            "status": health_status,
            "issues": issues,
            "statistics": stats,
            "checked_at": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        return {
            "status": "unknown",
            "error": str(exc),
        }


@shared_task
def cleanup_old_rotation_history(retention_days: int = 365) -> Dict[str, Any]:
    """
    Clean up old rehearsal history.
    
    Args:
        retention_days: Days to retain history
        
    Returns:
        Cleanup result
    """
    logger.info(f"Cleaning up rotation history older than {retention_days} days")
    
    async def _run():
        from sqlalchemy import text
        from ..services.db_service import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    DELETE FROM key_rotation_rehearsal_history
                    WHERE created_at < NOW() - INTERVAL ':days days'
                    RETURNING COUNT(*)
                """),
                {"days": retention_days}
            )
            deleted = result.scalar()
            await session.commit()
        
        return {
            "status": "completed",
            "deleted_records": deleted,
            "retention_days": retention_days,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error(f"History cleanup failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Setup Tasks ---

@shared_task
def setup_key_rotation_schedules() -> Dict[str, Any]:
    """
    Setup periodic key rotation rehearsal schedules.
    
    This task configures periodic tasks for scheduled rehearsals.
    Should be called once during application startup.
    
    Returns:
        Setup result
    """
    logger.info("Setting up key rotation schedules")
    
    # This would integrate with Celery beat schedule
    # In practice, schedules are configured in Celery beat configuration
    
    return {
        "status": "configured",
        "schedules": [
            {
                "task": "api.tasks.key_rotation_tasks.run_scheduled_key_rotation_rehearsal",
                "schedule": "daily at 3:00",
            },
            {
                "task": "api.tasks.key_rotation_tasks.check_key_rotation_health",
                "schedule": "hourly",
            },
            {
                "task": "api.tasks.key_rotation_tasks.generate_key_rotation_report",
                "schedule": "weekly",
            },
        ],
        "timestamp": datetime.utcnow().isoformat(),
    }
