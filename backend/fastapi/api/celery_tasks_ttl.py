"""
Celery Background Tasks for TTL Archival (#1413)

Provides automated background execution of TTL policies for
data retention management.
"""

import asyncio
import logging
from typing import Dict, Any

from .celery_app import celery_app

logger = logging.getLogger("api.celery_tasks.ttl")


@celery_app.task(bind=True, max_retries=3, default_retry_delay=300)
def run_ttl_archival_task(self, table_name: str, dry_run: bool = False) -> Dict[str, Any]:
    """
    Run TTL archival for a specific table.
    
    Args:
        table_name: Name of the table to process
        dry_run: If True, only simulate operations
        
    Returns:
        Dictionary with archival statistics
    """
    async def _execute():
        from .utils.ttl_partition_manager import get_ttl_manager
        
        manager = await get_ttl_manager()
        policy = await manager.get_policy(table_name)
        
        if not policy:
            raise ValueError(f"No TTL policy found for table: {table_name}")
        
        # Override dry_run setting
        policy.dry_run = dry_run
        
        stats = await manager.archive_expired_data(policy)
        return stats.to_dict()
    
    try:
        return asyncio.run(_execute())
    except Exception as exc:
        logger.error(f"TTL archival failed for {table_name}: {exc}")
        raise self.retry(exc=exc)


@celery_app.task(bind=True, max_retries=2, default_retry_delay=600)
def run_all_ttl_policies_task(self, dry_run: bool = False) -> Dict[str, Any]:
    """
    Run all active TTL policies.
    
    This task is designed to run on a schedule (e.g., daily) to
    automatically enforce data retention policies.
    
    Args:
        dry_run: If True, only simulate operations
        
    Returns:
        Dictionary with results for each table
    """
    async def _execute():
        from .utils.ttl_partition_manager import get_ttl_manager
        
        manager = await get_ttl_manager()
        
        # Set all policies to dry_run if specified
        if dry_run:
            policies = await manager.list_policies()
            for policy in policies:
                policy.dry_run = True
        
        results = await manager.run_all_policies()
        
        # Convert to serializable format
        return {
            table_name: stats.to_dict()
            for table_name, stats in results.items()
        }
    
    try:
        results = asyncio.run(_execute())
        
        # Log summary
        total_archived = sum(r.get("rows_archived", 0) for r in results.values())
        total_deleted = sum(r.get("rows_deleted", 0) for r in results.values())
        
        logger.info(
            f"TTL batch archival completed: "
            f"tables={len(results)}, "
            f"archived={total_archived}, "
            f"deleted={total_deleted}"
        )
        
        return {
            "success": True,
            "tables_processed": len(results),
            "total_archived": total_archived,
            "total_deleted": total_deleted,
            "details": results,
        }
        
    except Exception as exc:
        logger.error(f"TTL batch archival failed: {exc}")
        raise self.retry(exc=exc)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def cleanup_archived_data_task(self, table_name: str, older_than_days: int) -> Dict[str, Any]:
    """
    Clean up old archived data from archive tables.
    
    This helps manage the size of archive tables by removing
    very old data that is no longer needed.
    
    Args:
        table_name: Source table name
        older_than_days: Remove archive data older than this many days
        
    Returns:
        Dictionary with cleanup statistics
    """
    async def _execute():
        from sqlalchemy import text
        from .services.db_service import AsyncSessionLocal
        
        archive_table = f"{table_name}_archive"
        cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)
        
        async with AsyncSessionLocal() as session:
            # Count rows to delete
            result = await session.execute(
                text(f"""
                    SELECT COUNT(*) FROM {archive_table}
                    WHERE archived_at < :cutoff_date
                """),
                {"cutoff_date": cutoff_date}
            )
            count = result.scalar()
            
            if count > 0:
                # Delete old archive data
                await session.execute(
                    text(f"""
                        DELETE FROM {archive_table}
                        WHERE archived_at < :cutoff_date
                    """),
                    {"cutoff_date": cutoff_date}
                )
                await session.commit()
                
                logger.info(
                    f"Cleaned up {count} old records from {archive_table}"
                )
            
            return {
                "table": table_name,
                "archive_table": archive_table,
                "rows_deleted": count,
                "cutoff_date": cutoff_date.isoformat(),
            }
    
    try:
        return asyncio.run(_execute())
    except Exception as exc:
        logger.error(f"Archive cleanup failed for {table_name}: {exc}")
        raise self.retry(exc=exc)


# Schedule configuration (to be added to Celery beat schedule)
"""
CELERY_BEAT_SCHEDULE = {
    # ... existing schedules ...
    
    'ttl-archival-daily': {
        'task': 'api.celery_tasks_ttl.run_all_ttl_policies_task',
        'schedule': crontab(hour=2, minute=0),  # Run daily at 2 AM
        'kwargs': {'dry_run': False},
    },
    
    'ttl-cleanup-weekly': {
        'task': 'api.celery_tasks_ttl.cleanup_archived_data_task',
        'schedule': crontab(hour=3, minute=0, day_of_week='sunday'),
        'kwargs': {'table_name': 'notification_logs', 'older_than_days': 365},
    },
}
"""


# Import datetime for the cleanup task
from datetime import datetime, timedelta
