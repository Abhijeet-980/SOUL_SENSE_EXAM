"""
Background Task Service - Manages asynchronous job execution and tracking.

This service provides:
- Task creation and tracking with database persistence
- Automatic status updates (pending, processing, completed, failed)
- Integration with FastAPI's BackgroundTasks for lightweight operations
- Task failure handling with error capture
- Task status polling support

For heavier workloads (>60 seconds), Celery with Redis can be integrated,
but this implementation uses FastAPI's BackgroundTasks for simplicity.
"""

import uuid
import logging
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple
from functools import wraps

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models import BackgroundJob, User
from ..services.db_service import SessionLocal

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Task execution status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskType(str, Enum):
    """Types of background tasks."""
    EXPORT_PDF = "export_pdf"
    EXPORT_CSV = "export_csv"
    EXPORT_JSON = "export_json"
    EXPORT_XML = "export_xml"
    EXPORT_HTML = "export_html"
    SEND_EMAIL = "send_email"
    DATA_ANALYSIS = "data_analysis"
    REPORT_GENERATION = "report_generation"


class BackgroundTaskService:
    """
    Service for managing background task execution and tracking.
    
    Usage:
        # In a router:
        @router.post("/export")
        async def trigger_export(
            background_tasks: BackgroundTasks,
            current_user: User = Depends(get_current_user),
            db: Session = Depends(get_db)
        ):
            task = BackgroundTaskService.create_task(
                db=db,
                user_id=current_user.id,
                task_type=TaskType.EXPORT_PDF,
                params={"format": "pdf", "include_charts": True}
            )
            
            background_tasks.add_task(
                BackgroundTaskService.execute_task,
                task.job_id,
                generate_pdf_report,
                db,
                current_user
            )
            
            return {"status": "processing", "job_id": task.job_id}
    """

    @staticmethod
    def create_task(
        db: Session,
        user_id: int,
        task_type: TaskType,
        params: Optional[Dict[str, Any]] = None
    ) -> "BackgroundJob":
        """
        Create a new background task and persist to database.
        
        Args:
            db: Database session
            user_id: ID of the user who initiated the task
            task_type: Type of task being created
            params: Optional parameters for the task (stored as JSON)
            
        Returns:
            BackgroundJob object with generated job_id
        """
        import json
        
        job_id = str(uuid.uuid4())
        
        job = BackgroundJob(
            job_id=job_id,
            user_id=user_id,
            task_type=task_type.value,
            status=TaskStatus.PENDING.value,
            params=json.dumps(params) if params else None,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(job)
        db.commit()
        db.refresh(job)
        
        logger.info(f"Created background task {job_id} of type {task_type.value} for user {user_id}")
        
        return job

    @staticmethod
    def update_task_status(
        db: Session,
        job_id: str,
        status: TaskStatus,
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        progress: Optional[int] = None
    ) -> Optional["BackgroundJob"]:
        """
        Update the status of an existing task.
        
        Args:
            db: Database session
            job_id: Unique job identifier
            status: New status
            result: Optional result data (for completed tasks)
            error_message: Optional error message (for failed tasks)
            progress: Optional progress percentage (0-100)
            
        Returns:
            Updated BackgroundJob or None if not found
        """
        import json
        
        job = db.query(BackgroundJob).filter(BackgroundJob.job_id == job_id).first()
        
        if not job:
            logger.warning(f"Job {job_id} not found for status update")
            return None
        
        job.status = status.value
        job.updated_at = datetime.utcnow()
        
        if result is not None:
            job.result = json.dumps(result)
        
        if error_message is not None:
            job.error_message = error_message
            
        if progress is not None:
            job.progress = min(100, max(0, progress))
        
        if status == TaskStatus.COMPLETED:
            job.completed_at = datetime.utcnow()
            job.progress = 100
        elif status == TaskStatus.FAILED:
            job.completed_at = datetime.utcnow()
        elif status == TaskStatus.PROCESSING:
            job.started_at = datetime.utcnow()
        
        db.commit()
        db.refresh(job)
        
        logger.info(f"Updated task {job_id} to status {status.value}")
        
        return job

    @staticmethod
    def get_task(db: Session, job_id: str, user_id: Optional[int] = None) -> Optional["BackgroundJob"]:
        """
        Get a task by job_id, optionally filtering by user_id for security.
        
        Args:
            db: Database session
            job_id: Unique job identifier
            user_id: Optional user ID for ownership verification
            
        Returns:
            BackgroundJob or None if not found
        """
        query = db.query(BackgroundJob).filter(BackgroundJob.job_id == job_id)
        
        if user_id is not None:
            query = query.filter(BackgroundJob.user_id == user_id)
        
        return query.first()

    @staticmethod
    def get_user_tasks(
        db: Session,
        user_id: int,
        task_type: Optional[TaskType] = None,
        status: Optional[TaskStatus] = None,
        limit: int = 50
    ) -> list:
        """
        Get all tasks for a user with optional filtering.
        
        Args:
            db: Database session
            user_id: User ID
            task_type: Optional filter by task type
            status: Optional filter by status
            limit: Maximum number of results
            
        Returns:
            List of BackgroundJob objects
        """
        query = db.query(BackgroundJob).filter(BackgroundJob.user_id == user_id)
        
        if task_type:
            query = query.filter(BackgroundJob.task_type == task_type.value)
        
        if status:
            query = query.filter(BackgroundJob.status == status.value)
        
        return query.order_by(BackgroundJob.created_at.desc()).limit(limit).all()

    @staticmethod
    def execute_task(
        job_id: str,
        task_fn: Callable,
        *args,
        **kwargs
    ) -> None:
        """
        Execute a task function with automatic status tracking.
        
        This is designed to be used with FastAPI's BackgroundTasks:
        
            background_tasks.add_task(
                BackgroundTaskService.execute_task,
                job.job_id,
                my_heavy_function,
                arg1, arg2,
                kwarg1=value1
            )
        
        Args:
            job_id: Job ID to track
            task_fn: The actual function to execute
            *args: Positional arguments for task_fn
            **kwargs: Keyword arguments for task_fn
        """
        # Create a fresh database session for background execution
        with SessionLocal() as db:
            try:
                # Mark as processing
                BackgroundTaskService.update_task_status(
                    db, job_id, TaskStatus.PROCESSING
                )
                
                logger.info(f"Starting execution of task {job_id}")
                
                # Execute the actual task
                result = task_fn(*args, **kwargs)
                
                # Mark as completed with result
                result_data = None
                if isinstance(result, dict):
                    result_data = result
                elif isinstance(result, tuple) and len(result) == 2:
                    # Common pattern: (filepath, export_id)
                    result_data = {"filepath": result[0], "export_id": result[1]}
                elif result is not None:
                    result_data = {"result": str(result)}
                
                BackgroundTaskService.update_task_status(
                    db, job_id, TaskStatus.COMPLETED, result=result_data
                )
                
                logger.info(f"Task {job_id} completed successfully")
                
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                error_trace = traceback.format_exc()
                
                logger.error(f"Task {job_id} failed: {error_msg}\n{error_trace}")
                
                BackgroundTaskService.update_task_status(
                    db, job_id, TaskStatus.FAILED, error_message=error_msg
                )

    @staticmethod
    def cleanup_old_tasks(db: Session, days: int = 30) -> int:
        """
        Remove completed/failed tasks older than specified days.
        
        Args:
            db: Database session
            days: Age threshold in days
            
        Returns:
            Number of tasks deleted
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        deleted = db.query(BackgroundJob).filter(
            BackgroundJob.status.in_([TaskStatus.COMPLETED.value, TaskStatus.FAILED.value]),
            BackgroundJob.created_at < cutoff
        ).delete(synchronize_session=False)
        
        db.commit()
        
        logger.info(f"Cleaned up {deleted} old background tasks")
        
        return deleted

    @staticmethod
    def get_pending_tasks_count(db: Session, user_id: Optional[int] = None) -> int:
        """
        Get count of pending/processing tasks.
        
        Args:
            db: Database session
            user_id: Optional user filter
            
        Returns:
            Count of active tasks
        """
        query = db.query(BackgroundJob).filter(
            BackgroundJob.status.in_([TaskStatus.PENDING.value, TaskStatus.PROCESSING.value])
        )
        
        if user_id:
            query = query.filter(BackgroundJob.user_id == user_id)
        
        return query.count()


def background_task(task_type: TaskType):
    """
    Decorator to wrap a function for background execution with tracking.
    
    Usage:
        @background_task(TaskType.EXPORT_PDF)
        def generate_pdf(db: Session, user: User, options: dict):
            # Heavy processing
            return {"filepath": "/path/to/file.pdf"}
    
    The decorated function should be called with:
        - background_tasks: FastAPI BackgroundTasks
        - db: Database session
        - user_id: User ID
        - Plus original function arguments
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(
            background_tasks: Any,
            db: Session,
            user_id: int,
            *args,
            **kwargs
        ) -> str:
            # Create the task record
            params = {
                "args": [str(a) for a in args],
                "kwargs": {k: str(v) for k, v in kwargs.items()}
            }
            
            job = BackgroundTaskService.create_task(
                db=db,
                user_id=user_id,
                task_type=task_type,
                params=params
            )
            
            # Schedule background execution
            background_tasks.add_task(
                BackgroundTaskService.execute_task,
                job.job_id,
                fn,
                *args,
                **kwargs
            )
            
            return job.job_id
        
        return wrapper
    return decorator
