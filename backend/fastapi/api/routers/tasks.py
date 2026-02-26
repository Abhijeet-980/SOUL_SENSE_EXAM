"""
Tasks Router - Background task status polling and management.

This router provides endpoints for:
- Polling task status (GET /api/v1/tasks/{job_id})
- Listing user's tasks (GET /api/v1/tasks)
- Cancelling pending tasks (DELETE /api/v1/tasks/{job_id})

All endpoints require authentication and enforce user-level access control.
"""

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import logging

from ..services.db_service import get_db
from ..services.background_task_service import (
    BackgroundTaskService,
    TaskStatus,
    TaskType
)
from ..models import User, BackgroundJob
from .auth import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)


# ============================================================================
# Response Models (inline for simplicity)
# ============================================================================

from pydantic import BaseModel, Field
from typing import Any, Dict


class TaskStatusResponse(BaseModel):
    """Response schema for task status."""
    job_id: str = Field(..., description="Unique task identifier")
    task_type: str = Field(..., description="Type of task (export_pdf, send_email, etc.)")
    status: str = Field(..., description="Task status: pending, processing, completed, failed")
    progress: int = Field(0, description="Progress percentage (0-100)")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result data (if completed)")
    error_message: Optional[str] = Field(None, description="Error message (if failed)")
    created_at: Optional[str] = Field(None, description="When the task was created")
    started_at: Optional[str] = Field(None, description="When the task started processing")
    completed_at: Optional[str] = Field(None, description="When the task finished")
    
    class Config:
        from_attributes = True


class TaskListResponse(BaseModel):
    """Response schema for task list."""
    total: int = Field(..., description="Total number of tasks returned")
    tasks: List[TaskStatusResponse] = Field(..., description="List of tasks")


class PendingTasksResponse(BaseModel):
    """Response schema for pending tasks count."""
    pending_count: int = Field(..., description="Number of pending/processing tasks")


# ============================================================================
# Task Status Polling Endpoints
# ============================================================================

@router.get("/{job_id}", response_model=TaskStatusResponse)
async def get_task_status(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get the status of a background task.
    
    This endpoint is designed for polling long-running tasks.
    Recommended polling interval: 1-2 seconds for active tasks.
    
    **Status Values:**
    - `pending`: Task is queued and waiting to start
    - `processing`: Task is currently being executed
    - `completed`: Task finished successfully (check `result` field)
    - `failed`: Task encountered an error (check `error_message` field)
    
    **Returns:**
    - 200: Task status retrieved successfully
    - 404: Task not found or access denied
    """
    task = BackgroundTaskService.get_task(db, job_id, user_id=current_user.id)
    
    if not task:
        raise HTTPException(
            status_code=404,
            detail="Task not found or you don't have access to it."
        )
    
    return TaskStatusResponse(
        job_id=task.job_id,
        task_type=task.task_type,
        status=task.status,
        progress=task.progress or 0,
        result=_parse_json_field(task.result),
        error_message=task.error_message,
        created_at=task.created_at.isoformat() if task.created_at else None,
        started_at=task.started_at.isoformat() if task.started_at else None,
        completed_at=task.completed_at.isoformat() if task.completed_at else None,
    )


@router.get("", response_model=TaskListResponse)
async def list_user_tasks(
    task_type: Optional[str] = Query(None, description="Filter by task type"),
    status: Optional[str] = Query(None, description="Filter by status (pending, processing, completed, failed)"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of tasks to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    List all background tasks for the current user.
    
    **Query Parameters:**
    - `task_type`: Filter by task type (e.g., export_pdf, send_email)
    - `status`: Filter by status (pending, processing, completed, failed)
    - `limit`: Maximum number of tasks to return (1-100)
    
    **Returns:**
    - List of tasks ordered by creation date (newest first)
    """
    # Validate and convert status filter
    status_filter = None
    if status:
        try:
            status_filter = TaskStatus(status)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status: {status}. Valid values: pending, processing, completed, failed"
            )
    
    # Validate and convert task type filter
    type_filter = None
    if task_type:
        try:
            type_filter = TaskType(task_type)
        except ValueError:
            # Allow any task type string for flexibility
            pass
    
    tasks = BackgroundTaskService.get_user_tasks(
        db,
        user_id=current_user.id,
        task_type=type_filter,
        status=status_filter,
        limit=limit
    )
    
    task_responses = [
        TaskStatusResponse(
            job_id=task.job_id,
            task_type=task.task_type,
            status=task.status,
            progress=task.progress or 0,
            result=_parse_json_field(task.result),
            error_message=task.error_message,
            created_at=task.created_at.isoformat() if task.created_at else None,
            started_at=task.started_at.isoformat() if task.started_at else None,
            completed_at=task.completed_at.isoformat() if task.completed_at else None,
        )
        for task in tasks
    ]
    
    return TaskListResponse(total=len(task_responses), tasks=task_responses)


@router.get("/pending/count", response_model=PendingTasksResponse)
async def get_pending_tasks_count(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get the count of pending/processing tasks for the current user.
    
    Useful for:
    - Showing task queue status in UI
    - Rate limiting task submission
    - Displaying progress indicators
    """
    count = BackgroundTaskService.get_pending_tasks_count(db, user_id=current_user.id)
    return PendingTasksResponse(pending_count=count)


@router.delete("/{job_id}")
async def cancel_task(
    job_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Cancel a pending task.
    
    **Note:** Only tasks with status `pending` can be cancelled.
    Tasks that are already `processing`, `completed`, or `failed` cannot be cancelled.
    
    **Returns:**
    - 200: Task cancelled successfully
    - 400: Task cannot be cancelled (not pending)
    - 404: Task not found or access denied
    """
    task = BackgroundTaskService.get_task(db, job_id, user_id=current_user.id)
    
    if not task:
        raise HTTPException(
            status_code=404,
            detail="Task not found or you don't have access to it."
        )
    
    if task.status != TaskStatus.PENDING.value:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel task with status '{task.status}'. Only pending tasks can be cancelled."
        )
    
    # Update task to failed with cancellation message
    BackgroundTaskService.update_task_status(
        db,
        job_id,
        TaskStatus.FAILED,
        error_message="Task cancelled by user"
    )
    
    logger.info(f"Task {job_id} cancelled by user {current_user.id}")
    
    return {"status": "cancelled", "job_id": job_id}


# ============================================================================
# Utility Functions
# ============================================================================

def _parse_json_field(field: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse a JSON string field to dict, returning None on failure."""
    if not field:
        return None
    try:
        import json
        return json.loads(field)
    except (json.JSONDecodeError, TypeError):
        return None
