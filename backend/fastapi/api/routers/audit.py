from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from ..services.db_service import get_db
from ..services.audit_service import AuditService
from ..models import User
from ..utils.auth import get_current_user
from ..schemas import AuditLogResponse, AuditLogListResponse, AuditExportResponse

router = APIRouter()

@router.get("/logs", response_model=AuditLogListResponse)
async def get_audit_logs(
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    username: Optional[str] = Query(None, description="Filter by username"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    action: Optional[str] = Query(None, description="Filter by action"),
    outcome: Optional[str] = Query(None, description="Filter by outcome"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=100, description="Results per page"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Retrieve audit logs with filtering and pagination.
    Requires admin privileges.
    """
    # TODO: Add admin role check
    # if not current_user.is_admin:
    #     raise HTTPException(status_code=403, detail="Admin access required")

    filters = {}
    if event_type:
        filters['event_type'] = event_type
    if username:
        filters['username'] = username
    if resource_type:
        filters['resource_type'] = resource_type
    if action:
        filters['action'] = action
    if outcome:
        filters['outcome'] = outcome
    if severity:
        filters['severity'] = severity
    if start_date:
        filters['start_date'] = start_date
    if end_date:
        filters['end_date'] = end_date

    logs, total_count = AuditService.query_logs(filters, page, per_page, db)

    return AuditLogListResponse(
        logs=[AuditLogResponse.from_orm(log) for log in logs],
        total_count=total_count,
        page=page,
        per_page=per_page
    )

@router.get("/my-activity", response_model=AuditLogListResponse)
async def get_my_activity(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=50, description="Results per page"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get current user's own audit activity logs.
    """
    logs, total_count = AuditService.get_user_activity(current_user.id, page, per_page, db)

    return AuditLogListResponse(
        logs=[AuditLogResponse.from_orm(log) for log in logs],
        total_count=total_count,
        page=page,
        per_page=per_page
    )

@router.get("/export", response_model=AuditExportResponse)
async def export_audit_logs(
    format: str = Query("json", regex="^(json|csv)$", description="Export format"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    username: Optional[str] = Query(None, description="Filter by username"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Export audit logs in JSON or CSV format.
    Requires admin privileges.
    """
    # TODO: Add admin role check
    # if not current_user.is_admin:
    #     raise HTTPException(status_code=403, detail="Admin access required")

    filters = {}
    if event_type:
        filters['event_type'] = event_type
    if username:
        filters['username'] = username
    if start_date:
        filters['start_date'] = start_date
    if end_date:
        filters['end_date'] = end_date

    exported_data = AuditService.export_logs(filters, format, db)

    return AuditExportResponse(
        data=exported_data,
        format=format,
        timestamp=datetime.utcnow()
    )

@router.post("/archive")
async def archive_old_logs(
    retention_days: Optional[int] = Query(90, ge=1, description="Retention period in days"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Archive audit logs older than retention period.
    Requires admin privileges.
    """
    # TODO: Add admin role check
    # if not current_user.is_admin:
    #     raise HTTPException(status_code=403, detail="Admin access required")

    archived_count = AuditService.archive_old_logs(retention_days, db)

    return {
        "message": f"Archived {archived_count} audit logs",
        "archived_count": archived_count
    }

@router.post("/cleanup")
async def cleanup_expired_logs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Permanently delete expired audit logs.
    Requires admin privileges.
    """
    # TODO: Add admin role check
    # if not current_user.is_admin:
    #     raise HTTPException(status_code=403, detail="Admin access required")

    deleted_count = AuditService.cleanup_expired_logs(db)

    return {
        "message": f"Cleaned up {deleted_count} expired audit logs",
        "deleted_count": deleted_count
    }