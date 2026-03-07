"""
TTL Management API Endpoints (#1413)

Provides REST API endpoints for managing row-level TTL policies,
monitoring archival operations, and triggering manual archival runs.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.db_service import get_db
from ..utils.ttl_partition_manager import (
    get_ttl_manager,
    TTLPolicy,
    PartitionGranularity,
    ArchiveStrategy,
    PolicyStatus,
)
from .auth import require_admin


router = APIRouter(tags=["TTL Management"], prefix="/admin/ttl")


# --- Pydantic Schemas ---

class TTLPolicyCreate(BaseModel):
    """Schema for creating a TTL policy."""
    table_name: str = Field(..., description="Target database table")
    retention_days: int = Field(..., ge=1, description="Number of days to retain data")
    partition_granularity: PartitionGranularity = Field(
        default=PartitionGranularity.MONTHLY,
        description="Time granularity for partitions"
    )
    archive_strategy: ArchiveStrategy = Field(
        default=ArchiveStrategy.ARCHIVE_THEN_DELETE,
        description="How to handle expired data"
    )
    archive_table: Optional[str] = Field(
        None,
        description="Optional destination table for archived data"
    )
    dry_run: bool = Field(default=False, description="If True, only simulate operations")
    batch_size: int = Field(default=1000, ge=100, le=10000, description="Rows per batch")
    id_column: str = Field(default="id", description="Column name for row identification")
    timestamp_column: str = Field(default="created_at", description="Column for TTL calculation")
    filters: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional SQL filters for selective archiving"
    )


class TTLPolicyResponse(BaseModel):
    """Schema for TTL policy response."""
    table_name: str
    retention_days: int
    partition_granularity: str
    archive_strategy: str
    archive_table: Optional[str]
    status: str
    dry_run: bool
    batch_size: int
    id_column: str
    timestamp_column: str
    filters: Optional[Dict[str, Any]]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    last_run_stats: Optional[Dict[str, Any]] = None


class ArchivalRunRequest(BaseModel):
    """Schema for triggering an archival run."""
    dry_run: bool = Field(default=False, description="If True, only simulate the operation")


class ArchivalStatsResponse(BaseModel):
    """Schema for archival statistics."""
    table_name: str
    start_time: datetime
    end_time: Optional[datetime]
    rows_scanned: int
    rows_archived: int
    rows_deleted: int
    rows_skipped: int
    duration_ms: float
    success: bool
    errors: List[str]


class PartitionInfoResponse(BaseModel):
    """Schema for partition information."""
    name: str
    table_name: str
    start_date: datetime
    end_date: datetime
    row_count: Optional[int]
    size_bytes: Optional[int]
    is_archived: bool


class TTLStatsResponse(BaseModel):
    """Schema for overall TTL statistics."""
    policy_count: int
    active_policies: int
    total_rows_archived: int
    total_rows_deleted: int
    runs_last_24h: int


class TTLStatusResponse(BaseModel):
    """Schema for overall TTL system status."""
    status: str
    policies: List[TTLPolicyResponse]
    statistics: TTLStatsResponse


# --- API Endpoints ---

@router.get(
    "/status",
    response_model=TTLStatusResponse,
    summary="Get TTL system status",
    description="Returns overall TTL management status including policies and statistics."
)
async def get_ttl_status(
    db: AsyncSession = Depends(get_db),
    current_user: Any = Depends(require_admin)
) -> TTLStatusResponse:
    """Get overall TTL system status."""
    manager = await get_ttl_manager()
    
    # Get all policies
    policies = await manager.list_policies()
    
    # Get statistics
    stats = await manager.get_stats()
    
    # Build response
    policy_responses = []
    for policy in policies:
        # Get additional metadata from database
        async with db.begin():
            result = await db.execute(
                "SELECT created_at, updated_at, last_run_at, last_run_stats "
                "FROM ttl_policies WHERE table_name = :table_name",
                {"table_name": policy.table_name}
            )
            row = result.fetchone()
        
        policy_responses.append(TTLPolicyResponse(
            **policy.to_dict(),
            created_at=row.created_at if row else None,
            updated_at=row.updated_at if row else None,
            last_run_at=row.last_run_at if row else None,
            last_run_stats=row.last_run_stats if row else None,
        ))
    
    return TTLStatusResponse(
        status="healthy" if stats["active_policies"] > 0 else "inactive",
        policies=policy_responses,
        statistics=TTLStatsResponse(**stats),
    )


@router.get(
    "/policies",
    response_model=List[TTLPolicyResponse],
    summary="List all TTL policies",
    description="Returns a list of all configured TTL policies."
)
async def list_policies(
    current_user: Any = Depends(require_admin)
) -> List[TTLPolicyResponse]:
    """List all registered TTL policies."""
    manager = await get_ttl_manager()
    policies = await manager.list_policies()
    
    return [TTLPolicyResponse(**policy.to_dict()) for policy in policies]


@router.get(
    "/policies/{table_name}",
    response_model=TTLPolicyResponse,
    summary="Get a specific TTL policy",
    description="Returns details for a specific table's TTL policy."
)
async def get_policy(
    table_name: str,
    current_user: Any = Depends(require_admin)
) -> TTLPolicyResponse:
    """Get a specific TTL policy by table name."""
    manager = await get_ttl_manager()
    policy = await manager.get_policy(table_name)
    
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No TTL policy found for table: {table_name}"
        )
    
    return TTLPolicyResponse(**policy.to_dict())


@router.post(
    "/policies",
    response_model=TTLPolicyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update a TTL policy",
    description="Registers a new TTL policy or updates an existing one."
)
async def create_or_update_policy(
    policy_data: TTLPolicyCreate,
    current_user: Any = Depends(require_admin)
) -> TTLPolicyResponse:
    """Create or update a TTL policy."""
    manager = await get_ttl_manager()
    
    policy = TTLPolicy(
        table_name=policy_data.table_name,
        retention_days=policy_data.retention_days,
        partition_granularity=policy_data.partition_granularity,
        archive_strategy=policy_data.archive_strategy,
        archive_table=policy_data.archive_table,
        dry_run=policy_data.dry_run,
        batch_size=policy_data.batch_size,
        id_column=policy_data.id_column,
        timestamp_column=policy_data.timestamp_column,
        filters=policy_data.filters,
    )
    
    await manager.register_policy(policy)
    
    return TTLPolicyResponse(**policy.to_dict())


@router.patch(
    "/policies/{table_name}/status",
    response_model=TTLPolicyResponse,
    summary="Update policy status",
    description="Enable, disable, pause, or set dry-run mode for a policy."
)
async def update_policy_status(
    table_name: str,
    status: PolicyStatus,
    current_user: Any = Depends(require_admin)
) -> TTLPolicyResponse:
    """Update the status of a TTL policy."""
    manager = await get_ttl_manager()
    policy = await manager.get_policy(table_name)
    
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No TTL policy found for table: {table_name}"
        )
    
    policy.status = status
    await manager.register_policy(policy)
    
    return TTLPolicyResponse(**policy.to_dict())


@router.delete(
    "/policies/{table_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a TTL policy",
    description="Removes a TTL policy from the system."
)
async def delete_policy(
    table_name: str,
    current_user: Any = Depends(require_admin)
) -> None:
    """Delete a TTL policy."""
    manager = await get_ttl_manager()
    
    async with manager.engine.begin() as conn:
        await conn.execute(
            "DELETE FROM ttl_policies WHERE table_name = :table_name",
            {"table_name": table_name}
        )


@router.post(
    "/policies/{table_name}/run",
    response_model=ArchivalStatsResponse,
    summary="Run archival for a specific table",
    description="Triggers immediate archival/purging of expired data for a table."
)
async def run_archival(
    table_name: str,
    request: ArchivalRunRequest,
    background_tasks: BackgroundTasks,
    current_user: Any = Depends(require_admin)
) -> ArchivalStatsResponse:
    """Run archival for a specific table."""
    manager = await get_ttl_manager()
    policy = await manager.get_policy(table_name)
    
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No TTL policy found for table: {table_name}"
        )
    
    # Temporarily override dry_run if specified
    original_dry_run = policy.dry_run
    policy.dry_run = request.dry_run
    
    try:
        stats = await manager.archive_expired_data(policy)
        return ArchivalStatsResponse(**stats.to_dict())
    finally:
        policy.dry_run = original_dry_run


@router.post(
    "/run-all",
    response_model=Dict[str, ArchivalStatsResponse],
    summary="Run all active policies",
    description="Triggers archival for all active TTL policies."
)
async def run_all_policies(
    dry_run: bool = Query(default=False, description="Run in dry-run mode"),
    background_tasks: BackgroundTasks = None,
    current_user: Any = Depends(require_admin)
) -> Dict[str, ArchivalStatsResponse]:
    """Run all active TTL policies."""
    manager = await get_ttl_manager()
    
    # Get all policies and temporarily set dry_run
    policies = await manager.list_policies()
    original_settings = {}
    
    for policy in policies:
        original_settings[policy.table_name] = policy.dry_run
        policy.dry_run = dry_run
    
    try:
        results = await manager.run_all_policies()
        return {
            table_name: ArchivalStatsResponse(**stats.to_dict())
            for table_name, stats in results.items()
        }
    finally:
        # Restore original settings
        for policy in policies:
            policy.dry_run = original_settings.get(policy.table_name, False)


@router.get(
    "/partitions/{table_name}",
    response_model=List[PartitionInfoResponse],
    summary="Get partition information",
    description="Returns partition details for a table."
)
async def get_partitions(
    table_name: str,
    current_user: Any = Depends(require_admin)
) -> List[PartitionInfoResponse]:
    """Get partition information for a table."""
    manager = await get_ttl_manager()
    partitions = await manager.get_partitions(table_name)
    
    return [PartitionInfoResponse(**partition.to_dict()) for partition in partitions]


@router.get(
    "/history",
    response_model=List[Dict[str, Any]],
    summary="Get archival history",
    description="Returns history of archival operations."
)
async def get_archival_history(
    table_name: Optional[str] = Query(None, description="Filter by table name"),
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: Any = Depends(require_admin)
) -> List[Dict[str, Any]]:
    """Get archival operation history."""
    manager = await get_ttl_manager()
    history = await manager.get_archival_history(table_name, limit)
    return history


@router.get(
    "/statistics",
    response_model=TTLStatsResponse,
    summary="Get TTL statistics",
    description="Returns overall TTL management statistics."
)
async def get_statistics(
    current_user: Any = Depends(require_admin)
) -> TTLStatsResponse:
    """Get overall TTL statistics."""
    manager = await get_ttl_manager()
    stats = await manager.get_stats()
    return TTLStatsResponse(**stats)


@router.post(
    "/initialize",
    status_code=status.HTTP_200_OK,
    summary="Initialize TTL system",
    description="Initializes TTL management tables and schema."
)
async def initialize_ttl(
    current_user: Any = Depends(require_admin)
) -> Dict[str, str]:
    """Initialize TTL management system."""
    manager = await get_ttl_manager()
    await manager.initialize()
    return {"status": "initialized", "message": "TTL management system initialized successfully"}
