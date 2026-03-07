"""
Key Rotation Rehearsal API Endpoints (#1425)

Provides REST API endpoints for encryption key rotation rehearsal management,
testing, and monitoring.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.db_service import get_db
from ..utils.key_rotation_rehearsal import (
    get_key_rotation_orchestrator,
    KeyRotationRehearsalOrchestrator,
    RotationStrategy,
    RehearsalStatus,
    KeyStatus,
    EncryptionKey,
    RehearsalSchedule,
)
from .auth import require_admin


router = APIRouter(tags=["Key Rotation Rehearsal"], prefix="/admin/key-rotation")


# --- Pydantic Schemas ---

class EncryptionKeyRequest(BaseModel):
    """Schema for registering an encryption key."""
    key_id: str = Field(..., description="Unique key identifier")
    key_version: int = Field(default=1, ge=1)
    algorithm: str = Field(default="AES-256-GCM")
    key_hash: Optional[str] = None


class EncryptionKeyResponse(BaseModel):
    """Schema for encryption key response."""
    key_id: str
    key_version: int
    key_status: str
    algorithm: str
    key_hash: Optional[str]
    created_at: str
    rotated_at: Optional[str]
    retired_at: Optional[str]


class RotationRehearsalRequest(BaseModel):
    """Schema for running a key rotation rehearsal."""
    table_name: str = Field(..., description="Table containing encrypted data")
    column_name: str = Field(default="encrypted_value", description="Column with encrypted values")
    strategy: RotationStrategy = Field(default=RotationStrategy.SHADOW_ROTATION)
    source_key_id: Optional[str] = Field(None, description="Current encryption key")
    target_key_id: Optional[str] = Field(None, description="New encryption key")
    auto_rollback: bool = Field(default=True)
    dry_run: bool = Field(default=True, description="Simulate without actual changes")
    batch_size: int = Field(default=1000, ge=100, le=10000)


class DataValidationResultResponse(BaseModel):
    """Schema for data validation result."""
    table_name: str
    column_name: str
    rows_checked: int
    rows_valid: int
    rows_invalid: int
    checksum_before: Optional[str]
    checksum_after: Optional[str]
    validation_errors: List[str]
    is_valid: bool
    timestamp: str


class RotationRehearsalResultResponse(BaseModel):
    """Schema for rotation rehearsal result."""
    rehearsal_id: str
    table_name: str
    column_name: str
    strategy: str
    status: str
    started_at: str
    completed_at: Optional[str]
    source_key: Optional[EncryptionKeyResponse]
    target_key: Optional[EncryptionKeyResponse]
    total_rows: int
    rows_processed: int
    rows_failed: int
    progress_percentage: float
    pre_validation: Optional[DataValidationResultResponse]
    post_validation: Optional[DataValidationResultResponse]
    rotation_duration_ms: float
    validation_duration_ms: float
    rollback_duration_ms: float
    errors: List[str]
    rollback_performed: bool
    success: bool


class RehearsalScheduleRequest(BaseModel):
    """Schema for rehearsal schedule configuration."""
    enabled: bool = Field(default=False)
    frequency_days: int = Field(default=90, ge=1, le=365)
    preferred_hour: int = Field(default=3, ge=0, le=23)
    auto_rollback: bool = Field(default=True)
    tables_to_rotate: List[str] = Field(default_factory=list)
    strategies: List[RotationStrategy] = Field(default=[RotationStrategy.SHADOW_ROTATION])
    notify_on_failure: bool = Field(default=True)


class RehearsalScheduleResponse(BaseModel):
    """Schema for rehearsal schedule response."""
    enabled: bool
    frequency_days: int
    preferred_hour: int
    auto_rollback: bool
    tables_to_rotate: List[str]
    strategies: List[str]
    notify_on_failure: bool


class KeyRotationStatisticsResponse(BaseModel):
    """Schema for key rotation statistics."""
    total_rehearsals: int
    successful_rehearsals: int
    failed_rehearsals: int
    success_rate: float
    average_rotation_time_ms: float
    rehearsals_last_7_days: int
    active_keys: int


class KeyRotationStatusResponse(BaseModel):
    """Schema for key rotation status."""
    status: str
    statistics: KeyRotationStatisticsResponse
    schedule: RehearsalScheduleResponse


# --- API Endpoints ---

@router.get(
    "/status",
    response_model=KeyRotationStatusResponse,
    summary="Get key rotation status",
    description="Returns orchestrator status, statistics, and schedule."
)
async def get_key_rotation_status(
    current_user: Any = Depends(require_admin)
) -> KeyRotationStatusResponse:
    """Get key rotation rehearsal status."""
    orchestrator = await get_key_rotation_orchestrator()
    
    # Get statistics
    stats = await orchestrator.get_statistics()
    
    # Get schedule
    schedule = orchestrator.get_schedule()
    
    return KeyRotationStatusResponse(
        status="healthy",
        statistics=KeyRotationStatisticsResponse(**stats),
        schedule=RehearsalScheduleResponse(**schedule.to_dict()),
    )


@router.get(
    "/statistics",
    response_model=KeyRotationStatisticsResponse,
    summary="Get rehearsal statistics",
    description="Returns key rotation rehearsal statistics."
)
async def get_statistics(
    current_user: Any = Depends(require_admin)
) -> KeyRotationStatisticsResponse:
    """Get key rotation statistics."""
    orchestrator = await get_key_rotation_orchestrator()
    stats = await orchestrator.get_statistics()
    return KeyRotationStatisticsResponse(**stats)


@router.post(
    "/keys",
    response_model=EncryptionKeyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register encryption key",
    description="Registers an encryption key for rotation testing."
)
async def register_key(
    request: EncryptionKeyRequest,
    current_user: Any = Depends(require_admin)
) -> EncryptionKeyResponse:
    """Register an encryption key."""
    orchestrator = await get_key_rotation_orchestrator()
    
    key = EncryptionKey(
        key_id=request.key_id,
        key_version=request.key_version,
        key_status=KeyStatus.ACTIVE,
        created_at=datetime.utcnow(),
        algorithm=request.algorithm,
        key_hash=request.key_hash,
    )
    
    await orchestrator.register_key(key)
    
    return EncryptionKeyResponse(**key.to_dict())


@router.get(
    "/keys/{key_id}",
    response_model=EncryptionKeyResponse,
    summary="Get encryption key",
    description="Returns details for a specific encryption key."
)
async def get_key(
    key_id: str,
    current_user: Any = Depends(require_admin)
) -> EncryptionKeyResponse:
    """Get an encryption key."""
    orchestrator = await get_key_rotation_orchestrator()
    
    key = await orchestrator.get_key(key_id)
    
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key {key_id} not found"
        )
    
    return EncryptionKeyResponse(**key.to_dict())


@router.post(
    "/rehearsals",
    response_model=RotationRehearsalResultResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Run key rotation rehearsal",
    description="Executes a key rotation rehearsal with the specified strategy."
)
async def run_rehearsal(
    request: RotationRehearsalRequest,
    current_user: Any = Depends(require_admin)
) -> RotationRehearsalResultResponse:
    """Run a key rotation rehearsal."""
    orchestrator = await get_key_rotation_orchestrator()
    
    result = await orchestrator.run_rehearsal(
        table_name=request.table_name,
        column_name=request.column_name,
        strategy=request.strategy,
        source_key_id=request.source_key_id,
        target_key_id=request.target_key_id,
        auto_rollback=request.auto_rollback,
        dry_run=request.dry_run,
        batch_size=request.batch_size,
    )
    
    return RotationRehearsalResultResponse(**result.to_dict())


@router.get(
    "/rehearsals/history",
    response_model=List[Dict[str, Any]],
    summary="Get rehearsal history",
    description="Returns history of key rotation rehearsals."
)
async def get_rehearsal_history(
    table_name: Optional[str] = Query(None, description="Filter by table"),
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: Any = Depends(require_admin)
) -> List[Dict[str, Any]]:
    """Get rehearsal history."""
    orchestrator = await get_key_rotation_orchestrator()
    history = await orchestrator.get_rehearsal_history(table_name, limit)
    return history


@router.get(
    "/schedule",
    response_model=RehearsalScheduleResponse,
    summary="Get rehearsal schedule",
    description="Returns current rehearsal schedule configuration."
)
async def get_schedule(
    current_user: Any = Depends(require_admin)
) -> RehearsalScheduleResponse:
    """Get rehearsal schedule."""
    orchestrator = await get_key_rotation_orchestrator()
    schedule = orchestrator.get_schedule()
    return RehearsalScheduleResponse(**schedule.to_dict())


@router.put(
    "/schedule",
    response_model=RehearsalScheduleResponse,
    summary="Update rehearsal schedule",
    description="Updates rehearsal schedule configuration."
)
async def update_schedule(
    request: RehearsalScheduleRequest,
    current_user: Any = Depends(require_admin)
) -> RehearsalScheduleResponse:
    """Update rehearsal schedule."""
    orchestrator = await get_key_rotation_orchestrator()
    
    schedule = RehearsalSchedule(
        enabled=request.enabled,
        frequency_days=request.frequency_days,
        preferred_hour=request.preferred_hour,
        auto_rollback=request.auto_rollback,
        tables_to_rotate=request.tables_to_rotate,
        strategies=request.strategies,
        notify_on_failure=request.notify_on_failure,
    )
    
    orchestrator.configure_schedule(schedule)
    
    return RehearsalScheduleResponse(**schedule.to_dict())


@router.get(
    "/strategies",
    response_model=List[Dict[str, str]],
    summary="List available strategies",
    description="Returns list of available rotation strategies."
)
async def list_strategies(
    current_user: Any = Depends(require_admin)
) -> List[Dict[str, str]]:
    """List available rotation strategies."""
    return [
        {"value": s.value, "name": s.name.replace("_", " ").title()}
        for s in RotationStrategy
    ]


@router.post(
    "/initialize",
    status_code=status.HTTP_200_OK,
    summary="Initialize orchestrator",
    description="Initializes key rotation rehearsal orchestrator."
)
async def initialize_orchestrator(
    current_user: Any = Depends(require_admin)
) -> Dict[str, str]:
    """Initialize key rotation orchestrator."""
    orchestrator = await get_key_rotation_orchestrator()
    await orchestrator.initialize()
    return {
        "status": "initialized",
    }


@router.post(
    "/validate/{table_name}",
    response_model=DataValidationResultResponse,
    summary="Validate encrypted data",
    description="Validates encrypted data integrity."
)
async def validate_data(
    table_name: str,
    column_name: str = Query(default="encrypted_value"),
    current_user: Any = Depends(require_admin)
) -> DataValidationResultResponse:
    """Validate encrypted data."""
    orchestrator = await get_key_rotation_orchestrator()
    
    result = await orchestrator._validate_data(table_name, column_name, "manual")
    
    return DataValidationResultResponse(**result.to_dict())
