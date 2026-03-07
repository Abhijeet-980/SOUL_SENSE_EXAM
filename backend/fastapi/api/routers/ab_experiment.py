"""
A/B Experimentation Framework API Router (#1442)

REST API endpoints for managing A/B experiments on recommendations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.db_service import get_db
from ..utils.ab_experiment_framework import (
    get_experiment_framework,
    ABExperimentFramework,
    ExperimentConfig,
    Experiment,
    ExperimentStatus,
    ExperimentType,
    AssignmentMethod,
    MetricType,
    Variant,
    MetricConfig,
)
from .auth import require_admin, get_current_user


router = APIRouter(tags=["A/B Experiments"], prefix="/experiments")


# --- Pydantic Schemas ---

class VariantRequest(BaseModel):
    """Schema for creating a variant."""
    name: str = Field(..., description="Variant name")
    description: Optional[str] = Field(None)
    config: Dict[str, Any] = Field(default_factory=dict)
    traffic_percentage: float = Field(default=50.0, ge=0, le=100)
    is_control: bool = Field(default=False)


class MetricConfigRequest(BaseModel):
    """Schema for metric configuration."""
    metric_name: str = Field(..., description="Metric name")
    metric_type: MetricType = Field(default=MetricType.CONVERSION)
    event_name: str = Field(..., description="Event to track")
    is_primary: bool = Field(default=False)
    is_guardrail: bool = Field(default=False)
    minimum_detectable_effect: float = Field(default=0.05)
    direction: str = Field(default="increase", pattern="^(increase|decrease)$")


class ExperimentConfigRequest(BaseModel):
    """Schema for experiment configuration."""
    variants: List[VariantRequest] = Field(..., min_length=2)
    metrics: List[MetricConfigRequest] = Field(..., min_length=1)
    traffic_split: List[float] = Field(default=[50.0, 50.0])
    assignment_method: AssignmentMethod = Field(default=AssignmentMethod.HASH)
    min_sample_size: int = Field(default=1000, ge=100)
    max_sample_size: Optional[int] = Field(None)
    confidence_level: float = Field(default=0.95, ge=0.8, le=0.99)
    runtime_days: int = Field(default=14, ge=1, le=90)


class ExperimentCreateRequest(BaseModel):
    """Schema for creating an experiment."""
    name: str = Field(..., description="Experiment name")
    description: Optional[str] = Field(None)
    experiment_type: ExperimentType = Field(default=ExperimentType.AB_TEST)
    config: ExperimentConfigRequest = Field(...)


class ExperimentResponse(BaseModel):
    """Schema for experiment response."""
    experiment_id: str
    name: str
    description: Optional[str]
    experiment_type: str
    config: Dict[str, Any]
    status: str
    created_at: str
    started_at: Optional[str]
    ended_at: Optional[str]
    created_by: Optional[str]
    winner_variant_id: Optional[str]


class UserAssignmentRequest(BaseModel):
    """Schema for user assignment."""
    user_id: str = Field(..., description="User identifier")
    attributes: Optional[Dict[str, Any]] = Field(default_factory=dict)


class UserAssignmentResponse(BaseModel):
    """Schema for user assignment response."""
    assignment_id: str
    experiment_id: str
    user_id: str
    variant_id: str
    assigned_at: str
    attributes: Dict[str, Any]


class TrackEventRequest(BaseModel):
    """Schema for tracking an event."""
    user_id: str = Field(..., description="User identifier")
    event_name: str = Field(..., description="Event name")
    event_value: float = Field(default=1.0)
    event_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class TrackEventResponse(BaseModel):
    """Schema for event tracking response."""
    event_id: str
    experiment_id: str
    user_id: str
    variant_id: str
    event_name: str
    timestamp: str


class VariantStatsResponse(BaseModel):
    """Schema for variant statistics."""
    variant_id: str
    variant_name: str
    sample_size: int
    conversions: int
    conversion_rate: float
    total_value: float
    mean_value: float
    std_dev: float
    confidence_interval: List[float]


class ExperimentResultsResponse(BaseModel):
    """Schema for experiment results."""
    experiment_id: str
    metric_name: str
    control_variant: VariantStatsResponse
    treatment_variants: List[VariantStatsResponse]
    p_value: float
    is_statistically_significant: bool
    relative_lift: float
    absolute_lift: float
    recommendation: str
    calculated_at: str


class ExperimentStatsResponse(BaseModel):
    """Schema for experiment statistics."""
    experiment_id: str
    status: str
    total_assignments: int
    assignments_by_variant: Dict[str, int]
    total_events: int
    events_by_name: Dict[str, int]
    days_running: int


class GlobalStatsResponse(BaseModel):
    """Schema for global experiment statistics."""
    status: str
    experiments_by_status: Dict[str, int]
    total_assignments: int
    total_events: int
    timestamp: str


# --- API Endpoints ---

@router.post(
    "",
    response_model=ExperimentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create A/B experiment",
    description="Creates a new A/B experiment for recommendation testing."
)
async def create_experiment(
    request: ExperimentCreateRequest,
    current_user: Any = Depends(require_admin)
) -> ExperimentResponse:
    """Create a new A/B experiment."""
    framework = await get_experiment_framework()
    
    # Build config
    variants = [
        Variant(
            variant_id="",
            name=v.name,
            description=v.description,
            config=v.config,
            traffic_percentage=v.traffic_percentage,
            is_control=v.is_control,
        )
        for v in request.config.variants
    ]
    
    metrics = [
        MetricConfig(
            metric_name=m.metric_name,
            metric_type=m.metric_type,
            event_name=m.event_name,
            is_primary=m.is_primary,
            is_guardrail=m.is_guardrail,
            minimum_detectable_effect=m.minimum_detectable_effect,
            direction=m.direction,
        )
        for m in request.config.metrics
    ]
    
    config = ExperimentConfig(
        variants=variants,
        metrics=metrics,
        traffic_split=request.config.traffic_split,
        assignment_method=request.config.assignment_method,
        min_sample_size=request.config.min_sample_size,
        max_sample_size=request.config.max_sample_size,
        confidence_level=request.config.confidence_level,
        runtime_days=request.config.runtime_days,
    )
    
    experiment = await framework.create_experiment(
        name=request.name,
        description=request.description,
        experiment_type=request.experiment_type,
        config=config,
        created_by=getattr(current_user, 'id', None) if current_user else None,
    )
    
    return ExperimentResponse(**experiment.to_dict())


@router.get(
    "",
    response_model=List[ExperimentResponse],
    summary="List experiments",
    description="Returns list of A/B experiments."
)
async def list_experiments(
    status: Optional[ExperimentStatus] = Query(None, description="Filter by status"),
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: Any = Depends(require_admin)
) -> List[ExperimentResponse]:
    """List A/B experiments."""
    framework = await get_experiment_framework()
    experiments = await framework.list_experiments(status=status, limit=limit)
    return [ExperimentResponse(**e.to_dict()) for e in experiments]


@router.get(
    "/{experiment_id}",
    response_model=ExperimentResponse,
    summary="Get experiment details",
    description="Returns details for a specific experiment."
)
async def get_experiment(
    experiment_id: str,
    current_user: Any = Depends(require_admin)
) -> ExperimentResponse:
    """Get experiment details."""
    framework = await get_experiment_framework()
    experiment = await framework.get_experiment(experiment_id)
    
    if not experiment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Experiment {experiment_id} not found"
        )
    
    return ExperimentResponse(**experiment.to_dict())


@router.post(
    "/{experiment_id}/start",
    response_model=ExperimentResponse,
    summary="Start experiment",
    description="Starts a draft experiment."
)
async def start_experiment(
    experiment_id: str,
    current_user: Any = Depends(require_admin)
) -> ExperimentResponse:
    """Start an experiment."""
    framework = await get_experiment_framework()
    
    success = await framework.start_experiment(experiment_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not start experiment {experiment_id}. It may not be in draft status."
        )
    
    experiment = await framework.get_experiment(experiment_id)
    return ExperimentResponse(**experiment.to_dict())


@router.post(
    "/{experiment_id}/pause",
    response_model=ExperimentResponse,
    summary="Pause experiment",
    description="Pauses a running experiment."
)
async def pause_experiment(
    experiment_id: str,
    current_user: Any = Depends(require_admin)
) -> ExperimentResponse:
    """Pause an experiment."""
    framework = await get_experiment_framework()
    
    success = await framework.pause_experiment(experiment_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not pause experiment {experiment_id}. It may not be running."
        )
    
    experiment = await framework.get_experiment(experiment_id)
    return ExperimentResponse(**experiment.to_dict())


@router.post(
    "/{experiment_id}/stop",
    response_model=ExperimentResponse,
    summary="Stop experiment",
    description="Stops an experiment and optionally declares a winner."
)
async def stop_experiment(
    experiment_id: str,
    winner_variant_id: Optional[str] = Query(None, description="Winning variant ID"),
    current_user: Any = Depends(require_admin)
) -> ExperimentResponse:
    """Stop an experiment."""
    framework = await get_experiment_framework()
    
    success = await framework.stop_experiment(experiment_id, winner_variant_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not stop experiment {experiment_id}"
        )
    
    experiment = await framework.get_experiment(experiment_id)
    return ExperimentResponse(**experiment.to_dict())


@router.post(
    "/{experiment_id}/assign",
    response_model=UserAssignmentResponse,
    summary="Assign user to variant",
    description="Assigns a user to a variant in the experiment."
)
async def assign_user(
    experiment_id: str,
    request: UserAssignmentRequest,
    current_user: Any = Depends(get_current_user)
) -> UserAssignmentResponse:
    """Assign a user to a variant."""
    framework = await get_experiment_framework()
    
    assignment = await framework.assign_user(
        experiment_id=experiment_id,
        user_id=request.user_id,
        attributes=request.attributes,
    )
    
    if not assignment:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not assign user to experiment {experiment_id}. Experiment may not be running."
        )
    
    return UserAssignmentResponse(**assignment.to_dict())


@router.post(
    "/{experiment_id}/track",
    response_model=TrackEventResponse,
    summary="Track experiment event",
    description="Tracks an event for a user in the experiment."
)
async def track_event(
    experiment_id: str,
    request: TrackEventRequest,
    current_user: Any = Depends(get_current_user)
) -> TrackEventResponse:
    """Track an experiment event."""
    framework = await get_experiment_framework()
    
    event = await framework.track_event(
        experiment_id=experiment_id,
        user_id=request.user_id,
        event_name=request.event_name,
        event_value=request.event_value,
        event_metadata=request.event_metadata,
    )
    
    if not event:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not track event. User may not be assigned to experiment."
        )
    
    return TrackEventResponse(**event.to_dict())


@router.get(
    "/{experiment_id}/results",
    response_model=ExperimentResultsResponse,
    summary="Get experiment results",
    description="Calculates and returns experiment results."
)
async def get_results(
    experiment_id: str,
    metric_name: Optional[str] = Query(None, description="Metric to analyze (defaults to primary)"),
    current_user: Any = Depends(require_admin)
) -> ExperimentResultsResponse:
    """Get experiment results."""
    framework = await get_experiment_framework()
    
    results = await framework.calculate_results(experiment_id, metric_name)
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No results found for experiment {experiment_id}"
        )
    
    return ExperimentResultsResponse(**results.to_dict())


@router.get(
    "/{experiment_id}/stats",
    response_model=ExperimentStatsResponse,
    summary="Get experiment statistics",
    description="Returns comprehensive statistics for an experiment."
)
async def get_experiment_stats(
    experiment_id: str,
    current_user: Any = Depends(require_admin)
) -> ExperimentStatsResponse:
    """Get experiment statistics."""
    framework = await get_experiment_framework()
    
    stats = await framework.get_experiment_stats(experiment_id)
    
    if not stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Experiment {experiment_id} not found"
        )
    
    return ExperimentStatsResponse(**stats)


@router.get(
    "/running/all",
    response_model=List[ExperimentResponse],
    summary="Get running experiments",
    description="Returns all currently running experiments."
)
async def get_running_experiments(
    current_user: Any = Depends(require_admin)
) -> List[ExperimentResponse]:
    """Get all running experiments."""
    framework = await get_experiment_framework()
    experiments = await framework.get_running_experiments()
    return [ExperimentResponse(**e.to_dict()) for e in experiments]


@router.get(
    "/global/statistics",
    response_model=GlobalStatsResponse,
    summary="Get global statistics",
    description="Returns global experiment statistics."
)
async def get_global_statistics(
    current_user: Any = Depends(require_admin)
) -> GlobalStatsResponse:
    """Get global experiment statistics."""
    framework = await get_experiment_framework()
    stats = await framework.check_experiment_health()
    return GlobalStatsResponse(**stats)


@router.get(
    "/types/list",
    response_model=List[Dict[str, str]],
    summary="List experiment types",
    description="Returns available experiment types."
)
async def list_experiment_types(
    current_user: Any = Depends(get_current_user)
) -> List[Dict[str, str]]:
    """List experiment types."""
    return [
        {"value": t.value, "name": t.name.replace("_", " ").title()}
        for t in ExperimentType
    ]


@router.get(
    "/assignment-methods/list",
    response_model=List[Dict[str, str]],
    summary="List assignment methods",
    description="Returns available user assignment methods."
)
async def list_assignment_methods(
    current_user: Any = Depends(get_current_user)
) -> List[Dict[str, str]]:
    """List assignment methods."""
    return [
        {"value": m.value, "name": m.name.replace("_", " ").title()}
        for m in AssignmentMethod
    ]


@router.get(
    "/metric-types/list",
    response_model=List[Dict[str, str]],
    summary="List metric types",
    description="Returns available metric types."
)
async def list_metric_types(
    current_user: Any = Depends(get_current_user)
) -> List[Dict[str, str]]:
    """List metric types."""
    return [
        {"value": t.value, "name": t.name.replace("_", " ").title()}
        for t in MetricType
    ]


@router.post(
    "/initialize",
    status_code=status.HTTP_200_OK,
    summary="Initialize experiment framework",
    description="Initializes the experiment framework and database tables."
)
async def initialize_framework(
    current_user: Any = Depends(require_admin)
) -> Dict[str, str]:
    """Initialize experiment framework."""
    framework = await get_experiment_framework()
    await framework.initialize()
    return {"status": "initialized"}
