"""API router for assessment endpoints."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from ..services.db_service import get_db, AssessmentService
from backend.fastapi.app.core import NotFoundError
from ..schemas import (
    AssessmentListResponse,
    AssessmentResponse,
    AssessmentDetailResponse,
    AssessmentStatsResponse
)

router = APIRouter()


@router.get("/", response_model=AssessmentListResponse)
async def get_assessments(
    username: Optional[str] = Query(None, description="Filter by username"),
    age_group: Optional[str] = Query(None, description="Filter by age group"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get a paginated list of assessments.
    """
    skip = (page - 1) * page_size
    
    assessments, total = await AssessmentService.get_assessments(
        db=db,
        skip=skip,
        limit=page_size,
        username=username,
        age_group=age_group
    )
    
    return AssessmentListResponse(
        total=total,
        assessments=[AssessmentResponse.model_validate(a) for a in assessments],
        page=page,
        page_size=page_size
    )


@router.get("/stats", response_model=AssessmentStatsResponse)
async def get_assessment_stats(
    username: Optional[str] = Query(None, description="Filter stats by username"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get statistical summary of assessments.
    """
    stats = await AssessmentService.get_assessment_stats(db=db, username=username)
    
    return AssessmentStatsResponse(**stats)


@router.get("/{assessment_id}", response_model=AssessmentDetailResponse)
async def get_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed information for a specific assessment.
    """
    assessment = await AssessmentService.get_assessment_by_id(db=db, assessment_id=assessment_id)
    
    if not assessment:
        raise NotFoundError(resource="Assessment", resource_id=str(assessment_id))
    
    responses = await AssessmentService.get_assessment_responses(db=db, assessment_id=assessment_id)
    
    assessment_dict = {
        "id": assessment.id,
        "username": assessment.username,
        "total_score": assessment.total_score,
        "sentiment_score": assessment.sentiment_score,
        "reflection_text": assessment.reflection_text,
        "is_rushed": assessment.is_rushed,
        "is_inconsistent": assessment.is_inconsistent,
        "age": assessment.age,
        "detailed_age_group": assessment.detailed_age_group,
        "timestamp": assessment.timestamp,
        "responses_count": len(responses)
    }
    
    return AssessmentDetailResponse(**assessment_dict)
