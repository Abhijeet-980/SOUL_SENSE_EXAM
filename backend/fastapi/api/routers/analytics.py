"""Analytics API router - Aggregated, non-sensitive data only."""
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from pydantic import BaseModel
import logging
from ..services.db_service import get_db
from ..services.analytics_service import AnalyticsService
from ..services.user_analytics_service import UserAnalyticsService
from backend.fastapi.app.core import AuthorizationError, InternalServerError
from fastapi_cache.decorator import cache
from ..schemas import (
    AnalyticsSummary,
    TrendAnalytics,
    BenchmarkComparison,
    PopulationInsights,
    AnalyticsEventCreate,
    DashboardStatisticsResponse,
    ConversionRateKPI,
    RetentionKPI,
    ARPUKPI,
    KPISummary,
    UserAnalyticsSummary,
    UserTrendsResponse
)
from ..middleware.rate_limiter import rate_limit_analytics
from .auth import get_current_user
from ..models import User

logger = logging.getLogger("api.analytics")
router = APIRouter()


@router.post("/events", status_code=201, dependencies=[Depends(rate_limit_analytics)])
async def track_event(
    event: AnalyticsEventCreate,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Log a tracking event."""
    await AnalyticsService.log_event(db, event.model_dump(), ip_address=request.client.host)
    return {"status": "ok"}


@router.get("/summary", response_model=AnalyticsSummary, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_analytics_summary(db: AsyncSession = Depends(get_db)):
    """Get overall analytics summary."""
    summary = await AnalyticsService.get_overall_summary(db)
    return AnalyticsSummary(**summary)


@router.get("/trends", response_model=TrendAnalytics, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=1800)
async def get_trend_analytics(
    period: str = Query('monthly', pattern='^(daily|weekly|monthly)$'),
    limit: int = Query(12, ge=1, le=24),
    db: AsyncSession = Depends(get_db)
):
    """Get trend analytics over time."""
    trends = await AnalyticsService.get_trend_analytics(db, period_type=period, limit=limit)
    return TrendAnalytics(**trends)


@router.get("/benchmarks", response_model=List[BenchmarkComparison], dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_benchmark_comparison(db: AsyncSession = Depends(get_db)):
    """Get benchmark comparison data."""
    benchmarks = await AnalyticsService.get_benchmark_comparison(db)
    return [BenchmarkComparison(**b) for b in benchmarks]


@router.get("/insights", response_model=PopulationInsights, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_population_insights(db: AsyncSession = Depends(get_db)):
    """Get population-level insights."""
    insights = await AnalyticsService.get_population_insights(db)
    return PopulationInsights(**insights)


@router.get("/me/summary", response_model=UserAnalyticsSummary)
async def get_user_analytics_summary(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get personalized analytics summary for the current user."""
    return await UserAnalyticsService.get_dashboard_summary(db, current_user.id)


@router.get("/me/trends", response_model=UserTrendsResponse)
async def get_user_analytics_trends(
    days: int = Query(30, ge=7, le=365),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get time-series data for user charts."""
    eq_scores = await UserAnalyticsService.get_eq_trends(db, current_user.id, days)
    wellbeing = await UserAnalyticsService.get_wellbeing_trends(db, current_user.id, days)
    
    return UserTrendsResponse(
        eq_scores=eq_scores,
        wellbeing=wellbeing
    )


@router.get("/statistics", response_model=DashboardStatisticsResponse, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=1800)
async def get_dashboard_statistics(
    timeframe: str = Query('30d', pattern='^(7d|30d|90d)$'),
    exam_type: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None, pattern='^(positive|neutral|negative)$'),
    db: AsyncSession = Depends(get_db)
):
    """Get dashboard statistics with historical trends."""
    trends = await AnalyticsService.get_dashboard_statistics(db, timeframe=timeframe, exam_type=exam_type, sentiment=sentiment)
    return DashboardStatisticsResponse(historical_trends=trends)


@router.get("/age-groups", dependencies=[Depends(rate_limit_analytics)])
async def get_age_group_statistics(db: AsyncSession = Depends(get_db)):
    """Get detailed statistics by age group."""
    stats = await AnalyticsService.get_age_group_statistics(db)
    return {"age_group_statistics": stats}


@router.get("/distribution", dependencies=[Depends(rate_limit_analytics)])
async def get_score_distribution(db: AsyncSession = Depends(get_db)):
    """Get score distribution across ranges."""
    distribution = await AnalyticsService.get_score_distribution(db)
    return {"score_distribution": distribution}


@router.get("/kpis/conversion-rate", response_model=ConversionRateKPI, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_conversion_rate_kpi(
    period_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db)
):
    """Get Conversion Rate KPI."""
    return await AnalyticsService.calculate_conversion_rate(db, period_days)


@router.get("/kpis/retention-rate", response_model=RetentionKPI, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_retention_rate_kpi(
    period_days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db)
):
    """Get Retention Rate KPI."""
    return await AnalyticsService.calculate_retention_rate(db, period_days)


@router.get("/kpis/arpu", response_model=ARPUKPI, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=3600)
async def get_arpu_kpi(
    period_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db)
):
    """Get ARPU KPI."""
    return await AnalyticsService.calculate_arpu(db, period_days)


@router.get("/kpis/summary", response_model=KPISummary, dependencies=[Depends(rate_limit_analytics)])
@cache(expire=1800)
async def get_kpi_summary(
    conversion_period: int = Query(30, ge=1, le=365),
    retention_period: int = Query(7, ge=1, le=90),
    arpu_period: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db)
):
    """Get combined KPI summary."""
    kpi_summary = await AnalyticsService.get_kpi_summary(
        db,
        conversion_period,
        retention_period,
        arpu_period
    )
    return KPISummary(**kpi_summary)
