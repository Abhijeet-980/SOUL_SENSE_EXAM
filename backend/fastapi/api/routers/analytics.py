"""Analytics API router - Aggregated, non-sensitive data only."""
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel
import logging
from ..services.db_service import get_db
from ..services.analytics_service import AnalyticsService
from backend.fastapi.app.core import AuthorizationError, InternalServerError
from ..schemas import (
    AnalyticsSummary,
    TrendAnalytics,
    BenchmarkComparison,
    PopulationInsights,
    AnalyticsEventCreate,
    DashboardStatisticsResponse
)
from ..middleware.rate_limiter import rate_limit_analytics

logger = logging.getLogger("api.analytics")
router = APIRouter()


@router.post("/events", status_code=201, dependencies=[Depends(rate_limit_analytics)])
async def track_event(
    event: AnalyticsEventCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Log a tracking event (signup drop-off, etc).
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**:
    - No PII is logged (enforced by schema).
    - IP address is stored for security auditing.
    """
    AnalyticsService.log_event(db, event.dict(), ip_address=request.client.host)
    return {"status": "ok"}


@router.get("/summary", response_model=AnalyticsSummary, dependencies=[Depends(rate_limit_analytics)])
async def get_analytics_summary(db: Session = Depends(get_db)):
    """
    Get overall analytics summary with aggregated data only.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: This endpoint returns ONLY aggregated statistics.
    No individual user data or raw sensitive information is exposed.
    
    Returns:
    - Total assessment count
    - Unique user count (anonymized)
    - Global average scores
    - Age group statistics (aggregated)
    - Score distribution (aggregated)
    - Quality metrics (counts only)
    """
    summary = AnalyticsService.get_overall_summary(db)
    return AnalyticsSummary(**summary)


@router.get("/trends", response_model=TrendAnalytics, dependencies=[Depends(rate_limit_analytics)])
async def get_trend_analytics(
    period: str = Query('monthly', pattern='^(daily|weekly|monthly)$', description="Time period type"),
    limit: int = Query(12, ge=1, le=24, description="Number of periods to return"),
    db: Session = Depends(get_db)
):
    """
    Get trend analytics over time.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns aggregated time-series data only.
    No individual assessment data or user information.
    
    - **period**: Type of period (daily, weekly, monthly)
    - **limit**: Number of periods to return (max 24)
    
    Returns time-series data with:
    - Average scores per period
    - Assessment counts per period
    - Overall trend direction
    """
    trends = AnalyticsService.get_trend_analytics(db, period_type=period, limit=limit)
    return TrendAnalytics(**trends)


@router.get("/benchmarks", response_model=list[BenchmarkComparison], dependencies=[Depends(rate_limit_analytics)])
async def get_benchmark_comparison(db: Session = Depends(get_db)):
    """
    Get benchmark comparison data with percentiles.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns percentile-based aggregations only.
    No individual scores or user data exposed.
    
    Returns:
    - Global average score
    - 25th, 50th, 75th, 90th percentiles
    - Useful for comparing against population benchmarks
    """
    benchmarks = AnalyticsService.get_benchmark_comparison(db)
    return [BenchmarkComparison(**b) for b in benchmarks]


@router.get("/insights", response_model=PopulationInsights, dependencies=[Depends(rate_limit_analytics)])
async def get_population_insights(db: Session = Depends(get_db)):
    """
    Get population-level insights.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns population-level aggregations only.
    No individual user data or sensitive information.
    
    Returns:
    - Most common age group
    - Highest performing age group (by average)
    - Total population size
    - Assessment completion rate
    """
    insights = AnalyticsService.get_population_insights(db)
    return PopulationInsights(**insights)

@router.get("/statistics", response_model=DashboardStatisticsResponse, dependencies=[Depends(rate_limit_analytics)])
async def get_dashboard_statistics(
    timeframe: str = Query('30d', pattern='^(7d|30d|90d)$', description="Time period for historical data"),
    exam_type: Optional[str] = Query(None, description="Filter by exam type"),
    sentiment: Optional[str] = Query(None, pattern='^(positive|neutral|negative)$', description="Filter by sentiment"),
    db: Session = Depends(get_db)
):
    """
    Get dashboard statistics with historical trends.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns aggregated historical data only.
    No individual user information.
    
    - **timeframe**: Time period (7d, 30d, 90d)
    - **exam_type**: Optional filter by exam type
    - **sentiment**: Optional filter by sentiment (positive, neutral, negative)
    
    Returns historical trends with scores over time.
    """
    trends = AnalyticsService.get_dashboard_statistics(db, timeframe=timeframe, exam_type=exam_type, sentiment=sentiment)
    return DashboardStatisticsResponse(historical_trends=trends)

@router.get("/age-groups", dependencies=[Depends(rate_limit_analytics)])
async def get_age_group_statistics(db: Session = Depends(get_db)):
    """
    Get detailed statistics by age group.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns aggregated statistics per age group.
    No individual assessment data.
    
    Returns for each age group:
    - Total assessments
    - Average score
    - Min/max scores
    - Average sentiment
    """
    stats = AnalyticsService.get_age_group_statistics(db)
    return {"age_group_statistics": stats}


@router.get("/distribution", dependencies=[Depends(rate_limit_analytics)])
async def get_score_distribution(db: Session = Depends(get_db)):
    """
    Get score distribution across ranges.
    
    **Rate Limited**: 30 requests per minute per IP
    
    **Data Privacy**: Returns distribution counts only.
    No individual scores or user information.
    
    Returns distribution of scores in ranges:
    - 0-10, 11-20, 21-30, 31-40
    - Count and percentage for each range
    """

    distribution = AnalyticsService.get_score_distribution(db)
    return {"score_distribution": distribution}


# ============================================================================
# User Analytics Endpoints (PR 6.3)
# ============================================================================

from ..services.user_analytics_service import UserAnalyticsService
from ..schemas import UserAnalyticsSummary, UserTrendsResponse, DashboardStatisticsResponse
from ..models import User
from .auth import get_current_user

@router.get("/me/summary", response_model=UserAnalyticsSummary)
async def get_user_analytics_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get personalized analytics summary for the current user.
    
    Returns:
    - Total exams taken
    - Average score
    - Latest & Best scores
    - Trends and consistency analysis
    """
    return UserAnalyticsService.get_dashboard_summary(db, current_user.id)


@router.get("/me/trends", response_model=UserTrendsResponse)
async def get_user_analytics_trends(
    days: int = Query(30, ge=7, le=365, description="Number of days to analyze"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get time-series data for user charts.
    
    Params:
    - days: Number of days to look back (default 30, max 365)
    
    Returns:
    - EQ Score history
    - Wellbeing metrics (Sleep, Stress, etc.)
    """
    eq_scores = UserAnalyticsService.get_eq_trends(db, current_user.id, days)
    wellbeing = UserAnalyticsService.get_wellbeing_trends(db, current_user.id, days)
    
    return UserTrendsResponse(
        eq_scores=eq_scores,
        wellbeing=wellbeing
    )


@router.get("/statistics", response_model=DashboardStatisticsResponse)
async def get_dashboard_statistics(
    timeframe: str = Query("30d", description="Timeframe for data (e.g., 7d, 30d, 90d)"),
    exam_type: Optional[str] = Query(None, description="Filter by exam type"),
    sentiment: Optional[str] = Query(None, description="Filter by sentiment"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get dashboard statistics with historical trends.
    
    Params:
    - timeframe: Time period (7d, 30d, 90d)
    - exam_type: Optional filter by exam type
    - sentiment: Optional filter by sentiment
    
    Returns:
    - historical_trends: List of EQ score points
    """
    # Parse timeframe to days
    days_map = {"7d": 7, "30d": 30, "90d": 90}
    days = days_map.get(timeframe, 30)
    
    # For now, ignore exam_type and sentiment filters as they are not implemented in the data model
    # TODO: Add filtering logic when exam types and sentiments are added
    eq_scores = UserAnalyticsService.get_eq_trends(db, current_user.id, days)
    
    return DashboardStatisticsResponse(historical_trends=eq_scores)


# ============================================================================
# Advanced Analytics Endpoints (Feature #804)
# ============================================================================

from ..ml.analytics_service import AnalyticsService as AdvancedAnalyticsService

@router.get("/me/patterns")
async def get_user_patterns(
    time_range: str = Query("90d", pattern="^(30d|90d|6m|1y)$", description="Time range for pattern analysis"),
    current_user: User = Depends(get_current_user)
):
    """
    Get detected emotional patterns for the current user.

    **Authentication Required**
    **Privacy Notice**: This feature analyzes your personal emotional data to identify patterns.
    Only you can access this analysis. Data is processed locally and not shared.

    **Privacy Settings Required**: You must enable "Pattern Analysis" in your privacy settings.

    Params:
    - time_range: Time period to analyze (30d, 90d, 6m, 1y)

    Returns:
    - patterns: List of detected patterns with confidence scores
    - data_points: Number of data points analyzed
    - analysis_timestamp: When analysis was performed
    - privacy_notice: Reminder of data privacy practices
    """
    # Check privacy settings
    if not current_user.settings or not current_user.settings.pattern_analysis_enabled:
        raise AuthorizationError(
            message="Pattern analysis is disabled in your privacy settings. Please enable it in your account settings to use this feature.",
            code="PRIVACY_SETTING_DISABLED"
        )

    from ..ml.pattern_recognition import PatternRecognitionService
    service = PatternRecognitionService()
    result = service.detect_temporal_patterns(current_user.username, time_range)

    # Add privacy notice to response
    result["privacy_notice"] = "This analysis is based solely on your personal data and is not shared with anyone. You can disable this feature at any time in your privacy settings."
    return result


@router.get("/me/correlations")
async def get_user_correlations(
    metrics: str = Query(None, description="Comma-separated list of metrics to correlate"),
    current_user: User = Depends(get_current_user)
):
    """
    Get correlation matrix for user's emotional metrics.

    **Authentication Required**
    **Privacy Notice**: This feature analyzes relationships between your emotional metrics.
    Statistical correlations are calculated using scientific methods with p-values for significance.
    Only you can access this analysis.

    **Privacy Settings Required**: You must enable "Correlation Analysis" in your privacy settings.

    Params:
    - metrics: Optional comma-separated list of metrics (eq_score, sleep_hours, stress_level, energy_level, screen_time)

    Returns:
    - correlation_matrix: Matrix of correlations between metrics (Pearson and Spearman)
    - significant_correlations: List of statistically significant correlations (p < 0.05)
    - data_points: Number of data points used
    - statistical_notes: Explanation of statistical methods used
    - privacy_notice: Reminder of data privacy practices
    """
    # Check privacy settings
    if not current_user.settings or not current_user.settings.correlation_analysis_enabled:
        raise AuthorizationError(
            message="Correlation analysis is disabled in your privacy settings. Please enable it in your account settings to use this feature.",
            code="PRIVACY_SETTING_DISABLED"
        )

    from ..ml.analytics_service import AnalyticsService
    service = AnalyticsService()

    metrics_list = None
    if metrics:
        metrics_list = [m.strip() for m in metrics.split(",")]

    result = service.get_correlation_matrix(current_user.username, metrics_list)

    # Add privacy notice to response
    result["privacy_notice"] = "Correlations are calculated using statistical methods and are based solely on your personal data. Results include confidence intervals and p-values for scientific rigor."
    return result


@router.get("/me/forecast")
async def get_user_forecast(
    days: int = Query(7, ge=1, le=30, description="Number of days to forecast"),
    current_user: User = Depends(get_current_user)
):
    """
    Get emotional forecast for the current user.

    **Authentication Required**
    **Privacy Notice**: This feature uses advanced time series forecasting (ARIMA/Prophet models)
    to predict future emotional patterns based on your historical data.
    Forecasts include confidence intervals for uncertainty quantification.

    **Privacy Settings Required**: You must enable "Forecast Analysis" in your privacy settings.

    Params:
    - days: Number of days to forecast (1-30)

    Returns:
    - predictions: Forecasted mood scores with confidence intervals
    - model_used: Forecasting model employed (ARIMA, Prophet, or Linear Trend)
    - data_points_used: Historical data points analyzed
    - forecast_method: Description of forecasting approach
    - privacy_notice: Reminder of data privacy practices
    """
    # Check privacy settings
    if not current_user.settings or not current_user.settings.forecast_enabled:
        raise AuthorizationError(
            message="Forecast analysis is disabled in your privacy settings. Please enable it in your account settings to use this feature.",
            code="PRIVACY_SETTING_DISABLED"
        )

    from ..ml.analytics_service import AnalyticsService
    service = AnalyticsService()
    result = service.get_emotional_forecast(current_user.username, days)

    # Add privacy notice to response
    result["privacy_notice"] = "Forecasts are generated using statistical time series models and are based solely on your personal historical data. Confidence intervals indicate prediction uncertainty."
    return result


@router.get("/me/benchmarks")
async def get_user_benchmarks(
    age_group: str = Query(None, description="Age group for comparison (optional)"),
    current_user: User = Depends(get_current_user)
):
    """
    Get comparative benchmarks for the current user.

    **Authentication Required**
    **Privacy Notice**: This feature compares your anonymized data against aggregated benchmarks
    from other users who have opted into benchmark sharing. Your personal data is never directly shared.

    **Privacy Settings Required**: You must enable "Benchmark Sharing" in your privacy settings.
    **Opt-in Required**: Only data from users who have explicitly opted into sharing is included.

    Params:
    - age_group: Optional age group filter

    Returns:
    - benchmarks: Your percentile ranking compared to anonymized group data
    - insights: Benchmark-based insights and comparisons
    - privacy_notice: Detailed explanation of anonymization and opt-in practices
    - data_sources: Description of benchmark data sources and anonymization methods
    """
    # Check privacy settings
    if not current_user.settings or not current_user.settings.benchmark_sharing_enabled:
        raise AuthorizationError(
            message="Benchmark sharing is disabled in your privacy settings. Please enable it in your account settings to use this feature.",
            code="PRIVACY_SETTING_DISABLED"
        )

    from ..ml.analytics_service import AnalyticsService
    service = AnalyticsService()
    result = service.get_comparative_benchmarks(current_user.username, age_group)

    # Add detailed privacy notice to response
    result["privacy_notice"] = "Benchmarks are calculated from fully anonymized, aggregated data from users who have explicitly opted into sharing. Individual data points are never exposed, and comparisons use percentile rankings only."
    result["data_sources"] = "Data is sourced from users who have enabled benchmark sharing in their privacy settings. All personal identifiers are removed, and results are aggregated before comparison."
    result["opt_in_required"] = "Only users who have explicitly consented to benchmark sharing contribute to these comparisons."
    return result


@router.get("/me/recommendations")
async def get_user_recommendations(current_user: User = Depends(get_current_user)):
    """
    Get personalized recommendations and insights.

    **Authentication Required**
    **Privacy Notice**: This feature generates personalized insights and interventions
    based on your emotional patterns and historical data. Recommendations are tailored
    specifically to your patterns and are designed to support your emotional wellbeing.

    **Privacy Settings Required**: You must enable "Recommendation Engine" in your privacy settings.

    Returns:
    - insights: Pattern-based insights and recommendations
    - interventions: Suggested interventions based on risk level assessment
    - journal_prompts: Personalized journaling prompts for reflection
    - risk_level: Assessed risk level (low, medium, high) based on patterns
    - privacy_notice: Reminder of personalization and privacy practices
    """
    # Check privacy settings
    if not current_user.settings or not current_user.settings.recommendation_engine_enabled:
        raise AuthorizationError(
            message="Recommendation engine is disabled in your privacy settings. Please enable it in your account settings to use this feature.",
            code="PRIVACY_SETTING_DISABLED"
        )

    from ..ml.analytics_service import AnalyticsService
    service = AnalyticsService()
    result = service.get_personalized_recommendations(current_user.username)

    # Add privacy notice to response
    result["privacy_notice"] = "Recommendations are generated based on your personal emotional patterns and are designed to support your wellbeing. All analysis remains private to you."
    return result


@router.get("/me/dashboard")
async def get_user_analytics_dashboard(current_user: User = Depends(get_current_user)):
    """
    Get complete analytics dashboard for the current user.

    **Authentication Required**

    Returns:
    - forecast: Emotional forecast data
    - correlations: Correlation analysis
    - recommendations: Personalized insights
    - patterns: Detected patterns
    - triggers: Emotional triggers analysis
    """
    from ..ml.analytics_service import AnalyticsService
    service = AnalyticsService()
    return service.get_analytics_dashboard(current_user.username)


@router.post("/me/feedback")
async def submit_analytics_feedback(
    feedback: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Submit feedback on analytics recommendations.

    **Authentication Required**

    Body:
    - insight_id: ID of the insight being rated
    - rating: Rating (1-5)
    - helpful: Boolean indicating if helpful
    - comments: Optional comments

    Returns:
    - status: Success confirmation
    """
    # Store feedback for improving recommendations
    # This would typically go to a feedback table
    logger.info(f"Analytics feedback from {current_user.username}: {feedback}")

    return {"status": "success", "message": "Feedback recorded"}


# ============================================================================
# Performance Monitoring Endpoints
# ============================================================================

class WebVitalsMetric(BaseModel):
    """Web Vitals metric model."""
    name: str
    value: float
    rating: str
    timestamp: int
    url: str
    user_agent: Optional[str] = None


class PerformanceSummary(BaseModel):
    """Performance summary for analytics."""
    url: str
    metrics: dict
    user_agent: Optional[str] = None


@router.post("/web-vitals", status_code=201)
async def track_web_vitals(
    metric: WebVitalsMetric,
    request: Request
):
    """
    Track Web Vitals metrics from the frontend.

    Args:
        metric: Web Vitals metric data

    Returns:
        Confirmation message

    Logs performance metrics without storing PII.
    In production, integrate with analytics service.
    """
    try:
        logger.info(
            f"Web Vitals - {metric.name}: {metric.value:.2f}ms "
            f"({metric.rating}) - {metric.url}"
        )

        return {
            "status": "success",
            "message": "Metric recorded",
            "metric": metric.name,
            "value": metric.value,
            "rating": metric.rating,
        }

    except Exception as e:
        logger.error(f"Error tracking Web Vitals: {e}")
        raise InternalServerError(message="Failed to track metric")


@router.post("/performance-summary", status_code=201)
async def track_performance_summary(
    summary: PerformanceSummary
):
    """
    Track performance summary from the frontend.

    Args:
        summary: Performance summary data

    Returns:
        Confirmation message
    """
    try:
        logger.info(f"Performance Summary for {summary.url}")
        logger.debug(f"Metrics: {summary.metrics}")

        return {
            "status": "success",
            "message": "Performance summary recorded",
        }

    except Exception as e:
        logger.error(f"Error tracking performance summary: {e}")
        raise InternalServerError(message="Failed to track summary")
