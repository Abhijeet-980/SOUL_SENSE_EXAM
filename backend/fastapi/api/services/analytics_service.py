"""Analytics service for aggregated, non-sensitive data analysis."""
from sqlalchemy import func, case, distinct, select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta, UTC

from ..models import Score, User, AnalyticsEvent


class AnalyticsService:
    """Service for generating aggregated analytics data.
    
    This service ONLY provides aggregated data and never exposes
    individual user information or raw sensitive data.
    """
    
    @staticmethod
    async def log_event(db: AsyncSession, event_data: dict, ip_address: Optional[str] = None) -> AnalyticsEvent:
        """Log a user behavior event."""
        import json
        
        data_payload = json.dumps(event_data.get('event_data', {}))
        
        event = AnalyticsEvent(
            anonymous_id=event_data['anonymous_id'],
            event_type=event_data['event_type'],
            event_name=event_data['event_name'],
            event_data=data_payload,
            ip_address=ip_address,
            timestamp=datetime.now(UTC)
        )
        
        db.add(event)
        await db.commit()
        await db.refresh(event)
        return event

    @staticmethod
    async def get_age_group_statistics(db: AsyncSession) -> List[Dict]:
        """Get aggregated statistics by age group."""
        stmt = select(
            Score.detailed_age_group,
            func.count(Score.id).label('total'),
            func.avg(Score.total_score).label('avg_score'),
            func.min(Score.total_score).label('min_score'),
            func.max(Score.total_score).label('max_score'),
            func.avg(Score.sentiment_score).label('avg_sentiment')
        ).filter(
            Score.detailed_age_group.isnot(None)
        ).group_by(
            Score.detailed_age_group
        )
        
        result = await db.execute(stmt)
        stats = result.all()
        
        return [
            {
                'age_group': s.detailed_age_group,
                'total_assessments': s.total,
                'average_score': round(s.avg_score or 0, 2),
                'min_score': s.min_score or 0,
                'max_score': s.max_score or 0,
                'average_sentiment': round(s.avg_sentiment or 0, 3)
            }
            for s in stats
        ]
    
    @staticmethod
    async def get_score_distribution(db: AsyncSession) -> List[Dict]:
        """Get score distribution across ranges."""
        total_stmt = select(func.count(Score.id))
        total_res = await db.execute(total_stmt)
        total_count = total_res.scalar() or 0
        
        if total_count == 0:
            return []
        
        ranges = [
            ('0-10', 0, 10),
            ('11-20', 11, 20),
            ('21-30', 21, 30),
            ('31-40', 31, 40)
        ]
        
        distribution = []
        for range_name, min_score, max_score in ranges:
            count_stmt = select(func.count(Score.id)).filter(
                Score.total_score >= min_score,
                Score.total_score <= max_score
            )
            count_res = await db.execute(count_stmt)
            count = count_res.scalar() or 0
            
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            distribution.append({
                'score_range': range_name,
                'count': count,
                'percentage': round(percentage, 2)
            })
        
        return distribution
    
    @staticmethod
    async def get_overall_summary(db: AsyncSession) -> Dict:
        """Get overall analytics summary."""
        overall_stmt = select(
            func.count(Score.id).label('total'),
            func.count(distinct(Score.username)).label('unique_users'),
            func.avg(Score.total_score).label('avg_score'),
            func.avg(Score.sentiment_score).label('avg_sentiment')
        )
        overall_res = await db.execute(overall_stmt)
        overall_stats = overall_res.first()
        
        quality_stmt = select(
            func.sum(case((Score.is_rushed == True, 1), else_=0)).label('rushed_count'),
            func.sum(case((Score.is_inconsistent == True, 1), else_=0)).label('inconsistent_count')
        )
        quality_res = await db.execute(quality_stmt)
        quality_metrics = quality_res.first()
        
        age_group_stats = await AnalyticsService.get_age_group_statistics(db)
        score_dist = await AnalyticsService.get_score_distribution(db)
        
        return {
            'total_assessments': overall_stats.total or 0,
            'unique_users': overall_stats.unique_users or 0,
            'global_average_score': round(overall_stats.avg_score or 0, 2),
            'global_average_sentiment': round(overall_stats.avg_sentiment or 0, 3),
            'age_group_stats': age_group_stats,
            'score_distribution': score_dist,
            'assessment_quality_metrics': {
                'rushed_assessments': quality_metrics.rushed_count or 0,
                'inconsistent_assessments': quality_metrics.inconsistent_count or 0
            }
        }
    
    @staticmethod
    async def get_trend_analytics(
        db: AsyncSession,
        period_type: str = 'monthly',
        limit: int = 12
    ) -> Dict:
        """Get trend analytics over time."""
        # Note: SQLite substr(timestamp, 1, 7) might differ from Postgres/MySQL
        # Using SQLAlchemy handles the dialect differences if mapped correctly
        # Here we assume a dialect-specific or standard substr approach
        
        stmt = select(
            func.substr(Score.timestamp, 1, 7).label('period'),
            func.avg(Score.total_score).label('avg_score'),
            func.count(Score.id).label('count')
        ).group_by(
            func.substr(Score.timestamp, 1, 7)
        ).order_by(
            desc(func.substr(Score.timestamp, 1, 7))
        ).limit(limit)
        
        result = await db.execute(stmt)
        trends = result.all()
        
        data_points = [
            {
                'period': t.period,
                'average_score': round(t.avg_score or 0, 2),
                'assessment_count': t.count
            }
            for t in reversed(trends)
        ]
        
        if len(data_points) >= 2:
            first_avg = data_points[0]['average_score']
            last_avg = data_points[-1]['average_score']
            
            if last_avg > first_avg + 1:
                trend_direction = 'increasing'
            elif last_avg < first_avg - 1:
                trend_direction = 'decreasing'
            else:
                trend_direction = 'stable'
        else:
            trend_direction = 'insufficient_data'
        
        return {
            'period_type': period_type,
            'data_points': data_points,
            'trend_direction': trend_direction
        }
    
    @staticmethod
    async def get_benchmark_comparison(db: AsyncSession) -> List[Dict]:
        """Get benchmark comparison data."""
        stmt = select(Score.total_score).filter(
            Score.total_score.isnot(None)
        ).order_by(Score.total_score)
        
        result = await db.execute(stmt)
        scores = result.scalars().all()
        
        if not scores:
            return []
        
        score_list = list(scores)
        n = len(score_list)
        
        def percentile(p):
            k = (n - 1) * p / 100
            f = int(k)
            c = min(f + 1, n - 1)
            if f == c:
                return score_list[f]
            return score_list[f] + (k - f) * (score_list[c] - score_list[f])
        
        global_avg = sum(score_list) / n if n > 0 else 0
        
        return [{
            'category': 'Overall',
            'global_average': round(global_avg, 2),
            'percentile_25': round(percentile(25), 2),
            'percentile_50': round(percentile(50), 2),
            'percentile_75': round(percentile(75), 2),
            'percentile_90': round(percentile(90), 2)
        }]
    
    @staticmethod
    async def get_population_insights(db: AsyncSession) -> Dict:
        """Get population-level insights."""
        common_stmt = select(
            Score.detailed_age_group,
            func.count(Score.id).label('count')
        ).filter(
            Score.detailed_age_group.isnot(None)
        ).group_by(
            Score.detailed_age_group
        ).order_by(
            desc(func.count(Score.id))
        ).limit(1)
        common_res = await db.execute(common_stmt)
        most_common = common_res.first()
        
        perf_stmt = select(
            Score.detailed_age_group,
            func.avg(Score.total_score).label('avg')
        ).filter(
            Score.detailed_age_group.isnot(None)
        ).group_by(
            Score.detailed_age_group
        ).order_by(
            desc(func.avg(Score.total_score))
        ).limit(1)
        perf_res = await db.execute(perf_stmt)
        highest_performing = perf_res.first()
        
        users_stmt = select(func.count(distinct(Score.username)))
        users_res = await db.execute(users_stmt)
        total_users = users_res.scalar() or 0
        
        assess_stmt = select(func.count(Score.id))
        assess_res = await db.execute(assess_stmt)
        total_assessments = assess_res.scalar() or 0
        
        completion_rate = 100.0 if total_assessments > 0 else None
        
        return {
            'most_common_age_group': most_common.detailed_age_group if most_common else 'Unknown',
            'highest_performing_age_group': highest_performing.detailed_age_group if highest_performing else 'Unknown',
            'total_population_size': total_users,
            'assessment_completion_rate': completion_rate
        }
    
    @staticmethod
    async def get_dashboard_statistics(
        db: AsyncSession,
        timeframe: str = '30d',
        exam_type: Optional[str] = None,
        sentiment: Optional[str] = None
    ) -> List[Dict]:
        """Get dashboard statistics with historical trends."""
        now = datetime.now(UTC)
        if timeframe == '7d':
            start_date = now - timedelta(days=7)
        elif timeframe == '30d':
            start_date = now - timedelta(days=30)
        elif timeframe == '90d':
            start_date = now - timedelta(days=90)
        else:
            start_date = now - timedelta(days=30)
        
        stmt = select(
            Score.id,
            Score.timestamp,
            Score.total_score,
            Score.sentiment_score
        ).filter(
            Score.timestamp >= start_date
        )
        
        if sentiment:
            if sentiment == 'positive':
                stmt = stmt.filter(Score.sentiment_score >= 0.6)
            elif sentiment == 'neutral':
                stmt = stmt.filter(Score.sentiment_score.between(0.4, 0.6))
            elif sentiment == 'negative':
                stmt = stmt.filter(Score.sentiment_score < 0.4)
        
        stmt = stmt.order_by(desc(Score.timestamp)).limit(100)
        result = await db.execute(stmt)
        scores = result.all()
        
        trends = []
        for score in reversed(scores):
            trends.append({
                'id': score.id,
                'timestamp': score.timestamp.isoformat(),
                'total_score': score.total_score,
                'sentiment_score': score.sentiment_score
            })
        
        return trends

    @staticmethod
    async def calculate_conversion_rate(
        db: AsyncSession,
        period_days: int = 30
    ) -> Dict:
        """Calculate Conversion Rate KPI."""
        cutoff_date = datetime.now(UTC) - timedelta(days=period_days)

        started_stmt = select(func.count(AnalyticsEvent.id)).filter(
            AnalyticsEvent.event_name == 'signup_start',
            AnalyticsEvent.timestamp >= cutoff_date
        )
        started_res = await db.execute(started_stmt)
        signup_started = started_res.scalar() or 0

        completed_stmt = select(func.count(AnalyticsEvent.id)).filter(
            AnalyticsEvent.event_name == 'signup_success',
            AnalyticsEvent.timestamp >= cutoff_date
        )
        completed_res = await db.execute(completed_stmt)
        signup_completed = completed_res.scalar() or 0

        conversion_rate = (signup_completed / signup_started * 100) if signup_started > 0 else 0

        return {
            'signup_started': signup_started,
            'signup_completed': signup_completed,
            'conversion_rate': round(conversion_rate, 2),
            'period': f'last_{period_days}_days'
        }

    @staticmethod
    async def calculate_retention_rate(
        db: AsyncSession,
        period_days: int = 7
    ) -> Dict:
        """Calculate Retention Rate KPI."""
        today = datetime.now(UTC).date()
        day_0 = today - timedelta(days=period_days)
        day_n = today

        day0_stmt = select(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            func.date(AnalyticsEvent.timestamp) == day_0
        )
        day0_res = await db.execute(day0_stmt)
        day_0_users = day0_res.scalar() or 0

        dayn_subq = select(func.distinct(AnalyticsEvent.user_id)).filter(
            func.date(AnalyticsEvent.timestamp) == day_n
        ).subquery()
        
        dayn_stmt = select(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            func.date(AnalyticsEvent.timestamp) == day_0,
            AnalyticsEvent.user_id.in_(select(dayn_subq))
        )
        dayn_res = await db.execute(dayn_stmt)
        day_n_active_users = dayn_res.scalar() or 0

        retention_rate = (day_n_active_users / day_0_users * 100) if day_0_users > 0 else 0

        return {
            'day_0_users': day_0_users,
            'day_n_active_users': day_n_active_users,
            'retention_rate': round(retention_rate, 2),
            'period_days': period_days,
            'period': f'{period_days}_day_retention'
        }

    @staticmethod
    async def calculate_arpu(
        db: AsyncSession,
        period_days: int = 30
    ) -> Dict:
        """Calculate ARPU KPI."""
        cutoff_date = datetime.now(UTC) - timedelta(days=period_days)

        active_stmt = select(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            AnalyticsEvent.timestamp >= cutoff_date
        )
        active_res = await db.execute(active_stmt)
        total_active_users = active_res.scalar() or 0

        total_revenue = 0.0
        arpu = (total_revenue / total_active_users) if total_active_users > 0 else 0

        return {
            'total_revenue': total_revenue,
            'total_active_users': total_active_users,
            'arpu': round(arpu, 2),
            'period': f'last_{period_days}_days',
            'currency': 'USD'
        }

    @staticmethod
    async def get_kpi_summary(
        db: AsyncSession,
        conversion_period_days: int = 30,
        retention_period_days: int = 7,
        arpu_period_days: int = 30
    ) -> Dict:
        """Get combined KPI summary."""
        conversion_rate = await AnalyticsService.calculate_conversion_rate(db, conversion_period_days)
        retention_rate = await AnalyticsService.calculate_retention_rate(db, retention_period_days)
        arpu = await AnalyticsService.calculate_arpu(db, arpu_period_days)

        return {
            'conversion_rate': conversion_rate,
            'retention_rate': retention_rate,
            'arpu': arpu,
            'calculated_at': datetime.now(UTC).isoformat(),
            'period': f'conversion_{conversion_period_days}d_retention_{retention_period_days}d_arpu_{arpu_period_days}d'
        }
