"""Analytics service for aggregated, non-sensitive data analysis."""
from sqlalchemy import func, case, distinct
from sqlalchemy.orm import Session
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta

# Import models from models module
from ..models import Score, User, AnalyticsEvent


class AnalyticsService:
    """Service for generating aggregated analytics data.
    
    This service ONLY provides aggregated data and never exposes
    individual user information or raw sensitive data.
    """
    
    @staticmethod
    def log_event(db: Session, event_data: dict, ip_address: Optional[str] = None) -> AnalyticsEvent:
        """
        Log a user behavior event.
        """
        print(f"DEBUG: log_event called with event_data keys: {list(event_data.keys())}")
        import json
        
        # Serialize event_data JSON
        data_payload = json.dumps(event_data.get('event_data', {}))
        
        event = AnalyticsEvent(
            anonymous_id=event_data['anonymous_id'],
            event_type=event_data['event_type'],
            event_name=event_data['event_name'],
            event_data=data_payload,
            ip_address=ip_address,
            timestamp=datetime.utcnow()
        )
        
        db.add(event)
        db.commit()
        db.refresh(event)
        return event

    @staticmethod
    def get_age_group_statistics(db: Session) -> List[Dict]:
        """
        Get aggregated statistics by age group.
        
        Returns only aggregated data - no individual records.
        """
        stats = db.query(
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
        ).all()
        
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
    def get_score_distribution(db: Session) -> List[Dict]:
        """
        Get score distribution across ranges.
        
        Returns aggregated distribution - no individual scores.
        """
        total_count = db.query(func.count(Score.id)).scalar() or 0
        
        if total_count == 0:
            return []
        
        # Define score ranges
        ranges = [
            ('0-10', 0, 10),
            ('11-20', 11, 20),
            ('21-30', 21, 30),
            ('31-40', 31, 40)
        ]
        
        distribution = []
        for range_name, min_score, max_score in ranges:
            count = db.query(func.count(Score.id)).filter(
                Score.total_score >= min_score,
                Score.total_score <= max_score
            ).scalar() or 0
            
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            distribution.append({
                'score_range': range_name,
                'count': count,
                'percentage': round(percentage, 2)
            })
        
        return distribution
    
    @staticmethod
    def get_overall_summary(db: Session) -> Dict:
        """
        Get overall analytics summary.
        
        Returns aggregated metrics only - no individual user data.
        """
        # Overall statistics
        overall_stats = db.query(
            func.count(Score.id).label('total'),
            func.count(distinct(Score.username)).label('unique_users'),
            func.avg(Score.total_score).label('avg_score'),
            func.avg(Score.sentiment_score).label('avg_sentiment')
        ).first()
        
        # Quality metrics (aggregated counts)
        quality_metrics = db.query(
            func.sum(case((Score.is_rushed == True, 1), else_=0)).label('rushed_count'),
            func.sum(case((Score.is_inconsistent == True, 1), else_=0)).label('inconsistent_count')
        ).first()
        
        # Age group stats
        age_group_stats = AnalyticsService.get_age_group_statistics(db)
        
        # Score distribution
        score_dist = AnalyticsService.get_score_distribution(db)
        
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
    def get_trend_analytics(
        db: Session,
        period_type: str = 'monthly',
        limit: int = 12
    ) -> Dict:
        """
        Get trend analytics over time.
        
        Args:
            period_type: Type of period (daily, weekly, monthly)
            limit: Number of periods to return
            
        Returns aggregated time-series data.
        """
        # For simplicity, we'll do monthly trends
        # In production, you'd want more sophisticated date handling
        
        trends = db.query(
            func.substr(Score.timestamp, 1, 7).label('period'),  # YYYY-MM
            func.avg(Score.total_score).label('avg_score'),
            func.count(Score.id).label('count')
        ).group_by(
            func.substr(Score.timestamp, 1, 7)
        ).order_by(
            func.substr(Score.timestamp, 1, 7).desc()
        ).limit(limit).all()
        
        data_points = [
            {
                'period': t.period,
                'average_score': round(t.avg_score or 0, 2),
                'assessment_count': t.count
            }
            for t in reversed(trends)  # Chronological order
        ]
        
        # Determine trend direction
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
    def get_benchmark_comparison(db: Session) -> List[Dict]:
        """
        Get benchmark comparison data.
        
        Returns percentile-based benchmarks - no individual data.
        """
        # Get all scores for percentile calculation
        scores = db.query(Score.total_score).filter(
            Score.total_score.isnot(None)
        ).order_by(Score.total_score).all()
        
        if not scores:
            return []
        
        score_list = [s.total_score for s in scores]
        n = len(score_list)
        
        def percentile(p):
            """Calculate percentile value"""
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
    def get_population_insights(db: Session) -> Dict:
        """
        Get population-level insights.
        
        Returns aggregated population metrics - no individual data.
        """
        # Most common age group
        most_common = db.query(
            Score.detailed_age_group,
            func.count(Score.id).label('count')
        ).filter(
            Score.detailed_age_group.isnot(None)
        ).group_by(
            Score.detailed_age_group
        ).order_by(
            func.count(Score.id).desc()
        ).first()
        
        # Highest performing age group
        highest_performing = db.query(
            Score.detailed_age_group,
            func.avg(Score.total_score).label('avg')
        ).filter(
            Score.detailed_age_group.isnot(None)
        ).group_by(
            Score.detailed_age_group
        ).order_by(
            func.avg(Score.total_score).desc()
        ).first()
        
        # Total population
        total_users = db.query(func.count(distinct(Score.username))).scalar() or 0
        total_assessments = db.query(func.count(Score.id)).scalar() or 0
        
        # Completion rate (simplified - assumes all scores are completed)
        completion_rate = 100.0 if total_assessments > 0 else None
        
        return {
            'most_common_age_group': most_common.detailed_age_group if most_common else 'Unknown',
            'highest_performing_age_group': highest_performing.detailed_age_group if highest_performing else 'Unknown',
            'total_population_size': total_users,
            'assessment_completion_rate': completion_rate
        }
    
    @staticmethod
    def get_dashboard_statistics(
        db: Session,
        timeframe: str = '30d',
        exam_type: Optional[str] = None,
        sentiment: Optional[str] = None
    ) -> List[Dict]:
        """
        Get dashboard statistics with historical trends.
        
        Args:
            db: Database session
            timeframe: Time period ('7d', '30d', '90d')
            exam_type: Optional filter by exam type
            sentiment: Optional filter by sentiment
            
        Returns:
            List of historical trend data points
        """
        # Calculate date range
        now = datetime.utcnow()
        if timeframe == '7d':
            start_date = now - timedelta(days=7)
        elif timeframe == '30d':
            start_date = now - timedelta(days=30)
        elif timeframe == '90d':
            start_date = now - timedelta(days=90)
        else:
            start_date = now - timedelta(days=30)  # default to 30 days
        
        # Build query
        query = db.query(
            Score.id,
            Score.timestamp,
            Score.total_score,
            Score.sentiment_score
        ).filter(
            Score.timestamp >= start_date
        )
        
        # Apply filters
        if exam_type:
            # For now, we'll assume exam_type is stored somewhere or we can filter by other criteria
            # This might need to be adjusted based on your data model
            pass
            
        if sentiment:
            if sentiment == 'positive':
                query = query.filter(Score.sentiment_score >= 0.6)
            elif sentiment == 'neutral':
                query = query.filter(Score.sentiment_score.between(0.4, 0.6))
            elif sentiment == 'negative':
                query = query.filter(Score.sentiment_score < 0.4)
        
        # Order by timestamp
        query = query.order_by(Score.timestamp.desc())
        
        results = query.limit(100).all()  # Limit to prevent too much data
        
        # Format for response
        trends = []
        for score in reversed(results):  # Chronological order
            trends.append({
                'id': score.id,
                'timestamp': score.timestamp.isoformat(),
                'total_score': score.total_score,
                'sentiment_score': score.sentiment_score
            })
        
        return trends

    # ============================================================================
    # KPI Calculations (Issue #981)
    # ============================================================================

    @staticmethod
    def calculate_conversion_rate(
        db: Session,
        period_days: int = 30
    ) -> Dict:
        """
        Calculate Conversion Rate KPI: (signup_completed / signup_started) * 100

        Args:
            db: Database session
            period_days: Number of days to look back for calculation

        Returns:
            Dictionary with conversion rate metrics
        """
        cutoff_date = datetime.utcnow() - timedelta(days=period_days)

        # Count signup started events (signup_start event)
        signup_started = db.query(func.count(AnalyticsEvent.id)).filter(
            AnalyticsEvent.event_name == 'signup_start',
            AnalyticsEvent.timestamp >= cutoff_date
        ).scalar() or 0

        # Count signup completed events (signup_success event)
        signup_completed = db.query(func.count(AnalyticsEvent.id)).filter(
            AnalyticsEvent.event_name == 'signup_success',
            AnalyticsEvent.timestamp >= cutoff_date
        ).scalar() or 0

        # Calculate conversion rate
        conversion_rate = (signup_completed / signup_started * 100) if signup_started > 0 else 0

        return {
            'signup_started': signup_started,
            'signup_completed': signup_completed,
            'conversion_rate': round(conversion_rate, 2),
            'period': f'last_{period_days}_days'
        }

    @staticmethod
    def calculate_retention_rate(
        db: Session,
        period_days: int = 7
    ) -> Dict:
        """
        Calculate Retention Rate KPI: (day_n_active_users / day_0_users) * 100

        Args:
            db: Database session
            period_days: Number of days for retention calculation (N in day N)

        Returns:
            Dictionary with retention rate metrics
        """
        today = datetime.utcnow().date()
        day_0 = today - timedelta(days=period_days)
        day_n = today

        # Find users active on day 0 (had activity on that day)
        day_0_users = db.query(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            func.date(AnalyticsEvent.timestamp) == day_0
        ).scalar() or 0

        # Find users from day 0 who were also active on day N
        day_n_active_users = db.query(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            func.date(AnalyticsEvent.timestamp) == day_0,
            AnalyticsEvent.user_id.in_(
                db.query(func.distinct(AnalyticsEvent.user_id)).filter(
                    func.date(AnalyticsEvent.timestamp) == day_n
                ).subquery()
            )
        ).scalar() or 0

        # Calculate retention rate
        retention_rate = (day_n_active_users / day_0_users * 100) if day_0_users > 0 else 0

        return {
            'day_0_users': day_0_users,
            'day_n_active_users': day_n_active_users,
            'retention_rate': round(retention_rate, 2),
            'period_days': period_days,
            'period': f'{period_days}_day_retention'
        }

    @staticmethod
    def calculate_arpu(
        db: Session,
        period_days: int = 30
    ) -> Dict:
        """
        Calculate ARPU KPI: (total_revenue / total_active_users)

        Note: This is a placeholder implementation. In a real system,
        revenue would come from a payments/transactions table.

        Args:
            db: Database session
            period_days: Number of days for ARPU calculation

        Returns:
            Dictionary with ARPU metrics
        """
        cutoff_date = datetime.utcnow() - timedelta(days=period_days)

        # Count active users in the period (users with any activity)
        total_active_users = db.query(func.count(func.distinct(AnalyticsEvent.user_id))).filter(
            AnalyticsEvent.user_id.isnot(None),
            AnalyticsEvent.timestamp >= cutoff_date
        ).scalar() or 0

        # TODO: Replace with actual revenue calculation from payments table
        # For now, using a placeholder - in production this would query:
        # - Payment transactions
        # - Subscription revenue
        # - One-time purchases
        # - Refunds (negative revenue)
        total_revenue = 0.0  # Placeholder - no revenue tracking implemented yet

        # Calculate ARPU
        arpu = (total_revenue / total_active_users) if total_active_users > 0 else 0

        return {
            'total_revenue': total_revenue,
            'total_active_users': total_active_users,
            'arpu': round(arpu, 2),
            'period': f'last_{period_days}_days',
            'currency': 'USD'
        }

    @staticmethod
    def get_kpi_summary(
        db: Session,
        conversion_period_days: int = 30,
        retention_period_days: int = 7,
        arpu_period_days: int = 30
    ) -> Dict:
        """
        Get combined KPI summary for dashboard reporting.

        Args:
            db: Database session
            conversion_period_days: Period for conversion rate calculation
            retention_period_days: Period for retention rate calculation
            arpu_period_days: Period for ARPU calculation

        Returns:
            Combined KPI metrics
        """
        conversion_rate = AnalyticsService.calculate_conversion_rate(db, conversion_period_days)
        retention_rate = AnalyticsService.calculate_retention_rate(db, retention_period_days)
        arpu = AnalyticsService.calculate_arpu(db, arpu_period_days)

        return {
            'conversion_rate': conversion_rate,
            'retention_rate': retention_rate,
            'arpu': arpu,
            'calculated_at': datetime.utcnow().isoformat(),
            'period': f'conversion_{conversion_period_days}d_retention_{retention_period_days}d_arpu_{arpu_period_days}d'
        }
