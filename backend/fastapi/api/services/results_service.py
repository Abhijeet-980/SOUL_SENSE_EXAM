import logging
from typing import List, Optional
from sqlalchemy.orm import Session
from ..root_models import Score, Response, Question, QuestionCategory
from ..schemas import DetailedExamResult, CategoryScore, Recommendation

logger = logging.getLogger(__name__)

class AssessmentResultsService:
    @staticmethod
    def get_detailed_results(db: Session, assessment_id: int, user_id: int) -> Optional[DetailedExamResult]:
        """
        Fetches a comprehensive breakdown of an assessment.
        Security: Strictly filters by user_id to prevent unauthorized access.
        """
        # 1. Fetch the main score record
        score = db.query(Score).filter(Score.id == assessment_id, Score.user_id == user_id).first()
        if not score:
            logger.warning(f"Assessment {assessment_id} not found for user_id={user_id}")
            return None

        # 2. Get all responses for this session/user
        # Join with Question and Category to get rich data for the breakdown
        responses = (
            db.query(Response, Question, QuestionCategory)
            .join(Question, Response.question_id == Question.id)
            .join(QuestionCategory, Question.category_id == QuestionCategory.id)
            .filter(Response.session_id == score.session_id)
            .all()
        )

        if not responses:
            logger.info(f"No detailed responses found for assessment session {score.session_id}")
            # Provide a basic result if detailed responses are unavailable
            return DetailedExamResult(
                assessment_id=score.id,
                total_score=float(score.total_score),
                max_possible_score=0.0,
                overall_percentage=0.0,
                timestamp=score.timestamp,
                category_breakdown=[],
                recommendations=[]
            )

        # 3. Process categories and aggregate scores
        category_stats = {}
        for resp, quest, cat in responses:
            cat_name = cat.name or "Uncategorized"
            if cat_name not in category_stats:
                category_stats[cat_name] = {
                    "score": 0.0,
                    "max": 0.0
                }
            
            # Aggregation: response_value (assumed 1-5) weighted by quest.weight (default 1.0)
            weight = quest.weight if quest.weight is not None else 1.0
            category_stats[cat_name]["score"] += float(resp.response_value) * weight
            category_stats[cat_name]["max"] += 5.0 * weight # Assumes max response value is 5.0

        breakdown = []
        recommendations = []
        
        for name, data in category_stats.items():
            percentage = (data["score"] / data["max"]) * 100.0 if data["max"] > 0 else 0.0
            
            breakdown.append(CategoryScore(
                category_name=name,
                score=round(data["score"], 1),
                max_score=round(data["max"], 1),
                percentage=round(percentage, 1)
            ))

            # 4. Generate dynamic recommendations based on performance
            if percentage < 60:
                recommendations.append(Recommendation(
                    category_name=name,
                    message=f"Focus on strengthening your {name.lower()} skills through focused practice.",
                    priority="high"
                ))
            elif percentage < 85:
                recommendations.append(Recommendation(
                    category_name=name,
                    message=f"You're making good progress in {name.lower()}. Keep it up!",
                    priority="medium"
                ))
            else:
                recommendations.append(Recommendation(
                    category_name=name,
                    message=f"Excellent performance in {name.lower()}!",
                    priority="low"
                ))

        # Calculate overall percentage based on aggregates if needed, 
        # or use the one stored in score if available. 
        total_max = sum(d["max"] for d in category_stats.values())
        overall_pct = (score.total_score / total_max * 100.0) if total_max > 0 else 0.0

        return DetailedExamResult(
            assessment_id=score.id,
            total_score=float(score.total_score),
            max_possible_score=total_max,
            overall_percentage=round(overall_pct, 1),
            timestamp=score.timestamp,
            category_breakdown=breakdown,
            recommendations=recommendations
        )
