"""Database service for assessments and questions."""
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Optional, Tuple
from datetime import datetime
import logging
import traceback

# Import model classes from models module
from ..models import Base, Score, Response, Question, QuestionCategory

from ..config import get_settings

settings = get_settings()

# Create engine
engine = create_engine(
    settings.database_url,
    connect_args={"check_same_thread": False} if settings.database_type == "sqlite" else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logger = logging.getLogger("api.db")


def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}", extra={
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc()
        })
        raise
    finally:
        db.close()


class AssessmentService:
    """Service for managing assessments (scores)."""
    
    @staticmethod
    def get_assessments(
        db: Session,
        skip: int = 0,
        limit: int = 10,
        user_id: Optional[int] = None,
        username: Optional[str] = None,
        age_group: Optional[str] = None
    ) -> Tuple[List[Score], int]:
        """
        Get assessments with pagination and optional filters.
        When user_id is provided, results are scoped to that user only.
        """
        query = db.query(Score)
        
        # Apply filters — prefer user_id for session-bound isolation
        if user_id is not None:
            query = query.filter(Score.user_id == user_id)
        elif username:
            query = query.filter(Score.username == username)
        if age_group:
            query = query.filter(Score.detailed_age_group == age_group)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        assessments = query.order_by(Score.timestamp.desc()).offset(skip).limit(limit).all()
        
        return assessments, total
    
    @staticmethod
    def get_assessment_by_id(
        db: Session, assessment_id: int, user_id: Optional[int] = None
    ) -> Optional[Score]:
        """Get a single assessment by ID, optionally scoped to a specific user."""
        query = db.query(Score).filter(Score.id == assessment_id)
        if user_id is not None:
            query = query.filter(Score.user_id == user_id)
        return query.first()
    
    @staticmethod
    def get_assessment_stats(
        db: Session,
        user_id: Optional[int] = None,
        username: Optional[str] = None
    ) -> dict:
        """
        Get statistical summary of assessments.
        When user_id is provided, stats are scoped to that user.
        """
        query = db.query(Score)
        
        if user_id is not None:
            query = query.filter(Score.user_id == user_id)
        elif username:
            query = query.filter(Score.username == username)
        
        # Calculate statistics
        stats = query.with_entities(
            func.count(Score.id).label('total'),
            func.avg(Score.total_score).label('avg_score'),
            func.max(Score.total_score).label('max_score'),
            func.min(Score.total_score).label('min_score'),
            func.avg(Score.sentiment_score).label('avg_sentiment')
        ).first()
        
        # Get age group distribution
        age_query = db.query(
            Score.detailed_age_group,
            func.count(Score.id).label('count')
        )
        
        if user_id is not None:
            age_query = age_query.filter(Score.user_id == user_id)
        elif username:
            age_query = age_query.filter(Score.username == username)
        
        age_distribution = age_query.group_by(Score.detailed_age_group).all()
        
        return {
            'total_assessments': stats.total or 0,
            'average_score': round(stats.avg_score or 0, 2),
            'highest_score': stats.max_score or 0,
            'lowest_score': stats.min_score or 0,
            'average_sentiment': round(stats.avg_sentiment or 0, 2),
            'age_group_distribution': {
                age_group: count for age_group, count in age_distribution if age_group
            }
        }
    
    @staticmethod
    def get_assessment_responses(
        db: Session, assessment_id: int, user_id: Optional[int] = None
    ) -> List[Response]:
        """Get all responses for a specific assessment, scoped to user_id."""
        query = db.query(Score).filter(Score.id == assessment_id)
        if user_id is not None:
            query = query.filter(Score.user_id == user_id)
        assessment = query.first()
        if not assessment:
            return []
        
        resp_query = db.query(Response).filter(
            Response.session_id == assessment.session_id
        )
        if user_id is not None:
            resp_query = resp_query.filter(Response.user_id == user_id)
        return resp_query.all()


class QuestionService:
    """Service for managing questions."""
    
    @staticmethod
    def get_questions(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        min_age: Optional[int] = None,
        max_age: Optional[int] = None,
        category_id: Optional[int] = None,
        active_only: bool = True
    ) -> Tuple[List[Question], int]:
        """
        Get questions with pagination and filters.
        """
        query = db.query(Question)
        
        # Apply filters
        if active_only:
            query = query.filter(Question.is_active == 1)
        
        if category_id is not None:
            query = query.filter(Question.category_id == category_id)
        
        if min_age is not None:
            query = query.filter(Question.min_age <= min_age)
        
        if max_age is not None:
            query = query.filter(Question.max_age >= max_age)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        questions = query.order_by(Question.id).offset(skip).limit(limit).all()
        
        return questions, total
    
    @staticmethod
    def get_question_by_id(db: Session, question_id: int) -> Optional[Question]:
        """Get a single question by ID."""
        return db.query(Question).filter(Question.id == question_id).first()
    
    @staticmethod
    def get_questions_by_age(
        db: Session,
        age: int,
        limit: Optional[int] = None
    ) -> List[Question]:
        """
        Get questions appropriate for a specific age.
        """
        query = db.query(Question).filter(
            Question.is_active == 1,
            Question.min_age <= age,
            Question.max_age >= age
        ).order_by(Question.id)
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @staticmethod
    def get_categories(db: Session) -> List[QuestionCategory]:
        """Get all question categories."""
        return db.query(QuestionCategory).order_by(QuestionCategory.id).all()
    
    @staticmethod
    def get_category_by_id(db: Session, category_id: int) -> Optional[QuestionCategory]:
        """Get a category by ID."""
        return db.query(QuestionCategory).filter(QuestionCategory.id == category_id).first()
