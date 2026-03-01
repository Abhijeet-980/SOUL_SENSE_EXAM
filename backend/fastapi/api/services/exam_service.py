import logging
import uuid
from datetime import datetime, UTC
from typing import List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from ..schemas import ExamResponseCreate, ExamResultCreate
from ..models import User, Score, Response, UserSession
from .gamification_service import GamificationService
from ..utils.db_transaction import transactional, retry_on_transient
from ..utils.race_condition_protection import with_row_lock, generate_idempotency_key
import asyncio

try:
    from .crypto import EncryptionManager
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

logger = logging.getLogger("api.exam")

try:
    from .crypto import EncryptionManager
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

logger = logging.getLogger("api.exam")

class ExamService:
    """
    Service for handling Exam write operations via API.
    Uses 'Storage-First' approach: Client calculates, API validates and saves.
    """

    @staticmethod
    async def start_exam(db: AsyncSession, user: User):
        """Standardizes session initiation and returns a new session_id."""
        session_id = str(uuid.uuid4())
        logger.info(f"Exam session started", extra={
            "user_id": user.id,
            "session_id": session_id
        })
        return session_id

    @staticmethod
    async def save_response(db: AsyncSession, user: User, session_id: str, data: ExamResponseCreate):
        """Saves a single question response linked to the user and session."""
        try:
            # Use row-level locking to prevent concurrent duplicate submissions
            await with_row_lock(
                db,
                "responses",
                "user_id = :user_id AND question_id = :question_id",
                {"user_id": user.id, "question_id": data.question_id}
            )

            # Double-check for existing response after acquiring lock
            existing_response = await db.execute(
                select(Response).filter(
                    Response.user_id == user.id,
                    Response.question_id == data.question_id
                )
            )
            existing = existing_response.scalar_one_or_none()

            if existing:
                raise ConflictError(
                    message="Duplicate response submission",
                    details=[{
                        "field": "question_id",
                        "error": "User has already submitted a response for this question",
                        "question_id": data.question_id,
                        "existing_response_id": existing.id
                    }]
                )

            new_response = Response(
                username=user.username,
                user_id=user.id,
                question_id=data.question_id,
                response_value=data.value,
                detailed_age_group=data.age_group,
                session_id=session_id,
                timestamp=datetime.now(UTC).isoformat()
            )
            db.add(new_response)
            await db.commit()
            return True
        except IntegrityError as e:
            # Handle database constraint violations (additional safety net)
            await db.rollback()
            if "unique constraint" in str(e).lower() or "duplicate" in str(e).lower():
                raise ConflictError(
                    message="Duplicate response submission",
                    details=[{
                        "field": "question_id",
                        "error": "User has already submitted a response for this question",
                        "question_id": data.question_id
                    }]
                )
            else:
                logger.error(f"Database integrity error for user_id={user.id}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to save response for user_id={user.id}: {e}")
            await db.rollback()
            raise e

    @staticmethod
    @retry_on_transient(retries=3)
    async def save_score(db: AsyncSession, user: User, session_id: str, data: ExamResultCreate):
        """
        Saves the final exam score atomically together with gamification updates.

        Uses row-level locking and enhanced transaction handling to prevent:
        - Duplicate score submissions
        - Concurrent score miscalculations
        - Inconsistent gamification state
        """
        try:
            # Use row-level locking on user_session to prevent concurrent score submissions
            await with_row_lock(
                db,
                "user_sessions",
                "session_id = :session_id AND user_id = :user_id",
                {"session_id": session_id, "user_id": user.id}
            )

            # Check if score already exists for this session
            existing_score_stmt = select(Score).filter(
                Score.session_id == session_id,
                Score.user_id == user.id
            )
            existing_score_result = await db.execute(existing_score_stmt)
            existing_score = existing_score_result.scalar_one_or_none()

            if existing_score:
                logger.warning(f"Duplicate score submission attempt for session {session_id}, user {user.id}")
                raise ConflictError(
                    message="Score already submitted for this exam session",
                    details=[{
                        "field": "session_id",
                        "error": "A score has already been recorded for this exam session",
                        "session_id": session_id,
                        "existing_score_id": existing_score.id
                    }]
                )

            # Validate that all questions have been answered
            ExamService._validate_complete_responses(db, user, session_id, data.age)

            # Encrypt reflection text for privacy
            reflection = data.reflection_text
            if CRYPTO_AVAILABLE and reflection:
                try:
                    reflection = EncryptionManager.encrypt(reflection)
                except Exception as ce:
                    logger.error(f"Encryption failed for reflection: {ce}")
                    # Fall back to plain text – do not block submission

            # ── ATOMIC SCORE + GAMIFICATION WRITE ─────────────────────────────
            # All operations must succeed together to prevent inconsistent state
            async with db.begin():  # Use async transaction context manager
                new_score = Score(
                    username=user.username,
                    user_id=user.id,
                    age=data.age,
                    total_score=data.total_score,
                    sentiment_score=data.sentiment_score,
                    reflection_text=reflection,
                    is_rushed=data.is_rushed,
                    is_inconsistent=data.is_inconsistent,
                    timestamp=datetime.now(UTC).isoformat(),
                    detailed_age_group=data.detailed_age_group,
                    session_id=session_id
                )
                db.add(new_score)
                await db.flush()  # Assign new_score.id before gamification

                # Execute gamification updates atomically
                try:
                    await GamificationService.award_xp(db, user.id, 100, "Assessment completion")
                    await GamificationService.update_streak(db, user.id, "assessment")
                    await GamificationService.check_achievements(db, user.id, "assessment")
                except Exception as ge:
                    logger.error(f"Gamification update failed for user_id={user.id}: {ge}")
                    # Don't fail the entire transaction for gamification errors
                    # The score is still valid, gamification can be retried separately

                await db.refresh(new_score)
            # ─────────────────────────────────────────────────────────────────

            logger.info(f"Exam score saved successfully", extra={
                "user_id": user.id,
                "session_id": session_id,
                "score": data.total_score,
                "sentiment_score": data.sentiment_score
            })
            return new_score

        except Exception as e:
            logger.error(f"Failed to save exam score", extra={
                "user_id": user.id,
                "session_id": session_id,
                "error": str(e)
            }, exc_info=True)
            raise e

    @staticmethod
    async def get_history(db: AsyncSession, user: User, skip: int = 0, limit: int = 10):
        """Retrieves paginated exam history for the specified user."""
        limit = min(limit, 100)  # Guard: cap at 100 to prevent unbounded queries
        
        # Count total
        count_stmt = select(func.count(Score.id)).join(UserSession, Score.session_id == UserSession.session_id).filter(UserSession.user_id == user.id)
        count_res = await db.execute(count_stmt)
        total = count_res.scalar() or 0
        
        # Get results
        stmt = select(Score).join(UserSession, Score.session_id == UserSession.session_id).filter(UserSession.user_id == user.id).order_by(Score.timestamp.desc()).offset(skip).limit(limit)
        result = await db.execute(stmt)
        results = list(result.scalars().all())
        
        return results, total
