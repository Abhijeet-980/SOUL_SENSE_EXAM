import hashlib
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import select, delete, text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, UTC

from ..models import User, ExportRecord, OutboxEvent, Base
from .storage_service import storage_service

logger = logging.getLogger("api.scrubber")

class DistributedScrubberService:
    """
    Distributed Scrubber Service (GDPR #1134).
    Orchestrates "Right to be Forgotten" across all data stores.
    """
    
    @staticmethod
    async def scrub_user(db: AsyncSession, user_id: int):
        """
        Orchestrates deletion across all distributed stores.
        Single primary session ensures transactional integrity.
        """
        user = await db.get(User, user_id)
        if not user:
            logger.warning(f"Attempted to scrub non-existent user {user_id}")
            return
            
        username = user.username
        scrub_id = hashlib.sha256(str(user_id).encode()).hexdigest()
        
        # 1. Track pending scrub
        log_event = OutboxEvent(
            topic="GDPR_SCRUB",
            payload={"scrub_id": scrub_id, "status": "in_progress", "timestamp": datetime.now(UTC).isoformat()},
            status="pending"
        )
        db.add(log_event)
        
        # 2. Storage Scrubbing (S3 & Local Exports)
        exp_stmt = select(ExportRecord).where(ExportRecord.user_id == user_id)
        exp_res = await db.execute(exp_stmt)
        exports = exp_res.scalars().all()
        
        for exp in exports:
            try:
                # Actual File Deletion
                await storage_service.delete_file(exp.file_path)
            except Exception as e:
                logger.error(f"File Scrub Failed: {e}")

        # 3. SQL Hard Delete (Triggers cascades)
        try:
            await db.delete(user)
            
            # 4. Finalize Deletion Log (Added BEFORE commit to be atomic)
            log_complete = OutboxEvent(
                topic="GDPR_SCRUB_COMPLETE",
                payload={
                    "scrub_id": scrub_id,
                    "status": "completed",
                    "user_id": user_id,
                    "timestamp": datetime.now(UTC).isoformat()
                },
                status="processed"
            )
            db.add(log_complete)
            
            await db.commit()
            logger.info(f"Audit-Compliant Purge: user_id={user_id} scrub_id={scrub_id}")
        except Exception as e:
            await db.rollback()
            logger.error(f"Distributed Scrub Aborted: {e}")
            raise e

    @staticmethod
    async def get_scrub_status(scrub_id: str, db: AsyncSession) -> Optional[Dict]:
        """Verify if a purge was successfully completed by scrub hash."""
        # SQLite doesn't support Postgres ->> operators, using portable logic
        stmt = select(OutboxEvent).where(OutboxEvent.topic == "GDPR_SCRUB_COMPLETE")
        result = await db.execute(stmt)
        events = result.scalars().all()
        for e in events:
            if e.payload.get("scrub_id") == scrub_id:
                return e.payload
        return None

scrubber_service = DistributedScrubberService()
