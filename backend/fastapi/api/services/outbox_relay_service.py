import asyncio
import logging
import json
from datetime import datetime, UTC
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import OutboxEvent, JournalEntry
from .es_service import get_es_service

logger = logging.getLogger(__name__)

class OutboxRelayService:
    """
    Relay Service for the Outbox Pattern.
    Ensures that transactional outbox events are reliably pushed to external systems
    like Elasticsearch, providing At-Least-Once delivery guarantees.
    """

    @staticmethod
    async def process_pending_indexing_events(db: AsyncSession) -> int:
        """
        Poll pending search index events from the outbox and push to ES.
        Processes in strict order (by ID) to ensure sequential updates.
        """
        # 1. Fetch pending events for the 'search_indexing' topic
        stmt = select(OutboxEvent).filter(
            OutboxEvent.topic == "search_indexing",
            OutboxEvent.status == "pending"
        ).order_by(OutboxEvent.id).limit(50)
        
        result = await db.execute(stmt)
        events = result.scalars().all()
        
        if not events:
            return 0
            
        es_service = get_es_service()
        processed_count = 0
        
        for event in events:
            try:
                payload = event.payload
                journal_id = payload.get("journal_id")
                action = payload.get("action")
                
                if action == "upsert":
                    # Get latest journal content directly from SQL
                    # This ensures we index the most recent version even if there were multiple outbox events
                    journal_stmt = select(JournalEntry).filter(JournalEntry.id == journal_id)
                    journal_res = await db.execute(journal_stmt)
                    journal = journal_res.scalar_one_or_none()
                    
                    if journal and not journal.is_deleted:
                        # Push to Elasticsearch
                        await es_service.index_document(
                            entity="journal",
                            doc_id=journal.id,
                            data={
                                "user_id": journal.user_id,
                                "tenant_id": str(journal.tenant_id) if journal.tenant_id else None,
                                "content": journal.content,
                                "timestamp": journal.timestamp
                            }
                        )
                        logger.debug(f"Relayed UPSERT for journal {journal_id}")
                    elif journal and journal.is_deleted:
                        # Fallback for race condition: item marked deleted before indexing
                        await es_service.delete_document("journal", journal_id)
                
                elif action == "delete":
                    # Explicit removal from search index
                    await es_service.delete_document("journal", journal_id)
                    logger.debug(f"Relayed DELETE for journal {journal_id}")
                
                # 2. Update status to 'processed'
                event.status = "processed"
                event.processed_at = datetime.now(UTC)
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to relay OutboxEvent {event.id}: {str(e)}")
                # Increment retry count and mark as failed if threshold exceeded
                event.retry_count = (event.retry_count or 0) + 1
                event.error_message = str(e)
                if event.retry_count >= 5:
                    event.status = "failed"
                    logger.critical(f"Aborting OutboxEvent {event.id} after 5 retries.")
        
        # 3. Commit batch results
        await db.commit()
        return processed_count

    @classmethod
    async def start_relay_worker(cls, async_session_factory, interval_seconds: int = 2):
        """
        Background worker loop to continuously process outbox events.
        Usually started as a dedicated process or as part of app startup.
        """
        logger.info("Search Index Outbox Relay Worker started.")
        while True:
            try:
                async with async_session_factory() as db:
                    count = await cls.process_pending_indexing_events(db)
                    if count > 0:
                        logger.info(f"Successfully relayed {count} indexing events to Elasticsearch.")
            except Exception as e:
                logger.error(f"Critical error in Outbox Relay Worker: {e}", exc_info=True)
            
            # Use small sleep to allow for near real-time indexing while avoiding CPU hogging
            await asyncio.sleep(interval_seconds)
