import logging
from typing import List, Optional, Any, Dict
from datetime import datetime
from sqlalchemy import desc
from app.db import safe_db_context
from app.models import JournalEntry, User
from app.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class JournalService:
    """
    Service layer for handling Journal Entry operations.
    Decouples UI from direct Database access.
    """

    @staticmethod
    def create_entry(
        username: str, 
        content: str, 
        sentiment_score: float, 
        emotional_patterns: str,
        entry_date: Optional[str] = None,
        **kwargs
    ) -> JournalEntry:
        """
        Creates and saves a new journal entry.
        
        Args:
            username: The user's username
            content: The text content of the entry
            sentiment_score: Calculated sentiment score
            emotional_patterns: Stringified emotional patterns/tags
            entry_date: Optional specific date (YYYY-MM-DD HH:MM:SS), defaults to now
            **kwargs: Additional fields (sleep_hours, stress_level, etc.)
            
        Returns:
            The created JournalEntry object (detached)
            
        Raises:
            DatabaseError: If the save fails
        """
        try:
            if not entry_date:
                entry_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with safe_db_context() as session:
                session.expire_on_commit = False
                entry = JournalEntry(
                    username=username,
                    content=content,
                    sentiment_score=sentiment_score,
                    emotional_patterns=emotional_patterns,
                    entry_date=entry_date,
                    **kwargs
                )
                session.add(entry)
                # Commit is handled by safe_db_context
                
                # Refresh/Expunge to allow usage outside session if needed, 
                # but returning ID or simple DTO is often safer. 
                # For now, we rely on the fact that simple attributes are accessible.
                return entry
                
        except Exception as e:
            logger.error(f"Failed to create journal entry for {username}: {e}")
            raise DatabaseError("Failed to save journal entry", original_exception=e)

    @staticmethod
    def get_entries(
        username: str, 
        month_filter: Optional[str] = None, 
        type_filter: Optional[str] = None
    ) -> List[JournalEntry]:
        """
        Retrieves journal entries for a user with optional filters.
        
        Args:
            username: The user's username
            month_filter: Optional "Month Year" string (e.g. "January 2024")
            type_filter: Optional filter string ("High Stress", "Great Days", etc.)
            
        Returns:
            List of JournalEntry objects
        """
        try:
            with safe_db_context() as session:
                query = session.query(JournalEntry)\
                    .filter_by(username=username)\
                    .order_by(desc(JournalEntry.entry_date))
                
                session.expire_on_commit = False
                
                # Basic fetching - Logic for complex filters (Month/Type) 
                # is currently done in memory in the UI because of the 
                # nature of SQLite date strings and complex business logic.
                # Ideally, we would move that logic here.
                
                entries = query.all()
                
                # If we want to move filtering here later, we can.
                # For now, return all and let UI filter or implement basic filtering here.
                # Since the UI implementation was doing in-memory filtering, 
                # we'll return the full list or implement the filtering logic here 
                # if we want to be pure.
                
                # Let's return all and let the client filter for now to minimize risk 
                # of breaking the complex UI loop, BUT this is where we'd add 
                # server-side filtering logic in Phase 3.
                
                return entries
                
        except Exception as e:
            logger.error(f"Failed to retrieve journal entries for {username}: {e}")
            raise DatabaseError("Failed to retrieve journal history", original_exception=e)

    @staticmethod
    def get_recent_entries(username: str, days: int = 7) -> List[JournalEntry]:
        """
        Retrieves journal entries from the last N days.
        """
        try:
            from datetime import timedelta
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            with safe_db_context() as session:
                session.expire_on_commit = False
                entries = session.query(JournalEntry)\
                    .filter(JournalEntry.username == username)\
                    .filter(JournalEntry.entry_date >= start_date)\
                    .order_by(desc(JournalEntry.entry_date))\
                    .all()
                return entries
        except Exception as e:
            logger.error(f"Failed to retrieve recent entries: {e}")
            return []
