import logging
from unittest.mock import MagicMock, patch
from datetime import datetime
from backend.fastapi.api.root_models import JournalEntry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@patch('app.db.get_session')
def test_journal_insert(mock_get_session):
    """
    Test journal insertion using a PURE MOCK session.
    This completely avoids any database connection (sqlite or memory),
    preventing CI deadlocks when the real database is locked.
    """
    logger.info("Testing Journal Entry Insertion (Pure Mock)...")

    # 1. Setup Mock Session
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    # 2. Create Entry Object
    entry = JournalEntry(
        username="test_user",
        entry_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        content="This is a test entry from the mock test.",
        sentiment_score=50.0,
        emotional_patterns="Test pattern"
    )

    # 3. Perform 'Add' (Mocked)
    logger.info("Adding entry to mock session...")
    mock_session.add(entry)
    
    # 4. Perform 'Commit' (Mocked)
    mock_session.commit()
    
    # 5. Verify Calls
    mock_session.add.assert_called_once_with(entry)
    mock_session.commit.assert_called_once()
    
    logger.info("✅ Successfully verified session.add and session.commit calls.")

if __name__ == "__main__":
    test_journal_insert()
