
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add app to path
sys.path.append(os.getcwd())

from backend.fastapi.api.root_models import JournalEntry, Base
from app.config import DATABASE_URL
# Mock JournalFeature for context
class MockJournalFeature:
    def __init__(self, username):
        self.username = username

# Import the actual class to monkeypatch or just copy logic? 
# Better: Import the class and use it if possible, but it requires tkinter root.
# We will just verify the logic by instantiating a stripped down version or just testing the core query.
# Actually, let's verify by just running the logic query manually as we did before, but this time we can't easily call the method directly without GUI.
# PLAN B: We will test the DATA STATE that would trigger it, as we did before.
# BUT we want to ensure the CODE works.
# Let's import JournalFeature but Mock tk and messagebox
import tkinter as tk
from unittest.mock import MagicMock
tk.Toplevel = MagicMock()
tk.Label = MagicMock()
tk.Button = MagicMock()
tk.Frame = MagicMock()
tk.messagebox = MagicMock()

from app.ui.journal import JournalFeature

# Setup DB
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

USERNAME = "test_nudge_integrated"

def clean_data():
    session.query(JournalEntry).filter_by(username=USERNAME).delete()
    session.commit()

def create_entry(days_ago, sleep, energy):
    date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    entry = JournalEntry(
        username=USERNAME,
        entry_date=date,
        content="Testing nudges",
        sentiment_score=50,
        sleep_hours=sleep,
        sleep_quality=5,
        energy_level=energy,
        work_hours=8
    )
    session.add(entry)
    session.commit()

try:
    print("--- Testing Integrated Nudge Logic ---")
    clean_data()
    
    # Initialize feature with mock
    jf = JournalFeature(MagicMock())
    jf.username = USERNAME
    
    # Scene 1: Sleep Debt
    print("Creating sleep debt entries...")
    create_entry(2, 4, 8)
    create_entry(1, 4, 8)
    create_entry(0, 5, 8) 
    
    nudge = jf.check_for_nudges()
    print(f"Nudge Result: {nudge}")
    assert nudge is not None
    assert "less than 6 hours" in nudge
    print("✓ Sleep debt return confirmed")

    # Scene 2: No Nudge
    clean_data()
    print("\nCreating good entries...")
    create_entry(2, 8, 8)
    create_entry(1, 8, 8)
    
    nudge = jf.check_for_nudges()
    print(f"Nudge Result: {nudge}")
    assert nudge is None
    print("✓ No nudge confirmed")

    print("\nVERIFICATION SUCCESS: Integrated logic works.")
    clean_data()

except Exception as e:
    print(f"VERIFICATION FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    session.close()
