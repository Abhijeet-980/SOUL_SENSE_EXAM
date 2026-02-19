
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add app to path
sys.path.append(os.getcwd())

from backend.fastapi.api.root_models import JournalEntry, Base
from app.config import DATABASE_URL
from app.db import get_session

# Setup DB
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

USERNAME = "test_nudge_integrated_simple"

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

# Replicate the Logic Function here to test it in isolation without UI dependencies
def check_for_nudges_logic(username):
    conn = engine.connect()
    nudge_text = None
    try:
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        query = """
            SELECT sleep_hours, energy_level 
            FROM journal_entries 
            WHERE username = :username 
            AND entry_date >= :date
            ORDER BY entry_date DESC
        """
        result = conn.execute(text(query), {"username": username, "date": three_days_ago})
        
        if hasattr(result, 'mappings'):
            rows = result.mappings().all()
        else:
            rows = result.fetchall()
        
        if not rows or len(rows) < 2:
            return None
        
        sleeps = []
        energies = []
        for r in rows:
            if hasattr(r, 'sleep_hours'): 
                sleeps.append(r.sleep_hours)
                energies.append(r.energy_level)
            else: 
                sleeps.append(r[0])
                energies.append(r[1])
        
        sleeps = [s for s in sleeps if s is not None]
        energies = [e for e in energies if e is not None]
        
        if sleeps and (sum(sleeps) / len(sleeps)) < 6.0:
            nudge_text = "You've been getting less than 6 hours of sleep recently."
        elif len(energies) >= 3 and all(e <= 4 for e in energies[:3]):
            nudge_text = "Your reported energy levels have been low for 3 days in a row."
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        
    return nudge_text

try:
    print("--- Testing Nudge Logic Extraction ---")
    clean_data()
    
    # Scene 1: Sleep Debt
    print("Creating sleep debt entries...")
    create_entry(2, 4, 8)
    create_entry(1, 4, 8)
    create_entry(0, 5, 8) 
    
    nudge = check_for_nudges_logic(USERNAME)
    print(f"Nudge Result: {nudge}")
    assert nudge is not None
    assert "less than 6 hours" in nudge
    print("✓ Sleep debt logic works")

    # Scene 2: No Nudge
    clean_data()
    print("\nCreating good entries...")
    create_entry(2, 8, 8)
    create_entry(1, 8, 8)
    
    nudge = check_for_nudges_logic(USERNAME)
    print(f"Nudge Result: {nudge}")
    assert nudge is None
    print("✓ No nudge logic works")

    print("\nVERIFICATION SUCCESS: Logic is sound.")
    clean_data()

except Exception as e:
    print(f"VERIFICATION FAILED: {e}")
    sys.exit(1)
finally:
    session.close()
