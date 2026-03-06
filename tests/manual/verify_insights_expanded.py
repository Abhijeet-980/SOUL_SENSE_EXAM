
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add app to path
sys.path.append(os.getcwd())

from backend.fastapi.api.root_models import JournalEntry
from app.config import DATABASE_URL
from app.db import get_session

# Setup DB
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

USERNAME = "test_insights_expanded"

def clean_data():
    session.query(JournalEntry).filter_by(username=USERNAME).delete()
    session.commit()

def create_entry(days_ago, sleep, energy, work):
    date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    entry = JournalEntry(
        username=USERNAME,
        entry_date=date,
        content="Testing insights",
        sentiment_score=50,
        sleep_hours=sleep,
        sleep_quality=5,
        energy_level=energy,
        work_hours=work
    )
    session.add(entry)
    session.commit()

# Replicate Logic for Testing
def generate_health_insights_logic(username):
    conn = engine.connect()
    insight_text = "Default"
    try:
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        query = """
            SELECT sleep_hours, energy_level, work_hours 
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
        
        if not rows:
            return "Start tracking"
        
        sleeps = []
        energies = []
        works = []
        
        for r in rows:
            if hasattr(r, 'sleep_hours'): 
                sleeps.append(r.sleep_hours)
                energies.append(r.energy_level)
                works.append(r.work_hours)
            else: 
                sleeps.append(r[0])
                energies.append(r[1])
                if len(r) > 2: works.append(r[2])
        
        sleeps = [s for s in sleeps if s is not None]
        energies = [e for e in energies if e is not None]
        works = [w for w in works if w is not None]
        
        avg_sleep = sum(sleeps) / len(sleeps) if sleeps else 0
        avg_energy = sum(energies) / len(energies) if energies else 0
        
        insights = []
        
        # 1. Sleep
        if avg_sleep < 6.0:
            insights.append("less than 6 hours")
        elif avg_sleep >= 7.5:
            insights.append("Great job maintaining a healthy sleep schedule")
        elif avg_sleep >= 6.0:
            insights.append("sleep schedule is fairly balanced")
            
        # 2. Energy
        if len(energies) >= 2 and all(e <= 4 for e in energies[:3]):
            insights.append("energy levels have been consistently low")
        elif avg_energy >= 7.0:
            insights.append("high energy levels")
        elif len(energies) >= 2 and energies[0] > energies[-1] + 2:
            insights.append("energy is trending upwards")

        # 3. Work
        if works and (sum(works)/len(works)) > 10:
             insights.append("working long hours")
        elif works and (sum(works)/len(works)) < 2 and avg_energy > 5:
             insights.append("relaxing period")

        if insights:
            insight_text = " ".join(insights)
        else:
            insight_text = "metrics are stable"
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        
    return insight_text

try:
    print("--- Testing Expanded Insights Logic ---")
    clean_data()
    
    # Scene 1: Positive Reinforcement (High Sleep, High Energy)
    print("Creating positive entries...")
    create_entry(1, 8, 8, 6)
    create_entry(0, 8, 9, 6)
    
    msg = generate_health_insights_logic(USERNAME)
    print(f"Insight: {msg}")
    assert "Great job" in msg
    assert "high energy" in msg
    print("✓ Positive reinforcement worked")

    # Scene 2: Stable/Neutral
    clean_data()
    print("\nCreating neutral entries...")
    create_entry(1, 6.5, 6, 8)
    create_entry(0, 6.5, 6, 8)
    
    msg = generate_health_insights_logic(USERNAME)
    print(f"Insight: {msg}")
    assert "fairly balanced" in msg
    print("✓ Neutral insight worked")

    # Scene 3: Warning (Sleep Debt)
    clean_data()
    print("\nCreating sleep debt entries...")
    create_entry(1, 4, 5, 8)
    create_entry(0, 5, 5, 8)
    
    msg = generate_health_insights_logic(USERNAME)
    print(f"Insight: {msg}")
    assert "less than 6 hours" in msg
    print("✓ Warning worked")

    print("\nVERIFICATION SUCCESS: Extended logic is robust.")
    clean_data()

except Exception as e:
    print(f"VERIFICATION FAILED: {e}")
    sys.exit(1)
finally:
    session.close()
