import sys
import os

# Create mock root just in case (though we are testing DB)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import get_session, engine
from backend.fastapi.api.root_models import User, MedicalProfile, PersonalProfile, Base

def verify():
    print("Verifying Phase 1 Profile Enhancements...")
    
    session = get_session()
    
    # Clean up test user
    test_user_name = "phase1_tester"
    existing = session.query(User).filter_by(username=test_user_name).first()
    if existing:
        session.delete(existing)
        session.commit()
        
    # Create User
    user = User(username=test_user_name, password_hash="hash")
    session.add(user)
    session.commit()
    
    # 1. Test Medical Profile (Ongoing Health Issues)
    mp = MedicalProfile(user_id=user.id, ongoing_health_issues="Chronic Migraines")
    session.add(mp)
    session.commit()
    
    # 2. Test Personal Profile (High Pressure Events)
    pp = PersonalProfile(user_id=user.id, high_pressure_events="Final Exams, Moving House")
    session.add(pp)
    session.commit()
    
    # Reload from fresh session
    session.close()
    session = get_session()
    
    loaded_user = session.query(User).filter_by(username=test_user_name).first()
    
    # Check Medical
    if loaded_user.medical_profile and loaded_user.medical_profile.ongoing_health_issues == "Chronic Migraines":
        print("[PASS] Ongoing Health Issues persisted.")
    else:
        print(f"[FAIL] Medical Profile persistence failed. Got: {getattr(loaded_user.medical_profile, 'ongoing_health_issues', 'None')}")

    # Check Personal
    if loaded_user.personal_profile and loaded_user.personal_profile.high_pressure_events == "Final Exams, Moving House":
        print("[PASS] High Pressure Events persisted.")
    else:
        print(f"[FAIL] Personal Profile persistence failed. Got: {getattr(loaded_user.personal_profile, 'high_pressure_events', 'None')}")
        
    session.close()

if __name__ == "__main__":
    verify()
