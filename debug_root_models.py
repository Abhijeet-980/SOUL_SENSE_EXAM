
import sys
import os
from pathlib import Path

# Add root to sys.path
sys.path.insert(0, os.getcwd())

try:
    print("Attempting to import backend.fastapi.api.root_models...")
    from backend.fastapi.api.root_models import User, Score
    print(f"SUCCESS: Imported User={User}, Score={Score}")
except Exception as e:
    print(f"FAILURE: {e}")
    import traceback
    traceback.print_exc()
