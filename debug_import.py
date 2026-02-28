import sys
import os

# Set up the same path as conftest.py
test_dir = os.path.abspath(os.path.join(os.getcwd(), "backend", "fastapi", "tests"))
project_root = os.path.abspath(os.path.join(test_dir, ".."))
sys.path.insert(0, project_root)

print(f"PYTHONPATH: {sys.path[0]}")

try:
    from api.models import User
    print("STEP 1: User success")
    from api.services.db_router import PrimarySessionLocal
    print("STEP 2: PrimarySessionLocal success")
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"OTHER ERROR: {e}")
    import traceback
    traceback.print_exc()
