import sys
import os
print(f"Current Directory: {os.getcwd()}")
print(f"sys.path: {sys.path}")
print("Starting import check...")
try:
    import pytest
    print("pytest imported")
    from fastapi.testclient import TestClient
    print("fastapi.testclient imported")
    from sqlalchemy.orm import Session
    print("sqlalchemy.orm imported")
    from datetime import datetime
    print("datetime imported")
except Exception as e:
    print(f"Error in standard imports: {e}")

try:
    print("Attempting to import backend.fastapi.api.main...")
    from backend.fastapi.api.main import app
    print("backend.fastapi.api.main imported")
except Exception as e:
    print(f"Error importing backend.fastapi.api.main: {e}")
    import traceback
    traceback.print_exc()

try:
    print("Attempting to import backend.fastapi.api.root_models...")
    from backend.fastapi.api.root_models import User, Score, Response
    print("root_models imported")
except Exception as e:
    print(f"Error importing root_models: {e}")
    import traceback
    traceback.print_exc()

try:
    print("Attempting to import app.auth.auth...")
    from app.auth.auth import AuthManager
    print("app.auth.auth imported")
except Exception as e:
    print(f"Error importing app.auth.auth: {e}")
    import traceback
    traceback.print_exc()

print("Import check finished.")
