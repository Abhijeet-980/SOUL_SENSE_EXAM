import pytest
import tempfile
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base
import app.db

@pytest.fixture
def temp_db(monkeypatch):
    # Create temp file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    
    # Create temp engine
    test_url = f"sqlite:///{path}"
    test_engine = create_engine(test_url, echo=False)
    
    # Apply schema
    Base.metadata.create_all(bind=test_engine)
    
    # Monkeypatch app.db.engine and SessionLocal
    monkeypatch.setattr("app.db.engine", test_engine)
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    monkeypatch.setattr("app.db.SessionLocal", TestSessionLocal)
    
    yield path
    
    # Cleanup
    test_engine.dispose()
    os.remove(path)
