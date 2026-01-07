import os
import sqlite3
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, Session
from alembic.config import Config
from alembic import command
import logging

from app.config import DATABASE_URL, DB_PATH, BASE_DIR

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_engine():
    return engine

def get_session() -> Session:
    """
    Returns a new SQLAlchemy Session.
    Caller is responsible for closing it (or usage via context manager).
    """
    return SessionLocal()

def check_db_state():
    """
    Checks if the database exists and if it needs to be stamped with the initial migration.
    Then, upgrades the database to the latest head.
    """
    # Ensure dependencies are ready
    try:
        ini_path = os.path.join(BASE_DIR, "alembic.ini")
        alembic_cfg = Config(ini_path)
        script_location = os.path.join(BASE_DIR, "migrations")
        alembic_cfg.set_main_option("script_location", script_location)

        # Check for legacy state
        if os.path.exists(DB_PATH):
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            if "scores" in tables and "alembic_version" not in tables:
                logging.info("Existing database detected without migrations. Stamping 'head'...")
                command.stamp(alembic_cfg, "head")
        
        # Always attempt to upgrade to the latest version
        # This creates tables for fresh installs AND applies new migrations for existing users
        logging.info("Checking for database migrations...")
        command.upgrade(alembic_cfg, "head")
        logging.info("Database migration check complete.")
            
    except Exception as e:
        logging.error(f"Failed to verify/migrate database: {e}")

# Call verify on import (or explicit init)
check_db_state()

# Backward compatibility for old code (if any remains)
def get_connection(db_path=None):
    return sqlite3.connect(db_path or DB_PATH)
