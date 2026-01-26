#!/usr/bin/env python3
"""
Initialize the main SoulSense database tables.

This script creates all necessary database tables using SQLAlchemy models
before running tests or starting the application.
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import check_db_state

def main():
    """Initialize the main database."""
    print("Initializing main SoulSense database...")
    try:
        success = check_db_state()
        if success:
            print("✅ Main database initialized successfully!")
            return 0
        else:
            print("❌ Failed to initialize main database!")
            return 1
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
