# app/models.py
"""
Compatibility layer for tests and legacy imports.
Core models have been refactored elsewhere.
"""

from sqlalchemy.orm import declarative_base

Base = declarative_base()