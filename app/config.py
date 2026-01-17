"""
Configuration module for SoulSense application.

Supports configuration from:
1. Environment variables (SOULSENSE_* prefix) - highest priority
2. config.json file
3. Default values - lowest priority

Environment Variables:
- SOULSENSE_ENV: development, production, or test
- SOULSENSE_DB_PATH: Override database file path
- SOULSENSE_DEBUG: Enable debug mode (true/false)
- SOULSENSE_LOG_LEVEL: DEBUG, INFO, WARNING, ERROR
- SOULSENSE_ENABLE_JOURNAL: Enable journal feature (true/false)
- SOULSENSE_ENABLE_ANALYTICS: Enable analytics feature (true/false)
"""

import os
import json
import logging
import copy
from typing import Dict, Any, Optional

from app.exceptions import ConfigurationError

BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH: str = os.path.join(BASE_DIR, "config.json")

# ============================================================================
# Environment Detection
# ============================================================================

def get_env() -> str:
    """
    Get current environment from SOULSENSE_ENV variable.
    
    Returns:
        str: 'development', 'production', or 'test'
    """
    env = os.environ.get("SOULSENSE_ENV", "development").lower()
    if env not in ("development", "production", "test"):
        logging.warning(f"Unknown SOULSENSE_ENV '{env}', defaulting to 'development'")
        return "development"
    return env


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    """Parse string to boolean."""
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


# ============================================================================
# Default Configuration
# ============================================================================

DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "database": {
        "filename": "soulsense.db",
        "path": "db"
    },
    "ui": {
        "theme": "light",
        "window_size": "800x600"
    },
    "features": {
        "enable_journal": True,
        "enable_analytics": True
    },
    "app": {
        "debug": False,
        "log_level": "INFO"
    }
}


# ============================================================================
# Configuration Loading
# ============================================================================

def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.json or return defaults.
    
    Priority: Environment variables > config.json > defaults
    """
    # Start with defaults
    if not os.path.exists(CONFIG_PATH):
        logging.warning(f"Config file not found at {CONFIG_PATH}. Using defaults.")
        merged = copy.deepcopy(DEFAULT_CONFIG)
    else:
        try:
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
                # Use deepcopy to avoid mutating the global DEFAULT_CONFIG
                merged = copy.deepcopy(DEFAULT_CONFIG)
                for section in ["database", "ui", "features", "app"]:
                    if section in config:
                        merged[section].update(config[section])
        except json.JSONDecodeError as e:
            # Critical: File exists but is corrupt
            raise ConfigurationError(f"Configuration file is corrupt: {e}", original_exception=e)
        except Exception as e:
            raise ConfigurationError(f"Failed to load config file: {e}", original_exception=e)
    
    # Apply environment variable overrides
    _apply_env_overrides(merged)
    
    return merged


def _apply_env_overrides(config: Dict[str, Any]) -> None:
    """Apply environment variable overrides to configuration."""
    
    # Database path override
    env_db_path = os.environ.get("SOULSENSE_DB_PATH")
    if env_db_path:
        # If full path provided, extract filename and path
        config["database"]["filename"] = os.path.basename(env_db_path)
        config["database"]["path"] = os.path.dirname(env_db_path) or "db"
    
    # Debug mode override
    env_debug = os.environ.get("SOULSENSE_DEBUG")
    if env_debug is not None:
        config["app"]["debug"] = _parse_bool(env_debug)
    
    # Log level override
    env_log_level = os.environ.get("SOULSENSE_LOG_LEVEL")
    if env_log_level:
        config["app"]["log_level"] = env_log_level.upper()
    
    # Feature toggles override
    env_journal = os.environ.get("SOULSENSE_ENABLE_JOURNAL")
    if env_journal is not None:
        config["features"]["enable_journal"] = _parse_bool(env_journal, default=True)
    
    env_analytics = os.environ.get("SOULSENSE_ENABLE_ANALYTICS")
    if env_analytics is not None:
        config["features"]["enable_analytics"] = _parse_bool(env_analytics, default=True)


def save_config(new_config: Dict[str, Any]) -> bool:
    """Save configuration to config.json."""
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(new_config, f, indent=4)
        logging.info("Configuration saved successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to save config: {e}")
        # Raising error here allows caller to show UI error if needed
        raise ConfigurationError(f"Failed to save configuration: {e}", original_exception=e)


# ============================================================================
# Load Config on Import
# ============================================================================

_config: Dict[str, Any] = load_config()

# ============================================================================
# Expose Settings
# ============================================================================

# Environment
ENV: str = get_env()

# Database Settings
DB_DIR_NAME: str = _config["database"]["path"]
DB_FILENAME: str = _config["database"]["filename"]

# Directory Definitions
DATA_DIR: str = os.path.join(BASE_DIR, "data")
LOG_DIR: str = os.path.join(BASE_DIR, "logs")
MODELS_DIR: str = os.path.join(BASE_DIR, "models")

# Ensure directories exist
for directory in [DATA_DIR, LOG_DIR, MODELS_DIR]:
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError:
            pass

# Calculated Paths
# Check if SOULSENSE_DB_PATH provides an absolute path
_env_db_path = os.environ.get("SOULSENSE_DB_PATH")
if _env_db_path and os.path.isabs(_env_db_path):
    DB_PATH: str = _env_db_path
elif DB_DIR_NAME == "db":
    DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)
else:
    # Allow custom path relative to BASE_DIR if specified in config.json
    DB_PATH = os.path.join(BASE_DIR, DB_DIR_NAME, DB_FILENAME)

DATABASE_URL: str = f"sqlite:///{DB_PATH}"

# Ensure DB Directory Exists
_db_dir = os.path.dirname(DB_PATH)
if _db_dir and not os.path.exists(_db_dir):
    try:
        os.makedirs(_db_dir)
    except OSError:
        pass  # Handle race condition or permission error

# UI Settings
THEME: str = _config["ui"]["theme"]
WINDOW_SIZE: str = _config["ui"]["window_size"]

# App Settings
DEBUG: bool = _config["app"]["debug"]
LOG_LEVEL: str = _config["app"]["log_level"]

# Feature Toggles
ENABLE_JOURNAL: bool = _config["features"]["enable_journal"]
ENABLE_ANALYTICS: bool = _config["features"]["enable_analytics"]

# Full config object
APP_CONFIG: Dict[str, Any] = _config
