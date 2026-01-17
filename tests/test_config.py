import json
import os
import pytest
from app import config
import importlib

@pytest.fixture
def clean_config():
    """Ensure we start with a fresh config module for each test."""
    importlib.reload(config)
    yield
    importlib.reload(config)

def test_load_config_defaults_missing_file(monkeypatch, clean_config):
    """Test that default config is returned when config.json is missing."""
    monkeypatch.setattr(os.path, "exists", lambda x: False)
    
    # Reload config to re-execute the module-level load_config() call if needed
    # But since we are testing the function directly, we just call it.
    cfg = config.load_config()
    
    assert cfg["database"]["filename"] == "soulsense.db"
    assert cfg["ui"]["theme"] == "light"

def test_load_config_from_file(tmp_path, monkeypatch, clean_config):
    """Test full override."""
    temp_config = {
        "database": {"filename": "test_db.sqlite", "path": "custom_db"},
        "ui": {"theme": "dark"},
        "features": {"enable_journal": False}
    }
    
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(temp_config))
    
    monkeypatch.setattr("app.config.CONFIG_PATH", str(config_file))
    
    cfg = config.load_config()
    
    assert cfg["database"]["filename"] == "test_db.sqlite"
    assert cfg["ui"]["theme"] == "dark"

def test_load_config_partial_merge(tmp_path, monkeypatch, clean_config):
    """Test partial merge."""
    temp_config = {
        "ui": {"theme": "dark_blue"}
    }
    
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(temp_config))
    
    # CRITICAL: app.config.DEFAULT_CONFIG is a dictionary. 
    # If load_config modifies it in place (shallow copy issue?) that would be a bug.
    # Looking at implementation: `merged = DEFAULT_CONFIG.copy()` is a shallow copy.
    # But dictionary values are also dicts ("database": {...}).
    # If we update `merged["database"]` it might be modifying the reference in default if not deep copying?
    # Actually `merged["database"].update(...)` DOES modify the nested dict in DEFAULT_CONFIG if it's a shallow copy!
    
    monkeypatch.setattr("app.config.CONFIG_PATH", str(config_file))
    
    cfg = config.load_config()
    
    assert cfg["ui"]["theme"] == "dark_blue"
    # This assertion failed before because previous test mutated DEFAULT_CONFIG["database"]?
    assert cfg["database"]["filename"] == "soulsense.db" 


# ============================================================================
# Environment Variable Tests
# ============================================================================

def test_get_env_default(monkeypatch, clean_config):
    """Test that get_env returns 'development' by default."""
    monkeypatch.delenv("SOULSENSE_ENV", raising=False)
    
    assert config.get_env() == "development"


def test_get_env_production(monkeypatch, clean_config):
    """Test that get_env returns 'production' when set."""
    monkeypatch.setenv("SOULSENSE_ENV", "production")
    
    assert config.get_env() == "production"


def test_get_env_invalid_falls_back_to_development(monkeypatch, clean_config):
    """Test that invalid SOULSENSE_ENV falls back to development."""
    monkeypatch.setenv("SOULSENSE_ENV", "invalid_env")
    
    assert config.get_env() == "development"


def test_env_variable_override_debug(tmp_path, monkeypatch, clean_config):
    """Test that SOULSENSE_DEBUG environment variable overrides config."""
    # Create config.json with debug=false
    temp_config = {"app": {"debug": False}}
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(temp_config))
    
    monkeypatch.setattr("app.config.CONFIG_PATH", str(config_file))
    monkeypatch.setenv("SOULSENSE_DEBUG", "true")
    
    cfg = config.load_config()
    
    assert cfg["app"]["debug"] is True


def test_env_variable_override_log_level(tmp_path, monkeypatch, clean_config):
    """Test that SOULSENSE_LOG_LEVEL environment variable overrides config."""
    config_file = tmp_path / "config.json"
    config_file.write_text("{}")
    
    monkeypatch.setattr("app.config.CONFIG_PATH", str(config_file))
    monkeypatch.setenv("SOULSENSE_LOG_LEVEL", "DEBUG")
    
    cfg = config.load_config()
    
    assert cfg["app"]["log_level"] == "DEBUG"


def test_env_variable_override_feature_toggle(tmp_path, monkeypatch, clean_config):
    """Test that SOULSENSE_ENABLE_JOURNAL environment variable overrides config."""
    temp_config = {"features": {"enable_journal": True}}
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(temp_config))
    
    monkeypatch.setattr("app.config.CONFIG_PATH", str(config_file))
    monkeypatch.setenv("SOULSENSE_ENABLE_JOURNAL", "false")
    
    cfg = config.load_config()
    
    assert cfg["features"]["enable_journal"] is False


def test_parse_bool_helper(clean_config):
    """Test the _parse_bool helper function."""
    assert config._parse_bool("true") is True
    assert config._parse_bool("TRUE") is True
    assert config._parse_bool("1") is True
    assert config._parse_bool("yes") is True
    assert config._parse_bool("false") is False
    assert config._parse_bool("0") is False
    assert config._parse_bool(None, default=True) is True
    assert config._parse_bool(None, default=False) is False

