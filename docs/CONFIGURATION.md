# SoulSense Configuration Guide

This document explains how to configure the SoulSense application using environment variables and configuration files.

## Configuration Hierarchy

Configuration is loaded in the following order (later sources override earlier):

1. **Default values** (built into `app/config.py`)
2. **config.json** (project root)
3. **Environment variables** (highest priority)

## Environment Variables

All environment variables use the `SOULSENSE_` prefix.

| Variable | Description | Default | Values |
|----------|-------------|---------|--------|
| `SOULSENSE_ENV` | Application environment | `development` | `development`, `production`, `test` |
| `SOULSENSE_DB_PATH` | Database file path | `data/soulsense.db` | Any valid file path |
| `SOULSENSE_DEBUG` | Enable debug mode | `false` | `true`, `false` |
| `SOULSENSE_LOG_LEVEL` | Logging verbosity | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `SOULSENSE_ENABLE_JOURNAL` | Enable journal feature | `true` | `true`, `false` |
| `SOULSENSE_ENABLE_ANALYTICS` | Enable analytics | `true` | `true`, `false` |

## config.json

The `config.json` file in the project root provides file-based configuration:

```json
{
    "database": {
        "filename": "soulsense.db",
        "path": "db"
    },
    "ui": {
        "theme": "light",
        "window_size": "800x600"
    },
    "features": {
        "enable_journal": true,
        "enable_analytics": true
    },
    "app": {
        "debug": false,
        "log_level": "INFO"
    }
}
```

## Quick Start

### Local Development

No configuration needed - defaults work out of the box.

### Production Deployment

Set environment variables:

```bash
export SOULSENSE_ENV=production
export SOULSENSE_DEBUG=false
export SOULSENSE_LOG_LEVEL=WARNING
```

### Testing

```bash
export SOULSENSE_ENV=test
export SOULSENSE_DB_PATH=/tmp/test_soulsense.db
```

## Using Configuration in Code

Import the exposed settings:

```python
from app.config import (
    DB_PATH,
    DATABASE_URL,
    DEBUG,
    LOG_LEVEL,
    ENABLE_JOURNAL,
    ENABLE_ANALYTICS,
    WINDOW_SIZE,
    ENV
)

# Check current environment
if ENV == "development":
    print("Running in development mode")
```

## .env File Support

For local development, copy `.env.example` to `.env` and modify as needed. Note: The application does not automatically load `.env` files - use a package like `python-dotenv` if needed.
