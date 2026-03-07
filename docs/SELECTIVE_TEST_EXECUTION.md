# Monorepo Path-Based Selective Test Execution

## Overview

This document describes the path-based selective test execution feature for the SOUL_SENSE_EXAM monorepo (Issue #1433). This feature optimizes CI/CD pipelines by running only tests affected by code changes.

## How It Works

### 1. Change Detection

When a PR or push occurs, the `detect_changed_paths.py` script:
- Compares the base branch (main) with the current branch
- Identifies which files changed
- Maps changed files to their modules

### 2. Module Mapping

The monorepo is organized into independent modules:

| Module | Path | Tests |
|--------|------|-------|
| **app** | `app/` | `tests/` |
| **backend** | `backend/fastapi/`, `backend/core/`, `backend/agents/` | `backend/tests/`, `backend/fastapi/tests/` |
| **frontend-web** | `frontend-web/` | `frontend-web/tests/` |
| **mobile-app** | `mobile-app/` | `mobile-app/tests/` |
| **shared** | `shared/` | All tests (triggers full suite) |

### 3. Conditional Test Execution

Based on detected changes:
- **Only app/ changed** → Run `tests/` only
- **Only backend/ changed** → Run `backend/tests/`, `backend/fastapi/tests/` only
- **Multiple modules changed** → Run tests for all affected modules
- **shared/ or CI files changed** → Run all tests (fallback)

## Files & Components

### Core Scripts

**`scripts/detect_changed_paths.py`**
- Detects changed modules between git refs
- Outputs JSON with module list and flag for running all tests
- Usage: `python scripts/detect_changed_paths.py --base main --head HEAD`

**`scripts/run_selective_tests.py`**
- Executes tests for specified modules
- Reads configuration from `test-mapping.json`
- Handles test discovery, parallelization, and coverage
- Usage: `python scripts/run_selective_tests.py app backend`

### Configuration

**`test-mapping.json`**
- Maps modules to test paths, markers, and pytest options
- Defines which modules can run in parallel
- Specifies coverage targets per module
- Allows conditional skipping for modules without tests

### CI Integration

**`.github/workflows/python-app.yml`**
- New `detect-changes` job identifies affected modules
- Test jobs conditionally run based on detection results
- Maintains fallback to all tests when needed

## Usage

### Local Testing

Run selective tests locally:

```bash
# Test only app module
python scripts/run_selective_tests.py app

# Test multiple modules
python scripts/run_selective_tests.py app backend

# Run all tests
python scripts/run_selective_tests.py all
```

### In CI/CD

Tests automatically run selectively on pull requests and pushes:

```yaml
- Detect which modules changed
- Only run tests for affected modules
- Report test results
```

## Configuration Guide

### Adding a New Module

1. Update `test-mapping.json`:
```json
"my-module": {
  "description": "Module description",
  "test_paths": ["my-module/tests/"],
  "pytest_markers": "not serial",
  "parallel": true
}
```

2. Update `scripts/detect_changed_paths.py`:
```python
PATH_TO_MODULE = {
    "my-module/": "my-module",
    # ... rest of mappings
}
```

3. Create tests in `my-module/tests/`

4. Run locally:
```bash
python scripts/run_selective_tests.py my-module
```

## Performance Benefits

Selective test execution reduces:
- **CI feedback time**: Only run affected tests (~40-60% faster for small changes)
- **Resource consumption**: Fewer concurrent test runs
- **Flakiness risk**: Less test interference from unrelated modules

## Troubleshooting

### Tests Are Running When They Shouldn't

**Issue**: All tests run even though only `app/` changed.

**Solution**: Check if a "shared" file was modified:
- `requirements*.txt`
- `.github/workflows/`
- `pytest.ini` or `mypy.ini`

These trigger full test suite by design.

### Specific Module Tests Aren't Found

**Issue**: `detect_changed_paths.py` doesn't recognize your module.

**Solution**: 
1. Verify path in `PATH_TO_MODULE` dict matches your file changes
2. Add explicit path mapping if needed

### Test Command Fails

**Issue**: `pytest` command generated for your module is invalid.

**Solution**:
1. Check that test paths in `test-mapping.json` exist
2. Verify pytest is installed: `pip install pytest pytest-xdist pytest-cov`
3. Check `pytest.ini` or `setup.cfg` for pytest configuration

## Testing the Feature

Run unit and integration tests for this feature:

```bash
pytest tests/test_selective_execution.py -v
```

Tests cover:
- Change detection accuracy
- Module path mapping
- Test discovery
- Edge cases (empty changes, multiple modules, etc.)
- Configuration validation

## Observability

When running selective tests, logs show:

```
🧪 Testing backend...
   Command: pytest backend/tests/ backend/fastapi/tests/ -v ...
✅ backend passed
```

Summary shows which modules were tested and their status.

## Rollback Plan

If selective execution causes issues:

1. **Temporarily disable**: Set all test jobs to `if: always()` in workflow
2. **Investigate**: Check CI logs for which tests failed
3. **Fix**: Update mapping or test configuration
4. **Re-enable**: Deploy the fix

## Future Enhancements

Possible improvements:
- Cache dependencies per module for faster startup
- Integration with coverage dashboards
- Automatic flaky test detection per module
- Documentation of test execution metrics

## References

- **Issue**: #1433 - Monorepo path-based selective test execution
- **Related**: pytest-xdist for parallel execution
- **Config**: `test-mapping.json` for module definitions
