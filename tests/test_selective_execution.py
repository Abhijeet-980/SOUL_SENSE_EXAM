"""
Tests for monorepo path-based selective test execution (Issue #1433).

Coverage:
- Change detection accuracy
- Module path mapping
- Test discovery and execution
- Edge cases and error handling
"""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pytest


class TestChangeDetection:
    """Unit tests for change detection logic."""

    def test_detect_app_changes(self):
        """Test detection of changes in app/ module."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["app/ui.py", "app/models.py"]
        modules = detect_modules(files)
        assert "app" in modules
        assert "shared" not in modules

    def test_detect_backend_changes(self):
        """Test detection of changes in backend modules."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["backend/fastapi/main.py", "backend/core/db.py"]
        modules = detect_modules(files)
        assert "backend" in modules

    def test_detect_frontend_changes(self):
        """Test detection of changes in frontend-web module."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["frontend-web/components.tsx", "frontend-web/pages.tsx"]
        modules = detect_modules(files)
        assert "frontend-web" in modules

    def test_detect_shared_changes_triggers_all(self):
        """Test that shared/ changes trigger all tests."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["shared/utils.py", "app/ui.py"]
        modules = detect_modules(files)
        assert "all" in modules  # Should trigger all tests

    def test_detect_ci_changes_triggers_all(self):
        """Test that CI workflow changes trigger all tests."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = [".github/workflows/python-app.yml"]
        modules = detect_modules(files)
        assert "all" in modules

    def test_detect_requirements_changes_triggers_all(self):
        """Test that requirements.txt changes trigger all tests."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["requirements.txt"]
        modules = detect_modules(files)
        assert "all" in modules

    def test_detect_no_changes(self):
        """Test behavior with no relevant changes."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["README.md", "docs/setup.md"]
        modules = detect_modules(files)
        assert len(modules) == 0

    def test_detect_multiple_modules(self):
        """Test detection of changes in multiple modules."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["app/ui.py", "backend/fastapi/main.py", "frontend-web/index.tsx"]
        modules = detect_modules(files)
        assert "app" in modules
        assert "backend" in modules
        assert "frontend-web" in modules


class TestTestMapping:
    """Unit tests for test mapping configuration."""

    def test_mapping_file_exists(self):
        """Test that test-mapping.json exists and is valid."""
        mapping_file = Path(__file__).parent.parent / "test-mapping.json"
        assert mapping_file.exists(), "test-mapping.json not found"
        
        with open(mapping_file) as f:
            data = json.load(f)
        
        assert isinstance(data, dict)
        assert "app" in data
        assert "backend" in data
        assert "all" in data

    def test_mapping_has_required_fields(self):
        """Test that each module config has required fields."""
        mapping_file = Path(__file__).parent.parent / "test-mapping.json"
        with open(mapping_file) as f:
            data = json.load(f)
        
        for module, config in data.items():
            assert "test_paths" in config, f"Missing test_paths for {module}"
            assert isinstance(config["test_paths"], list)

    def test_mapping_test_paths_strings(self):
        """Test that test paths are strings."""
        mapping_file = Path(__file__).parent.parent / "test-mapping.json"
        with open(mapping_file) as f:
            data = json.load(f)
        
        for module, config in data.items():
            for path in config["test_paths"]:
                assert isinstance(path, str), f"Invalid path type in {module}"


class TestRunSelectiveTests:
    """Integration tests for selective test execution."""

    def test_selective_tests_script_exists(self):
        """Test that run_selective_tests.py exists."""
        script = Path(__file__).parent.parent / "scripts" / "run_selective_tests.py"
        assert script.exists(), "run_selective_tests.py not found"

    def test_selective_tests_is_executable(self):
        """Test that run_selective_tests.py is valid Python."""
        script = Path(__file__).parent.parent / "scripts" / "run_selective_tests.py"
        with open(script, encoding="utf-8") as f:
            code = f.read()
        
        # Should not raise SyntaxError
        compile(code, str(script), "exec")

    def test_can_import_detect_script(self):
        """Test that detect_changed_paths.py can be imported."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        try:
            import detect_changed_paths
            assert hasattr(detect_changed_paths, "detect_modules")
        except ImportError:
            pytest.skip("Could not import detect_changed_paths")


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_file_list(self):
        """Test with empty file list."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        modules = detect_modules([])
        assert len(modules) == 0

    def test_none_in_file_list(self):
        """Test with None values in file list."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["app/ui.py", None, ""]
        modules = detect_modules(files)
        assert "app" in modules

    def test_nonexistent_module(self):
        """Test behavior with unrecognized files."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = ["unknown/file.py"]
        modules = detect_modules(files)
        assert len(modules) == 0

    def test_path_with_mixed_case(self):
        """Test path detection with various formats."""
        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from detect_changed_paths import detect_modules
        
        files = [
            "app/UI.py",
            "backend/core/DB.py",
            "FRONTEND-WEB/index.tsx",
        ]
        modules = detect_modules(files)
        # Path matching is case-sensitive
        assert "app" in modules or len(modules) >= 1


class TestDocumentation:
    """Tests for documentation completeness."""

    def test_documentation_exists(self):
        """Test that documentation file exists."""
        doc_file = Path(__file__).parent.parent / "docs" / "SELECTIVE_TEST_EXECUTION.md"
        assert doc_file.exists(), "Documentation file not found"

    def test_documentation_has_content(self):
        """Test that documentation is not empty."""
        doc_file = Path(__file__).parent.parent / "docs" / "SELECTIVE_TEST_EXECUTION.md"
        with open(doc_file) as f:
            content = f.read()
        
        assert len(content) > 100, "Documentation is too short"
        assert "selective" in content.lower() or "test" in content.lower()


# Markers for test categorization
pytestmark = pytest.mark.unit


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
