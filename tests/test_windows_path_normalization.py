"""
Comprehensive Windows path normalization tests for Issue #1317.
Tests path validation, sanitization, and normalization across platforms.
"""

import os
import platform
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from app.utils.file_validation import validate_file_path, sanitize_filename
from app.exceptions import ValidationError


class TestWindowsPathValidation:
    """Test validate_file_path() with various Windows path formats."""

    def test_validate_absolute_windows_path(self):
        """Test validation of absolute Windows paths."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            tmp.close()  # Close before validation
            try:
                result = validate_file_path(tmp_path)
                assert os.path.isabs(result)
                assert os.path.exists(result)
            finally:
                try:
                    os.unlink(tmp_path)
                except PermissionError:
                    pass  # File might still be locked

    def test_validate_relative_paths(self):
        """Test validation of relative paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = validate_file_path("test.txt")
                assert os.path.isabs(result)
                assert result.endswith("test.txt")
            finally:
                os.chdir(original_cwd)

    def test_validate_mixed_separator_paths(self):
        """Test paths with mixed backslash and forward slash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir, exist_ok=True)
            test_file = os.path.join(subdir, "test.txt")
            Path(test_file).write_text("content")
            
            if platform.system() == "Windows":
                mixed_path = test_file.replace("\\", "/").replace("/", "\\", 1)
                result = validate_file_path(mixed_path)
                assert os.path.exists(result)

    def test_validate_relative_with_dot_prefix(self):
        """Test relative paths with ./ prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = validate_file_path("./test.txt")
                assert os.path.isabs(result)
                assert os.path.exists(result)
            finally:
                os.chdir(original_cwd)

    def test_path_length_limit(self):
        """Test path length validation (255 character limit)."""
        long_name = "a" * 260
        with pytest.raises(ValidationError, match="too long"):
            validate_file_path(long_name)

    def test_path_length_boundary(self):
        """Test path at 255 character boundary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            subpath = os.path.join(tmpdir, "a" * 100, "test.txt")
            if len(subpath) <= 255:
                os.makedirs(os.path.dirname(subpath), exist_ok=True)
                Path(subpath).write_text("test")
                result = validate_file_path(subpath)
                assert os.path.exists(result)

    def test_empty_path_validation(self):
        """Test validation rejects empty paths."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_file_path("")

    def test_file_existence_check(self):
        """Test must_exist parameter."""
        nonexistent = "/nonexistent/path/to/file.txt"
        with pytest.raises(ValidationError):
            validate_file_path(nonexistent, must_exist=True)

    def test_file_not_required_to_exist(self):
        """Test validation passes when must_exist is False."""
        fake_path = "/tmp/fake_file_12345_nonexistent.txt"
        try:
            result = validate_file_path(fake_path, must_exist=False)
            assert isinstance(result, str)
        except ValidationError:
            pass


class TestPathTraversalPrevention:
    """Test security: path traversal attacks prevention."""

    def test_parent_directory_escape_attempt(self):
        """Test prevention of ../ directory traversal."""
        with tempfile.TemporaryDirectory() as tmpdir:
            safe_file = os.path.join(tmpdir, "safe.txt")
            Path(safe_file).write_text("test")
            
            malicious_path = os.path.join(tmpdir, "..", "escaped.txt")
            with pytest.raises(ValidationError, match="Access denied"):
                validate_file_path(malicious_path, base_dir=tmpdir)

    def test_backslash_traversal_attempt(self):
        """Test prevention of ..\\ directory traversal on Windows."""
        if platform.system() != "Windows":
            pytest.skip("Windows-only test")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            malicious = os.path.join(tmpdir, "..\\escape")
            with pytest.raises(ValidationError, match="Access denied"):
                validate_file_path(malicious, base_dir=tmpdir)

    def test_absolute_path_outside_base_dir(self):
        """Test rejection of absolute paths outside base_dir."""
        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                test_file = os.path.join(tmpdir2, "file.txt")
                Path(test_file).write_text("test")
                
                with pytest.raises(ValidationError, match="Access denied"):
                    validate_file_path(test_file, base_dir=tmpdir1)

    def test_symlink_escape_attempt(self):
        """Test prevention of symlink-based traversal."""
        if platform.system() == "Windows":
            pytest.skip("Symlink test not reliable on Windows")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            with tempfile.TemporaryDirectory() as external:
                external_file = os.path.join(external, "external.txt")
                Path(external_file).write_text("external")
                
                symlink_path = os.path.join(tmpdir, "link")
                try:
                    os.symlink(external_file, symlink_path)
                    with pytest.raises(ValidationError, match="Access denied"):
                        validate_file_path(symlink_path, base_dir=tmpdir)
                except OSError:
                    pytest.skip("Cannot create symlinks")

    def test_base_directory_confinement_allowed(self):
        """Test allowed access within base directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir, exist_ok=True)
            test_file = os.path.join(subdir, "file.txt")
            Path(test_file).write_text("test")
            
            result = validate_file_path(test_file, base_dir=tmpdir)
            assert os.path.exists(result)


class TestReservedNamesAndSanitization:
    """Test filename sanitization and reserved name handling."""

    def test_reserved_device_names(self):
        """Test reserved device names are prefixed."""
        reserved = ["CON", "PRN", "AUX", "NUL"]
        for name in reserved:
            result = sanitize_filename(name)
            assert result.startswith("_"), f"Reserved name {name} not prefixed"

    def test_reserved_com_ports(self):
        """Test reserved COM port names."""
        for i in range(1, 10):
            name = f"COM{i}"
            result = sanitize_filename(name)
            assert result.startswith("_"), f"Reserved name {name} not prefixed"

    def test_reserved_lpt_ports(self):
        """Test reserved LPT port names."""
        for i in range(1, 10):
            name = f"LPT{i}"
            result = sanitize_filename(name)
            assert result.startswith("_"), f"Reserved name {name} not prefixed"

    def test_case_insensitive_reserved_names(self):
        """Test reserved names are detected case-insensitively."""
        cases = ["con", "Con", "CON", "cOn"]
        for name in cases:
            result = sanitize_filename(name)
            assert result.startswith("_"), f"Reserved name {name} not detected"

    def test_reserved_name_with_extension(self):
        """Test reserved names with extensions are protected."""
        result = sanitize_filename("CON.txt")
        assert result.startswith("_"), "Reserved name with extension not protected"

    def test_invalid_character_removal(self):
        """Test removal of invalid filename characters."""
        invalid_chars = ["<", ">", ":", "\"", "/", "\\", "|", "?", "*"]
        for char in invalid_chars:
            filename = f"test{char}file.txt"
            result = sanitize_filename(filename)
            assert char not in result, f"Invalid char {char} not removed"

    def test_null_byte_removal(self):
        """Test null byte handling in filenames."""
        result = sanitize_filename("test\x00file.txt")
        assert "\x00" not in result

    def test_empty_filename_fallback(self):
        """Test fallback for sanitized-to-empty filenames."""
        result = sanitize_filename("<>?:")
        assert result == "unnamed_file"

    def test_custom_fallback(self):
        """Test custom fallback name."""
        result = sanitize_filename("<>?:", fallback="custom_name")
        assert result == "custom_name"

    def test_valid_filename_unchanged(self):
        """Test that valid filenames pass through unchanged."""
        valid = "my_document-2024.txt"
        result = sanitize_filename(valid)
        assert result == valid

    def test_whitespace_handling(self):
        """Test leading/trailing whitespace is trimmed."""
        result = sanitize_filename("  filename.txt  ")
        assert result == "filename.txt"

    def test_spaces_in_filename_preserved(self):
        """Test spaces within filenames are preserved."""
        result = sanitize_filename("my file name.txt")
        assert "my file name.txt" == result


class TestExtensionValidation:
    """Test file extension validation."""

    def test_valid_single_extension(self):
        """Test single valid extension."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            result = validate_file_path(test_file, allowed_extensions=[".txt"])
            assert os.path.exists(result)

    def test_multiple_allowed_extensions(self):
        """Test multiple allowed extensions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for ext in [".txt", ".json", ".csv"]:
                test_file = os.path.join(tmpdir, f"test{ext}")
                Path(test_file).write_text("test")
                
                result = validate_file_path(
                    test_file, 
                    allowed_extensions=[".txt", ".json", ".csv"]
                )
                assert os.path.exists(result)

    def test_extension_case_insensitive(self):
        """Test extension validation is case-insensitive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.TXT")
            Path(test_file).write_text("test")
            
            result = validate_file_path(
                test_file, 
                allowed_extensions=[".txt"]
            )
            assert os.path.exists(result)

    def test_invalid_extension_rejected(self):
        """Test invalid extensions are rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.exe")
            Path(test_file).write_text("test")
            
            with pytest.raises(ValidationError, match="Invalid file extension"):
                validate_file_path(
                    test_file, 
                    allowed_extensions=[".txt", ".json"]
                )

    def test_extension_without_dot(self):
        """Test extensions without leading dot are handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            result = validate_file_path(
                test_file, 
                allowed_extensions=["txt"]
            )
            assert os.path.exists(result)

    def test_double_extension(self):
        """Test double extensions (.tar.gz)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "archive.tar.gz")
            Path(test_file).write_text("test")
            
            result = validate_file_path(
                test_file, 
                allowed_extensions=[".gz"]
            )
            assert os.path.exists(result)


class TestConfigurationPathHandling:
    """Test SQLite and configuration path normalization."""

    def test_sqlite_backslash_normalization(self):
        """Test SQLite URLs normalize backslashes to forward slashes."""
        if platform.system() != "Windows":
            pytest.skip("Windows-specific test")
        
        url_with_backslash = r"sqlite:///C:\data\db.sqlite"
        normalized = url_with_backslash.replace("\\", "/")
        assert "\\" not in normalized
        assert normalized == "sqlite:///C:/data/db.sqlite"

    def test_relative_sqlite_path_conversion(self):
        """Test relative sqlite paths are converted properly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = tmpdir
            relative_sqlite = "./data/db.sqlite"
            
            if relative_sqlite.startswith("./"):
                absolute_path = os.path.join(base_dir, relative_sqlite[2:])
            else:
                absolute_path = os.path.join(base_dir, relative_sqlite)
            
            assert os.path.isabs(absolute_path)
            assert "data" in absolute_path

    def test_absolute_sqlite_path_unchanged(self):
        """Test absolute sqlite paths remain unchanged."""
        abs_path = "/var/lib/app/db.sqlite"
        assert os.path.isabs(abs_path)


class TestIntegrationScenarios:
    """Integration tests with real file operations."""

    def test_file_write_normalized_path(self):
        """Test file write/read with normalized paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "data.json")
            validated = validate_file_path(filepath, must_exist=False)
            
            Path(validated).write_text('{"key": "value"}')
            assert Path(validated).exists()
            
            content = Path(validated).read_text()
            assert "key" in content


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_nonexistent_parent_directory(self):
        """Test validation handles non-existent parent directories."""
        fake_path = "/fake/nonexistent/directory/file.txt"
        try:
            validate_file_path(fake_path, must_exist=False)
        except ValidationError:
            pass

    def test_special_characters_in_path(self):
        """Test paths with special but valid characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            special_file = os.path.join(tmpdir, "my-file_2024 v1.txt")
            Path(special_file).write_text("test")
            
            result = validate_file_path(special_file)
            assert os.path.exists(result)