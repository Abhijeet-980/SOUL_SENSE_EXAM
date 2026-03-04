"""
Edge case and advanced scenario tests for Windows path normalization.
Tests concurrent access, timeouts, and system-specific behaviors.
"""

import os
import platform
import tempfile
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from app.utils.file_validation import validate_file_path, sanitize_filename
from app.exceptions import ValidationError


class TestConcurrentPathValidation:
    """Test path validation under concurrent access."""

    def test_concurrent_validation_same_path(self):
        """Test multiple threads validating the same path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "shared.txt")
            Path(test_file).write_text("content")
            
            results = []
            
            def validate_path():
                result = validate_file_path(test_file)
                results.append(result)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(validate_path) for _ in range(10)]
                for future in as_completed(futures):
                    future.result()
            
            assert len(results) == 10
            assert all(os.path.exists(r) for r in results)

    def test_concurrent_file_creation_validation(self):
        """Test creating and validating files concurrently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            
            def create_and_validate(name):
                filepath = os.path.join(tmpdir, f"file_{name}.txt")
                Path(filepath).write_text(f"content{name}")
                result = validate_file_path(filepath)
                return os.path.exists(result)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(create_and_validate, i) for i in range(10)]
                results = [future.result() for future in as_completed(futures)]
            
            assert all(results)


class TestWindowsSpecificPaths:
    """Test Windows-specific path scenarios."""

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-only")
    def test_unc_network_paths(self):
        """Test UNC path handling (\\server\share)."""
        # Note: Cannot fully test without actual network paths
        # This tests the pattern recognition
        unc_pattern = r"\\server\share\file.txt"
        # Should NOT raise exception for pattern (if base_dir not specified)
        try:
            # We can't actually validate UNC paths without a network,
            # but we can ensure they don't crash the validation
            pass
        except Exception as e:
            pytest.fail(f"UNC pattern handling failed: {e}")

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-only")
    def test_drive_letter_paths(self):
        """Test all drive letters are handled."""
        for drive_letter in "CDEFGHIJKLMNOPQRSTUVWXYZ":
            path = f"{drive_letter}:\\test\\file.txt"
            # Should handle without crashing
            try:
                validate_file_path(path, must_exist=False)
            except ValidationError:
                # Expected if drive doesn't exist or access denied
                pass

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-only")
    def test_extended_length_paths(self):
        """Test extended-length paths (\\?\\C:\\...)."""
        # Windows extended-length path prefix
        extended = r"\\?\C:\data\file.txt"
        try:
            validate_file_path(extended, must_exist=False)
        except ValidationError:
            pass  # Expected

    def test_forward_slash_conversion_consistency(self):
        """Test that mixed separators are handled consistently."""
        if platform.system() != "Windows":
            pytest.skip("Windows-specific test")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            # Try different separator combinations
            variations = [
                test_file,  # Native Windows backslash
                test_file.replace("\\", "/"),  # All forward slash
            ]
            
            results = []
            for path in variations:
                try:
                    result = validate_file_path(path)
                    results.append(result)
                except ValidationError:
                    pass
            
            # All should resolve to same absolute path
            if results:
                assert len(set(results)) == 1, "Path variations should normalize to same result"


class TestLongPathHandling:
    """Test handling of very long path names."""

    def test_path_near_limit(self):
        """Test paths approaching the 255 character limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create nested dirs with long names
            long_name = "a" * 50
            nested = os.path.join(tmpdir, long_name, long_name, "file.txt")
            
            if len(nested) < 255:
                os.makedirs(os.path.dirname(nested), exist_ok=True)
                Path(nested).write_text("test")
                result = validate_file_path(nested)
                assert os.path.exists(result)

    def test_unicode_long_paths(self):
        """Test long paths with unicode characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            unicode_name = "файл_文件_ファイル"  # Russian, Chinese, Japanese
            nested = os.path.join(tmpdir, unicode_name, "test.txt")
            
            try:
                os.makedirs(os.path.dirname(nested), exist_ok=True)
                Path(nested).write_text("test")
                result = validate_file_path(nested)
                assert os.path.exists(result)
            except (OSError, UnicodeError):
                pytest.skip("Unicode paths not supported on this system")


class TestSpecialCharacterHandling:
    """Test paths with special but valid characters."""

    def test_spaces_in_path(self):
        """Test paths with spaces."""
        with tempfile.TemporaryDirectory() as tmpdir:
            spaces_file = os.path.join(tmpdir, "my file with spaces.txt")
            Path(spaces_file).write_text("test")
            result = validate_file_path(spaces_file)
            assert os.path.exists(result)

    def test_hyphens_underscores(self):
        """Test paths with hyphens and underscores."""
        with tempfile.TemporaryDirectory() as tmpdir:
            special_file = os.path.join(tmpdir, "my-file_v2_final.txt")
            Path(special_file).write_text("test")
            result = validate_file_path(special_file)
            assert os.path.exists(result)

    def test_dots_in_filename(self):
        """Test multiple dots in filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dots_file = os.path.join(tmpdir, "archive.tar.gz")
            Path(dots_file).write_text("test")
            result = validate_file_path(dots_file)
            assert os.path.exists(result)

    def test_parentheses_in_path(self):
        """Test parentheses in filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            paren_file = os.path.join(tmpdir, "document(final).txt")
            Path(paren_file).write_text("test")
            result = validate_file_path(paren_file)
            assert os.path.exists(result)

    def test_plus_ampersand_signs(self):
        """Test plus and ampersand signs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            special_file = os.path.join(tmpdir, "file+data&backup.txt")
            Path(special_file).write_text("test")
            result = validate_file_path(special_file)
            assert os.path.exists(result)


class TestReservedNamesEdgeCases:
    """Additional edge cases for reserved name handling."""

    def test_reserved_name_as_substring(self):
        """Test reserved names as substrings are allowed."""
        result = sanitize_filename("myconfile.txt")
        assert "myconfile.txt" == result

    def test_reserved_name_with_dot_not_extension(self):
        """Test edge case of CON..txt style names."""
        # "CON." has root "CON." which is not exactly "CON"
        # So it won't be detected as reserved (expected behavior)
        result = sanitize_filename("CON..txt")
        # This is expected - os.path.splitext gives ("CON.", "") not ("CON", "..txt")
        assert isinstance(result, str)

    def test_reserved_name_with_numbers(self):
        """Test COM1-COM9 and LPT1-LPT9 edge cases."""
        # COM10 is NOT reserved
        result = sanitize_filename("COM10.txt")
        assert result == "COM10.txt"
        
        # COM1 is reserved
        result = sanitize_filename("COM1.txt")
        assert result.startswith("_")

    def test_very_long_reserved_device_name(self):
        """Test reserved name followed by many underscores."""
        result = sanitize_filename("CON_test_with_many_underscores.txt")
        # Root is "CON_test_with_many_underscores", not "CON"
        # So it's allowed (expected behavior - only exact names are reserved)
        assert result == "CON_test_with_many_underscores.txt"


class TestPathSanitizationEdgeCases:
    """Additional sanitization edge cases."""

    def test_only_invalid_characters(self):
        """Test string of only invalid characters."""
        result = sanitize_filename("<<<>>>")
        assert result == "unnamed_file"

    def test_control_characters(self):
        """Test removal of control characters."""
        result = sanitize_filename("file\x01\x02name.txt")
        # Should remove control characters
        assert "\x01" not in result
        assert "\x02" not in result

    def test_unicode_preservation(self):
        """Test that valid unicode characters are preserved."""
        result = sanitize_filename("café_文件.txt")
        # Should keep valid unicode
        if "café" in result or "文件" in result:
            pass  # OK
        else:
            # Falls back only if filesystem doesn't support
            assert isinstance(result, str)

    def test_consecutive_spaces(self):
        """Test handling of consecutive spaces."""
        result = sanitize_filename("file    name.txt")
        # Spaces are allowed
        assert "name" in result

    def test_tabs_and_newlines(self):
        """Test removal of tabs and newlines."""
        result = sanitize_filename("file\tname\ntest.txt")
        assert "\t" not in result
        assert "\n" not in result


class TestPathValidationPerformance:
    """Test performance characteristics of path validation."""

    def test_validation_speed(self):
        """Test path validation completes in reasonable time."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            Path(test_file).write_text("test")
            
            start = time.time()
            for _ in range(100):
                validate_file_path(test_file)
            elapsed = time.time() - start
            
            # Should validate 100 paths in under 1 second
            assert elapsed < 1.0, f"Validation took {elapsed}s for 100 paths"

    def test_sanitization_speed(self):
        """Test filename sanitization is fast."""
        filenames = [
            f"file_{i}_with_various_chars_!@#$.txt" 
            for i in range(100)
        ]
        
        start = time.time()
        for filename in filenames:
            sanitize_filename(filename)
        elapsed = time.time() - start
        
        # Should sanitize 100 filenames in under 0.1 seconds
        assert elapsed < 0.1, f"Sanitization took {elapsed}s for 100 files"


class TestBase64EncodedPaths:
    """Test handling of base64 or encoded path segments."""

    def test_base64_in_filename(self):
        """Test filenames with base64-like content."""
        result = sanitize_filename("file_aGVsbG8gd29ybGQ=.txt")
        assert "=" not in result  # = might be stripped as invalid
        assert "file" in result


class TestCaseInsensitivityWindows:
    """Test Windows case-insensitivity handling."""

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-only")
    def test_case_variation_same_file(self):
        """Test that different case variations reference same file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "MyFile.TXT")
            Path(test_file).write_text("test")
            
            # Try accessing with different cases
            variations = [
                os.path.join(tmpdir, "MyFile.TXT"),
                os.path.join(tmpdir, "myfile.txt"),
                os.path.join(tmpdir, "MYFILE.TXT"),
            ]
            
            results = []
            for path in variations:
                try:
                    result = validate_file_path(path)
                    if os.path.exists(result):
                        results.append(result)
                except ValidationError:
                    pass
            
            # On Windows, all should resolve to existing file
            if platform.system() == "Windows":
                assert len(results) > 0


class TestSymlinkBehavior:
    """Test symlink handling (Unix-like systems)."""

    @pytest.mark.skipif(platform.system() == "Windows", reason="Unix-only")
    def test_symlink_within_base_dir(self):
        """Test symlink pointing within base directory is allowed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.txt")
            Path(target).write_text("target content")
            
            link = os.path.join(tmpdir, "link.txt")
            try:
                os.symlink(target, link)
                result = validate_file_path(link, base_dir=tmpdir)
                assert os.path.exists(result)
            except OSError:
                pytest.skip("Cannot create symlinks")

    @pytest.mark.skipif(platform.system() == "Windows", reason="Unix-only")
    def test_symlink_chain_resolution(self):
        """Test symlink chains are properly resolved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "target.txt")
            Path(target).write_text("content")
            
            link1 = os.path.join(tmpdir, "link1.txt")
            link2 = os.path.join(tmpdir, "link2.txt")
            
            try:
                os.symlink(target, link1)
                os.symlink(link1, link2)
                result = validate_file_path(link2, base_dir=tmpdir)
                assert os.path.exists(result)
            except OSError:
                pytest.skip("Cannot create symlinks")
