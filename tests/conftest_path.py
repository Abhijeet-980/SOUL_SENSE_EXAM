"""
Shared pytest fixtures for path normalization tests.
"""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that is cleaned up after test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def temp_file(temp_dir):
    """Provide a temporary file in temp directory."""
    filepath = os.path.join(temp_dir, "test_file.txt")
    Path(filepath).write_text("test content")
    yield filepath


@pytest.fixture
def temp_dir_structure(temp_dir):
    """Create a directory structure for testing."""
    structure = {
        "root": temp_dir,
        "subdir1": os.path.join(temp_dir, "subdir1"),
        "subdir2": os.path.join(temp_dir, "subdir1", "subdir2"),
        "file1": os.path.join(temp_dir, "file1.txt"),
        "file2": os.path.join(temp_dir, "subdir1", "file2.json"),
    }
    
    # Create directories
    for subdir in [structure["subdir1"], structure["subdir2"]]:
        os.makedirs(subdir, exist_ok=True)
    
    # Create files
    Path(structure["file1"]).write_text("content1")
    Path(structure["file2"]).write_text('{"key": "value"}')
    
    yield structure


@pytest.fixture
def windows_path_samples():
    """Provide sample Windows-style paths for testing."""
    return {
        "absolute": r"C:\Users\test\document.txt",
        "relative": r".\subfolder\file.txt",
        "relative_with_parent": r"..\parent\file.txt",
        "unc": r"\\server\share\file.txt",
        "mixed_separators": r"C:/Users\test/file.txt",
    }


@pytest.fixture
def reserved_filenames():
    """Provide reserved Windows filename examples."""
    return {
        "devices": ["CON", "PRN", "AUX", "NUL"],
        "com_ports": [f"COM{i}" for i in range(1, 10)],
        "lpt_ports": [f"LPT{i}" for i in range(1, 10)],
    }


@pytest.fixture
def invalid_characters():
    """Provide invalid filesystem characters."""
    return {
        "windows": ["<", ">", ":", "\"", "/", "\\", "|", "?", "*"],
        "unix": ["\x00"],  # Null byte
        "both": ["\t", "\n", "\r"],  # Control characters
    }
