#!/usr/bin/env python3
"""
Detect which module paths have changed between two git refs.
Outputs JSON with changed modules for selective test execution.

Usage:
    python detect_changed_paths.py --base main --head HEAD
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Set

# Map file paths to modules
PATH_TO_MODULE = {
    "app/": "app",
    "backend/fastapi/": "backend",
    "backend/core/": "backend",
    "backend/agents/": "backend",
    "backend/tests/": "backend",
    "frontend-web/": "frontend-web",
    "mobile-app/": "mobile-app",
    "shared/": "shared",
    ".github/workflows/": "ci",
    "requirements": "shared",
    "pytest.ini": "shared",
    "mypy.ini": "shared",
}


def get_changed_files(base_ref: str, head_ref: str) -> list:
    """Get list of changed files between two git refs."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", f"{base_ref}...{head_ref}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip().split("\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running git diff: {e.stderr}", file=sys.stderr)
        return []


def detect_modules(files: list) -> Set[str]:
    """Map changed files to modules."""
    modules = set()
    
    for file in files:
        if not file:
            continue
        
        # Check exact matches first
        for path, module in PATH_TO_MODULE.items():
            if file.startswith(path):
                modules.add(module)
                break
    
    # If shared or CI changed, run all tests
    if "shared" in modules or "ci" in modules:
        modules = {"all"}
    
    return modules


def main():
    if len(sys.argv) < 5:
        print("Usage: python detect_changed_paths.py --base <ref> --head <ref>")
        sys.exit(1)
    
    base_ref = sys.argv[2]
    head_ref = sys.argv[4]
    
    changed_files = get_changed_files(base_ref, head_ref)
    changed_modules = detect_modules(changed_files)
    
    # Convert modules to test list
    test_matrix = sorted(list(changed_modules))
    run_all = "all" in test_matrix
    
    output = {
        "modules": test_matrix if not run_all else ["all"],
        "run_all": run_all,
        "count": len(changed_files),
    }
    
    print(json.dumps(output))


if __name__ == "__main__":
    main()
