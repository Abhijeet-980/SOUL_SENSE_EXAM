#!/usr/bin/env python3
"""
Run selective tests based on changed modules in a monorepo.

Usage:
    python run_selective_tests.py app backend
    python run_selective_tests.py all
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional

CONFIG_FILE = "test-mapping.json"


def load_config() -> Dict:
    """Load test mapping configuration."""
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def path_exists(path: str) -> bool:
    """Check if test path exists."""
    return Path(path).exists()


def build_pytest_command(module: str, config: Dict) -> Optional[List[str]]:
    """Build pytest command for a module."""
    module_config = config.get(module)
    
    if not module_config:
        print(f"⚠️  Unknown module: {module}")
        return None
    
    test_paths = module_config.get("test_paths", [])
    
    # Check if paths exist
    existing_paths = [p for p in test_paths if path_exists(p)]
    
    if not existing_paths:
        if module_config.get("skip_if_not_exists"):
            print(f"⏭️  Skipping {module} (no tests found)")
            return None
        else:
            print(f"❌ No test paths found for {module}")
            return None
    
    # Build command
    cmd = ["pytest"] + existing_paths + ["-v", "--tb=short"]
    
    # Add markers
    markers = module_config.get("pytest_markers")
    if markers:
        cmd.extend(["-m", markers])
    
    # Add coverage
    coverage_targets = module_config.get("coverage_targets", [])
    for target in coverage_targets:
        cmd.extend(["--cov=" + target])
    
    if coverage_targets:
        cmd.extend(["--cov-report=term-missing", "--cov-report=html:htmlcov"])
    
    # Add parallelization
    if module_config.get("parallel"):
        cmd.extend(["-n", "auto"])
    
    return cmd


def run_tests(cmd: List[str], module: str) -> bool:
    """Run test command and return success status."""
    print(f"\n🧪 Testing {module}...")
    print(f"   Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd)
    success = result.returncode == 0
    
    if success:
        print(f"✅ {module} passed")
    else:
        print(f"❌ {module} failed")
    
    return success


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_selective_tests.py <module1> [module2] ...")
        print("       python run_selective_tests.py all")
        sys.exit(1)
    
    modules = sys.argv[1:]
    config = load_config()
    
    results = {}
    failed_modules = []
    
    for module in modules:
        cmd = build_pytest_command(module, config)
        
        if cmd is None:
            if module not in config or not config[module].get("skip_if_not_exists"):
                results[module] = "skipped"
            continue
        
        success = run_tests(cmd, module)
        results[module] = "passed" if success else "failed"
        
        if not success:
            failed_modules.append(module)
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 Test Summary")
    print("=" * 50)
    
    for module, status in results.items():
        symbol = "✅" if status == "passed" else "❌" if status == "failed" else "⏭️"
        print(f"{symbol} {module}: {status}")
    
    if failed_modules:
        print(f"\n❌ Failed modules: {', '.join(failed_modules)}")
        sys.exit(1)
    else:
        print(f"\n✅ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
