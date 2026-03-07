#!/usr/bin/env python3
"""
Flaky Test Detection Runner

Detects flaky tests by running them multiple times and capturing failures.
Generates a report JSON with flaky test metrics.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run_flaky_detection(test_count=3, output_file="flaky_test_report.json"):
    """
    Run all tests multiple times to detect flaky patterns.
    
    Args:
        test_count: Number of times to run each test
        output_file: Path to output JSON report
    
    Returns:
        exit_code: 0 if only quarantined tests fail, 1 if non-quarantined fail
    """
    print(f"🧪 Running flaky test detection (count={test_count})...")
    
    # Run tests with reruns using pytest-rerunfailures
    cmd = [
        "pytest",
        "tests/",
        f"--reruns={test_count-1}",
        "--reruns-delay=1",
        "-v",
        "--tb=short",
        f"--json-report --json-report-file={output_file}",
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        stdout = result.stdout
        stderr = result.stderr
    except FileNotFoundError:
        print("❌ pytest not found. Install with: pip install pytest pytest-rerunfailures")
        return 1
    
    # Parse output to identify flaky tests
    flaky_report = parse_test_output(stdout, stderr, test_count)
    
    # Save report
    report_path = Path(output_file)
    report_path.write_text(json.dumps(flaky_report, indent=2))
    print(f"✅ Report saved to {output_file}")
    
    # Check if quarantined tests
    quarantined = load_quarantined_tests()
    
    # Determine exit code
    has_non_quarantined_failures = any(
        test not in quarantined 
        for test in flaky_report.get("flaky_tests", [])
    )
    
    if has_non_quarantined_failures:
        print("❌ Non-quarantined flaky tests found - blocking merge")
        return 1
    else:
        print("✅ Only quarantined tests failed - allowing merge")
        return 0


def parse_test_output(stdout, stderr, expected_runs):
    """Extract flaky test information from pytest output."""
    flaky_tests = []
    
    # Simple pattern matching in output
    lines = stdout.split("\n")
    for line in lines:
        if "FLAKY" in line or "flaky" in line or "rerun" in line.lower():
            flaky_tests.append(line.strip())
    
    return {
        "timestamp": datetime.now().isoformat(),
        "test_runs_per_case": expected_runs,
        "flaky_tests": flaky_tests,
        "total_flaky": len(flaky_tests),
        "stdout_snippet": stdout[-500:] if stdout else "",
    }


def load_quarantined_tests():
    """Load quarantined test list from YAML registry."""
    import yaml
    
    try:
        yaml_file = Path("tests/flaky_tests.yaml")
        if not yaml_file.exists():
            return []
        
        with open(yaml_file) as f:
            data = yaml.safe_load(f) or {}
        
        quarantined = data.get("quarantined_tests", [])
        return [test.get("name") for test in quarantined if isinstance(test, dict)]
    except ImportError:
        print("⚠️  PyYAML not installed - skipping quarantine check")
        return []
    except Exception as e:
        print(f"⚠️  Error loading quarantine registry: {e}")
        return []


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Detect and quarantine flaky tests")
    parser.add_argument("--count", type=int, default=3, help="Times to run each test")
    parser.add_argument("--output", default="flaky_test_report.json", help="Output report file")
    
    args = parser.parse_args()
    
    exit_code = run_flaky_detection(test_count=args.count, output_file=args.output)
    sys.exit(exit_code)
