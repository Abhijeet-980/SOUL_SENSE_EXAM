#!/usr/bin/env python3
"""
Flaky Test Manager CLI

Commands for managing quarantined and flaky tests:
  - list: Show all quarantined tests
  - quarantine: Add test to quarantine
  - unquarantine: Remove test from quarantine
  - investigate: Get details about a failing test
  - metrics: Generate metrics report
"""

import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


def load_registry():
    """Load the flaky tests YAML registry."""
    if not yaml:
        print("❌ PyYAML not installed. Install with: pip install pyyaml")
        return None
    
    registry_path = Path("tests/flaky_tests.yaml")
    if not registry_path.exists():
        return {"quarantined_tests": [], "needs_investigation": [], "metadata": {}}
    
    with open(registry_path) as f:
        return yaml.safe_load(f) or {"quarantined_tests": [], "needs_investigation": []}


def save_registry(data):
    """Save the flaky tests YAML registry."""
    if not yaml:
        return False
    
    registry_path = Path("tests/flaky_tests.yaml")
    with open(registry_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    return True


def list_flaky():
    """List all quarantined and investigation tests."""
    registry = load_registry()
    if not registry:
        return 1
    
    quarantined = registry.get("quarantined_tests", [])
    investigating = registry.get("needs_investigation", [])
    
    print(f"\n📋 Quarantined Tests ({len(quarantined)}):")
    if quarantined:
        for test in quarantined:
            name = test.get("name", "unknown")
            rate = test.get("failure_rate", "?")
            assigned = test.get("assigned_to", "unassigned")
            print(f"  • {name}")
            print(f"    Failure Rate: {rate*100:.0f}% | Assigned: {assigned}")
    else:
        print("  (none)")
    
    print(f"\n🔍 Needs Investigation ({len(investigating)}):")
    if investigating:
        for test in investigating:
            name = test.get("name", "unknown")
            occurs = test.get("occurrences", 1)
            print(f"  • {name} (failed {occurs} times)")
    else:
        print("  (none)")
    
    print()
    return 0


def quarantine(test_name, failure_rate=None, reason=None, assigned_to=None):
    """Add a test to quarantine."""
    if not yaml:
        return 1
    
    registry = load_registry()
    if not registry:
        return 1
    
    quarantined = registry.get("quarantined_tests", [])
    
    # Check if already quarantined
    if any(t.get("name") == test_name for t in quarantined):
        print(f"⚠️  Test already quarantined: {test_name}")
        return 1
    
    # Add to quarantine
    entry = {
        "name": test_name,
        "created_date": datetime.now().strftime("%Y-%m-%d"),
        "failure_rate": failure_rate or 0.5,
        "root_cause": reason or "Unknown",
        "assigned_to": assigned_to or None,
        "priority": "medium",
    }
    
    quarantined.append(entry)
    registry["quarantined_tests"] = quarantined
    registry["metadata"]["total_quarantined"] = len(quarantined)
    registry["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    
    if save_registry(registry):
        print(f"✅ Quarantined: {test_name}")
        return 0
    else:
        print("❌ Failed to save registry")
        return 1


def unquarantine(test_name):
    """Remove test from quarantine."""
    if not yaml:
        return 1
    
    registry = load_registry()
    if not registry:
        return 1
    
    quarantined = registry.get("quarantined_tests", [])
    
    # Find and remove
    original_count = len(quarantined)
    quarantined = [t for t in quarantined if t.get("name") != test_name]
    
    if len(quarantined) == original_count:
        print(f"⚠️  Test not found in quarantine: {test_name}")
        return 1
    
    registry["quarantined_tests"] = quarantined
    registry["metadata"]["total_quarantined"] = len(quarantined)
    registry["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    
    if save_registry(registry):
        print(f"✅ Removed from quarantine: {test_name}")
        return 0
    else:
        print("❌ Failed to save registry")
        return 1


def metrics():
    """Generate metrics report."""
    registry = load_registry()
    if not registry:
        return 1
    
    quarantined = registry.get("quarantined_tests", [])
    investigating = registry.get("needs_investigation", [])
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "total_quarantined": len(quarantined),
        "total_investigating": len(investigating),
        "total_flaky": len(quarantined) + len(investigating),
        "by_priority": {
            "critical": len([t for t in quarantined if t.get("priority") == "critical"]),
            "high": len([t for t in quarantined if t.get("priority") == "high"]),
            "medium": len([t for t in quarantined if t.get("priority") == "medium"]),
            "low": len([t for t in quarantined if t.get("priority") == "low"]),
        },
        "avg_failure_rate": sum(t.get("failure_rate", 0) for t in quarantined) / len(quarantined) if quarantined else 0,
    }
    
    print(json.dumps(metrics, indent=2))
    return 0


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage flaky tests")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # List command
    subparsers.add_parser("list", help="List all flaky tests")
    
    # Quarantine command
    q_parser = subparsers.add_parser("quarantine", help="Add test to quarantine")
    q_parser.add_argument("--test", required=True, help="Test name (path::test_name)")
    q_parser.add_argument("--rate", type=float, help="Failure rate (0-1)")
    q_parser.add_argument("--reason", help="Root cause")
    q_parser.add_argument("--assigned", help="Assigned developer")
    
    # Unquarantine command
    u_parser = subparsers.add_parser("unquarantine", help="Remove test from quarantine")
    u_parser.add_argument("--test", required=True, help="Test name (path::test_name)")
    
    # Metrics command
    subparsers.add_parser("metrics", help="Show metrics")
    
    args = parser.parse_args()
    
    if args.command == "list":
        return list_flaky()
    elif args.command == "quarantine":
        return quarantine(args.test, args.rate, args.reason, args.assigned)
    elif args.command == "unquarantine":
        return unquarantine(args.test)
    elif args.command == "metrics":
        return metrics()
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
