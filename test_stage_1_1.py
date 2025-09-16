#!/usr/bin/env python3
"""
Test script for Stage 1.1: Container Structure Setup
Verifies that the application works correctly from the new worker/ directory structure.
"""

import subprocess
import sys
import os
from pathlib import Path

def test_help_command():
    """Test that main.py --help works from new location."""
    print("Testing: uv run worker/main.py --help")
    try:
        result = subprocess.run(
            ["uv", "run", "worker/main.py", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0 and "product_id" in result.stdout:
            print("✓ Help command works correctly")
            return True
        else:
            print(f"✗ Help command failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Help command error: {e}")
        return False

def test_directory_structure():
    """Test that files are in the correct locations."""
    print("Testing directory structure...")

    # Check worker directory exists and contains expected files
    worker_files = ["main.py", "directus_tools.py"]
    worker_dirs = ["debug", "exports", "graphql"]

    for file in worker_files:
        path = Path(f"worker/{file}")
        if path.exists():
            print(f"✓ Found worker/{file}")
        else:
            print(f"✗ Missing worker/{file}")
            return False

    for dir in worker_dirs:
        path = Path(f"worker/{dir}")
        if path.exists() and path.is_dir():
            print(f"✓ Found worker/{dir}/")
        else:
            print(f"✗ Missing worker/{dir}/")
            return False

    # Check root files remain in place
    root_files = [".env.example", "pyproject.toml", "README.md"]
    for file in root_files:
        path = Path(file)
        if path.exists():
            print(f"✓ Found {file} in root")
        else:
            print(f"✗ Missing {file} in root")
            return False

    print("✓ Directory structure is correct")
    return True

def test_import_validation():
    """Test that imports work correctly from new location."""
    print("Testing import validation...")
    try:
        # Test that we can import the modules
        result = subprocess.run(
            ["python", "-c", "import sys; sys.path.append('worker'); import main; import directus_tools; print('Imports successful')"],
            capture_output=True,
            text=True,
            timeout=15
        )
        if result.returncode == 0 and "Imports successful" in result.stdout:
            print("✓ Imports work correctly")
            return True
        else:
            print(f"✗ Import test failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Import test error: {e}")
        return False

def main():
    """Run all tests for Stage 1.1."""
    print("=" * 50)
    print("Stage 1.1: Container Structure Setup Tests")
    print("=" * 50)

    tests = [
        test_directory_structure,
        test_import_validation,
        test_help_command,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        print()
        if test():
            passed += 1
        else:
            print("Test failed!")

    print("\n" + "=" * 50)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("✓ Stage 1.1 tests PASSED - Ready for Stage 1.2")
        return True
    else:
        print("✗ Stage 1.1 tests FAILED - Fix issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)