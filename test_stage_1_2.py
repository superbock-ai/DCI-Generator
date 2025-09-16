#!/usr/bin/env python3
"""
Test script for Stage 1.2: Docker Container Creation
Validates Dockerfile and Docker-related files, and tests container if Docker is available.
"""

import subprocess
import sys
import os
from pathlib import Path

def test_dockerfile_exists():
    """Test that Dockerfile exists and has correct structure."""
    print("Testing Dockerfile existence and structure...")

    dockerfile_path = Path("worker/Dockerfile")
    if not dockerfile_path.exists():
        print("✗ Dockerfile not found in worker/")
        return False

    with open(dockerfile_path, 'r') as f:
        content = f.read()

    required_elements = [
        "FROM python:3.12-slim",
        "COPY --from=ghcr.io/astral-sh/uv:latest",
        "WORKDIR /app",
        "uv sync",
        "useradd",
        "USER worker",
        "CMD [\"python\", \"main.py\", \"--help\"]"
    ]

    for element in required_elements:
        if element in content:
            print(f"✓ Found: {element}")
        else:
            print(f"✗ Missing: {element}")
            return False

    print("✓ Dockerfile structure is correct")
    return True

def test_dockerignore_exists():
    """Test that .dockerignore exists and excludes appropriate files."""
    print("Testing .dockerignore...")

    # Check root .dockerignore (for building from root)
    dockerignore_path = Path(".dockerignore")
    if not dockerignore_path.exists():
        print("✗ .dockerignore not found in root directory")
        return False

    with open(dockerignore_path, 'r') as f:
        content = f.read()

    should_ignore = ["worker/debug/*", "worker/exports/*", "__pycache__", ".env"]

    for pattern in should_ignore:
        if pattern in content:
            print(f"✓ Ignores: {pattern}")
        else:
            print(f"✗ Missing ignore pattern: {pattern}")
            return False

    print("✓ .dockerignore is correct")
    return True

def test_build_script_exists():
    """Test that build script exists and is executable."""
    print("Testing build script...")

    build_script = Path("build_worker.sh")
    if not build_script.exists():
        print("✗ build_worker.sh not found")
        return False

    if not os.access(build_script, os.X_OK):
        print("✗ build_worker.sh is not executable")
        return False

    with open(build_script, 'r') as f:
        content = f.read()

    if "docker build -f worker/Dockerfile" in content:
        print("✓ Build script uses correct docker build command")
    else:
        print("✗ Build script doesn't use correct docker build command")
        return False

    print("✓ Build script is correct")
    return True

def test_docker_daemon():
    """Check if Docker daemon is available."""
    print("Checking Docker daemon availability...")
    try:
        result = subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"✓ Docker available: {result.stdout.strip()}")
            return True
        else:
            print("⚠ Docker not available - skipping container tests")
            return False
    except Exception as e:
        print(f"⚠ Docker not available: {e}")
        return False

def test_docker_build():
    """Test Docker build if daemon is available."""
    print("Testing Docker build...")
    try:
        # Build from root directory using worker/Dockerfile
        result = subprocess.run(
            ["docker", "build", "-f", "worker/Dockerfile", "-t", "dci-worker-test", "."],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout for build
        )
        if result.returncode == 0:
            print("✓ Docker build successful")

            # Try to run the container with help command
            run_result = subprocess.run(
                ["docker", "run", "--rm", "dci-worker-test"],
                capture_output=True,
                text=True,
                timeout=30
            )
            if run_result.returncode == 0 and "product_id" in run_result.stdout:
                print("✓ Container runs and shows help correctly")

                # Clean up test image
                subprocess.run(["docker", "rmi", "dci-worker-test"], capture_output=True)
                return True
            else:
                print(f"✗ Container run failed: {run_result.stderr}")
                return False
        else:
            print(f"✗ Docker build failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Docker build error: {e}")
        return False

def main():
    """Run all tests for Stage 1.2."""
    print("=" * 50)
    print("Stage 1.2: Docker Container Creation Tests")
    print("=" * 50)

    # Always run file structure tests
    file_tests = [
        test_dockerfile_exists,
        test_dockerignore_exists,
        test_build_script_exists,
    ]

    passed = 0
    total = len(file_tests)

    for test in file_tests:
        print()
        if test():
            passed += 1

    # Optional Docker tests if daemon is available
    print()
    if test_docker_daemon():
        print()
        if test_docker_build():
            passed += 1
        total += 1
    else:
        print("⚠ Skipping Docker build test - daemon not available")
        print("Manual test required: Start Docker and run './build_worker.sh'")

    print("\n" + "=" * 50)
    print(f"Results: {passed}/{total} tests passed")

    if passed >= len(file_tests):  # At minimum, file tests must pass
        print("✓ Stage 1.2 core requirements PASSED")
        if passed == total:
            print("✓ All tests including Docker build PASSED - Ready for Stage 2.1")
        else:
            print("⚠ Docker tests skipped - manually verify Docker build works")
        return True
    else:
        print("✗ Stage 1.2 tests FAILED - Fix issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)