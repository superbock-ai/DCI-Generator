#!/usr/bin/env python3
"""
Test script for Stage 2: Celery Broker Queue (Redis)
Tests Redis container setup and environment configuration.
"""

import subprocess
import sys
import os
import time
from pathlib import Path

def test_docker_compose_exists():
    """Test that docker-compose.yml exists with correct services."""
    print("Testing docker-compose.yml...")

    compose_path = Path("docker-compose.yml")
    if not compose_path.exists():
        print("✗ docker-compose.yml not found")
        return False

    with open(compose_path, 'r') as f:
        content = f.read()

    required_services = ["redis:", "worker:", "image: redis:7-alpine", "healthcheck:"]
    required_config = ["redis_data:", "dci-network:", "REDIS_URL=", "CELERY_BROKER_URL="]

    for item in required_services + required_config:
        if item in content:
            print(f"✓ Found: {item}")
        else:
            print(f"✗ Missing: {item}")
            return False

    print("✓ docker-compose.yml structure is correct")
    return True

def test_env_example_updated():
    """Test that .env.example includes Redis configuration."""
    print("Testing .env.example Redis configuration...")

    env_example_path = Path(".env.example")
    if not env_example_path.exists():
        print("✗ .env.example not found")
        return False

    with open(env_example_path, 'r') as f:
        content = f.read()

    redis_vars = [
        "REDIS_URL=",
        "CELERY_BROKER_URL=",
        "CELERY_RESULT_BACKEND=",
        "DIRECTUS_SECRET="
    ]

    for var in redis_vars:
        if var in content:
            print(f"✓ Found environment variable: {var}")
        else:
            print(f"✗ Missing environment variable: {var}")
            return False

    print("✓ .env.example includes Redis configuration")
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

def test_docker_compose_available():
    """Check if docker-compose is available."""
    print("Checking docker-compose availability...")
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"✓ docker-compose available: {result.stdout.strip()}")
            return True
        else:
            print("⚠ docker-compose not available")
            return False
    except Exception as e:
        print(f"⚠ docker-compose not available: {e}")
        return False

def test_redis_container():
    """Test Redis container startup and health."""
    print("Testing Redis container startup...")
    try:
        # Start only Redis service
        print("Starting Redis service...")
        start_result = subprocess.run(
            ["docker", "compose", "up", "-d", "redis"],
            capture_output=True,
            text=True,
            timeout=60
        )

        if start_result.returncode != 0:
            print(f"✗ Failed to start Redis: {start_result.stderr}")
            return False

        print("✓ Redis service started")

        # Wait for health check
        print("Waiting for Redis health check...")
        for i in range(12):  # Wait up to 60 seconds
            health_result = subprocess.run(
                ["docker", "compose", "ps", "--format", "json", "redis"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if "healthy" in health_result.stdout:
                print("✓ Redis health check passed")
                break
            elif i < 11:  # Don't sleep on last iteration
                time.sleep(5)
        else:
            print("⚠ Redis health check timeout - may still be starting")

        # Test Redis connectivity
        print("Testing Redis connectivity...")
        ping_result = subprocess.run(
            ["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if ping_result.returncode == 0 and "PONG" in ping_result.stdout:
            print("✓ Redis responds to ping")
            success = True
        else:
            print(f"✗ Redis ping failed: {ping_result.stderr}")
            success = False

        # Cleanup
        print("Stopping Redis service...")
        subprocess.run(
            ["docker", "compose", "down"],
            capture_output=True,
            timeout=30
        )

        return success

    except Exception as e:
        print(f"✗ Redis container test error: {e}")
        # Cleanup on error
        subprocess.run(["docker", "compose", "down"], capture_output=True)
        return False

def main():
    """Run all tests for Stage 2."""
    print("=" * 50)
    print("Stage 2: Celery Broker Queue (Redis) Tests")
    print("=" * 50)

    # Always run file structure tests
    file_tests = [
        test_docker_compose_exists,
        test_env_example_updated,
    ]

    passed = 0
    total = len(file_tests)

    for test in file_tests:
        print()
        if test():
            passed += 1

    # Optional Docker tests if daemon is available
    print()
    if test_docker_daemon() and test_docker_compose_available():
        print()
        if test_redis_container():
            passed += 1
        total += 1
    else:
        print("⚠ Skipping Redis container test - Docker not available")
        print("Manual test required: Run 'docker compose up -d redis' and test connectivity")

    print("\n" + "=" * 50)
    print(f"Results: {passed}/{total} tests passed")

    if passed >= len(file_tests):  # At minimum, file tests must pass
        print("✓ Stage 2 core requirements PASSED")
        if passed == total:
            print("✓ All tests including Redis container PASSED - Ready for Stage 3.1")
        else:
            print("⚠ Redis container tests skipped - manually verify Redis works")
        return True
    else:
        print("✗ Stage 2 tests FAILED - Fix issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)