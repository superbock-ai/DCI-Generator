#!/usr/bin/env python3
"""
Test script for Celery task functionality.
This script can be used to test task submission when Redis is running.
"""

import os
import sys
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_task_import():
    """Test that tasks can be imported correctly."""
    print("Testing task import...")

    try:
        # Change to worker directory for imports
        sys.path.insert(0, 'worker')
        from tasks import analyze_document_task, cleanup_product_task
        from celery_app import app

        print("✓ Successfully imported Celery tasks")
        print(f"✓ Celery app broker: {app.conf.broker_url}")
        print(f"✓ Analysis task: {analyze_document_task.name}")
        print(f"✓ Cleanup task: {cleanup_product_task.name}")

        return True

    except Exception as e:
        print(f"✗ Task import failed: {e}")
        return False

def test_task_delay():
    """Test task submission (requires Redis to be running)."""
    print("\nTesting task submission...")

    try:
        sys.path.insert(0, 'worker')
        from tasks import analyze_document_task, cleanup_product_task

        # Test analysis task submission
        print("Submitting test analysis task...")
        result = analyze_document_task.delay(
            product_id="test-product-id",
            debug=True,
            export=False
        )

        print(f"✓ Analysis task submitted with ID: {result.id}")
        print(f"✓ Task state: {result.state}")

        # Test cleanup task submission
        print("Submitting test cleanup task...")
        cleanup_result = cleanup_product_task.delay("test-product-id")

        print(f"✓ Cleanup task submitted with ID: {cleanup_result.id}")
        print(f"✓ Task state: {cleanup_result.state}")

        return True

    except Exception as e:
        print(f"⚠ Task submission failed (Redis may not be running): {e}")
        return False

def main():
    """Run Celery task tests."""
    print("=" * 50)
    print("Celery Task Functionality Tests")
    print("=" * 50)

    # Always test imports
    if not test_task_import():
        return False

    # Test task submission (requires Redis)
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    print(f"\nRedis URL: {redis_url}")

    if test_task_delay():
        print("\n✓ Task submission works - Redis is accessible")
    else:
        print("\n⚠ Task submission failed - Start Redis with 'docker compose up -d redis'")

    print("\n" + "=" * 50)
    print("Task import tests PASSED")
    print("For full testing, ensure Redis is running and worker is started")

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)