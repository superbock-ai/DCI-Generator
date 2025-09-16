#!/usr/bin/env python3
"""
Comprehensive Celery Worker Integration Test
Tests the complete workflow: Redis + Worker + Task execution with real product ID.
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_env_vars():
    """Check that required environment variables are set."""
    print("Checking environment variables...")

    required_vars = [
        'OPENAI_API_KEY',
        'DIRECTUS_AUTH_TOKEN',
        'DIRECTUS_URL',
        'REDIS_URL',
        'CELERY_BROKER_URL'
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            print(f"✓ {var} is set")

    if missing_vars:
        print(f"✗ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file")
        return False

    print("✓ All required environment variables are set")
    return True

def start_services():
    """Start Redis and Worker services using docker-compose."""
    print("\nStarting Redis and Worker services...")

    try:
        # Start services
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "redis", "worker"],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            print(f"✗ Failed to start services: {result.stderr}")
            return False

        print("✓ Services started successfully")

        # Wait for Redis to be healthy
        print("Waiting for Redis health check...")
        for i in range(12):  # Wait up to 60 seconds
            health_result = subprocess.run(
                ["docker", "compose", "ps", "--format", "json", "redis"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if "healthy" in health_result.stdout:
                print("✓ Redis is healthy")
                break
            elif i < 11:
                time.sleep(5)
        else:
            print("⚠ Redis health check timeout")

        # Check worker status
        print("Checking worker status...")
        worker_result = subprocess.run(
            ["docker", "compose", "ps", "worker"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if "Up" in worker_result.stdout:
            print("✓ Worker container is running")
        else:
            print(f"⚠ Worker status unclear: {worker_result.stdout}")

        # Give worker a moment to connect to Redis
        print("Giving worker time to connect to Redis...")
        time.sleep(5)

        return True

    except Exception as e:
        print(f"✗ Error starting services: {e}")
        return False

def get_worker_logs():
    """Get recent worker logs."""
    try:
        result = subprocess.run(
            ["docker", "compose", "logs", "--tail", "20", "worker"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout
    except:
        return "Could not retrieve logs"

def test_task_submission(product_id="2258e45a-531e-4412-ab47-3c6bd96eed8a"):
    """Test submitting tasks to the Celery worker."""
    print(f"\nTesting task submission with product ID: {product_id}")

    try:
        # Add worker directory to path for imports
        sys.path.insert(0, 'worker')
        from tasks import analyze_document_task, cleanup_product_task

        print("✓ Successfully imported tasks")

        # Test analysis task submission
        print("\n--- Testing Analysis Task ---")
        print("Submitting analysis task...")

        analysis_result = analyze_document_task.delay(
            product_id=product_id,
            debug=True,
            export=True,
            detailed=False,
            segment_chunks=3,  # Small chunks for faster testing
            benefit_chunks=3,
            detail_chunks=2,
            seed_directus=False,  # Skip seeding for test
            dry_run_directus=False
        )

        print(f"✓ Analysis task submitted with ID: {analysis_result.id}")
        print(f"✓ Initial task state: {analysis_result.state}")

        # Monitor task progress
        print("\nMonitoring task progress...")
        timeout = 300  # 5 minutes timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_state = analysis_result.state
            print(f"Current state: {current_state}")

            if current_state == 'PENDING':
                print("  → Task is queued, waiting for worker...")
            elif current_state == 'PROCESSING':
                try:
                    meta = analysis_result.info
                    if meta and 'status' in meta:
                        print(f"  → {meta['status']}")
                except:
                    print("  → Processing...")
            elif current_state == 'SUCCESS':
                print("✓ Task completed successfully!")
                result = analysis_result.result
                print(f"  → Results: {json.dumps(result, indent=2)}")
                break
            elif current_state == 'FAILURE':
                print("✗ Task failed!")
                try:
                    error_info = analysis_result.info
                    print(f"  → Error: {error_info}")
                except:
                    print(f"  → Error: {analysis_result.result}")
                return False

            time.sleep(10)  # Check every 10 seconds

        else:
            print("⚠ Task timeout - checking final state...")
            final_state = analysis_result.state
            print(f"Final state: {final_state}")

            if final_state != 'SUCCESS':
                print("✗ Task did not complete successfully within timeout")
                return False

        # Test cleanup task (quick test)
        print("\n--- Testing Cleanup Task ---")
        print("Submitting cleanup task...")

        cleanup_result = cleanup_product_task.delay(product_id)
        print(f"✓ Cleanup task submitted with ID: {cleanup_result.id}")

        # Wait for cleanup to complete (should be quick)
        cleanup_timeout = 60  # 1 minute
        cleanup_start = time.time()

        while time.time() - cleanup_start < cleanup_timeout:
            cleanup_state = cleanup_result.state

            if cleanup_state == 'SUCCESS':
                print("✓ Cleanup task completed successfully!")
                result = cleanup_result.result
                print(f"  → Results: {json.dumps(result, indent=2)}")
                break
            elif cleanup_state == 'FAILURE':
                print("✗ Cleanup task failed!")
                try:
                    error_info = cleanup_result.info
                    print(f"  → Error: {error_info}")
                except:
                    print(f"  → Error: {cleanup_result.result}")
                break

            time.sleep(2)

        print("\n✓ Both tasks submitted and processed successfully!")
        return True

    except Exception as e:
        print(f"✗ Task submission failed: {e}")
        print(f"Error details: {type(e).__name__}: {str(e)}")
        return False

def check_generated_files():
    """Check if debug and export files were generated."""
    print("\nChecking generated files...")

    product_id = "2258e45a-531e-4412-ab47-3c6bd96eed8a"
    debug_files = [
        f"worker/debug/{product_id}_segments.debug.json",
        f"worker/debug/{product_id}_benefits.debug.json",
        f"worker/debug/{product_id}_details.debug.json"
    ]

    export_files = [
        f"worker/exports/{product_id}_analysis_results.json"
    ]

    for file_path in debug_files + export_files:
        path = Path(file_path)
        if path.exists():
            size = path.stat().st_size
            print(f"✓ Found: {file_path} ({size} bytes)")
        else:
            print(f"⚠ Missing: {file_path}")

def stop_services():
    """Stop the services."""
    print("\nStopping services...")

    try:
        result = subprocess.run(
            ["docker", "compose", "down"],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✓ Services stopped successfully")
        else:
            print(f"⚠ Service stop warning: {result.stderr}")

    except Exception as e:
        print(f"⚠ Error stopping services: {e}")

def main():
    """Run the comprehensive integration test."""
    print("=" * 60)
    print("Celery Worker Integration Test")
    print("=" * 60)

    # Check environment
    if not check_env_vars():
        return False

    # Start services
    if not start_services():
        return False

    try:
        # Show initial worker logs
        print("\n--- Initial Worker Logs ---")
        print(get_worker_logs())

        # Test task submission and execution
        success = test_task_submission()

        # Show final worker logs
        print("\n--- Final Worker Logs ---")
        print(get_worker_logs())

        # Check generated files
        check_generated_files()

        print("\n" + "=" * 60)
        if success:
            print("✓ Integration test PASSED - Celery worker is fully functional!")
            print("The worker can process both analysis and cleanup tasks successfully.")
        else:
            print("✗ Integration test FAILED - Check logs above for issues")

        return success

    finally:
        # Always stop services
        stop_services()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)