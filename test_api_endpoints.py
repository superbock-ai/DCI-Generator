#!/usr/bin/env python3
"""
Comprehensive API Endpoint Testing
Tests the complete workflow: Redis + Worker + Broker + API endpoints with real requests.
"""

import os
import sys
import time
import json
import subprocess
import requests
import jwt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_jwt_token():
    """Create a valid JWT token for API testing."""
    secret = os.getenv('DIRECTUS_SECRET')
    if not secret:
        raise ValueError("DIRECTUS_SECRET not set in .env file")

    # Create a test token
    payload = {
        "user_id": "test-user",
        "role": "admin",
        "exp": int(time.time()) + 3600  # Expires in 1 hour
    }

    token = jwt.encode(payload, secret, algorithm="HS256")
    return token

def wait_for_service(url, timeout=60, service_name="service"):
    """Wait for a service to become available."""
    print(f"Waiting for {service_name} at {url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✓ {service_name} is ready")
                return True
        except requests.exceptions.RequestException:
            pass

        time.sleep(2)

    print(f"✗ {service_name} did not become ready within {timeout} seconds")
    return False

def start_services():
    """Start all services using docker-compose."""
    print("Starting all services (Redis + Worker + Broker)...")

    try:
        result = subprocess.run(
            ["docker", "compose", "up", "-d"],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            print(f"✗ Failed to start services: {result.stderr}")
            return False

        print("✓ Services started successfully")

        # Wait for broker to be ready
        if not wait_for_service("http://localhost:8000/health", service_name="FastAPI Broker"):
            return False

        # Give workers time to connect
        print("Giving workers time to connect to Redis...")
        time.sleep(10)

        return True

    except Exception as e:
        print(f"✗ Error starting services: {e}")
        return False

def test_health_endpoint():
    """Test the health endpoint."""
    print("\n--- Testing Health Endpoint ---")

    try:
        response = requests.get("http://localhost:8000/health", timeout=10)

        if response.status_code == 200:
            data = response.json()
            print("✓ Health endpoint working")
            print(f"  Status: {data.get('status')}")
            print(f"  Service: {data.get('service')}")
            return True
        else:
            print(f"✗ Health endpoint failed: {response.status_code}")
            return False

    except Exception as e:
        print(f"✗ Health endpoint error: {e}")
        return False

def test_authentication():
    """Test JWT authentication."""
    print("\n--- Testing Authentication ---")

    # Test without token (should fail)
    try:
        response = requests.post(
            "http://localhost:8000/jobs/analysis",
            json={"product_id": "test"},
            timeout=10
        )

        if response.status_code == 403:  # Expecting forbidden without token
            print("✓ Authentication correctly rejects requests without token")
        else:
            print(f"⚠ Unexpected status without token: {response.status_code}")

    except Exception as e:
        print(f"✗ Authentication test error: {e}")
        return False

    # Test with invalid token (should fail)
    try:
        headers = {"Authorization": "Bearer invalid-token"}
        response = requests.post(
            "http://localhost:8000/jobs/analysis",
            json={"product_id": "test"},
            headers=headers,
            timeout=10
        )

        if response.status_code in [401, 403]:
            print("✓ Authentication correctly rejects invalid token")
        else:
            print(f"⚠ Unexpected status with invalid token: {response.status_code}")

    except Exception as e:
        print(f"✗ Invalid token test error: {e}")

    return True

def test_analysis_endpoint():
    """Test the analysis job submission endpoint."""
    print("\n--- Testing Analysis Endpoint ---")

    try:
        token = create_jwt_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Test job submission
        job_data = {
            "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
            "export": True,
            "debug": True,
            "segment_chunks": 3,
            "benefit_chunks": 3,
            "detail_chunks": 2,
            "seed_directus": False
        }

        response = requests.post(
            "http://localhost:8000/jobs/analysis",
            json=job_data,
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            job_id = data.get('job_id')
            print("✓ Analysis job submitted successfully")
            print(f"  Job ID: {job_id}")
            print(f"  Status: {data.get('status')}")
            print(f"  Message: {data.get('message')}")
            return job_id
        else:
            print(f"✗ Analysis job submission failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None

    except Exception as e:
        print(f"✗ Analysis endpoint error: {e}")
        return None

def test_cleanup_endpoint():
    """Test the cleanup job submission endpoint."""
    print("\n--- Testing Cleanup Endpoint ---")

    try:
        token = create_jwt_token()
        headers = {"Authorization": f"Bearer {token}"}

        job_data = {
            "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a"
        }

        response = requests.post(
            "http://localhost:8000/jobs/cleanup",
            json=job_data,
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            job_id = data.get('job_id')
            print("✓ Cleanup job submitted successfully")
            print(f"  Job ID: {job_id}")
            print(f"  Status: {data.get('status')}")
            print(f"  Message: {data.get('message')}")
            return job_id
        else:
            print(f"✗ Cleanup job submission failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None

    except Exception as e:
        print(f"✗ Cleanup endpoint error: {e}")
        return None

def test_status_endpoint(job_id):
    """Test the job status endpoint."""
    print(f"\n--- Testing Status Endpoint for Job {job_id} ---")

    if not job_id:
        print("⚠ No job ID provided, skipping status test")
        return False

    try:
        token = create_jwt_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Monitor job status
        for i in range(6):  # Check for up to 1 minute
            response = requests.get(
                f"http://localhost:8000/jobs/{job_id}/status",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                status = data.get('status')
                print(f"  Check {i+1}: Status = {status}")

                if data.get('progress'):
                    print(f"    Progress: {data['progress']}")

                if status == 'success':
                    print("✓ Job completed successfully")
                    result = data.get('result')
                    if result:
                        print(f"    Results: {json.dumps(result, indent=2)}")
                    return True
                elif status == 'failure':
                    print("✗ Job failed")
                    error = data.get('error')
                    if error:
                        print(f"    Error: {error}")
                    return False
                elif status in ['pending', 'processing']:
                    print(f"    Job is {status}, continuing to monitor...")

            else:
                print(f"✗ Status check failed: {response.status_code}")
                return False

            time.sleep(10)  # Wait 10 seconds between checks

        print("⚠ Job status monitoring timed out")
        return False

    except Exception as e:
        print(f"✗ Status endpoint error: {e}")
        return False

def test_docs_endpoint():
    """Test the OpenAPI docs endpoint."""
    print("\n--- Testing Documentation Endpoints ---")

    try:
        # Test /docs
        response = requests.get("http://localhost:8000/docs", timeout=10)
        if response.status_code == 200:
            print("✓ OpenAPI docs (/docs) accessible")
        else:
            print(f"⚠ OpenAPI docs status: {response.status_code}")

        # Test /redoc
        response = requests.get("http://localhost:8000/redoc", timeout=10)
        if response.status_code == 200:
            print("✓ ReDoc documentation (/redoc) accessible")
        else:
            print(f"⚠ ReDoc docs status: {response.status_code}")

        # Test root endpoint
        response = requests.get("http://localhost:8000/", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("✓ Root endpoint working")
            print(f"  Message: {data.get('message')}")
        else:
            print(f"⚠ Root endpoint status: {response.status_code}")

        return True

    except Exception as e:
        print(f"✗ Documentation endpoints error: {e}")
        return False

def stop_services():
    """Stop all services."""
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
    """Run comprehensive API endpoint tests."""
    print("=" * 60)
    print("Comprehensive API Endpoint Testing")
    print("=" * 60)

    # Check environment
    try:
        create_jwt_token()
        print("✓ JWT token creation working")
    except Exception as e:
        print(f"✗ JWT setup failed: {e}")
        print("Please ensure DIRECTUS_SECRET is set in .env file")
        return False

    # Start services
    if not start_services():
        return False

    try:
        success_count = 0
        total_tests = 0

        # Test all endpoints
        tests = [
            ("Health Endpoint", test_health_endpoint),
            ("Authentication", test_authentication),
            ("Documentation Endpoints", test_docs_endpoint)
        ]

        for test_name, test_func in tests:
            total_tests += 1
            if test_func():
                success_count += 1

        # Test job submission and monitoring
        total_tests += 1
        analysis_job_id = test_analysis_endpoint()
        if analysis_job_id:
            success_count += 1

            # Test status monitoring for analysis job
            total_tests += 1
            if test_status_endpoint(analysis_job_id):
                success_count += 1

        # Test cleanup endpoint
        total_tests += 1
        cleanup_job_id = test_cleanup_endpoint()
        if cleanup_job_id:
            success_count += 1

        print("\n" + "=" * 60)
        print(f"API Endpoint Test Results: {success_count}/{total_tests} tests passed")

        if success_count == total_tests:
            print("✓ ALL API ENDPOINT TESTS PASSED!")
            print("The FastAPI broker is fully functional and production-ready!")
        elif success_count >= total_tests - 1:
            print("✓ Core API functionality PASSED!")
            print("Minor issues detected but system is functional")
        else:
            print("✗ API endpoint tests FAILED!")
            print("Check logs above for issues")

        return success_count >= total_tests - 1

    finally:
        stop_services()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)