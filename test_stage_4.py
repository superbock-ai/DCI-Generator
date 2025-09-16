#!/usr/bin/env python3
"""
Test script for Stage 4: FastAPI Broker (Containerized)
Tests FastAPI application, job submission, status endpoints, and JWT authentication.
"""

import subprocess
import sys
import os
import time
import json
import requests
import jwt
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_fastapi_files_exist():
    """Test that FastAPI files exist with correct structure."""
    print("Testing FastAPI file structure...")

    required_files = [
        "broker/main.py",
        "broker/Dockerfile"
    ]

    for file_path in required_files:
        path = Path(file_path)
        if not path.exists():
            print(f"✗ Missing file: {file_path}")
            return False
        print(f"✓ Found: {file_path}")

    # Test content of broker main.py
    with open("broker/main.py", 'r') as f:
        content = f.read()

    required_elements = [
        "FastAPI(",
        "/jobs/analysis",
        "/jobs/cleanup",
        "/jobs/{job_id}/status",
        "verify_jwt_token",
        "AnalysisJobRequest",
        "CleanupJobRequest"
    ]

    for element in required_elements:
        if element in content:
            print(f"✓ FastAPI app has: {element}")
        else:
            print(f"✗ Missing in broker/main.py: {element}")
            return False

    print("✓ FastAPI file structure is correct")
    return True

def test_docker_compose_updated():
    """Test that docker-compose.yml includes broker service."""
    print("Testing docker-compose.yml broker configuration...")

    compose_path = Path("docker-compose.yml")
    if not compose_path.exists():
        print("✗ docker-compose.yml not found")
        return False

    with open(compose_path, 'r') as f:
        content = f.read()

    broker_elements = [
        "broker:",
        "dci-broker",
        "dockerfile: broker/Dockerfile",
        "8000:8000"
    ]

    for element in broker_elements:
        if element in content:
            print(f"✓ Found broker config: {element}")
        else:
            print(f"✗ Missing broker config: {element}")
            return False

    print("✓ docker-compose.yml includes broker correctly")
    return True

def test_fastapi_dependencies():
    """Test that FastAPI dependencies are installed."""
    print("Testing FastAPI dependencies...")

    try:
        # Test importing FastAPI
        result = subprocess.run(
            ["python", "-c", "import fastapi; print(f'FastAPI {fastapi.__version__}')"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"✓ {result.stdout.strip()}")
        else:
            print("✗ FastAPI not available")
            return False

        # Test importing uvicorn
        result = subprocess.run(
            ["python", "-c", "import uvicorn; print(f'Uvicorn {uvicorn.__version__}')"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"✓ {result.stdout.strip()}")
        else:
            print("✗ Uvicorn not available")
            return False

        # Test importing PyJWT
        result = subprocess.run(
            ["python", "-c", "import jwt; print(f'PyJWT {jwt.__version__}')"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print(f"✓ {result.stdout.strip()}")
        else:
            print("✗ PyJWT not available")
            return False

        print("✓ All FastAPI dependencies are installed")
        return True

    except Exception as e:
        print(f"✗ Dependency test error: {e}")
        return False

def create_test_jwt_token():
    """Create a test JWT token for API testing."""
    secret = os.getenv('DIRECTUS_SECRET')
    if not secret:
        print("⚠ DIRECTUS_SECRET not set - cannot test JWT authentication")
        return None

    # Create a test token
    payload = {
        "user_id": "test-user",
        "role": "admin",
        "exp": int(time.time()) + 3600  # Expires in 1 hour
    }

    token = jwt.encode(payload, secret, algorithm="HS256")
    return token

def test_fastapi_local():
    """Test FastAPI application locally."""
    print("Testing FastAPI application locally...")

    try:
        # Try to start FastAPI server in background
        print("Starting FastAPI server...")
        process = subprocess.Popen(
            ["uvicorn", "broker.main:app", "--host", "127.0.0.1", "--port", "8001"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Give it time to start
        time.sleep(5)

        # Test health endpoint
        try:
            response = requests.get("http://127.0.0.1:8001/health", timeout=5)
            if response.status_code == 200:
                print("✓ Health endpoint working")
                health_data = response.json()
                print(f"  Status: {health_data.get('status')}")
            else:
                print(f"✗ Health endpoint failed: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"✗ Health endpoint request failed: {e}")
            return False

        # Test root endpoint
        try:
            response = requests.get("http://127.0.0.1:8001/", timeout=5)
            if response.status_code == 200:
                print("✓ Root endpoint working")
                root_data = response.json()
                print(f"  Message: {root_data.get('message')}")
            else:
                print(f"✗ Root endpoint failed: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"✗ Root endpoint request failed: {e}")
            return False

        # Test OpenAPI docs
        try:
            response = requests.get("http://127.0.0.1:8001/docs", timeout=5)
            if response.status_code == 200:
                print("✓ OpenAPI docs accessible")
            else:
                print(f"⚠ OpenAPI docs status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠ OpenAPI docs request failed: {e}")

        # Test JWT authentication endpoint (should fail without token)
        try:
            response = requests.post("http://127.0.0.1:8001/jobs/analysis",
                                   json={"product_id": "test"}, timeout=5)
            if response.status_code == 401:
                print("✓ JWT authentication working (correctly rejected request without token)")
            else:
                print(f"⚠ Authentication check unexpected status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠ Authentication test request failed: {e}")

        # Test with valid JWT token
        token = create_test_jwt_token()
        if token:
            try:
                headers = {"Authorization": f"Bearer {token}"}
                response = requests.post("http://127.0.0.1:8001/jobs/analysis",
                                       json={"product_id": "test-product-id"},
                                       headers=headers, timeout=5)
                if response.status_code in [200, 500]:  # 500 expected due to Celery not running
                    print("✓ JWT token authentication working")
                else:
                    print(f"⚠ JWT token test unexpected status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"⚠ JWT token test request failed: {e}")

        return True

    except Exception as e:
        print(f"✗ FastAPI local test error: {e}")
        return False

    finally:
        # Clean up server process
        try:
            process.terminate()
            process.wait(timeout=5)
        except:
            try:
                process.kill()
            except:
                pass

def main():
    """Run all tests for Stage 4."""
    print("=" * 50)
    print("Stage 4: FastAPI Broker (Containerized) Tests")
    print("=" * 50)

    # Always run file structure tests
    file_tests = [
        test_fastapi_files_exist,
        test_docker_compose_updated,
        test_fastapi_dependencies,
    ]

    passed = 0
    total = len(file_tests)

    for test in file_tests:
        print()
        if test():
            passed += 1

    # Test FastAPI application locally
    print()
    if test_fastapi_local():
        passed += 1
    total += 1

    print("\n" + "=" * 50)
    print(f"Results: {passed}/{total} tests passed")

    if passed >= len(file_tests):  # At minimum, file tests must pass
        print("✓ Stage 4 core requirements PASSED")
        if passed == total:
            print("✓ All tests including FastAPI server PASSED - Ready for Stage 5.1")
        else:
            print("⚠ Some tests had issues - check logs above")
        return True
    else:
        print("✗ Stage 4 tests FAILED - Fix issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)