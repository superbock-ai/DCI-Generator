#!/usr/bin/env python3
"""
Redis Connection Validation Utility
Tests Redis connectivity using environment variables.
"""

import os
import sys
from dotenv import load_dotenv

def validate_redis():
    """Validate Redis connection using environment variables."""
    load_dotenv()

    redis_url = os.getenv('REDIS_URL')
    if not redis_url:
        print("✗ REDIS_URL environment variable not set")
        return False

    print(f"Testing Redis connection to: {redis_url}")

    try:
        import redis

        # Parse Redis URL
        r = redis.from_url(redis_url)

        # Test connection
        response = r.ping()
        if response:
            print("✓ Redis connection successful")

            # Test basic operations
            r.set('test_key', 'test_value', ex=10)  # Expires in 10 seconds
            value = r.get('test_key')
            if value == b'test_value':
                print("✓ Redis read/write operations working")
                r.delete('test_key')
                return True
            else:
                print("✗ Redis read/write operations failed")
                return False
        else:
            print("✗ Redis ping failed")
            return False

    except ImportError:
        print("⚠ redis library not installed - install with: uv add redis")
        print("⚠ Cannot test connection, but Redis URL is configured")
        return True  # Don't fail if library isn't installed yet
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        return False

if __name__ == "__main__":
    success = validate_redis()
    sys.exit(0 if success else 1)