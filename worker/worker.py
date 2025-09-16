#!/usr/bin/env python3
"""
Celery worker startup script for DCI Generator.
This script starts the Celery worker that processes analysis and cleanup tasks.
"""

import os
import sys
from celery_app import app

# Import tasks to register them with the worker
import tasks

def main():
    """Start the Celery worker."""
    print("Starting DCI Generator Celery Worker...")
    print("=" * 50)

    # Worker configuration
    worker_args = [
        'worker',
        '--loglevel=info',
        '--hostname=dci-worker@%h',
        '--concurrency=2',  # Process 2 tasks concurrently
        '--prefetch-multiplier=1',  # Only prefetch 1 task per worker process
        '--max-tasks-per-child=10',  # Restart worker after 10 tasks to prevent memory leaks
    ]

    # Start the worker
    app.worker_main(argv=worker_args)

if __name__ == '__main__':
    main()