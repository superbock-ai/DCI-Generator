#!/bin/bash
# Build script for DCI Worker container
# Usage: ./build_worker.sh [tag]

set -e

TAG=${1:-dci-worker}

echo "Building DCI Worker container with tag: $TAG"
echo "Build context: $(pwd)"

# Build from root directory using worker/Dockerfile
docker build -f worker/Dockerfile -t "$TAG" .

echo "✓ Container built successfully: $TAG"
echo "Test with: docker run --rm $TAG"