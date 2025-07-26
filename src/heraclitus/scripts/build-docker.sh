#!/bin/bash
set -e

# Script to build Heraclitus Docker image
# Usage: ./scripts/build-docker.sh [tag]

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HERACLITUS_DIR="$(dirname "$SCRIPT_DIR")"
WORKSPACE_ROOT="$(dirname "$(dirname "$HERACLITUS_DIR")")"

# Default tag is 'latest'
TAG="${1:-latest}"

echo "Building Heraclitus Docker image..."
echo "Workspace root: $WORKSPACE_ROOT"
echo "Tag: $TAG"

# Build from workspace root to include all necessary files
cd "$WORKSPACE_ROOT"

# Build the Docker image
docker build \
    -f src/heraclitus/Dockerfile \
    -t heraclitus:$TAG \
    .

echo "Successfully built heraclitus:$TAG"

# Optionally run a quick test
if [ "${RUN_TEST:-false}" = "true" ]; then
    echo "Running quick test..."
    docker run --rm heraclitus:$TAG --version
fi