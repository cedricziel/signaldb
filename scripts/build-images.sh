#!/bin/bash
# Build SignalDB Docker images
# Usage: ./scripts/build-images.sh [service]
#   service: acceptor, router, writer, querier, or all (default)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SERVICES=("acceptor" "router" "writer" "querier")
BUILD_SERVICE=""

# Parse arguments
if [ $# -eq 0 ]; then
    BUILD_SERVICE="all"
elif [ $# -eq 1 ]; then
    BUILD_SERVICE="$1"
else
    echo -e "${RED}Error: Too many arguments${NC}"
    echo "Usage: $0 [service]"
    echo "  service: acceptor, router, writer, querier, or all (default)"
    exit 1
fi

# Function to build a single service
build_service() {
    local service=$1
    echo -e "${YELLOW}Building $service...${NC}"

    if docker compose build "signaldb-$service"; then
        echo -e "${GREEN}✓ Successfully built $service${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed to build $service${NC}"
        return 1
    fi
}

# Main build logic
if [ "$BUILD_SERVICE" == "all" ]; then
    echo -e "${YELLOW}Building all SignalDB services...${NC}"
    echo ""

    FAILED_SERVICES=()

    for service in "${SERVICES[@]}"; do
        if ! build_service "$service"; then
            FAILED_SERVICES+=("$service")
        fi
        echo ""
    done

    # Summary
    echo -e "${YELLOW}Build Summary:${NC}"
    if [ ${#FAILED_SERVICES[@]} -eq 0 ]; then
        echo -e "${GREEN}✓ All services built successfully!${NC}"
        echo ""
        echo "Run 'docker compose up' to start the services"
        exit 0
    else
        echo -e "${RED}✗ Failed to build: ${FAILED_SERVICES[*]}${NC}"
        exit 1
    fi
else
    # Check if service is valid
    if [[ ! " ${SERVICES[*]} " =~ " ${BUILD_SERVICE} " ]]; then
        echo -e "${RED}Error: Invalid service '${BUILD_SERVICE}'${NC}"
        echo "Valid services: ${SERVICES[*]}"
        exit 1
    fi

    build_service "$BUILD_SERVICE"
fi
