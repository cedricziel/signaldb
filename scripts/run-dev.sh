#!/bin/bash
# SignalDB Local Development Runner
# Runs SignalDB services locally using cargo with local file storage
#
# Usage:
#   ./scripts/run-dev.sh [mode] [options]
#
# Modes:
#   monolithic  - Run all services in a single process (default)
#   services    - Run services as separate processes
#
# Options:
#   --with-deps     - Start PostgreSQL via docker compose
#   --sqlite        - Use SQLite instead of PostgreSQL (default)
#   --postgres      - Use PostgreSQL (requires --with-deps or running instance)
#   --help          - Show this help message

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
MODE="monolithic"
START_DEPS=false
USE_POSTGRES=false
BASE_DIR=".data"
LOG_DIR="${BASE_DIR}/logs"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        monolithic|services)
            MODE=$1
            shift
            ;;
        --with-deps)
            START_DEPS=true
            shift
            ;;
        --postgres)
            USE_POSTGRES=true
            shift
            ;;
        --sqlite)
            USE_POSTGRES=false
            shift
            ;;
        --help|-h)
            head -n 20 "$0" | grep "^#" | grep -v "^#!/" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# If using PostgreSQL, ensure dependencies are started
if [ "$USE_POSTGRES" = true ]; then
    START_DEPS=true
fi

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  SignalDB Local Development Environment                   ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Mode:${NC}     $MODE"
echo -e "${GREEN}Database:${NC} $([ "$USE_POSTGRES" = true ] && echo "PostgreSQL" || echo "SQLite")"
echo -e "${GREEN}Storage:${NC}  Local filesystem (${BASE_DIR}/storage)"
echo ""

# Setup storage directories
echo -e "${YELLOW}Setting up storage directories...${NC}"
mkdir -p "${BASE_DIR}/wal/acceptor"
mkdir -p "${BASE_DIR}/wal/writer"
mkdir -p "${BASE_DIR}/storage"
mkdir -p "${LOG_DIR}"

# Database configuration
if [ "$USE_POSTGRES" = true ]; then
    DATABASE_DSN="postgres://signaldb:password@localhost:5432/signaldb"
    DISCOVERY_DSN="postgres://signaldb:password@localhost:5432/signaldb"
else
    DATABASE_DSN="sqlite://${BASE_DIR}/signaldb.db"
    DISCOVERY_DSN="sqlite://${BASE_DIR}/signaldb.db"
fi

# Export environment variables
export SIGNALDB_DATABASE_DSN="$DATABASE_DSN"
export SIGNALDB_DISCOVERY_DSN="$DISCOVERY_DSN"
export SIGNALDB_STORAGE_DSN="file://$(pwd)/${BASE_DIR}/storage"
export SIGNALDB_SCHEMA_CATALOG_TYPE="sql"
export SIGNALDB_SCHEMA_CATALOG_URI="sqlite://$(pwd)/${BASE_DIR}/catalog.db"
export RUST_LOG="${RUST_LOG:-info,signaldb=debug}"

# Service-specific configuration
export ACCEPTOR_GRPC_ADDR="0.0.0.0:4317"
export ACCEPTOR_HTTP_ADDR="0.0.0.0:4318"
export ACCEPTOR_WAL_DIR="${BASE_DIR}/wal/acceptor"

export WRITER_FLIGHT_ADDR="0.0.0.0:50051"
export WRITER_ADVERTISE_ADDR="localhost:50051"
export WRITER_WAL_DIR="${BASE_DIR}/wal/writer"

export QUERIER_FLIGHT_ADDR="0.0.0.0:50054"
export QUERIER_ADVERTISE_ADDR="localhost:50054"

export ROUTER_FLIGHT_ADDR="0.0.0.0:50053"
export ROUTER_HTTP_ADDR="0.0.0.0:3001"

# Start dependencies if requested
if [ "$START_DEPS" = true ]; then
    echo -e "${YELLOW}Starting dependencies via docker compose...${NC}"
    docker compose up -d postgres

    # Wait for PostgreSQL to be ready
    if [ "$USE_POSTGRES" = true ]; then
        echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"
        timeout=30
        elapsed=0
        until docker compose exec -T postgres pg_isready -U signaldb > /dev/null 2>&1; do
            sleep 1
            elapsed=$((elapsed + 1))
            if [ $elapsed -ge $timeout ]; then
                echo -e "${RED}PostgreSQL failed to start within ${timeout}s${NC}"
                exit 1
            fi
            echo -n "."
        done
        echo ""
        echo -e "${GREEN}PostgreSQL is ready!${NC}"
    fi
fi

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down services...${NC}"
    if [ -n "${PIDS:-}" ]; then
        for pid in $PIDS; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
            fi
        done
        wait 2>/dev/null || true
    fi

    if [ "$START_DEPS" = true ]; then
        echo -e "${YELLOW}Stopping dependencies...${NC}"
        docker compose down
    fi

    echo -e "${GREEN}Cleanup complete${NC}"
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# Run services
echo -e "${GREEN}Starting SignalDB services...${NC}"
echo ""

if [ "$MODE" = "monolithic" ]; then
    # Monolithic mode - single process
    echo -e "${BLUE}Running in monolithic mode (single process)${NC}"
    echo -e "${YELLOW}Logs: ${LOG_DIR}/monolithic.log${NC}"
    echo ""
    echo -e "${GREEN}Services starting on:${NC}"
    echo "  • OTLP gRPC: http://localhost:4317"
    echo "  • OTLP HTTP: http://localhost:4318"
    echo "  • HTTP API:  http://localhost:3001"
    echo "  • Flight:    http://localhost:50053"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    echo ""

    cargo run --bin signaldb 2>&1 | tee "${LOG_DIR}/monolithic.log"

else
    # Microservices mode - separate processes
    echo -e "${BLUE}Running in microservices mode (separate processes)${NC}"
    echo ""

    PIDS=""

    # Start acceptor
    echo -e "${GREEN}Starting Acceptor...${NC}"
    cargo run --bin signaldb-acceptor > "${LOG_DIR}/acceptor.log" 2>&1 &
    PIDS="$PIDS $!"
    echo "  • OTLP gRPC: http://localhost:4317"
    echo "  • OTLP HTTP: http://localhost:4318"
    echo "  • Logs: ${LOG_DIR}/acceptor.log"

    # Start writer
    echo -e "${GREEN}Starting Writer...${NC}"
    cargo run --bin signaldb-writer > "${LOG_DIR}/writer.log" 2>&1 &
    PIDS="$PIDS $!"
    echo "  • Flight: http://localhost:50051"
    echo "  • Logs: ${LOG_DIR}/writer.log"

    # Start querier
    echo -e "${GREEN}Starting Querier...${NC}"
    cargo run --bin signaldb-querier > "${LOG_DIR}/querier.log" 2>&1 &
    PIDS="$PIDS $!"
    echo "  • Flight: http://localhost:50054"
    echo "  • Logs: ${LOG_DIR}/querier.log"

    # Start router
    echo -e "${GREEN}Starting Router...${NC}"
    cargo run --bin signaldb-router > "${LOG_DIR}/router.log" 2>&1 &
    PIDS="$PIDS $!"
    echo "  • HTTP API: http://localhost:3001"
    echo "  • Flight: http://localhost:50053"
    echo "  • Logs: ${LOG_DIR}/router.log"

    echo ""
    echo -e "${YELLOW}All services started!${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
    echo ""
    echo -e "${BLUE}Tail logs with:${NC}"
    echo "  tail -f ${LOG_DIR}/*.log"
    echo ""

    # Wait for all processes
    wait
fi
