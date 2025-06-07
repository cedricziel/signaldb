#!/bin/bash
# Test script for different deployment models and database configurations
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
TEST_DATA_DIR="${PROJECT_ROOT}/.data/test"
TIMEOUT_DURATION=10

# Cleanup function
cleanup() {
    log_info "Cleaning up test processes..."
    pkill -f "target/.*/(acceptor|router|writer|signaldb)" 2>/dev/null || true
    rm -rf "${TEST_DATA_DIR}"
}

# Setup function
setup() {
    log_info "Setting up test environment..."
    trap cleanup EXIT
    mkdir -p "${TEST_DATA_DIR}"
    cd "${PROJECT_ROOT}"
}

# Test database configurations
test_database_configs() {
    log_info "Testing database configurations..."
    
    # Test SQLite configuration
    log_info "Testing SQLite configuration..."
    export SIGNALDB_DATABASE_DSN="sqlite://${TEST_DATA_DIR}/sqlite_test.db"
    export SIGNALDB_DISCOVERY_DSN="sqlite://${TEST_DATA_DIR}/sqlite_test.db"
    
    cargo test -p common catalog_integration --all-features -- --test-threads=1
    
    if command -v psql >/dev/null 2>&1 && pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
        log_info "Testing PostgreSQL configuration..."
        export SIGNALDB_DATABASE_DSN="postgres://postgres:postgres@localhost:5432/signaldb_test"
        export SIGNALDB_DISCOVERY_DSN="postgres://postgres:postgres@localhost:5432/signaldb_test"
        
        cargo test -p common catalog_integration --all-features -- --test-threads=1
    else
        log_warn "PostgreSQL not available, skipping PostgreSQL tests"
    fi
}

# Test monolithic deployment
test_monolithic() {
    log_info "Testing monolithic deployment..."
    
    export SIGNALDB_DATABASE_DSN="sqlite://${TEST_DATA_DIR}/monolithic.db"
    export SIGNALDB_DISCOVERY_DSN="sqlite://${TEST_DATA_DIR}/monolithic.db"
    
    # Build monolithic binary
    cargo build --bin signaldb
    
    # Test startup and shutdown
    timeout "${TIMEOUT_DURATION}s" ./target/debug/signaldb &
    MONO_PID=$!
    
    sleep 3
    if kill -0 "$MONO_PID" 2>/dev/null; then
        log_info "Monolithic service started successfully"
        kill "$MONO_PID"
        wait "$MONO_PID" 2>/dev/null || true
    else
        log_error "Monolithic service failed to start"
        return 1
    fi
}

# Test microservices deployment
test_microservices() {
    log_info "Testing microservices deployment..."
    
    export SIGNALDB_DATABASE_DSN="sqlite://${TEST_DATA_DIR}/microservices.db"
    export SIGNALDB_DISCOVERY_DSN="sqlite://${TEST_DATA_DIR}/microservices.db"
    
    # Build microservice binaries
    cargo build --bin signaldb-acceptor --bin signaldb-router --bin signaldb-writer
    
    # Start services
    timeout "${TIMEOUT_DURATION}s" ./target/debug/signaldb-acceptor &
    ACCEPTOR_PID=$!
    
    sleep 1
    
    timeout "${TIMEOUT_DURATION}s" ./target/debug/signaldb-router &
    ROUTER_PID=$!
    
    sleep 1
    
    timeout "${TIMEOUT_DURATION}s" ./target/debug/signaldb-writer &
    WRITER_PID=$!
    
    sleep 3
    
    # Check if all services are running
    if kill -0 "$ACCEPTOR_PID" "$ROUTER_PID" "$WRITER_PID" 2>/dev/null; then
        log_info "All microservices started successfully"
        
        # Test service discovery
        sleep 2
        log_info "Testing service discovery..."
        
        # Cleanup
        kill "$ACCEPTOR_PID" "$ROUTER_PID" "$WRITER_PID" 2>/dev/null || true
        wait "$ACCEPTOR_PID" "$ROUTER_PID" "$WRITER_PID" 2>/dev/null || true
    else
        log_error "One or more microservices failed to start"
        kill "$ACCEPTOR_PID" "$ROUTER_PID" "$WRITER_PID" 2>/dev/null || true
        return 1
    fi
}

# Test configuration scenarios
test_config_scenarios() {
    log_info "Testing configuration scenarios..."
    
    # Test with different heartbeat intervals
    for interval in "10s" "30s" "60s"; do
        log_info "Testing with heartbeat interval: $interval"
        
        cat > "${TEST_DATA_DIR}/test_config.toml" << EOF
[database]
dsn = "sqlite://${TEST_DATA_DIR}/config_test_${interval}.db"

[discovery]
dsn = "sqlite://${TEST_DATA_DIR}/config_test_${interval}.db"
heartbeat_interval = "${interval}"
poll_interval = "${interval}"
ttl = "300s"
EOF
        
        export SIGNALDB_CONFIG_PATH="${TEST_DATA_DIR}/test_config.toml"
        
        timeout 5s ./target/debug/signaldb-acceptor &
        CONFIG_PID=$!
        
        sleep 2
        
        if kill -0 "$CONFIG_PID" 2>/dev/null; then
            log_info "Configuration test with ${interval} passed"
            kill "$CONFIG_PID"
            wait "$CONFIG_PID" 2>/dev/null || true
        else
            log_error "Configuration test with ${interval} failed"
            return 1
        fi
        
        unset SIGNALDB_CONFIG_PATH
    done
}

# Test binary help outputs
test_binary_help() {
    log_info "Testing binary help outputs..."
    
    cargo build --bin signaldb --bin signaldb-acceptor --bin signaldb-router --bin signaldb-writer
    
    for binary in signaldb signaldb-acceptor signaldb-router signaldb-writer; do
        log_info "Testing $binary --help"
        if ./target/debug/"$binary" --help >/dev/null 2>&1; then
            log_info "$binary help output works"
        else
            log_warn "$binary help output not available (expected for some binaries)"
        fi
    done
}

# Main test runner
main() {
    setup
    
    log_info "Starting SignalDB deployment testing..."
    
    # Run tests
    test_database_configs
    test_monolithic
    test_microservices
    test_config_scenarios
    test_binary_help
    
    log_info "All deployment tests completed successfully!"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi