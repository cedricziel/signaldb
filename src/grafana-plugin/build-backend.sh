#!/bin/bash
set -e

echo "Building SignalDB Grafana datasource backend..."

# Determine the platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Map architectures
case "$ARCH" in
    x86_64)
        GOARCH="amd64"
        ;;
    aarch64|arm64)
        GOARCH="arm64"
        ;;
    *)
        GOARCH="$ARCH"
        ;;
esac

# Build the Rust binary (from workspace root)
cargo build --release -p signaldb-grafana-datasource

# Create dist directory if it doesn't exist
mkdir -p dist

# Copy binary to dist with Grafana's expected naming convention
cp ../../target/release/gpx_signaldb_datasource "dist/gpx_signaldb_datasource_${OS}_${GOARCH}"

echo "Backend built successfully: dist/gpx_signaldb_datasource_${OS}_${GOARCH}"
