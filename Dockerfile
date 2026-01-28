# Multi-stage production Dockerfile for SignalDB services
# Uses Alpine Linux for minimal image sizes (~15-25MB per service)

# Builder stage - compile all services with musl for Alpine compatibility
FROM rust:1.93-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    musl-dev \
    protobuf-dev \
    protoc \
    ca-certificates \
    openssl-dev \
    openssl-libs-static \
    pkgconfig

WORKDIR /build

# Copy dependency manifests for caching
COPY Cargo.toml Cargo.lock ./
COPY deny.toml rustfmt.toml ./

# Copy all workspace Cargo.toml files
COPY src/acceptor/Cargo.toml src/acceptor/
COPY src/router/Cargo.toml src/router/
COPY src/writer/Cargo.toml src/writer/
COPY src/querier/Cargo.toml src/querier/
COPY src/common/Cargo.toml src/common/
COPY src/tempo-api/Cargo.toml src/tempo-api/
COPY src/signaldb-bin/Cargo.toml src/signaldb-bin/
COPY src/signal-producer/Cargo.toml src/signal-producer/
COPY src/grafana-plugin/backend/Cargo.toml src/grafana-plugin/backend/
COPY tests-integration/Cargo.toml tests-integration/

# Create dummy source files to build dependencies
RUN mkdir -p src/acceptor/src && echo "fn main() {}" > src/acceptor/src/main.rs && \
    mkdir -p src/router/src && echo "fn main() {}" > src/router/src/main.rs && \
    mkdir -p src/writer/src && echo "fn main() {}" > src/writer/src/main.rs && \
    mkdir -p src/querier/src && echo "fn main() {}" > src/querier/src/main.rs && \
    mkdir -p src/common/src && echo "pub fn dummy() {}" > src/common/src/lib.rs && \
    mkdir -p src/tempo-api/src && echo "pub fn dummy() {}" > src/tempo-api/src/lib.rs && \
    mkdir -p src/signaldb-bin/src && echo "fn main() {}" > src/signaldb-bin/src/main.rs && \
    mkdir -p src/signal-producer/src && echo "fn main() {}" > src/signal-producer/src/main.rs && \
    mkdir -p src/grafana-plugin/backend/src && echo "fn main() {}" > src/grafana-plugin/backend/src/main.rs && \
    mkdir -p tests-integration/src && echo "pub fn dummy() {}" > tests-integration/src/lib.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release \
    --bin signaldb-acceptor \
    --bin signaldb-router \
    --bin signaldb-writer \
    --bin signaldb-querier \
    --bin signaldb

# Remove dummy files and build artifacts (keep cached dependencies)
RUN rm -rf src/*/src && \
    find target/release -type f -executable -delete && \
    rm -f target/release/deps/signaldb*

# Copy actual source code
COPY src/ src/

# Build all service binaries in release mode
RUN cargo build --release \
    --bin signaldb-acceptor \
    --bin signaldb-router \
    --bin signaldb-writer \
    --bin signaldb-querier \
    --bin signaldb

# Strip debug symbols to reduce binary size
RUN strip target/release/signaldb-acceptor && \
    strip target/release/signaldb-router && \
    strip target/release/signaldb-writer && \
    strip target/release/signaldb-querier && \
    strip target/release/signaldb

# Runtime base image - minimal Alpine with required libraries
FROM alpine:3.23 AS runtime-base

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libgcc \
    && addgroup -g 1000 signaldb \
    && adduser -D -u 1000 -G signaldb signaldb

# ============================================================================
# Service-specific stages
# ============================================================================

# Acceptor service - OTLP ingestion endpoint
FROM runtime-base AS acceptor

COPY --from=builder /build/target/release/signaldb-acceptor /usr/local/bin/signaldb-acceptor

USER signaldb
EXPOSE 4317 4318
ENTRYPOINT ["/usr/local/bin/signaldb-acceptor"]

# Router service - HTTP API and Flight endpoint
FROM runtime-base AS router

COPY --from=builder /build/target/release/signaldb-router /usr/local/bin/signaldb-router

USER signaldb
EXPOSE 50053 3001
ENTRYPOINT ["/usr/local/bin/signaldb-router"]

# Writer service - Data persistence and storage
FROM runtime-base AS writer

COPY --from=builder /build/target/release/signaldb-writer /usr/local/bin/signaldb-writer

USER signaldb
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/signaldb-writer"]

# Querier service - Query execution engine
FROM runtime-base AS querier

COPY --from=builder /build/target/release/signaldb-querier /usr/local/bin/signaldb-querier

USER signaldb
EXPOSE 50054
ENTRYPOINT ["/usr/local/bin/signaldb-querier"]

# Monolithic service - all services in one binary
FROM runtime-base AS monolithic

COPY --from=builder /build/target/release/signaldb /usr/local/bin/signaldb

USER signaldb
EXPOSE 4317 4318 50051 50053 3000
ENTRYPOINT ["/usr/local/bin/signaldb"]
