# Multi-stage Dockerfile for Heraclitus
FROM rust:1.85 AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy entire workspace
COPY . .

# Build heraclitus in release mode
RUN cargo build --release -p heraclitus

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 heraclitus

# Copy binary from builder
COPY --from=builder /app/target/release/heraclitus /usr/local/bin/heraclitus

# Create data directory
RUN mkdir -p /var/lib/heraclitus && \
    chown heraclitus:heraclitus /var/lib/heraclitus

# Copy default configuration
COPY src/heraclitus/heraclitus.docker.toml /etc/heraclitus/config.toml

# Switch to non-root user
USER heraclitus

# Expose ports
EXPOSE 9092 9093

# Set default storage path
ENV HERACLITUS_STORAGE_PATH=/var/lib/heraclitus

# Run heraclitus
ENTRYPOINT ["/usr/local/bin/heraclitus"]
CMD ["--config", "/etc/heraclitus/config.toml"]